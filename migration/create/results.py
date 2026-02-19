"""
Create results in target Qase workspace.
"""
import logging
from typing import Dict, Any
from qase.api_client_v1.api.runs_api import RunsApi
from qase_service import QaseService
from migration.utils import MigrationMappings, MigrationStats, retry_with_backoff, chunks, QaseRawApiClient
from migration.extract.results import extract_results
from migration.extract.authors import extract_authors

logger = logging.getLogger(__name__)

def transform_result_data(
    result_dict: Dict[str, Any],
    target_case_id: int,
    mappings: MigrationMappings,
    author_uuid_to_id_mapping: Dict[str, int]
) -> Dict[str, Any]:
    """
    Transform a result dictionary from source format to raw API bulk format.
    
    Returns:
        Dictionary with case_id, status, author_id for raw API bulk creation
    """
    # Map status
    status_map = {
        1: "passed",
        2: "blocked",
        3: "skipped",
        4: "retest",
        5: "failed"
    }
    status_id = result_dict.get('status_id')
    status_str = result_dict.get('status', '').lower() if result_dict.get('status') else None
    
    if status_id and status_id in status_map:
        status = status_map[status_id]
    elif status_str:
        status_str_lower = status_str.lower()
        if status_str_lower in ['passed', 'pass']:
            status = "passed"
        elif status_str_lower in ['failed', 'fail']:
            status = "failed"
        elif status_str_lower in ['blocked', 'block']:
            status = "blocked"
        elif status_str_lower in ['skipped', 'skip']:
            status = "skipped"
        elif status_str_lower in ['retest', 'retry']:
            status = "retest"
        else:
            status = "skipped"
    else:
        status = "skipped"
    
    # Map author_id from author_uuid
    author_uuid = result_dict.get('author_uuid')
    target_author_id = 1
    
    if author_uuid:
        source_author_id = author_uuid_to_id_mapping.get(author_uuid)
        if source_author_id:
            try:
                source_author_id_int = int(source_author_id)
                if source_author_id_int == 0:
                    target_author_id = 1
                else:
                    target_author_id = mappings.get_user_id(source_author_id_int)
            except (ValueError, TypeError):
                target_author_id = 1
    
    return {
        'case_id': target_case_id,
        'status': status,
        'author_id': target_author_id
    }


def migrate_results(
    source_service: QaseService,
    target_service: QaseService,
    project_code_source: str,
    project_code_target: str,
    run_mapping: Dict[int, int],
    case_mapping: Dict[int, int],
    mappings: MigrationMappings,
    stats: MigrationStats
):
    """
    Migrate test results for a project using raw HTTP API bulk endpoint.
    """
    runs_api_target = RunsApi(target_service.client)
    
    # Extract authors from source workspace to build UUID -> ID mapping
    author_uuid_to_id_mapping = extract_authors(source_service)
    mappings.author_uuid_to_id_mapping = author_uuid_to_id_mapping
    
    # Initialize raw API client for target workspace
    try:
        base_url = target_service.client.configuration.host
        api_key_dict = target_service.client.configuration.api_key
        if isinstance(api_key_dict, dict):
            api_token = api_key_dict.get('TokenAuth') or api_key_dict.get('Token') or api_key_dict.get('token')
        else:
            api_token = None
    except Exception:
        logger.error("Cannot get API token/URL from target service")
        return
    
    if not api_token or not base_url:
        logger.error("API token or base URL not available for target service")
        return
    
    api_base = base_url.rstrip('/')
    if not api_base.endswith('/v1'):
        api_base = f"{api_base}/v1"
    
    raw_api_client = QaseRawApiClient(api_base, api_token)
    
    total_results = 0
    created_results = 0
    
    for source_run_id, target_run_id in run_mapping.items():
        source_results = extract_results(source_service, project_code_source, source_run_id)
        if not source_results:
            continue
        
        results_to_create = []
        
        for result_dict in source_results:
            total_results += 1
            
            source_case_id = result_dict.get('case_id')
            target_case_id = case_mapping.get(source_case_id)
            if not target_case_id:
                continue
            
            try:
                result_data = transform_result_data(
                    result_dict, target_case_id,
                    mappings, author_uuid_to_id_mapping
                )
                results_to_create.append(result_data)
            except Exception as create_error:
                logger.error(f"Error transforming result data: {create_error}")
                continue
        
        if results_to_create:
            chunk_list = list(chunks(results_to_create, 500))
            
            for chunk in chunk_list:
                if not chunk:
                    continue
                
                try:
                    success = raw_api_client.create_results_bulk(
                        project_code_target,
                        int(target_run_id),
                        chunk
                    )
                    if success:
                        created_results += len(chunk)
                except Exception as e:
                    logger.error(f"Exception creating results bulk: {type(e).__name__}: {e}", exc_info=True)
    
    stats.add_entity('results', total_results, created_results)
    
    if hasattr(mappings, '_runs_to_complete') and project_code_source in mappings._runs_to_complete:
        runs_to_complete = mappings._runs_to_complete[project_code_source]
        for target_run_id, run_info in runs_to_complete.items():
            if run_info.get('is_completed'):
                try:
                    retry_with_backoff(
                        runs_api_target.complete_run,
                        code=run_info['project_code'],
                        id=target_run_id
                    )
                except Exception:
                    pass
