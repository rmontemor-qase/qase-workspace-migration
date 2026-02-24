"""
Create runs in target Qase workspace.
"""
import logging
from typing import Dict, Any
from qase.api_client_v1.api.runs_api import RunsApi
from qase.api_client_v1.models import RunCreate
from qase_service import QaseService
from migration.utils import MigrationMappings, MigrationStats, retry_with_backoff, format_datetime, QaseRawApiClient
from migration.extract.runs import extract_runs, extract_run_cases

logger = logging.getLogger(__name__)


def migrate_runs(
    source_service: QaseService,
    target_service: QaseService,
    project_code_source: str,
    project_code_target: str,
    case_mapping: Dict[int, int],
    config_mapping: Dict[int, int],
    milestone_mapping: Dict[int, int],
    plan_mapping: Dict[int, int],
    user_mapping: Dict[int, int],
    mappings: MigrationMappings,
    stats: MigrationStats
) -> Dict[int, int]:
    """
    Migrate test runs from source to target workspace.
    
    Returns:
        Dictionary mapping source run ID to target run ID
    """
    runs_api_target = RunsApi(target_service.client)
    
    # Initialize raw API client for creating runs with milestone_id support
    try:
        base_url = target_service.client.configuration.host
        api_key_dict = target_service.client.configuration.api_key
        if isinstance(api_key_dict, dict):
            api_token = api_key_dict.get('TokenAuth') or api_key_dict.get('Token') or api_key_dict.get('token')
        else:
            api_token = None
        api_base = base_url.rstrip('/')
        if not api_base.endswith('/v1'):
            api_base = f"{api_base}/v1"
        raw_api_client = QaseRawApiClient(api_base, api_token) if api_token else None
    except Exception:
        raw_api_client = None
    
    # Normalize user_mapping: ensure keys are integers (JSON may store them as strings)
    if user_mapping:
        first_key = next(iter(user_mapping.keys()), None)
        if first_key is not None and isinstance(first_key, str):
            user_mapping = {int(k): v for k, v in user_mapping.items()}
    else:
        # Fallback to mappings.users if user_mapping is empty
        user_mapping = getattr(mappings, 'users', {})
        if user_mapping:
            first_key = next(iter(user_mapping.keys()), None)
            if first_key is not None and isinstance(first_key, str):
                user_mapping = {int(k): v for k, v in user_mapping.items()}
    
    # Normalize milestone_mapping: ensure keys are integers (JSON may store them as strings)
    if milestone_mapping:
        first_key = next(iter(milestone_mapping.keys()), None)
        if first_key is not None and isinstance(first_key, str):
            milestone_mapping = {int(k): v for k, v in milestone_mapping.items()}
    else:
        # Fallback to mappings.milestones if milestone_mapping is empty
        if project_code_source in getattr(mappings, 'milestones', {}):
            milestone_mapping = mappings.milestones[project_code_source]
            if milestone_mapping:
                first_key = next(iter(milestone_mapping.keys()), None)
                if first_key is not None and isinstance(first_key, str):
                    milestone_mapping = {int(k): v for k, v in milestone_mapping.items()}
        else:
            milestone_mapping = {}
    
    # Build a mapping from milestone title to source milestone ID
    # This is needed because runs return milestone as {title, description} not milestone_id
    milestone_title_to_id = {}
    try:
        from migration.extract.milestones import extract_milestones
        source_milestones = extract_milestones(source_service, project_code_source)
        for milestone in source_milestones:
            milestone_id = milestone.get('id')
            milestone_title = milestone.get('title')
            if milestone_id and milestone_title:
                milestone_title_to_id[milestone_title] = milestone_id
    except Exception:
        pass
    
    run_mapping = {}
    source_runs = extract_runs(source_service, project_code_source)
    
    for run_dict in source_runs:
        source_run_id = run_dict.get('id')
        
        target_cases = extract_run_cases(
            source_service, project_code_source, source_run_id, case_mapping
        )
        
        if not target_cases:
            source_cases = run_dict.get('cases', [])
            if source_cases:
                for case_item in source_cases:
                    case_id = None
                    if isinstance(case_item, int):
                        case_id = case_item
                    elif isinstance(case_item, dict):
                        case_id = case_item.get('id') or case_item.get('case_id')
                    elif hasattr(case_item, 'id'):
                        case_id = case_item.id
                    
                    if case_id:
                        target_case_id = case_mapping.get(int(case_id))
                        if target_case_id and target_case_id not in target_cases:
                            target_cases.append(target_case_id)
        
        target_configs = []
        source_configs = run_dict.get('configurations', [])
        if source_configs:
            for config_item in source_configs:
                config_id = None
                if isinstance(config_item, int):
                    config_id = config_item
                elif isinstance(config_item, dict):
                    config_id = config_item.get('id') or config_item.get('configuration_id')
                elif hasattr(config_item, 'id'):
                    config_id = config_item.id
                
                if config_id:
                    target_config_id = config_mapping.get(int(config_id))
                    if target_config_id:
                        target_configs.append(target_config_id)
        
        # Map author_id: Qase API returns 'user_id' field in run data
        source_user_id = run_dict.get('user_id') or run_dict.get('created_by') or run_dict.get('author_id') or run_dict.get('member_id')
        if source_user_id:
            try:
                source_user_id_int = int(source_user_id)
                # Skip if user_id is 0 (system/automated runs)
                if source_user_id_int == 0:
                    target_author_id = 1
                else:
                    target_author_id = mappings.get_user_id(source_user_id_int)
            except (ValueError, TypeError):
                target_author_id = 1
        else:
            target_author_id = 1
        
        run_data_dict = {
            'title': run_dict.get('title', ''),
            'description': run_dict.get('description', ''),
            'author_id': target_author_id,
        }
        
        # Format and include start_time only if valid
        start_time_formatted = format_datetime(run_dict.get('start_time'))
        if start_time_formatted:
            run_data_dict['start_time'] = start_time_formatted
        
        # Format and include end_time only if valid
        end_time_formatted = format_datetime(run_dict.get('end_time'))
        if end_time_formatted:
            run_data_dict['end_time'] = end_time_formatted
        
        if target_cases:
            run_data_dict['cases'] = target_cases
        if target_configs:
            run_data_dict['configurations'] = target_configs
        
        # Handle milestone: runs return milestone as {title, description} object, not milestone_id
        source_milestone_id = None
        milestone_obj = run_dict.get('milestone')
        if milestone_obj:
            # Extract milestone title from milestone object
            if isinstance(milestone_obj, dict):
                milestone_title = milestone_obj.get('title')
                if milestone_title and milestone_title_to_id:
                    source_milestone_id = milestone_title_to_id.get(milestone_title)
        elif run_dict.get('milestone_id'):
            # Fallback: if milestone_id is directly available (shouldn't happen based on API response)
            source_milestone_id = run_dict.get('milestone_id')
        
        if source_milestone_id is not None:
            try:
                # Ensure milestone_id is an integer for lookup
                source_milestone_id_int = int(source_milestone_id)
                mapped_milestone = milestone_mapping.get(source_milestone_id_int)
                if mapped_milestone is not None:
                    run_data_dict['milestone_id'] = mapped_milestone
            except (ValueError, TypeError):
                pass
        if run_dict.get('plan_id'):
            mapped_plan = plan_mapping.get(run_dict.get('plan_id'))
            if mapped_plan:
                run_data_dict['plan_id'] = mapped_plan
        
        # Use raw API client if milestone_id is present (SDK may not support it properly)
        # Otherwise fall back to SDK
        if raw_api_client and 'milestone_id' in run_data_dict:
            target_run_id = raw_api_client.create_run(project_code_target, run_data_dict)
            create_response = target_run_id is not None
        else:
            run_data = RunCreate(**run_data_dict)
            create_response = retry_with_backoff(
                runs_api_target.create_run,
                code=project_code_target,
                run_create=run_data
            )
            
            if create_response:
                target_run_id = None
                if hasattr(create_response, 'status') and hasattr(create_response, 'result'):
                    if create_response.status and create_response.result:
                        target_run_id = getattr(create_response.result, 'id', None)
                elif hasattr(create_response, 'id'):
                    target_run_id = create_response.id
                elif hasattr(create_response, 'result'):
                    result = create_response.result
                    target_run_id = getattr(result, 'id', None)
            else:
                target_run_id = None
        
        if create_response and target_run_id:
            run_mapping[source_run_id] = target_run_id
            is_completed = run_dict.get('is_completed', False)
            has_end_time = bool(run_dict.get('end_time'))
            
            if is_completed or has_end_time:
                if not hasattr(migrate_runs, '_runs_to_complete'):
                    migrate_runs._runs_to_complete = {}
                migrate_runs._runs_to_complete[target_run_id] = {
                    'project_code': project_code_target,
                    'is_completed': True,
                    'source_is_completed': is_completed,
                    'has_end_time': has_end_time
                }
    
    if project_code_source not in mappings.runs:
        mappings.runs[project_code_source] = {}
    mappings.runs[project_code_source].update(run_mapping)
    
    if hasattr(migrate_runs, '_runs_to_complete'):
        if not hasattr(mappings, '_runs_to_complete'):
            mappings._runs_to_complete = {}
        mappings._runs_to_complete[project_code_source] = migrate_runs._runs_to_complete
        delattr(migrate_runs, '_runs_to_complete')
    
    stats.add_entity('runs', len(source_runs), len(run_mapping))
    return run_mapping
