"""
Create results in target Qase workspace.
"""
import logging
import re
from datetime import datetime
from typing import Dict, Any, List
from qase.api_client_v2.api.results_api import ResultsApi as ResultsApiV2
from qase.api_client_v1.api.results_api import ResultsApi
from qase.api_client_v1.api.cases_api import CasesApi
from qase.api_client_v1.api.runs_api import RunsApi
from qase.api_client_v2.models import (
    CreateResultsRequestV2,
    ResultCreate as ResultCreateV2,
    ResultExecution,
    ResultStep,
    ResultStepData,
    ResultStepExecution,
    ResultStepStatus
)
from qase.api_client_v1.exceptions import ApiException
from qase_service import QaseService
from migration.utils import MigrationMappings, MigrationStats, retry_with_backoff, chunks
from migration.transform.attachments import replace_attachment_hashes_in_text
from migration.extract.results import extract_results

logger = logging.getLogger(__name__)


def transform_result_data(
    result_dict: Dict[str, Any],
    target_case_id: int,
    case_title: str,
    attachment_mapping: Dict[str, str],
    mappings: MigrationMappings
) -> ResultCreateV2:
    """
    Transform a result dictionary from source format to ResultCreateV2 format.
    
    Returns:
        ResultCreateV2 object
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
    
    # Prepare execution
    start_time = result_dict.get('start_time')
    end_time = result_dict.get('end_time')
    duration = result_dict.get('time_ms', 0)
    
    # Convert timestamps to Unix timestamp (integer)
    if start_time:
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        elif isinstance(start_time, str):
            try:
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                start_time = int(start_dt.timestamp())
            except:
                start_time = None
        elif isinstance(start_time, (int, float)):
            start_time = int(start_time)
        else:
            start_time = None
    else:
        start_time = None
    
    if end_time:
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())
        elif isinstance(end_time, str):
            try:
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                end_time = int(end_dt.timestamp())
            except:
                end_time = None
        elif isinstance(end_time, (int, float)):
            end_time = int(end_time)
        else:
            end_time = None
    else:
        end_time = None
    
    execution = ResultExecution(
        status=status,
        duration=duration if duration else None,
        start_time=start_time if start_time is not None else None,
        end_time=end_time if end_time is not None else None
    )
    
    # Prepare steps
    result_steps = []
    if 'steps' in result_dict and result_dict['steps']:
        for step in result_dict['steps']:
            step_dict = to_dict(step) if not isinstance(step, dict) else step
            
            step_action = step_dict.get('action', '') or ''
            step_expected_result = step_dict.get('expected_result')
            step_comment = step_dict.get('comment')
            
            if not step_action or not step_action.strip():
                continue
            
            if attachment_mapping:
                target_workspace_hash = getattr(mappings, 'target_workspace_hash', None)
                step_action = replace_attachment_hashes_in_text(step_action, attachment_mapping, target_workspace_hash)
                if step_expected_result:
                    step_expected_result = replace_attachment_hashes_in_text(step_expected_result, attachment_mapping, target_workspace_hash)
                if step_comment:
                    step_comment = replace_attachment_hashes_in_text(step_comment, attachment_mapping, target_workspace_hash)
            
            step_data = ResultStepData(
                action=step_action.strip(),
                expected_result=step_expected_result if step_expected_result else None
            )
            
            # Handle status
            step_status_raw = step_dict.get('status', 'SKIPPED')
            if isinstance(step_status_raw, int):
                status_map = {
                    1: 'PASSED', 2: 'BLOCKED', 3: 'SKIPPED',
                    4: 'RETEST', 5: 'FAILED'
                }
                step_status_str = status_map.get(step_status_raw, 'SKIPPED')
            else:
                step_status_str = str(step_status_raw).upper()
            
            try:
                step_status = ResultStepStatus[step_status_str]
            except KeyError:
                step_status = ResultStepStatus.SKIPPED
            
            step_execution = ResultStepExecution(
                status=step_status,
                comment=step_comment if step_comment else None
            )
            result_steps.append(
                ResultStep(
                    data=step_data,
                    execution=step_execution
                )
            )
    
    # Map attachment hashes for result-level attachments
    result_attachments = result_dict.get('attachments', []) or []
    mapped_result_attachments = []
    if result_attachments and attachment_mapping:
        for att_item in result_attachments:
            source_hash = None
            if isinstance(att_item, str):
                source_hash = att_item
            elif isinstance(att_item, dict):
                if 'hash' in att_item:
                    source_hash = att_item['hash']
                elif 'url' in att_item:
                    url = att_item['url']
                    match = re.search(r'/attachment/([a-f0-9]{32,64})/', url, re.IGNORECASE)
                    if match:
                        source_hash = match.group(1)
            
            if source_hash:
                mapped_hash = attachment_mapping.get(source_hash)
                if mapped_hash:
                    mapped_result_attachments.append(mapped_hash)
    
    # Replace attachment hashes in result comment
    result_comment = result_dict.get('comment', '')
    if result_comment and attachment_mapping:
        target_workspace_hash = getattr(mappings, 'target_workspace_hash', None)
        result_comment = replace_attachment_hashes_in_text(result_comment, attachment_mapping, target_workspace_hash)
    
    return ResultCreateV2(
        title=case_title,
        testops_id=target_case_id,
        execution=execution,
        message=result_comment if result_comment else None,
        attachments=mapped_result_attachments if mapped_result_attachments else None,
        steps=result_steps if result_steps else None
    )


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
    Migrate test results for a project using API v2.
    """
    results_api_source = ResultsApi(source_service.client)
    results_api_target = ResultsApiV2(target_service.client_v2)
    cases_api_target = CasesApi(target_service.client)
    runs_api_target = RunsApi(target_service.client)
    
    total_results = 0
    created_results = 0
    total_skipped_results = 0
    
    attachment_mapping = {}
    if project_code_source in mappings.attachments:
        attachment_mapping = mappings.attachments[project_code_source]
    
    for source_run_id, target_run_id in run_mapping.items():
        source_results = extract_results(source_service, project_code_source, source_run_id)
        if not source_results:
            continue
        
        results_to_create = []
        skipped_results = 0
        
        for result_dict in source_results:
            total_results += 1
            
            # Map case ID
            source_case_id = result_dict.get('case_id')
            target_case_id = case_mapping.get(source_case_id)
            if not target_case_id:
                skipped_results += 1
                continue
            
            try:
                case_response = cases_api_target.get_case(
                    code=project_code_target,
                    id=target_case_id
                )
                case_title = case_response.result.title if case_response and case_response.status else "Test Case"
            except:
                case_title = "Test Case"
            
            try:
                result_data = transform_result_data(
                    result_dict, target_case_id, case_title,
                    attachment_mapping, mappings
                )
                results_to_create.append(result_data)
            except Exception as create_error:
                logger.error(f"Error creating ResultCreateV2: {create_error}")
                continue
        
        total_skipped_results += skipped_results
        
        if results_to_create:
            chunk_list = list(chunks(results_to_create, 500))
            
            for chunk_idx, chunk in enumerate(chunk_list):
                if not chunk:
                    continue
                
                try:
                    bulk_request = CreateResultsRequestV2(results=chunk)
                    bulk_response = results_api_target.create_results_v2(
                        project_code=project_code_target,
                        run_id=int(target_run_id),
                        create_results_request_v2=bulk_request
                    )
                    
                    if bulk_response is not None:
                        if hasattr(bulk_response, 'status'):
                            if bulk_response.status:
                                created_results += len(chunk)
                        elif hasattr(bulk_response, 'result'):
                            created_results += len(chunk)
                        else:
                            created_results += len(chunk)
                    else:
                        created_results += len(chunk)
                except ApiException as api_error:
                    logger.error(f"API exception: status={api_error.status}, reason={api_error.reason}")
                    if api_error.status >= 500:
                        bulk_response = retry_with_backoff(
                            results_api_target.create_results_v2,
                            project_code=project_code_target,
                            run_id=int(target_run_id),
                            create_results_request_v2=bulk_request
                        )
                        if bulk_response:
                            created_results += len(chunk)
                except Exception as e:
                    logger.error(f"Exception creating results: {type(e).__name__}: {e}", exc_info=True)
    
    stats.add_entity('results', total_results, created_results)
    
    if hasattr(mappings, '_runs_to_complete') and project_code_source in mappings._runs_to_complete:
        runs_to_complete = mappings._runs_to_complete[project_code_source]
        completed_count = 0
        failed_count = 0
        for target_run_id, run_info in runs_to_complete.items():
            if run_info.get('is_completed'):
                try:
                    complete_response = retry_with_backoff(
                        runs_api_target.complete_run,
                        code=run_info['project_code'],
                        id=target_run_id
                    )
                    if complete_response:
                        completed_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
