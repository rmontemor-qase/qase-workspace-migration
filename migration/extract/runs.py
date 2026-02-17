"""
Extract runs from source Qase workspace.
"""
import logging
from typing import List, Dict, Any, Optional
from qase.api_client_v1.api.runs_api import RunsApi
from qase_service import QaseService
from migration.utils import retry_with_backoff, extract_entities_from_response, to_dict

logger = logging.getLogger(__name__)


def extract_runs(source_service: QaseService, project_code: str) -> List[Dict[str, Any]]:
    """
    Extract test runs from source project.
    
    Args:
        source_service: Source Qase service
        project_code: Project code
    
    Returns:
        List of run dictionaries
    """
    runs_api_source = RunsApi(source_service.client)
    
    runs = []
    offset = 0
    limit = 100
    
    while True:
        api_response = retry_with_backoff(
            runs_api_source.get_runs,
            code=project_code,
            limit=limit,
            offset=offset
        )
        
        entities = extract_entities_from_response(api_response)
        if not entities:
            break
        
        for run in entities:
            runs.append(to_dict(run))
        
        if len(entities) < limit:
            break
        offset += limit
    
    return runs


def extract_run_cases(
    source_service: QaseService,
    project_code: str,
    run_id: int,
    case_mapping: Dict[int, int]
) -> List[int]:
    """
    Extract all case IDs from a run (including untested cases).
    
    Uses get_run with include='cases' if supported, otherwise falls back to get_tests().
    
    Args:
        source_service: Source Qase service
        project_code: Project code
        run_id: Run ID
        case_mapping: Mapping of source case ID -> target case ID
    
    Returns:
        List of target case IDs
    """
    runs_api_source = RunsApi(source_service.client)
    target_cases = []
    
    try:
        # Try get_run with include='cases' first
        run_response = retry_with_backoff(
            runs_api_source.get_run,
            code=project_code,
            id=run_id,
            include='cases'
        )
        
        if run_response and hasattr(run_response, 'result'):
            run_result = run_response.result
            run_result_dict = to_dict(run_result)
            
            cases_data = run_result_dict.get('cases', [])
            if not cases_data and hasattr(run_result, 'cases'):
                cases_data = to_dict(run_result.cases) if hasattr(run_result.cases, '__dict__') else run_result.cases
            
            if cases_data:
                for case_item in cases_data:
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
                return target_cases
    except TypeError as e:
        if 'include' in str(e).lower() or 'unexpected keyword' in str(e).lower():
            try:
                tests_offset = 0
                tests_limit = 250
                while True:
                    tests_response = retry_with_backoff(
                        runs_api_source.get_tests,
                        code=project_code,
                        id=run_id,
                        limit=tests_limit,
                        offset=tests_offset
                    )
                    
                    if tests_response and hasattr(tests_response, 'result'):
                        tests_entities = extract_entities_from_response(tests_response)
                        if not tests_entities:
                            break
                        
                        for test in tests_entities:
                            test_dict = to_dict(test)
                            source_case_id = test_dict.get('case_id')
                            if source_case_id:
                                target_case_id = case_mapping.get(int(source_case_id))
                                if target_case_id and target_case_id not in target_cases:
                                    target_cases.append(target_case_id)
                        
                        if len(tests_entities) < tests_limit:
                            break
                        tests_offset += tests_limit
                    else:
                        break
                return target_cases
            except Exception:
                pass
    except Exception:
        pass
    
    return []
