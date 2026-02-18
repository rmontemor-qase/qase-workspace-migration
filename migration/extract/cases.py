"""
Extract cases from source Qase workspace.
"""
import logging
from typing import List, Dict, Any
from qase.api_client_v1.api.cases_api import CasesApi
from qase_service import QaseService
from migration.utils import retry_with_backoff, extract_entities_from_response, to_dict

logger = logging.getLogger(__name__)


def extract_cases(source_service: QaseService, project_code: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Extract test cases from source project.
    Fetches full case details including steps with shared step references.
    
    Args:
        source_service: Source Qase service
        project_code: Project code
        limit: Batch size (default 100, or 20 for enterprise)
    
    Returns:
        List of case dictionaries with full step details
    """
    cases_api_source = CasesApi(source_service.client)
    
    cases = []
    offset = 0
    
    # First, get list of case IDs
    case_ids = []
    while True:
        source_cases_response = retry_with_backoff(
            cases_api_source.get_cases,
            code=project_code,
            limit=limit,
            offset=offset
        )
        
        source_cases_entities = extract_entities_from_response(source_cases_response)
        if not source_cases_entities:
            break
        
        for source_case in source_cases_entities:
            case_dict = to_dict(source_case)
            case_id = case_dict.get('id')
            if case_id:
                case_ids.append(case_id)
        
        if len(source_cases_entities) < limit:
            break
        offset += limit
    
    # Fetch full details for each case to preserve shared step references
    for case_id in case_ids:
        try:
            case_response = retry_with_backoff(
                cases_api_source.get_case,
                code=project_code,
                id=case_id
            )
            
            if case_response and hasattr(case_response, 'result') and case_response.result:
                case_dict = to_dict(case_response.result)
                cases.append(case_dict)
        except Exception:
            continue
    
    return cases
