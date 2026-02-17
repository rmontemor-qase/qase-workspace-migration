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
    
    Args:
        source_service: Source Qase service
        project_code: Project code
        limit: Batch size (default 100, or 20 for enterprise)
    
    Returns:
        List of case dictionaries
    """
    cases_api_source = CasesApi(source_service.client)
    
    cases = []
    offset = 0
    
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
            cases.append(to_dict(source_case))
        
        if len(source_cases_entities) < limit:
            break
        offset += limit
    
    return cases
