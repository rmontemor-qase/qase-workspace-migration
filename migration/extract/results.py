"""
Extract results from source Qase workspace.
"""
import logging
from typing import List, Dict, Any
from qase.api_client_v1.api.results_api import ResultsApi
from qase_service import QaseService
from migration.utils import retry_with_backoff, extract_entities_from_response, to_dict

logger = logging.getLogger(__name__)


def extract_results(source_service: QaseService, project_code: str, run_id: int) -> List[Dict[str, Any]]:
    """
    Extract test results from a specific run.
    
    Args:
        source_service: Source Qase service
        project_code: Project code
        run_id: Run ID
    
    Returns:
        List of result dictionaries
    """
    results_api_source = ResultsApi(source_service.client)
    results = []
    offset = 0
    limit = 100  # API limit is max 100
    
    while True:
        source_results_response = retry_with_backoff(
            results_api_source.get_results,
            code=project_code,
            run=str(run_id),  # Parameter is 'run' and expects a string
            limit=limit,
            offset=offset
        )
        
        source_results_entities = extract_entities_from_response(source_results_response)
        if not source_results_entities:
            break
        
        for result in source_results_entities:
            results.append(to_dict(result))
        
        if len(source_results_entities) < limit:
            break
        offset += limit
    
    return results
