"""
Extract attachment references from source Qase workspace.
"""
import logging
import re
from typing import Dict, Set, List, Any
from qase.api_client_v1.api.cases_api import CasesApi
from qase.api_client_v1.api.runs_api import RunsApi
from qase.api_client_v1.api.results_api import ResultsApi
from qase_service import QaseService
from migration.utils import retry_with_backoff, extract_entities_from_response, to_dict
from migration.transform.attachments import (
    extract_attachment_hashes_from_dict,
    extract_attachment_urls_from_dict
)

logger = logging.getLogger(__name__)


def extract_attachment_hashes_from_cases(
    source_service: QaseService,
    project_code: str
) -> tuple[Set[str], Dict[str, str]]:
    """
    Extract attachment hashes and URLs from all cases in a project.
    
    Returns:
        Tuple of (set of hashes, dict of hash -> URL)
    """
    cases_api_source = CasesApi(source_service.client)
    all_hashes = set()
    url_map = {}
    offset = 0
    limit = 100
    
    while True:
        try:
            cases_response = retry_with_backoff(
                cases_api_source.get_cases,
                code=project_code,
                limit=limit,
                offset=offset
            )
            
            cases_entities = extract_entities_from_response(cases_response)
            if not cases_entities:
                break
            
            for case in cases_entities:
                case_dict = to_dict(case)
                
                # Case-level attachments
                if case_dict.get('attachments'):
                    for att_item in case_dict['attachments']:
                        if isinstance(att_item, str):
                            all_hashes.add(att_item.lower())
                        elif isinstance(att_item, dict):
                            if 'hash' in att_item:
                                all_hashes.add(str(att_item['hash']).lower())
                            elif 'url' in att_item:
                                url = att_item['url']
                                match = re.search(r'/attachment/([a-f0-9]{32,64})/', url, re.IGNORECASE)
                                if match:
                                    all_hashes.add(match.group(1).lower())
                
                # Case text fields
                case_text_fields = ['description', 'preconditions', 'postconditions']
                case_text_hashes = extract_attachment_hashes_from_dict(case_dict, case_text_fields)
                case_text_urls = extract_attachment_urls_from_dict(case_dict, case_text_fields)
                all_hashes.update(case_text_hashes)
                url_map.update(case_text_urls)
                
                # Custom fields
                if case_dict.get('custom_fields'):
                    custom_field_hashes = extract_attachment_hashes_from_dict(
                        {'custom_fields': case_dict['custom_fields']}, ['custom_fields']
                    )
                    custom_field_urls = extract_attachment_urls_from_dict(
                        {'custom_fields': case_dict['custom_fields']}, ['custom_fields']
                    )
                    all_hashes.update(custom_field_hashes)
                    url_map.update(custom_field_urls)
                
                # Steps
                if case_dict.get('steps'):
                    for step in case_dict['steps']:
                        step_dict = to_dict(step)
                        if step_dict.get('attachments'):
                            normalized = {str(att).lower() for att in step_dict['attachments'] if att}
                            all_hashes.update(normalized)
                        
                        step_text_fields = ['action', 'expected_result', 'data']
                        step_text_hashes = extract_attachment_hashes_from_dict(step_dict, step_text_fields)
                        step_text_urls = extract_attachment_urls_from_dict(step_dict, step_text_fields)
                        all_hashes.update(step_text_hashes)
                        url_map.update(step_text_urls)
            
            if len(cases_entities) < limit:
                break
            offset += limit
        except Exception as e:
            break
    
    return all_hashes, url_map


def extract_attachment_hashes_from_results(
    source_service: QaseService,
    project_code: str
) -> tuple[Set[str], Dict[str, str]]:
    """
    Extract attachment hashes and URLs from all results in a project.
    
    Returns:
        Tuple of (set of hashes, dict of hash -> URL)
    """
    runs_api_source = RunsApi(source_service.client)
    results_api_source = ResultsApi(source_service.client)
    all_hashes = set()
    url_map = {}
    offset = 0
    limit = 100
    
    while True:
        try:
            runs_response = retry_with_backoff(
                runs_api_source.get_runs,
                code=project_code,
                limit=limit,
                offset=offset
            )
            
            runs_entities = extract_entities_from_response(runs_response)
            if not runs_entities:
                break
            
            for run in runs_entities:
                run_dict = to_dict(run)
                run_id = run_dict.get('id')
                
                results_offset = 0
                results_limit = 100
                
                while True:
                    try:
                        results_response = retry_with_backoff(
                            results_api_source.get_results,
                            code=project_code,
                            run=str(run_id),
                            limit=results_limit,
                            offset=results_offset
                        )
                        
                        results_entities = extract_entities_from_response(results_response)
                        if not results_entities:
                            break
                        
                        for result in results_entities:
                            result_dict = to_dict(result)
                            
                            # Result attachments
                            if result_dict.get('attachments'):
                                attachment_list = result_dict['attachments']
                                if isinstance(attachment_list, list):
                                    attachment_hashes = [h for h in attachment_list if isinstance(h, str)]
                                    all_hashes.update(attachment_hashes)
                            
                            # Result text fields
                            result_text_fields = ['comment']
                            result_text_hashes = extract_attachment_hashes_from_dict(result_dict, result_text_fields)
                            result_text_urls = extract_attachment_urls_from_dict(result_dict, result_text_fields)
                            all_hashes.update(result_text_hashes)
                            url_map.update(result_text_urls)
                            
                            # Result steps
                            if result_dict.get('steps'):
                                for step in result_dict['steps']:
                                    try:
                                        step_dict = to_dict(step) if not isinstance(step, dict) else step
                                        if isinstance(step_dict, dict):
                                            step_text_fields = ['action', 'expected_result', 'comment']
                                            step_text_hashes = extract_attachment_hashes_from_dict(step_dict, step_text_fields)
                                            step_text_urls = extract_attachment_urls_from_dict(step_dict, step_text_fields)
                                            valid_hashes = {h for h in step_text_hashes if isinstance(h, str)}
                                            all_hashes.update(valid_hashes)
                                            url_map.update(step_text_urls)
                                    except Exception:
                                        continue
                        
                        if len(results_entities) < results_limit:
                            break
                        results_offset += results_limit
                    except Exception as e:
                        break
            
            if len(runs_entities) < limit:
                break
            offset += limit
        except Exception as e:
            break
    
    return all_hashes, url_map


def extract_all_attachment_hashes(
    source_service: QaseService,
    projects: List[Dict[str, Any]]
) -> Dict[str, tuple[Set[str], Dict[str, str]]]:
    """
    Extract all attachment hashes from all projects.
    
    Returns:
        Dictionary mapping project_code -> (set of hashes, dict of hash -> URL)
    """
    all_project_attachments = {}
    
    for project in projects:
        project_code_source = project['source_code']
        
        case_hashes, case_urls = extract_attachment_hashes_from_cases(source_service, project_code_source)
        result_hashes, result_urls = extract_attachment_hashes_from_results(source_service, project_code_source)
        
        all_hashes = case_hashes | result_hashes
        all_urls = {**case_urls, **result_urls}
        
        all_project_attachments[project_code_source] = (all_hashes, all_urls)
    
    return all_project_attachments
