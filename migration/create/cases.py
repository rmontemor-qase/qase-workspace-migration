"""
Create cases in target Qase workspace.
"""
import logging
import re
from typing import Dict, Any, List, Optional
from qase.api_client_v1.api.cases_api import CasesApi
from qase.api_client_v1.models import TestCasebulk, TestCasebulkCasesInner
from qase_service import QaseService
from migration.utils import (
    MigrationMappings, MigrationStats, retry_with_backoff,
    extract_entities_from_response, to_dict, preserve_or_hash_id,
    QaseRawApiClient
)
from migration.transform.attachments import replace_attachment_hashes_in_text

logger = logging.getLogger(__name__)


def transform_case_data(
    case_dict: Dict[str, Any],
    suite_mapping: Dict[int, int],
    custom_field_mapping: Dict[int, int],
    milestone_mapping: Dict[int, int],
    shared_step_mapping: Dict[str, str],
    user_mapping: Dict[int, int],
    attachment_mapping: Dict[str, str],
    mappings: MigrationMappings,
    preserve_ids: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Transform a case dictionary from source format to target format.
    
    Returns:
        Transformed case data dict, or None if transformation fails
    """
    source_suite_id = case_dict.get('suite_id')
    target_suite_id = None
    
    if source_suite_id:
        target_suite_id = suite_mapping.get(source_suite_id)
    
    case_id = case_dict.get('id')
    if case_id and preserve_ids:
        case_id = preserve_or_hash_id(case_id, preserve_ids)
    
    tags_list = case_dict.get('tags', [])
    processed_tags = []
    for tag in tags_list:
        if isinstance(tag, dict):
            tag_title = tag.get('title') or tag.get('name') or str(tag)
            processed_tags.append(tag_title)
        elif isinstance(tag, str):
            processed_tags.append(tag)
        else:
            processed_tags.append(str(tag))
    
    created_at = case_dict.get('created_at')
    if created_at and hasattr(created_at, 'isoformat'):
        created_at = created_at.isoformat()
    elif created_at and isinstance(created_at, str):
        pass  # Already a string
    else:
        created_at = None
    
    updated_at = case_dict.get('updated_at')
    if updated_at and hasattr(updated_at, 'isoformat'):
        updated_at = updated_at.isoformat()
    elif updated_at and isinstance(updated_at, str):
        pass  # Already a string
    else:
        updated_at = None
    
    case_data = {
        'title': case_dict.get('title', ''),
        'description': case_dict.get('description', ''),
        'preconditions': case_dict.get('preconditions', ''),
        'postconditions': case_dict.get('postconditions', ''),
        'severity': case_dict.get('severity', 2),
        'priority': case_dict.get('priority', 2),
        'type': case_dict.get('type', 1),
        'behavior': case_dict.get('behavior', 1),
        'automation': case_dict.get('automation', 0),
        'status': case_dict.get('status', 1),
        'tags': processed_tags,
        'created_at': created_at,
        'updated_at': updated_at,
        'author_id': user_mapping.get(case_dict.get('author_id'), 1),
        'milestone_id': milestone_mapping.get(case_dict.get('milestone_id')) if case_dict.get('milestone_id') else None,
        'attachments': [],  # Will be mapped below
        'params': {},  # Must be a dict, not a list
        'is_flaky': case_dict.get('is_flaky', 0),
        'custom_field': {}
    }
    
    if case_id:
        case_data['id'] = case_id
    
    if target_suite_id:
        case_data['suite_id'] = target_suite_id
    
    case_attachments = case_dict.get('attachments', []) or []
    mapped_case_attachments = []
    if case_attachments and attachment_mapping:
        for att_item in case_attachments:
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
                    mapped_case_attachments.append(mapped_hash)
    case_data['attachments'] = mapped_case_attachments
    
    target_workspace_hash = getattr(mappings, 'target_workspace_hash', None)
    if attachment_mapping:
        case_data['description'] = replace_attachment_hashes_in_text(
            case_data.get('description', ''), attachment_mapping, target_workspace_hash
        )
        case_data['preconditions'] = replace_attachment_hashes_in_text(
            case_data.get('preconditions', ''), attachment_mapping, target_workspace_hash
        )
        case_data['postconditions'] = replace_attachment_hashes_in_text(
            case_data.get('postconditions', ''), attachment_mapping, target_workspace_hash
        )
    
    if 'custom_fields' in case_dict and case_dict['custom_fields']:
        for custom_field_item in case_dict['custom_fields']:
            if isinstance(custom_field_item, dict):
                field_id_source = custom_field_item.get('id')
                value = custom_field_item.get('value')
                if field_id_source is not None:
                    field_id_target = custom_field_mapping.get(int(field_id_source))
                    if field_id_target:
                        mapped_value = value
                        if isinstance(value, str) and attachment_mapping:
                            mapped_value = replace_attachment_hashes_in_text(value, attachment_mapping, target_workspace_hash)
                        case_data['custom_field'][str(field_id_target)] = mapped_value
    elif 'custom_field' in case_dict and case_dict['custom_field']:
        for field_id_source, value in case_dict['custom_field'].items():
            field_id_target = custom_field_mapping.get(int(field_id_source))
            if field_id_target:
                mapped_value = value
                if isinstance(value, str) and attachment_mapping:
                    mapped_value = replace_attachment_hashes_in_text(value, attachment_mapping, target_workspace_hash)
                case_data['custom_field'][str(field_id_target)] = mapped_value
    
    if 'steps' in case_dict and case_dict['steps']:
        processed_steps = []
        for step in case_dict['steps']:
            step_dict = to_dict(step)
            if isinstance(step_dict, dict) and 'shared' in step_dict:
                source_hash = step_dict['shared']
                target_hash = shared_step_mapping.get(source_hash)
                if target_hash:
                    processed_steps.append({'shared': target_hash})
            else:
                step_attachments = step_dict.get('attachments', []) or []
                mapped_step_attachments = []
                if step_attachments and attachment_mapping:
                    for att_hash in step_attachments:
                        mapped_hash = attachment_mapping.get(att_hash)
                        if mapped_hash:
                            mapped_step_attachments.append(mapped_hash)
                
                step_action = step_dict.get('action', '')
                step_expected_result = step_dict.get('expected_result')
                step_data = step_dict.get('data')
                
                if attachment_mapping:
                    step_action = replace_attachment_hashes_in_text(step_action, attachment_mapping, target_workspace_hash)
                    if step_expected_result:
                        step_expected_result = replace_attachment_hashes_in_text(step_expected_result, attachment_mapping, target_workspace_hash)
                    if step_data:
                        step_data = replace_attachment_hashes_in_text(step_data, attachment_mapping, target_workspace_hash)
                
                processed_steps.append({
                    'action': step_action,
                    'expected_result': step_expected_result,
                    'data': step_data,
                    'position': step_dict.get('position', len(processed_steps) + 1),
                    'attachments': mapped_step_attachments
                })
        
        case_data['steps'] = processed_steps
    
    return case_data


def migrate_cases(
    source_service: QaseService,
    target_service: QaseService,
    project_code_source: str,
    project_code_target: str,
    suite_mapping: Dict[int, int],
    custom_field_mapping: Dict[int, int],
    milestone_mapping: Dict[int, int],
    shared_step_mapping: Dict[str, str],
    user_mapping: Dict[int, int],
    mappings: MigrationMappings,
    stats: MigrationStats,
    preserve_ids: bool = True
) -> Dict[int, int]:
    """
    Migrate test cases from source to target workspace.
    
    Returns:
        Dictionary mapping source case ID to target case ID
    """
    from migration.extract.cases import extract_cases
    
    cases_api_target = CasesApi(target_service.client)
    
    case_mapping = {}
    limit = 100 if not target_service.enterprise else 20
    
    raw_api_client = QaseRawApiClient(
        base_url=target_service.client.configuration.host,
        api_token=target_service.api_token
    )
    
    attachment_mapping = {}
    if project_code_source in mappings.attachments:
        attachment_mapping = mappings.attachments[project_code_source]
        normalized_mapping = {}
        for key, value in attachment_mapping.items():
            normalized_mapping[key.lower()] = value
            normalized_mapping[key] = value
        attachment_mapping = normalized_mapping
    
    all_source_cases = extract_cases(source_service, project_code_source, limit)
    
    if not all_source_cases:
        if project_code_source not in mappings.cases:
            mappings.cases[project_code_source] = {}
        stats.add_entity('cases', 0, 0)
        return {}
    
    batch_size = limit
    total_cases_processed = len(all_source_cases)
    
    for batch_start in range(0, len(all_source_cases), batch_size):
        batch_cases = all_source_cases[batch_start:batch_start + batch_size]
        
        cases_to_create = []
        cases_with_shared_steps = []
        
        for case_dict in batch_cases:
            case_data = transform_case_data(
                case_dict, suite_mapping, custom_field_mapping,
                milestone_mapping, shared_step_mapping, user_mapping,
                attachment_mapping, mappings, preserve_ids
            )
            
            if not case_data:
                continue
            
            source_id = case_dict.get('id')
            has_shared_steps = any(
                isinstance(step, dict) and 'shared' in step
                for step in case_data.get('steps', [])
            )
            
            if has_shared_steps:
                case_data['_source_id'] = source_id
                cases_with_shared_steps.append(case_data)
            else:
                cases_to_create.append((source_id, TestCasebulkCasesInner(**case_data)))
        
        # Create cases without shared steps using SDK
        if cases_to_create:
            case_objects = [case_obj for _, case_obj in cases_to_create]
            source_ids = [source_id for source_id, _ in cases_to_create]
            
            bulk_data = TestCasebulk(cases=case_objects)
            bulk_response = retry_with_backoff(
                cases_api_target.bulk,
                code=project_code_target,
                test_casebulk=bulk_data
            )
            
            response_ids = None
            if bulk_response:
                if hasattr(bulk_response, 'ids') and bulk_response.ids:
                    response_ids = bulk_response.ids
                elif hasattr(bulk_response, 'result') and bulk_response.result:
                    if hasattr(bulk_response.result, 'ids') and bulk_response.result.ids:
                        response_ids = bulk_response.result.ids
                    elif hasattr(bulk_response.result, 'entities'):
                        response_ids = [e.id for e in bulk_response.result.entities if hasattr(e, 'id')]
            
            if response_ids:
                for idx, source_id in enumerate(source_ids):
                    if idx < len(response_ids):
                        case_mapping[source_id] = response_ids[idx]
        
        if cases_with_shared_steps:
            for case_data in cases_with_shared_steps:
                source_id = case_data.pop('_source_id')
                success = raw_api_client.create_cases_bulk(project_code_target, [case_data])
                if success:
                    try:
                        created_cases_response = cases_api_target.get_cases(
                            code=project_code_target,
                            filters={'title': case_data['title']},
                            limit=1
                        )
                        created_cases_entities = extract_entities_from_response(created_cases_response)
                        if created_cases_entities:
                            case_mapping[source_id] = created_cases_entities[0].id
                    except:
                        pass
    
    if project_code_source not in mappings.cases:
        mappings.cases[project_code_source] = {}
    mappings.cases[project_code_source].update(case_mapping)
    
    stats.add_entity('cases', total_cases_processed, len(case_mapping))
    return case_mapping
