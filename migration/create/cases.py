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


def _get_target_user_id(source_user_id: Any, user_mapping: Dict[int, int], default_user_id: int = 1) -> int:
    """
    Get target user ID from source user ID using user_mapping.
    
    Following QASE_AUTHOR_ID_BREAKDOWN.md pattern:
    - Maps source workspace user ID to target workspace user ID
    - Falls back to default_user_id if mapping not found
    
    The user_mapping is already built based on email matching:
    - Source users are matched to target users by email (case-insensitive)
    - Mapping is: source_id -> target_id
    
    Args:
        source_user_id: Source user ID (from created_by or author_id field)
        user_mapping: Dictionary mapping source_id -> target_id (built via email matching)
        default_user_id: Fallback user ID if mapping not found (default: 1)
    
    Returns:
        Target user ID or default_user_id
    """
    if not source_user_id:
        return default_user_id
    
    try:
        source_user_id_int = int(source_user_id)
        return user_mapping.get(source_user_id_int, default_user_id)
    except (ValueError, TypeError):
        return default_user_id


def transform_case_data(
    case_dict: Dict[str, Any],
    suite_mapping: Dict[int, int],
    custom_field_mapping: Dict[int, int],
    milestone_mapping: Dict[int, int],
    shared_step_mapping: Dict[str, str],
    shared_parameter_mapping: Dict[str, str],
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
    
    # Map author_id: Qase API returns 'author_uuid' field in case data
    # Use UUID mapping first (most reliable), then fallback to member_id/created_by/author_id
    target_author_id = 1  # Default
    author_uuid = case_dict.get('author_uuid')
    
    if author_uuid:
        # Try UUID mapping first (author_uuid -> target_user_id)
        user_uuid_mapping = getattr(mappings, 'user_uuid_mapping', {})
        target_author_id = user_uuid_mapping.get(str(author_uuid), 1)
    else:
        # Fallback to member_id/created_by/author_id if UUID not available
        source_user_id = case_dict.get('member_id') or case_dict.get('created_by') or case_dict.get('author_id')
        if source_user_id:
            try:
                source_user_id_int = int(source_user_id)
                # Skip if user_id is 0 (system/automated cases)
                if source_user_id_int == 0:
                    target_author_id = 1  # Default user for automated cases
                else:
                    # Use mappings.get_user_id() method (per document pattern)
                    target_author_id = mappings.get_user_id(source_user_id_int)
            except (ValueError, TypeError):
                target_author_id = 1
    
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
        'author_id': target_author_id,
        'milestone_id': milestone_mapping.get(case_dict.get('milestone_id')) if case_dict.get('milestone_id') else None,
        'attachments': [],  # Will be mapped below
        'is_flaky': case_dict.get('is_flaky', 0),
        'custom_field': {}
    }
    
    # Extract and process params
    # Qase SDK expects params as dict format: {"Browser": ["Chrome"], "User1": ["Password"]}
    source_params = case_dict.get('params')
    source_parameters = case_dict.get('parameters')
    
    # Check if case has parameters structure (need raw HTTP API to preserve structure)
    has_parameters_structure = False
    has_group_parameters = False
    if source_parameters and isinstance(source_parameters, list):
        has_parameters_structure = True
        has_group_parameters = any(
            isinstance(p, dict) and p.get('type') == 'group'
            for p in source_parameters
        )
    
    if source_params and isinstance(source_params, dict) and len(source_params) > 0:
        case_data['params'] = source_params
    else:
        # Empty params must be empty dict for SDK validation
        case_data['params'] = {}
    
    # Preserve parameters structure for cases with parameters field (will use raw HTTP API)
    # This preserves both single and group parameters in their proper structure
    if has_parameters_structure and source_parameters:
        parameters_list = []
        for param_item in source_parameters:
            param_dict = to_dict(param_item) if not isinstance(param_item, dict) else param_item
            if isinstance(param_dict, dict):
                param_type = param_dict.get('type')
                source_shared_id = param_dict.get('shared_id')
                
                # Map shared parameter ID if it exists
                target_shared_id = None
                if source_shared_id:
                    # Convert to string for consistent lookup
                    source_shared_id_str = str(source_shared_id)
                    if source_shared_id_str in shared_parameter_mapping:
                        target_shared_id = shared_parameter_mapping[source_shared_id_str]
                    # Also try original format
                    elif source_shared_id in shared_parameter_mapping:
                        target_shared_id = shared_parameter_mapping[source_shared_id]
                
                # If this is a shared parameter reference, use simplified format: {"shared_id": "..."}
                if target_shared_id:
                    parameters_list.append({
                        'shared_id': str(target_shared_id)
                    })
                    continue
                
                # Handle single type parameter (non-shared)
                # API format: {"title": "...", "values": [...]}
                if param_type == 'single' and 'item' in param_dict:
                    item = param_dict['item']
                    item_dict = to_dict(item) if not isinstance(item, dict) else item
                    if isinstance(item_dict, dict):
                        param_name = item_dict.get('title') or item_dict.get('name')
                        param_values = item_dict.get('values') or []
                        if param_name and param_values:
                            parameters_list.append({
                                'title': param_name,
                                'values': param_values if isinstance(param_values, list) else [param_values]
                            })
                # Handle group type parameter - preserve as group (non-shared)
                # API format: {"items": [{"title": "...", "values": [...]}, ...]}
                elif param_type == 'group' and 'items' in param_dict:
                    items = param_dict['items']
                    if isinstance(items, list):
                        group_items = []
                        for item in items:
                            item_dict = to_dict(item) if not isinstance(item, dict) else item
                            if isinstance(item_dict, dict):
                                param_name = item_dict.get('title') or item_dict.get('name')
                                param_values = item_dict.get('values') or []
                                if param_name and param_values:
                                    group_items.append({
                                        'title': param_name,
                                        'values': param_values if isinstance(param_values, list) else [param_values]
                                    })
                        if group_items:
                            parameters_list.append({
                                'items': group_items
                            })
        
        if parameters_list:
            case_data['parameters'] = parameters_list
            case_data['_has_parameters_structure'] = True
    
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
            
            # Check for shared step reference in various formats
            source_hash = None
            if isinstance(step_dict, dict):
                # Format 1: {"shared": "hash"}
                if 'shared' in step_dict:
                    source_hash = step_dict['shared']
                # Format 2: {"shared_step": {"hash": "..."}}
                elif 'shared_step' in step_dict:
                    shared_step_obj = step_dict['shared_step']
                    if isinstance(shared_step_obj, dict):
                        source_hash = shared_step_obj.get('hash')
                    elif hasattr(shared_step_obj, 'hash'):
                        source_hash = getattr(shared_step_obj, 'hash', None)
                # Format 3: Check if step has a shared_step_hash field
                elif 'shared_step_hash' in step_dict:
                    source_hash = step_dict['shared_step_hash']
            
            if source_hash:
                target_hash = shared_step_mapping.get(source_hash)
                if target_hash:
                    processed_steps.append({'shared': target_hash})
                continue
            
            # Regular step processing
            if isinstance(step_dict, dict):
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
    shared_parameter_mapping: Dict[str, str],
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
    
    # Use workspace-level shared parameter mapping from mappings
    if hasattr(mappings, 'shared_parameters') and mappings.shared_parameters:
        # Merge with any passed mapping (workspace-level takes precedence)
        if shared_parameter_mapping:
            mappings.shared_parameters.update(shared_parameter_mapping)
        shared_parameter_mapping = mappings.shared_parameters
    elif not shared_parameter_mapping:
        shared_parameter_mapping = {}
    
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
        cases_with_shared_steps = []  # Also includes cases with group parameters
        
        for case_dict in batch_cases:
            case_data = transform_case_data(
                case_dict, suite_mapping, custom_field_mapping,
                milestone_mapping, shared_step_mapping, shared_parameter_mapping,
                user_mapping, attachment_mapping, mappings, preserve_ids
            )
            
            if not case_data:
                continue
            
            source_id = case_dict.get('id')
            has_shared_steps = any(
                isinstance(step, dict) and 'shared' in step
                for step in case_data.get('steps', [])
            )
            has_parameters_structure = case_data.pop('_has_parameters_structure', False)
            
            if has_shared_steps or has_parameters_structure:
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
                created_ids = raw_api_client.create_cases_bulk(project_code_target, [case_data])
                if created_ids and len(created_ids) > 0:
                    case_mapping[source_id] = created_ids[0]
    
    if project_code_source not in mappings.cases:
        mappings.cases[project_code_source] = {}
    mappings.cases[project_code_source].update(case_mapping)
    
    stats.add_entity('cases', total_cases_processed, len(case_mapping))
    return case_mapping
