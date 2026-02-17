"""
Create milestones in target Qase workspace.
"""
import logging
from typing import Dict, Any, List, Optional
from qase.api_client_v1.api.milestones_api import MilestonesApi
from qase.api_client_v1.models import MilestoneCreate
from qase_service import QaseService
from migration.utils import MigrationMappings, MigrationStats, retry_with_backoff, format_date, to_dict

logger = logging.getLogger(__name__)


def create_milestone_recursive(
    milestone_dict: Dict[str, Any],
    milestones_list: List[Dict[str, Any]],
    project_code_target: str,
    milestones_api_target: MilestonesApi,
    milestone_mapping: Dict[int, int],
    parent_id: Optional[int] = None
):
    """Recursively create milestone and its children."""
    milestone_data = MilestoneCreate(
        title=milestone_dict['title'],
        description=milestone_dict.get('description', ''),
        status=milestone_dict.get('status', 'active'),
        due_date=format_date(milestone_dict.get('due_date'))
    )
    
    create_response = retry_with_backoff(
        milestones_api_target.create_milestone,
        code=project_code_target,
        milestone_create=milestone_data
    )
    
    if create_response:
        source_id = milestone_dict.get('id')
        target_id = None
        if hasattr(create_response, 'status') and hasattr(create_response, 'result'):
            if create_response.status and create_response.result:
                target_id = getattr(create_response.result, 'id', None)
        elif hasattr(create_response, 'id'):
            target_id = create_response.id
        elif hasattr(create_response, 'result'):
            result = create_response.result
            target_id = getattr(result, 'id', None)
        
        if target_id:
                    milestone_mapping[source_id] = target_id
        else:
            if hasattr(create_response, 'result'):
                result_dict = to_dict(create_response.result)
                if 'id' in result_dict:
                    target_id = result_dict['id']
                    milestone_mapping[source_id] = target_id
        
        if target_id:
            children = [m for m in milestones_list if m.get('parent_id') == source_id]
            for child_dict in children:
                create_milestone_recursive(
                    child_dict, milestones_list, project_code_target,
                    milestones_api_target, milestone_mapping, target_id
                )


def migrate_milestones(
    source_service: QaseService,
    target_service: QaseService,
    project_code_source: str,
    project_code_target: str,
    mappings: MigrationMappings,
    stats: MigrationStats
) -> Dict[int, int]:
    """
    Migrate milestones from source to target workspace.
    
    Args:
        source_service: Source Qase service
        target_service: Target Qase service
        project_code_source: Source project code
        project_code_target: Target project code
        mappings: Migration mappings object
        stats: Migration stats object
    
    Returns:
        Dictionary mapping source milestone ID to target milestone ID
    """
    from migration.extract.milestones import extract_milestones
    
    milestones_list = extract_milestones(source_service, project_code_source)
    
    milestones_api_target = MilestonesApi(target_service.client)
    milestone_mapping = {}
    
    root_milestones = [m for m in milestones_list if not m.get('parent_id')]
    for milestone_dict in root_milestones:
        create_milestone_recursive(
            milestone_dict, milestones_list, project_code_target,
            milestones_api_target, milestone_mapping
        )
    
    if project_code_source not in mappings.milestones:
        mappings.milestones[project_code_source] = {}
    mappings.milestones[project_code_source].update(milestone_mapping)
    
    stats.add_entity('milestones', len(milestones_list), len(milestone_mapping))
    return milestone_mapping
