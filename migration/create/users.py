"""
Map users between source and target Qase workspaces.
Note: Users are not created, only mapped by email.
"""
import logging
from typing import Dict
from qase.api_client_v1.api.authors_api import AuthorsApi
from qase_service import QaseService
from migration.utils import MigrationMappings, MigrationStats, retry_with_backoff, extract_entities_from_response, to_dict

logger = logging.getLogger(__name__)


def map_users(
    source_users: Dict[str, int],
    target_service: QaseService,
    mappings: MigrationMappings,
    stats: MigrationStats
) -> Dict[int, int]:
    """
    Map users from source to target workspace by email.
    
    Args:
        source_users: Dictionary mapping email -> source_user_id
        target_service: Target Qase service
        mappings: Migration mappings object
        stats: Migration stats object
    
    Returns:
        Dictionary mapping source user ID to target user ID
    """
    authors_api_target = AuthorsApi(target_service.client)
    
    user_mapping = {}
    offset = 0
    limit = 100
    
    # Get target users and map by email
    while True:
        api_response = retry_with_backoff(
            authors_api_target.get_authors,
            limit=limit,
            offset=offset,
            type="user"
        )
        
        entities = extract_entities_from_response(api_response)
        if not entities:
            break
        
        for user in entities:
            user_dict = to_dict(user)
            email = user_dict.get('email', '')
            if email in source_users:
                source_id = source_users[email]
                target_id = user_dict.get('id')
                user_mapping[source_id] = target_id
        
        if len(entities) < limit:
            break
        offset += limit
    
    mappings.users = user_mapping
    stats.add_entity('users', len(source_users), len(user_mapping))
    return user_mapping


def migrate_users(
    source_service: QaseService,
    target_service: QaseService,
    mappings: MigrationMappings,
    stats: MigrationStats
) -> Dict[int, int]:
    """
    Migrate users from source to target workspace.
    
    This is a convenience function that combines extraction and mapping.
    
    Args:
        source_service: Source Qase service
        target_service: Target Qase service
        mappings: Migration mappings object
        stats: Migration stats object
    
    Returns:
        Dictionary mapping source user ID to target user ID
    """
    from migration.extract.users import extract_users
    
    source_users = extract_users(source_service)
    return map_users(source_users, target_service, mappings, stats)
