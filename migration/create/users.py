"""
Migrate users between source and target Qase workspaces.
Supports SCIM-based user creation and mapping.
"""
import logging
from typing import Dict, List, Any, Optional
from qase.api_client_v1.api.authors_api import AuthorsApi
from qase_service import QaseService
from migration.utils import MigrationMappings, MigrationStats, retry_with_backoff, extract_entities_from_response, to_dict

logger = logging.getLogger(__name__)


def parse_name(full_name: str):
    """
    Parse full name into first and last name.
    
    Args:
        full_name: Full name string
    
    Returns:
        Tuple of (first_name, last_name)
    """
    if not full_name:
        return ("", "")
    
    parts = full_name.strip().split(maxsplit=1)
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], parts[1])


def migrate_users(
    source_service: QaseService,
    target_service: QaseService,
    mappings: MigrationMappings,
    stats: MigrationStats,
    config: Optional[Dict[str, Any]] = None
) -> Dict[int, int]:
    """
    Migrate users from source to target workspace.
    
    Supports:
    - Mapping existing users by email
    - Creating missing users via SCIM API (if enabled)
    - Using default user ID for unmapped users
    
    Args:
        source_service: Source Qase service
        target_service: Target Qase service
        mappings: Migration mappings object
        stats: Migration stats object
        config: Configuration dictionary with user migration options:
            - users.migrate: Main flag to enable/disable user migration (default: False)
            - users.create: Whether to create missing users (default: False)
            - users.inactive: Whether to create inactive users (default: False)
            - users.default: Default user ID for unmapped users (default: 1)
    
    Returns:
        Dictionary mapping source user ID to target user ID
    """
    from migration.extract.users import extract_users
    
    config = config or {}
    users_config = config.get('users', {})
    migrate_users_flag = users_config.get('migrate', False)
    
    # If migration is disabled, return empty mapping
    if not migrate_users_flag:
        logger.info("User migration is disabled in config (users.migrate: false)")
        return {}
    
    # Validate SCIM tokens are provided when migration is enabled
    if not source_service.scim_token:
        logger.error("User migration requires source SCIM token (source.scim_token) but it's not provided")
        raise ValueError("Source SCIM token is required when users.migrate is true")
    
    if not target_service.scim_token:
        logger.error("User migration requires target SCIM token (target.scim_token) but it's not provided")
        raise ValueError("Target SCIM token is required when users.migrate is true")
    
    create_users = users_config.get('create', False)
    create_inactive = users_config.get('inactive', False)
    default_user_id = users_config.get('default', 1)
    
    # Extract source users
    source_users = extract_users(source_service)
    
    # Get target users (via SCIM if available, otherwise via API)
    target_users_by_email = {}
    
    if target_service.scim_client:
        try:
            scim_users = target_service.scim_client.get_all_users()
            for user in scim_users:
                email = user.get('userName', '').lower()
                if email:
                    user_id = user.get('id')
                    # Convert SCIM user ID (string) to integer if possible for API compatibility
                    # SCIM IDs are usually strings, but Qase API expects integer user IDs
                    try:
                        if isinstance(user_id, str):
                            # Try to convert to int if it's numeric
                            user_id_int = int(user_id)
                        else:
                            user_id_int = user_id
                    except (ValueError, TypeError):
                        # If conversion fails, keep as string (might be UUID)
                        user_id_int = user_id
                    target_users_by_email[email] = {
                        'id': user_id_int,
                        'active': user.get('active', True)
                    }
            logger.info(f"Retrieved {len(target_users_by_email)} users from target via SCIM")
        except Exception as e:
            logger.warning(f"Failed to get users via SCIM, falling back to API: {e}")
            target_users_by_email = {}
    
    # If SCIM didn't work or isn't available, use API
    if not target_users_by_email:
        authors_api_target = AuthorsApi(target_service.client)
        offset = 0
        limit = 100
        
        while True:
            try:
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
                    email = user_dict.get('email', '').lower()
                    if email:
                        target_users_by_email[email] = {
                            'id': user_dict.get('id'),
                            'active': user_dict.get('is_active', True)
                        }
                
                if len(entities) < limit:
                    break
                offset += limit
            except Exception as e:
                logger.error(f"Error fetching target users: {e}")
                break
        
        logger.info(f"Retrieved {len(target_users_by_email)} users from target via API")
    
    # Build user mapping: source_id -> target_id
    # Process:
    # 1. Get emails and ids from source (already done above)
    # 2. Match emails from source to emails from target (target_users_by_email)
    # 3. Create mapping: source_id -> target_id (based on email matching)
    # 4. This mapping will be used when creating runs/cases to replace source author_id with target author_id
    user_mapping = {}
    created_count = 0
    mapped_count = 0
    skipped_count = 0
    
    # Build UUID mapping: source_user_uuid -> target_user_id
    user_uuid_mapping = {}
    
    for source_user in source_users:
        source_id = source_user.get('id')
        source_email = source_user.get('email', '').lower()
        source_name = source_user.get('name', '')
        source_is_active = source_user.get('is_active', True)
        source_role = source_user.get('role', 'Member')
        source_uuid = source_user.get('uuid') or source_user.get('id')  # UUID or fallback to ID
        
        if not source_id or not source_email:
            skipped_count += 1
            continue
        
        # Check if user exists in target
        if source_email in target_users_by_email:
            target_user = target_users_by_email[source_email]
            target_user_id = target_user['id']
            # Ensure target user ID is integer for API compatibility
            try:
                if isinstance(target_user_id, str):
                    target_user_id_int = int(target_user_id)
                else:
                    target_user_id_int = int(target_user_id)
            except (ValueError, TypeError):
                target_user_id_int = target_user_id
            user_mapping[int(source_id)] = target_user_id_int
            
            # Build UUID mapping: source_user_uuid -> target_user_id
            # Extract UUID from source user (could be 'uuid', 'id', or other fields)
            source_user_uuid = source_user.get('uuid') or source_user.get('author_uuid')
            if source_user_uuid:
                user_uuid_mapping[str(source_user_uuid)] = target_user_id_int
            
            mapped_count += 1
        elif create_users and target_service.scim_client:
            # Create user via SCIM
            if not source_is_active and not create_inactive:
                # Skip inactive users if not configured to create them
                user_mapping[int(source_id)] = default_user_id
                skipped_count += 1
                continue
            
            first_name, last_name = parse_name(source_name)
            if not first_name:
                first_name = source_email.split('@')[0]
            
            try:
                target_user_id = target_service.scim_client.create_user(
                    email=source_email,
                    first_name=first_name,
                    last_name=last_name,
                    role_title=source_role or "Member",
                    is_active=source_is_active
                )
                
                if target_user_id:
                    # Convert SCIM user ID (string) to integer if possible for API compatibility
                    try:
                        if isinstance(target_user_id, str):
                            target_user_id_int = int(target_user_id)
                        else:
                            target_user_id_int = target_user_id
                    except (ValueError, TypeError):
                        # If conversion fails, keep as string (might be UUID)
                        target_user_id_int = target_user_id
                    
                    user_mapping[int(source_id)] = target_user_id_int
                    
                    # Build UUID mapping: source_user_uuid -> target_user_id
                    source_user_uuid = source_user.get('uuid') or source_user.get('author_uuid')
                    if source_user_uuid:
                        user_uuid_mapping[str(source_user_uuid)] = target_user_id_int
                    
                    created_count += 1
                    # Update cache
                    target_users_by_email[source_email] = {
                        'id': target_user_id_int,
                        'active': source_is_active
                    }
                else:
                    # Creation failed, use default
                    user_mapping[int(source_id)] = default_user_id
                    skipped_count += 1
            except Exception as e:
                logger.error(f"Failed to create user {source_email}: {e}")
                user_mapping[int(source_id)] = default_user_id
                skipped_count += 1
        else:
            # Use default user ID
            user_mapping[int(source_id)] = default_user_id
            skipped_count += 1
    
    mappings.users = user_mapping
    
    # Store UUID mapping for cases and results (author_uuid -> target_user_id)
    mappings.user_uuid_mapping = user_uuid_mapping
    
    # Build email-to-target-user-ID mapping for use in runs/cases
    email_to_target_id = {}
    for source_user in source_users:
        source_id = source_user.get('id')
        source_email = source_user.get('email', '').lower()
        if source_id and source_email and source_id in user_mapping:
            email_to_target_id[source_email] = user_mapping[source_id]
    
    # Store email mapping in mappings for later use
    if not hasattr(mappings, 'user_email_mapping'):
        mappings.user_email_mapping = {}
    mappings.user_email_mapping.update(email_to_target_id)
    
    stats.add_entity('users', len(source_users), len(user_mapping))
    
    logger.info(f"User migration complete: {mapped_count} mapped, {created_count} created, {skipped_count} skipped/defaulted")
    
    return user_mapping
