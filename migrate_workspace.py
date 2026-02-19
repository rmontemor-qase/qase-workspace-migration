#!/usr/bin/env python3
"""
Qase Workspace Migration Script

Migrates all content from one Qase workspace to another, preserving structure,
links, attachments, and relationships.
"""
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

from qase_service import QaseService
from migration.utils import MigrationMappings, MigrationStats
from migration.create import (
    migrate_projects,
    migrate_users,
    migrate_custom_fields,
    migrate_shared_parameters,
    migrate_attachments_workspace,
    migrate_milestones,
    migrate_configurations,
    migrate_environments,
    migrate_shared_steps,
    migrate_suites,
    migrate_cases,
    migrate_plans,
    migrate_runs,
    migrate_results,
    migrate_groups
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        raise


def parse_args():
    """Parse command line arguments - loads from config.json by default."""
    config_path = 'config.json'
    config_defaults = {}
    
    if Path(config_path).exists():
        try:
            config = load_config(config_path)
            source_config = config.get('source', {})
            target_config = config.get('target', {})
            options_config = config.get('options', {})
            
            config_defaults = {
                'source_token': source_config.get('api_token'),
                'source_host': source_config.get('host', 'qase.io'),
                'source_enterprise': source_config.get('enterprise', False),
                'source_ssl': source_config.get('ssl', True),
                'target_token': target_config.get('api_token'),
                'target_host': target_config.get('host', 'qase.io'),
                'target_enterprise': target_config.get('enterprise', False),
                'target_ssl': target_config.get('ssl', True),
                'mappings_file': options_config.get('mappings_file', 'mappings.json'),
                'preserve_ids': options_config.get('preserve_ids', False),
                'skip_projects': options_config.get('skip_projects') or [],
                'only_projects': options_config.get('only_projects') or [],
                'resume': options_config.get('resume', False),
            }
        except Exception as e:
            logger.warning(f"Could not load config.json: {e}. Using command-line arguments only.")
    
    parser = argparse.ArgumentParser(
        description='Migrate Qase workspace from source to target (reads from config.json by default)'
    )
    
    parser.add_argument('--source-token', default=config_defaults.get('source_token'),
                       help='Source workspace API token')
    parser.add_argument('--source-host', default=config_defaults.get('source_host', 'qase.io'),
                       help='Source workspace host (default: qase.io)')
    parser.add_argument('--source-enterprise', action='store_true',
                       help='Source is enterprise instance')
    parser.add_argument('--source-ssl', action='store_true', 
                       help='Use SSL for source')
    
    parser.add_argument('--target-token', default=config_defaults.get('target_token'),
                       help='Target workspace API token')
    parser.add_argument('--target-host', default=config_defaults.get('target_host', 'qase.io'),
                       help='Target workspace host (default: qase.io)')
    parser.add_argument('--target-enterprise', action='store_true',
                       help='Target is enterprise instance')
    parser.add_argument('--target-ssl', action='store_true',
                       help='Use SSL for target')
    
    parser.add_argument('--mappings-file', default=config_defaults.get('mappings_file', 'mappings.json'),
                       help='File to save/load ID mappings (default: mappings.json)')
    parser.add_argument('--preserve-ids', action='store_true',
                       help='Preserve entity IDs when possible')
    parser.add_argument('--skip-projects', nargs='+',
                       default=config_defaults.get('skip_projects'),
                       help='List of project codes to skip')
    parser.add_argument('--only-projects', nargs='+',
                       default=config_defaults.get('only_projects'),
                       help='List of project codes to migrate (only these)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume migration from saved mappings')
    
    args = parser.parse_args()
    
    if config_defaults:
        if not args.source_enterprise:
            args.source_enterprise = config_defaults.get('source_enterprise', False)
        if not args.source_ssl:
            args.source_ssl = config_defaults.get('source_ssl', True)
        if not args.target_enterprise:
            args.target_enterprise = config_defaults.get('target_enterprise', False)
        if not args.target_ssl:
            args.target_ssl = config_defaults.get('target_ssl', True)
        if not args.preserve_ids:
            args.preserve_ids = config_defaults.get('preserve_ids', False)
        if not args.resume:
            args.resume = config_defaults.get('resume', False)
    
    if not args.source_token:
        parser.error("--source-token is required (or provide config.json with source.api_token)")
    if not args.target_token:
        parser.error("--target-token is required (or provide config.json with target.api_token)")
    
    return args


def main():
    """Main migration function."""
    args = parse_args()
    
    # Load full config for user/group migration options
    config_path = 'config.json'
    config = {}
    if Path(config_path).exists():
        try:
            config = load_config(config_path)
        except Exception as e:
            logger.warning(f"Could not load config.json: {e}")
    
    logger.info("="*60)
    logger.info("QASE WORKSPACE MIGRATION")
    logger.info("="*60)
    logger.info(f"Source: {args.source_host}")
    logger.info(f"Target: {args.target_host}")
    logger.info("="*60)
    
    # Get SCIM configuration
    source_config = config.get('source', {})
    target_config = config.get('target', {})
    
    source_scim_token = source_config.get('scim_token')
    source_scim_host = source_config.get('scim_host')
    target_scim_token = target_config.get('scim_token')
    target_scim_host = target_config.get('scim_host')
    
    source_service = QaseService(
        api_token=args.source_token,
        host=args.source_host,
        ssl=args.source_ssl,
        enterprise=args.source_enterprise,
        scim_token=source_scim_token,
        scim_host=source_scim_host
    )
    
    target_service = QaseService(
        api_token=args.target_token,
        host=args.target_host,
        ssl=args.target_ssl,
        enterprise=args.target_enterprise,
        scim_token=target_scim_token,
        scim_host=target_scim_host
    )
    
    mappings = MigrationMappings()
    stats = MigrationStats()
    
    if args.resume:
        mappings.load_from_file(args.mappings_file)
    
    try:
        logger.info("\n" + "="*60)
        logger.info("STEP 1: Migrating Projects")
        logger.info("="*60)
        projects = migrate_projects(
            source_service, 
            target_service, 
            mappings, 
            stats,
            only_projects=args.only_projects if args.only_projects else None
        )
        
        if args.skip_projects:
            projects = [p for p in projects if p['source_code'] not in args.skip_projects]
        
        if not projects:
            logger.error("No projects to migrate!")
            return
        
        logger.info(f"Found {len(projects)} project(s) to migrate")
        
        mappings.save_to_file(args.mappings_file)
        
        # Check if user migration is enabled
        users_config = config.get('users', {})
        migrate_users_flag = users_config.get('migrate', False)
        
        if migrate_users_flag:
            logger.info("\n" + "="*60)
            logger.info("STEP 2: Migrating Users (Workspace Level)")
            logger.info("="*60)
            try:
                user_mapping = migrate_users(source_service, target_service, mappings, stats, config)
                # Ensure user_mapping keys are integers (in case it was loaded from JSON with string keys)
                if user_mapping:
                    first_key = next(iter(user_mapping.keys()), None)
                    if first_key is not None and isinstance(first_key, str):
                        user_mapping = {int(k): v for k, v in user_mapping.items()}
                mappings.save_to_file(args.mappings_file)
                
                logger.info("\n" + "="*60)
                logger.info("STEP 2.5: Migrating Groups (Workspace Level)")
                logger.info("="*60)
                try:
                    group_mapping = migrate_groups(source_service, target_service, user_mapping, mappings, stats, config)
                    mappings.save_to_file(args.mappings_file)
                except Exception as e:
                    logger.error(f"✗ Groups migration failed: {e}", exc_info=True)
                    mappings.save_to_file(args.mappings_file)
            except Exception as e:
                logger.error(f"✗ User migration failed: {e}", exc_info=True)
                # Create fallback mapping using default user ID
                logger.warning("Creating fallback user mapping using default user ID")
                from migration.extract.users import extract_users
                try:
                    source_users = extract_users(source_service)
                    default_user_id = users_config.get('default', 1)
                    user_mapping = {user.get('id'): default_user_id for user in source_users if user.get('id')}
                    mappings.users = user_mapping
                    stats.add_entity('users', len(source_users), len(user_mapping))
                except Exception as fallback_error:
                    logger.error(f"Failed to create fallback user mapping: {fallback_error}")
                    user_mapping = {}
                mappings.save_to_file(args.mappings_file)
        else:
            logger.info("\n" + "="*60)
            logger.info("STEP 2: User Migration (SKIPPED - users.migrate: false)")
            logger.info("="*60)
            logger.info("User migration is disabled. Skipping user and group migration entirely.")
            # Create empty user mapping - will use default user ID (1) for all references
            user_mapping = {}
            mappings.users = user_mapping
            mappings.save_to_file(args.mappings_file)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 3: Migrating Custom Fields (Workspace Level)")
        logger.info("="*60)
        custom_field_mapping = migrate_custom_fields(
            source_service, target_service,
            mappings, stats
        )
        mappings.save_to_file(args.mappings_file)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 4: Migrating Shared Parameters (Workspace Level)")
        logger.info("="*60)
        project_codes_list = [p['source_code'] for p in projects]
        shared_parameter_mapping = migrate_shared_parameters(
            source_service, target_service,
            project_codes_list, mappings, stats
        )
        mappings.save_to_file(args.mappings_file)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 5: Migrating Attachments (Workspace Level)")
        logger.info("="*60)
        attachment_mapping = migrate_attachments_workspace(
            source_service, target_service,
            projects, mappings, stats
        )
        mappings.save_to_file(args.mappings_file)
        
        for project in projects:
            project_code_source = project['source_code']
            project_code_target = project['target_code']
            
            logger.info("\n" + "="*60)
            logger.info(f"Migrating project: {project_code_source} -> {project_code_target}")
            logger.info("="*60)
            
            logger.info(f"\nMigrating milestones for {project_code_source}...")
            try:
                milestone_mapping = migrate_milestones(
                    source_service, target_service,
                    project_code_source, project_code_target,
                    mappings, stats
                )
                mappings.save_to_file(args.mappings_file)
            except Exception as e:
                logger.error(f"✗ Milestones migration failed: {e}", exc_info=True)
                milestone_mapping = {}
                mappings.save_to_file(args.mappings_file)
            
            logger.info(f"\nMigrating configurations for {project_code_source}...")
            try:
                config_group_mapping, config_mapping = migrate_configurations(
                    source_service, target_service,
                    project_code_source, project_code_target,
                    mappings, stats
                )
                mappings.save_to_file(args.mappings_file)
            except Exception as e:
                logger.error(f"✗ Configurations migration failed: {e}", exc_info=True)
                config_group_mapping = {}
                config_mapping = {}
                mappings.save_to_file(args.mappings_file)
            
            logger.info(f"\nMigrating environments for {project_code_source}...")
            try:
                environment_mapping = migrate_environments(
                    source_service, target_service,
                    project_code_source, project_code_target,
                    mappings, stats
                )
                mappings.save_to_file(args.mappings_file)
            except Exception as e:
                logger.error(f"✗ Environments migration failed: {e}", exc_info=True)
                environment_mapping = {}
                mappings.save_to_file(args.mappings_file)
            
            logger.info(f"\nMigrating shared steps for {project_code_source}...")
            try:
                shared_step_mapping = migrate_shared_steps(
                    source_service, target_service,
                    project_code_source, project_code_target,
                    mappings, stats
                )
                mappings.save_to_file(args.mappings_file)
            except Exception as e:
                logger.error(f"✗ Shared steps migration failed: {e}", exc_info=True)
                shared_step_mapping = {}
                mappings.save_to_file(args.mappings_file)
            
            logger.info(f"\nMigrating suites for {project_code_source}...")
            try:
                suite_mapping = migrate_suites(
                    source_service, target_service,
                    project_code_source, project_code_target,
                    mappings, stats
                )
                mappings.save_to_file(args.mappings_file)
            except Exception as e:
                logger.error(f"✗ Suites migration failed: {e}", exc_info=True)
                suite_mapping = {}
                mappings.save_to_file(args.mappings_file)
            
            logger.info(f"\nMigrating test cases for {project_code_source}...")
            try:
                case_mapping = migrate_cases(
                    source_service, target_service,
                    project_code_source, project_code_target,
                    suite_mapping,
                    custom_field_mapping,
                    milestone_mapping,
                    shared_step_mapping,
                    shared_parameter_mapping,
                    user_mapping,
                    mappings,
                    stats,
                    preserve_ids=args.preserve_ids
                )
                mappings.save_to_file(args.mappings_file)
            except Exception as e:
                logger.error(f"✗ Test cases migration failed: {e}", exc_info=True)
                case_mapping = {}
                mappings.save_to_file(args.mappings_file)
            
            logger.info(f"\nMigrating test plans for {project_code_source}...")
            try:
                plan_mapping = migrate_plans(
                    source_service, target_service,
                    project_code_source, project_code_target,
                    case_mapping,
                    mappings,
                    stats
                )
                mappings.save_to_file(args.mappings_file)
            except Exception as e:
                logger.error(f"✗ Test plans migration failed: {e}", exc_info=True)
                plan_mapping = {}
                mappings.save_to_file(args.mappings_file)
            
            logger.info(f"\nMigrating test runs for {project_code_source}...")
            try:
                run_mapping = migrate_runs(
                    source_service, target_service,
                    project_code_source, project_code_target,
                    case_mapping,
                    config_mapping,
                    milestone_mapping,
                    plan_mapping,
                    user_mapping,
                    mappings,
                    stats
                )
                mappings.save_to_file(args.mappings_file)
            except Exception as e:
                logger.error(f"✗ Test runs migration failed: {e}", exc_info=True)
                run_mapping = {}
                mappings.save_to_file(args.mappings_file)
            
            logger.info(f"\nMigrating test results for {project_code_source}...")
            try:
                migrate_results(
                    source_service, target_service,
                    project_code_source, project_code_target,
                    run_mapping,
                    case_mapping,
                    mappings,
                    stats,
                    user_mapping
                )
                mappings.save_to_file(args.mappings_file)
            except Exception as e:
                logger.error(f"✗ Test results migration failed: {e}", exc_info=True)
                mappings.save_to_file(args.mappings_file)
            
            logger.info(f"\nCompleted migration for project {project_code_source}")
            logger.info(f"Summary for {project_code_source}:")
            project_stats = {}
            for entity_type in ['milestones', 'configurations', 'shared_steps', 'suites', 'cases', 'plans', 'runs', 'results']:
                if hasattr(stats, 'entities_created') and entity_type in stats.entities_created:
                    created = stats.entities_created.get(entity_type, 0)
                    processed = stats.entities_processed.get(entity_type, 0)
                    project_stats[entity_type] = f"{created}/{processed}"
            for entity_type, count in project_stats.items():
                logger.info(f"  {entity_type}: {count}")
        
        stats.print_summary()
        
        mappings.save_to_file(args.mappings_file)
        logger.info(f"\nMigration complete! Mappings saved to {args.mappings_file}")
        
    except KeyboardInterrupt:
        logger.warning("\nMigration interrupted by user")
        mappings.save_to_file(args.mappings_file)
        logger.info(f"Mappings saved to {args.mappings_file}. Use --resume to continue.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nMigration failed with error: {e}", exc_info=True)
        mappings.save_to_file(args.mappings_file)
        logger.info(f"Mappings saved to {args.mappings_file}. Use --resume to continue.")
        sys.exit(1)


if __name__ == '__main__':
    main()
