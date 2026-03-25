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
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Optional

from qase_service import QaseService
from migration.run_single_project import run_single_project_migration
from migration.utils import (
    MigrationMappings,
    MigrationStats,
    fork_mappings_for_parallel_project,
    merge_migration_stats,
    merge_parallel_project_into_main,
)
from migration.create import (
    migrate_projects,
    migrate_users,
    migrate_custom_fields,
    migrate_shared_parameters,
    migrate_attachments_workspace,
    migrate_groups,
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

    opts = config.get("options", {}) if config else {}
    trace_file_cfg = opts.get("migration_trace_file", "migration_trace.jsonl")
    if "migration_trace_file" in opts and opts["migration_trace_file"] in (False, None, ""):
        trace_file_cfg = None
    trace_full = bool(opts.get("migration_trace_full_payloads", False))
    if trace_file_cfg:
        from migration.trace_log import MigrationTrace

        mappings.trace = MigrationTrace(str(trace_file_cfg), full_payloads=trace_full)
        logger.info("Migration trace (JSONL): %s (full_payloads=%s)", trace_file_cfg, trace_full)
        mappings.trace.event(
            "migration_start",
            source_host=args.source_host,
            target_host=args.target_host,
            mappings_file=args.mappings_file,
            resume=args.resume,
        )
    
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

        parallel_projects = bool(opts.get("parallel_project_migration", True))
        try:
            max_parallel_projects = max(1, int(opts.get("max_parallel_projects", 4)))
        except (TypeError, ValueError):
            max_parallel_projects = 4
        max_parallel_projects = min(max_parallel_projects, max(1, len(projects)))

        source_kw: Dict[str, Any] = {
            "api_token": args.source_token,
            "host": args.source_host,
            "ssl": args.source_ssl,
            "enterprise": args.source_enterprise,
            "scim_token": source_scim_token,
            "scim_host": source_scim_host,
        }
        target_kw: Dict[str, Any] = {
            "api_token": args.target_token,
            "host": args.target_host,
            "ssl": args.target_ssl,
            "enterprise": args.target_enterprise,
            "scim_token": target_scim_token,
            "scim_host": target_scim_host,
        }

        def _run_parallel_project_worker(project: Dict[str, Any]):
            src = QaseService(**source_kw)
            tgt = QaseService(**target_kw)
            wm = fork_mappings_for_parallel_project(mappings)
            wstats = MigrationStats()
            run_single_project_migration(
                project,
                src,
                tgt,
                wm,
                wstats,
                user_mapping,
                custom_field_mapping,
                shared_parameter_mapping,
                args.preserve_ids,
                mappings_file=None,
            )
            return project["source_code"], wm, wstats

        if parallel_projects and len(projects) > 1:
            logger.info(
                "Running per-project migration in parallel (%s workers). "
                "Intermediate mappings saves only after each project completes; "
                "set options.parallel_project_migration false for step-by-step saves.",
                max_parallel_projects,
            )
            futures = {}
            with ThreadPoolExecutor(max_workers=max_parallel_projects) as pool:
                for project in projects:
                    futures[pool.submit(_run_parallel_project_worker, project)] = project
                for fut in as_completed(futures):
                    proj = futures[fut]
                    try:
                        psrc, wm, wst = fut.result()
                        merge_parallel_project_into_main(mappings, wm, psrc)
                        merge_migration_stats(stats, wst)
                        mappings.save_to_file(args.mappings_file)
                    except Exception as e:
                        logger.error(
                            "✗ Project %s migration failed: %s",
                            proj.get("source_code"),
                            e,
                            exc_info=True,
                        )
                        mappings.save_to_file(args.mappings_file)
        else:
            if len(projects) > 1 and not parallel_projects:
                logger.info("Parallel project migration disabled (options.parallel_project_migration).")
            for project in projects:
                run_single_project_migration(
                    project,
                    source_service,
                    target_service,
                    mappings,
                    stats,
                    user_mapping,
                    custom_field_mapping,
                    shared_parameter_mapping,
                    args.preserve_ids,
                    mappings_file=args.mappings_file,
                )
        
        stats.print_summary()
        
        mappings.save_to_file(args.mappings_file)
        logger.info(f"\nMigration complete! Mappings saved to {args.mappings_file}")
        if getattr(mappings, "trace", None):
            mappings.trace.event("migration_complete", mappings_file=args.mappings_file)
        
    except KeyboardInterrupt:
        logger.warning("\nMigration interrupted by user")
        mappings.save_to_file(args.mappings_file)
        logger.info(f"Mappings saved to {args.mappings_file}. Use --resume to continue.")
        if getattr(mappings, "trace", None):
            mappings.trace.event("migration_interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nMigration failed with error: {e}", exc_info=True)
        mappings.save_to_file(args.mappings_file)
        logger.info(f"Mappings saved to {args.mappings_file}. Use --resume to continue.")
        if getattr(mappings, "trace", None):
            mappings.trace.event("migration_failed", error=str(e))
        sys.exit(1)
    finally:
        if getattr(mappings, "trace", None):
            try:
                mappings.trace.close()
            except Exception:
                pass
            mappings.trace = None


if __name__ == '__main__':
    main()
