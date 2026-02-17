"""
Creation module - functions to create entities in target Qase workspace.
"""
from migration.create.projects import migrate_projects
from migration.create.users import migrate_users
from migration.create.custom_fields import migrate_custom_fields
from migration.create.milestones import migrate_milestones
from migration.create.configurations import migrate_configurations
from migration.create.shared_steps import migrate_shared_steps
from migration.create.suites import migrate_suites
from migration.create.cases import migrate_cases
from migration.create.runs import migrate_runs
from migration.create.results import migrate_results
from migration.create.attachments import migrate_attachments_workspace

__all__ = [
    'migrate_projects',
    'migrate_users',
    'migrate_custom_fields',
    'migrate_milestones',
    'migrate_configurations',
    'migrate_shared_steps',
    'migrate_suites',
    'migrate_cases',
    'migrate_runs',
    'migrate_results',
    'migrate_attachments_workspace',
]
