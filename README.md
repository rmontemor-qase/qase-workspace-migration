# Qase Workspace Migration Tool

A Python tool for migrating **supported** Qase workspace data from a source workspace to a target workspace, preserving structure, links, attachments, and relationships **within the limits of the public API** (see [included scope](#what-this-tool-migrates-included-scope) and [limitations](#what-is-not-included-known-limitations)).

## Features

- ✅ **Core test data**: Migrates projects and the main test-artifact graph (suites, cases, plans, runs, results) plus supporting entities where the public API allows (see below)
- ✅ **Preserves structure**: Hierarchical relationships (suites, milestones) within what is migrated
- ✅ **Preserves links**: Relationships between migrated entities (e.g. cases to suites, runs to cases) within API limits
- ✅ **Attachments**: Migrates attachments when referenced; see limitations for edge cases
- ✅ **ID mapping**: Optionally preserves entity IDs or generates new ones (`--preserve-ids`)
- ✅ **Resume**: Can resume interrupted migrations from saved mappings
- ✅ **Resilience**: Retry logic for rate limits; detailed logging and statistics

## What this tool migrates (included scope)

The orchestrator in `migrate_workspace.py` is built to migrate the following **when exposed and writable through the public API**:

| Area | What the script targets |
|------|-------------------------|
| **Projects** | Projects from source to target (with filters such as `--only-projects`, `--skip-projects`, `--only-active`) |
| **People & access helpers** | Optional user migration and group mapping (config-driven; see `example_config.json`) |
| **Fields & parameters** | Custom fields and shared parameters (migrated at workspace level in the orchestrator, scoped to selected projects) |
| **Attachments** | Workspace-level attachment pass, then per-project use when migrating defects |
| **Planning & structure** | Milestones (hierarchical), configuration groups and configurations, environments |
| **Shared steps** | Project-scoped shared steps **where the API supports create/read** (workspace-level shared step libraries are not covered; see limitations) |
| **Tests** | Suites (hierarchical), test cases, test plans, test runs, test results |
| **Defects** | Defect records are migrated in a **limited** way (see limitations: linking to runs/results may be incomplete) |

Anything not listed above, or called out in [What is not included](#what-is-not-included-known-limitations), is outside the intended scope of this tool or is blocked by current public API capabilities.

## What is not included (known limitations)

The following gaps reflect **current public API limitations** and product areas this script does not migrate end-to-end. Treat this as the source of truth for planning a workspace move; the script may still *attempt* some related calls where code exists, but you should not rely on full parity for the items below.

### Workspace level

| Item | Notes |
|------|--------|
| **Roles** | Workspace role definitions and assignments are not migrated via the public API. |
| **Shared steps** | Workspace-level shared step libraries / assets are not covered. |
| **Dashboards** | Dashboards are not migrated. |
| **Queries** | Saved queries are not migrated. |

### Project level

| Item | Notes |
|------|--------|
| **Test cases in review** | Cases in review workflow states are not handled as a complete migration path. |
| **Traceability reports** | Traceability report configuration and data are not migrated. |
| **Aiden Test Converter converted tests** | Tests produced or managed by Aiden Test Converter are not migrated by this tool. |
| **Aiden authorization settings** | Aiden-related authorization settings are not migrated. |
| **Requirements** | Requirements (and their links) are not migrated. |
| **Project settings** | General project settings beyond what the API exposes for the entities we migrate are not replicated. |

### Additional limitations

| Item | Notes |
|------|--------|
| **Defects** | You can create and resolve defects in principle, but **linking defects to runs and results** is not fully supported through the public API in a way this migration guarantees. Plan for manual verification or follow-up linking in the target workspace. |
| **Integrations** | Integrations (e.g. Jira) are not migrated as a turnkey handoff. During cutover you may need a **separate process** to re-point Jira (or other tools) from the old workspace to the new one and to preserve or recreate integration metadata the API does not carry over. |

## Installation

### Using Virtual Environment (Recommended)

1. Create virtual environment:
```bash
python -m venv venv
```

2. Activate virtual environment:
- **Windows:** `venv\Scripts\activate`
- **Linux/Mac:** `source venv/bin/activate`

3. Install dependencies:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Direct Installation

1. Install Python 3.8 or higher

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Using Configuration File (Recommended)

Create a `config.json` file (see `example_config.json` for template) and run:

```bash
python migrate_workspace.py --config config.json
```

### Basic Migration (Command Line)

Migrate supported project data from source to target workspace:

```bash
python migrate_workspace.py \
  --source-token YOUR_SOURCE_API_TOKEN \
  --target-token YOUR_TARGET_API_TOKEN
```

### Enterprise Instances

For enterprise instances with custom domains:

```bash
python migrate_workspace.py \
  --source-token YOUR_SOURCE_API_TOKEN \
  --source-host your-enterprise-domain.com \
  --source-enterprise \
  --target-token YOUR_TARGET_API_TOKEN \
  --target-host your-target-domain.com \
  --target-enterprise
```

### Selective Migration

Migrate only specific projects:

```bash
python migrate_workspace.py \
  --source-token YOUR_SOURCE_API_TOKEN \
  --target-token YOUR_TARGET_API_TOKEN \
  --only-projects PROJ1 PROJ2 PROJ3
```

Skip specific projects:

```bash
python migrate_workspace.py \
  --source-token YOUR_SOURCE_API_TOKEN \
  --target-token YOUR_TARGET_API_TOKEN \
  --skip-projects PROJ1 PROJ2
```

### Resume Interrupted Migration

If migration is interrupted, resume from saved mappings:

```bash
python migrate_workspace.py \
  --source-token YOUR_SOURCE_API_TOKEN \
  --target-token YOUR_TARGET_API_TOKEN \
  --resume
```

### Preserve Entity IDs

Preserve original entity IDs when possible (within int32 range):

```bash
python migrate_workspace.py \
  --source-token YOUR_SOURCE_API_TOKEN \
  --target-token YOUR_TARGET_API_TOKEN \
  --preserve-ids
```

### Migrate Only Active Projects

Migrate only active (non-archived) projects:

```bash
python migrate_workspace.py \
  --source-token YOUR_SOURCE_API_TOKEN \
  --target-token YOUR_TARGET_API_TOKEN \
  --only-active
```

## Command Line Arguments

### Configuration
- `--config`: Path to configuration JSON file (alternative to command-line args)

### Source Workspace
- `--source-token`: Source workspace API token (required if not in config)
- `--source-host`: Source workspace host (default: qase.io)
- `--source-enterprise`: Source is enterprise instance
- `--source-ssl`: Use SSL for source (default: True)

### Target Workspace
- `--target-token`: Target workspace API token (required if not in config)
- `--target-host`: Target workspace host (default: qase.io)
- `--target-enterprise`: Target is enterprise instance
- `--target-ssl`: Use SSL for target (default: True)

### Migration Options
- `--mappings-file`: File to save/load ID mappings (default: mappings.json)
- `--preserve-ids`: Preserve entity IDs when possible
- `--skip-projects`: List of project codes to skip
- `--only-projects`: List of project codes to migrate (only these)
- `--only-active`: Migrate only active (non-archived) projects
- `--resume`: Resume migration from saved mappings

**Note:** Command-line arguments override values from the config file.

## Migration Order

Within [the included scope](#what-this-tool-migrates-included-scope), the script migrates entities in this order so dependencies stay consistent. Steps marked **(workspace)** run once for the migration; the rest run **per project**.

1. **Projects** — Foundation entities  
2. **Users** — Optional **(workspace)**; map users by email when enabled in config  
3. **Groups** — Optional **(workspace)**; after users when enabled  
4. **Custom fields** — **(workspace)** pass over selected projects  
5. **Shared parameters** — **(workspace)** pass over selected projects  
6. **Attachments** — **(workspace)** pass; attachments are also applied when migrating cases, results, and defects  
7. **Milestones** — Per project (hierarchical)  
8. **Configurations** — Per project (groups and configs)  
9. **Environments** — Per project  
10. **Shared steps** — Per project (subject to [workspace/API limits](#what-is-not-included-known-limitations))  
11. **Suites** — Per project (hierarchical)  
12. **Test cases** — Per project (cases in review and other excluded items still apply)  
13. **Test plans** — Per project  
14. **Test runs** — Per project  
15. **Test results** — Per run  
16. **Defects** — Per project (partial; see [defects limitation](#additional-limitations))  

Steps that are skipped via config (e.g. users) are omitted at runtime.

## Output Files

- `mappings.json`: Contains mappings between source and target entity IDs
- `migration.log`: Detailed migration log

## Important Notes

### API Tokens

You need API tokens for both source and target workspaces. Get them from:
- Qase Cloud: Settings → API Tokens
- Qase Enterprise: Settings → API Tokens

### Rate Limiting

The script includes retry logic with exponential backoff for rate limiting. For large migrations:
- Cloud instances: ~100 requests/minute
- Enterprise instances: Configurable, typically higher

### ID Preservation

- IDs are preserved only if they are within int32 range (≤ 2,147,483,647)
- Larger IDs are hashed to fit within the range
- Use `--preserve-ids` flag to enable ID preservation

### Attachments

- Attachments are migrated on-demand when referenced in cases/results
- Large attachments may take time to migrate
- Attachment references are preserved in text fields

### Shared steps

- At **project** scope, the script migrates shared steps before cases that reference them (implementation may use raw HTTP where the SDK is too strict).
- **Workspace-level** shared step libraries are **not** in scope; see [Workspace level](#workspace-level) under limitations.

## Troubleshooting

### Authentication Errors (401)

- Verify API tokens are correct
- Check token permissions in Qase settings

### Rate Limiting (429)

- The script automatically retries with exponential backoff
- For very large migrations, consider running during off-peak hours

### Missing Dependencies

- If a case references a suite that doesn't exist, it will be skipped
- Check migration.log for details on skipped entities

### Resume Migration

- If migration is interrupted, use `--resume` flag
- Mappings are saved after each entity type
- Already migrated entities are skipped

## Architecture

The migration tool consists of:

- `qase_service.py`: API client initialization and configuration
- `migration_utils.py`: Utility functions (mappings, stats, error handling, etc.)
- `migrators.py`: Individual migration functions for each entity type
- `migrate_workspace.py`: Main orchestrator script

## License

This tool is provided as-is for migrating Qase workspaces.

## Support

For issues or questions:
- Check the migration.log file for detailed error messages
- Review QASE_API_DOCUMENTATION.md for API details
- Consult Qase API documentation: https://developers.qase.io
