# Qase Workspace Migration Tool

A comprehensive Python script for migrating all content from one Qase workspace to another, preserving structure, links, attachments, and relationships.

## Features

- ✅ **Complete Migration**: Migrates projects, suites, test cases, runs, results, attachments, milestones, configurations, shared steps, custom fields, and users
- ✅ **Preserves Structure**: Maintains hierarchical relationships (suites, milestones)
- ✅ **Preserves Links**: Maintains relationships between entities (cases to suites, runs to cases, etc.)
- ✅ **Preserves Attachments**: Migrates attachments and maintains references
- ✅ **ID Mapping**: Optionally preserves entity IDs or generates new ones
- ✅ **Resume Capability**: Can resume interrupted migrations
- ✅ **Error Handling**: Robust error handling with retry logic
- ✅ **Progress Tracking**: Detailed logging and statistics

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

Migrate all content from source to target workspace:

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

The script migrates entities in the following order to respect dependencies:

1. **Projects** - Foundation entities
2. **Users** - Map users by email
3. **Custom Fields** - Per project
4. **Milestones** - Per project (hierarchical)
5. **Configurations** - Per project (groups and configs)
6. **Shared Steps** - Per project
7. **Suites** - Per project (hierarchical)
8. **Test Cases** - Per project
9. **Test Runs** - Per project
10. **Test Results** - Per run

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

### Shared Steps

- Cases with shared steps use raw HTTP API (bypasses SDK validation)
- Shared steps must be migrated before cases that reference them

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
