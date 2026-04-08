# Qase Workspace Migration Tool

A Python tool for migrating **supported** Qase workspace data from a source workspace to a target workspace, preserving structure, links, attachments, and relationships **within the limits of the public API** (see [included scope](#what-this-tool-migrates-included-scope) and [limitations](#what-is-not-included-known-limitations)).

Connection settings are **host-driven**: set `host` (and optionally `scim_host`) per side in `config.json`. A common pattern is **Qase Cloud → custom-domain** workspace: source `host` is `qase.io`, target `host` is your deployment domain.

## Features

- ✅ **Core test data**: Migrates projects and the main test-artifact graph (suites, cases, plans, runs, results) plus supporting entities where the public API allows (see below)
- ✅ **Preserves structure**: Hierarchical relationships (suites, milestones) within what is migrated
- ✅ **Preserves links**: Relationships between migrated entities (e.g. cases to suites, runs to cases) within API limits
- ✅ **Attachments**: Migrates attachments when referenced; see limitations for edge cases
- ✅ **ID mapping**: Optionally preserves entity IDs or generates new ones (`options.preserve_ids` in `config.json`)
- ✅ **Resume**: Can resume interrupted migrations from saved mappings (`options.resume`)
- ✅ **Resilience**: Retry logic for rate limits; detailed logging and statistics
- ✅ **Case list/create batching**: Test cases are read and created in batches of **20** against the API (works consistently on cloud and custom-domain instances)

## What this tool migrates (included scope)

The orchestrator in `migrate_workspace.py` is built to migrate the following **when exposed and writable through the public API**:

| Area | What the script targets |
|------|-------------------------|
| **Projects** | Projects from source to target (with filters in `config.json` → `options.only_projects`, `options.skip_projects`) |
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
| **Unlinked results** | Results from **automated reporters** or runs whose **cases were deleted** cannot be tied back to the original case or suite through the API. They appear as generic entries (e.g. "Automated Test 123") **without suite structure**. |
| **Parameterized tests results** | The public API references the **case ID only**, not each **parameter variation**. Migrated results are all attributed to a **single variation** of the case rather than split per variation. |
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

### How to run

1. Copy `example_config.json` to `config.json` in the directory from which you will run the tool.
2. Edit `config.json` (see [Configuration reference](#configuration-reference) below).
3. From that directory:

```bash
python migrate_workspace.py
```

The script loads **`./config.json`** automatically. There is no path option for the config file; use a working directory per environment if you need multiple configs.

### Configuration reference

Top-level JSON shape:

```json
{
  "source": { },
  "target": { },
  "users": { },
  "groups": { },
  "options": { }
}
```

`example_config.json` is the template; every key below is explained in the same terms you should use in `config.json`.

---

#### `source` and `target`

Each workspace has its own object. The API base URL is derived only from `host`.

| Key | Required | Description |
|-----|----------|-------------|
| `api_token` | Yes | REST API token for that workspace (Settings → API Tokens in Qase). |
| `host` | No | Domain for the REST API. Default **`qase.io`** (Qase Cloud). For a custom deployment, set this to your Qase hostname (e.g. `company.qase.io`). See [How `host` builds the API URL](#how-host-builds-the-api-url). |
| `ssl` | No | If **`true`** (default), clients use `https://`. Set **`false`** only for special local or non-TLS setups. |
| `scim_token` | When migrating users | Required on **both** sides if `users.migrate` is **`true`**. Used for listing/creating users and (with groups) group membership. |
| `scim_host` | No | SCIM hostname if it must differ from the default implied by `host`. See [How SCIM host is chosen](#how-scim-host-is-chosen). |

Typical **cloud → custom domain** setup: `source.host` is `qase.io`, `target.host` is your deployment hostname. Tokens and optional `scim_host` differ per side as needed.

---

#### `users`

Controls workspace user migration and how authorship fields map when users differ between workspaces.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `migrate` | boolean | `false` | If **`true`**, runs user migration (SCIM on source and target). If **`false`**, user and **group** migration are skipped; the tool does not build a user map from SCIM. |
| `create` | boolean | `false` | If **`true`**, users that exist on the source but not on the target (matched by email) may be **created** on the target via SCIM after [interactive confirmation](#user-creation-confirmation). If **`false`**, only existing target users are mapped. |
| `inactive` | boolean | `false` | If **`true`**, inactive source users may be created on the target when `create` is **`true`**. If **`false`**, inactive users are skipped for creation. |
| `default` | number | `1` | Target user **ID** used when there is no mapping for a source user (fallback for references in migrated data). |
| `skip_creation_confirm` | boolean | `false` | If **`true`**, skips the interactive prompt before creating users (for automation only). See [User creation confirmation](#user-creation-confirmation). |

When `migrate` is **`true`**, you must supply **`scim_token`** under both `source` and `target`.

##### User creation confirmation

When **`users.migrate`** and **`users.create`** are **`true`** and there is at least one source user who would be created on the target (email not already present, and not excluded by **`users.inactive`**), the tool:

1. Loads **all existing users** on the target (via SCIM, with a fallback listing if SCIM listing fails but authors were loaded earlier).
2. Prints them under **EXISTING USERS ON TARGET WORKSPACE**.
3. Prints **USERS TO BE CREATED ON TARGET** (email, display name, role, and inactive marker when relevant).
4. Asks you to type **`yes`** exactly (case-sensitive) to proceed with SCIM user creation.

If you enter anything else, creation is skipped and unmapped users fall back to **`users.default`**, same as if **`users.create`** were **`false`** for those accounts.

If **stdin is not a terminal** (for example in some CI jobs), the tool will **not** create users without confirmation and logs an error unless you set **`users.skip_creation_confirm`** to **`true`** (use only when you accept creating users with no prompt).

If there are **no** users to create, the prompt is skipped.

---

#### `groups`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `create` | boolean | `false` | If **`true`**, groups are read from the source (via SCIM), created on the target when missing, and members are added using the user mapping from user migration. |

Group migration runs **only when `users.migrate` is `true`** (it is part of the same step sequence). If `users.migrate` is **`false`**, the `groups` block has no effect.

---

#### `options`

General migration behavior, project selection, parallelism, and tracing.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `preserve_ids` | boolean | `false` | If **`true`**, the tool tries to keep original numeric IDs where the API allows (within int32; see [ID preservation](#id-preservation)). |
| `mappings_file` | string | `"mappings.json"` | Path to the JSON file storing source→target ID mappings. Used on every run and required for [resume](#resume). |
| `only_projects` | array of strings | `[]` | If non-empty, only project **codes** in this list are considered (others are not migrated). Empty means all projects from the source (subject to `skip_projects`). |
| `skip_projects` | array of strings | `[]` | Project **codes** to exclude after the project list is built. |
| `resume` | boolean | `false` | If **`true`**, load existing mappings from `mappings_file` before migrating so completed work is skipped. |
| `parallel_project_migration` | boolean | `true` | If **`true`** and more than one project is selected, projects may be migrated concurrently (see `max_parallel_projects`). If **`false`**, projects run one after another (more frequent saves to `mappings_file`). |
| `max_parallel_projects` | number | `4` | Maximum concurrent project workers when `parallel_project_migration` is **`true`**. |
| `migration_trace_file` | string | `"migration_trace.jsonl"` | JSONL path for structured trace events. To **disable** tracing, set this key to **`false`**, **`null`**, or **`""`** (empty string). |
| `migration_trace_full_payloads` | boolean | `false` | If **`true`**, trace events may include fuller payloads (larger files). |

---

### How `host` builds the API URL

Implemented in `qase_service.py`:

- If `host` is **`qase.io`**, the API base is `https://api.qase.io/v1` and `https://api.qase.io/v2`.
- For **any other** `host`, the base is `https://api-{host}/v1` and `https://api-{host}/v2` (hyphen between `api` and your domain).

Use the hostname only (no `https://`).

### How SCIM host is chosen

If you omit `scim_host` on a side:

- When `host` is **`qase.io`**, SCIM defaults to **`app.qase.io`** (Qase Cloud).
- Otherwise SCIM defaults to the **same string as `host`** (common when API and SCIM share one hostname).

Set `scim_host` explicitly when SCIM lives on a different hostname than the rule above.

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

- **`mappings.json`** (or whatever you set in `options.mappings_file`): Source→target entity ID mappings; required for resume.
- **`migration.log`**: Detailed run log in the working directory.
- **Trace JSONL** (optional): If `options.migration_trace_file` is set to a non-empty path, structured events are appended there; omit or disable per [options](#options) to skip tracing.

## Important Notes

### Test case batch size

Listing and creating test cases uses a fixed batch size of **20** per request. This matches stricter rate limits on many deployments without varying batch size by environment.

### API Tokens

You need API tokens for both source and target workspaces. Get them from:
- Qase Cloud: Settings → API Tokens
- Qase Enterprise: Settings → API Tokens

### Rate Limiting

The script includes retry logic with exponential backoff for rate limiting. For large migrations:
- Qase Cloud: ~100 requests/minute
- Custom-domain / self-hosted deployments: limits are often configurable and higher

### ID preservation

- Enable with **`options.preserve_ids`: `true`** in `config.json`.
- IDs are kept only when they fit in int32 (≤ 2,147,483,647); larger source IDs are hashed to fit when preservation is on.

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

### Resume

- Set **`options.resume`: `true`** and keep the same **`options.mappings_file`** path so the tool reloads mappings and skips work already recorded.
- With parallel project migration, mappings are flushed after each project finishes; with it disabled, saves occur more often during the run.

## Architecture

The migration tool consists of:

- `qase_service.py`: API v1/v2 client initialization; builds base URLs from `host` and configures SCIM
- `migration/utils.py` and `migration/utils/`: Mappings, stats, retries, SCIM client, etc.
- `migration/extract/` and `migration/create/`: Read from source and write to target per entity type
- `migrate_workspace.py`: Main orchestrator script

## License

This tool is provided as-is for migrating Qase workspaces.

## Support

For issues or questions:
- Check the migration.log file for detailed error messages
- Review QASE_API_DOCUMENTATION.md for API details
- Consult Qase API documentation: https://developers.qase.io
