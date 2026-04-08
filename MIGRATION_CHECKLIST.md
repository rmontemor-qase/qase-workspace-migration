# Migration checklist (code-derived)

This document lists **everything the repository actually migrates or processes**, based on `migrate_workspace.py` and the `migration/` package. Use it as a verification checklist for a run. Items are **not** guaranteed to succeed on every Qase instance (API limits, permissions, and data shape still apply).

**Entry point:** `migrate_workspace.py` → `migration/create/*` (create) + `migration/extract/*` (read source) + `migration/transform/*` (text/hash rewriting) + `migration/utils.py` (`MigrationMappings`, `QaseRawApiClient`, retries).

---

## 1. Orchestrator sequence (always runs unless noted)

Check these in order; each step depends on earlier mappings where applicable.

- [ ] **1.1 Projects** — `migrate_projects`
- [ ] **1.2 Users** — `migrate_users` only if `config.users.migrate === true` (requires source + target SCIM tokens)
- [ ] **1.3 Groups** — `migrate_groups` only if users migration ran, target has `scim_client`, and `config.groups.create === true`
- [ ] **1.4 Custom fields** — `migrate_custom_fields` (workspace-wide list from source)
- [ ] **1.5 Shared parameters** — `migrate_shared_parameters` (filtered by migrated project codes)
- [ ] **1.6 Attachments (workspace pass)** — `migrate_attachments_workspace` for all migrated projects
- [ ] **Per migrated project**, in order:
  - [ ] **1.7 Milestones** — `migrate_milestones`
  - [ ] **1.8 Configurations** — `migrate_configurations` (groups + configs)
  - [ ] **1.9 Environments** — `migrate_environments`
  - [ ] **1.10 Shared steps** — `migrate_shared_steps`
  - [ ] **1.11 Suites** — `migrate_suites`
  - [ ] **1.12 Test cases** — `migrate_cases`
  - [ ] **1.13 Test plans** — `migrate_plans`
  - [ ] **1.14 Test runs** — `migrate_runs`
  - [ ] **1.15 Test results** — `migrate_results` (also completes runs flagged during run migration)
  - [ ] **1.16 Defects** — `migrate_defects`

CLI filters applied before/at project list: `--only-projects`, `--skip-projects` (no `only_active` / archived filter in `migrate_workspace.py` as of this audit).

---

## 2. Projects (`migration/create/projects.py`, `migration/extract/projects.py`)

**Source:** paginated `get_projects` (limit 100).

**Create / reuse on target:**

- [ ] **2.1** `title`
- [ ] **2.2** `code`
- [ ] **2.3** `description` (default `''`)
- [ ] **2.4** `settings` (default `{'runs': {'auto_complete': False}}` if missing)
- [ ] **2.5** `access` (default `'all'`)

**Behavior:**

- [ ] **2.6** If project code already exists on target, mapping reuses existing project (no duplicate create).
- [ ] **2.7** `mappings.projects[source_code] → target_code`

---

## 3. Users (`migration/create/users.py`, `migration/extract/users.py`)

**Only if** `users.migrate` is true **and** SCIM tokens are set on both sides.

**Source:** `get_authors` with `type="user"` (paginated).

**Mapping logic (not “copy user object”):**

- [ ] **3.1** Match by **email** → target user id (from SCIM `get_all_users` or `AuthorsApi.get_authors` on target)
- [ ] **3.2** Optional **create** missing users on target via SCIM (`users.create`), with `users.inactive` controlling inactive users
- [ ] **3.3** Unmapped users → `users.default` id (default `1`)
- [ ] **3.4** `mappings.users` — source numeric user id → target user id
- [ ] **3.5** `mappings.user_uuid_mapping` — source `uuid` / `author_uuid` → target user id (when present)
- [ ] **3.6** `mappings.user_email_mapping` — email → target user id

**Fields read from source for create:** email, name (split into first/last), `is_active`, `role` (passed as `role_title` to SCIM).

---

## 4. Groups (`migration/create/groups.py`, `migration/extract/groups.py`)

**Only if** `groups.create` is true **and** target `scim_client` exists **and** user migration path ran.

**Source:** SCIM `get_all_groups` on **source** (no REST groups list).

**On target:**

- [ ] **4.1** Create or match group by `displayName` (case-insensitive)
- [ ] **4.2** `mappings.groups` — source group id → target group id (in-memory; **not** included in `MigrationMappings.save_to_file()` payload today)

**Members:**

- [ ] **4.3** Source `members` resolved through `user_mapping` → `add_users_to_group` on target SCIM (numeric member ids; string UUID members skipped)

---

## 5. Custom fields (`migration/create/custom_fields.py`, `migration/extract/custom_fields.py`)

**Source:** paginated `get_custom_fields` (workspace).

**Deduplication on target:** by normalized field **title** (case-insensitive).

**On create, fields copied:**

- [ ] **5.1** `title`
- [ ] **5.2** `entity` — `case` / `run` / `defect` (string or int mapped)
- [ ] **5.3** `type` — string names mapped to API ints (`string`, `number`, `text`, `selectbox`, `checkbox`, `radio`, `multiselect`, `url`, `user`, `date`)
- [ ] **5.4** `value` — option list → `CustomFieldCreateValueInner` (duplicate titles get numeric suffixes)
- [ ] **5.5** `is_filterable`, `is_visible`, `is_required` (defaults true/true/false)
- [ ] **5.6** `is_enabled_for_all_projects` and `projects_codes` (when not enabled for all)
- [ ] **5.7** `default_value`
- [ ] **5.8** `mappings.custom_fields[source_field_id] → target_field_id`

---

## 6. Shared parameters (`migration/create/shared_parameters.py`, `migration/extract/shared_parameters.py`)

**Source:** raw GET `{host}/shared_parameter` with pagination; optional `filters[project_codes][n]` for selected project codes.

**Deduplication on target:** by normalized **title**.

**Create payload:**

- [ ] **6.1** `type`
- [ ] **6.2** `title`
- [ ] **6.3** `is_enabled_for_all_projects`
- [ ] **6.4** `parameters` — list of `{ title, values[] }` built from source `parameters` (skips empty parameter sets)
- [ ] **6.5** `project_codes` when not enabled for all
- [ ] **6.6** UUIDs normalized via `convert_uuids_to_strings`
- [ ] **6.7** `mappings.shared_parameters[source_id] → target_id` (string keys in mapping usage elsewhere)

---

## 7. Attachments — workspace pass (`migration/create/attachments.py`, `migration/extract/attachments.py`)

**Discovery (per source project):**

- [ ] **7.1** From **cases:** attachment list, `description` / `preconditions` / `postconditions`, custom fields text, step attachments + step `action` / `expected_result` / `data`
- [ ] **7.2** From **results:** per run, result `attachments`, `comment`, step `action` / `expected_result` / `comment`
- [ ] **7.3** From **defects:** defect attachments, `actual_result`

**Process:**

- [ ] **7.4** Global **dedupe** by hash across projects
- [ ] **7.5** Skip if hash already in target case attachments (scan target cases per project) or `get_attachment` succeeds on target
- [ ] **7.6** Download from source (`AttachmentsApi.get_attachment`, optional URL fetch with token); upload via `upload_attachment` to a **target project** that referenced the hash on source
- [ ] **7.7** `mappings.attachments[source_project_code][source_hash] → target_hash` (multiple key casings for lookup)
- [ ] **7.8** `mappings.target_workspace_hash` — parsed from upload URL `/public/team/{hash}/` when first seen

**Note:** Result/defect attachment **content** is collected for migration; **result** objects themselves only carry over a subset of fields (see §15).

---

## 8. Milestones (`migration/create/milestones.py`, `migration/extract/milestones.py`)

**Source:** paginated `get_milestones`.

**Create (per milestone node):**

- [ ] **8.1** `title`
- [ ] **8.2** `description`
- [ ] **8.3** `status` (default `active`)
- [ ] **8.4** `due_date` via `format_date`

**Tree:**

- [ ] **8.5** Roots: `parent_id` falsy; children processed recursively **after** parent exists

**Code caveat:** `MilestoneCreate` does **not** include `parent_id`; hierarchy relies on creation order only (sub-milestones may appear flat on target depending on API behavior).

- [ ] **8.6** `mappings.milestones[source_project][source_id] → target_id`

---

## 9. Configurations (`migration/create/configurations.py`, `migration/extract/configurations.py`)

**Source:** `get_configurations` for project (groups with nested configs).

**Per group:**

- [ ] **9.1** Create configuration **group** with `title`
- [ ] **9.2** For each config in `configs` / `configurations` / `entities`: create **configuration** with `title` + `group_id`

- [ ] **9.3** `mappings.configuration_groups[source_project][source_group_id] → target_group_id`
- [ ] **9.4** `mappings.configurations[source_project][source_config_id] → target_config_id`

---

## 10. Environments (`migration/create/environments.py`, `migration/extract/environments.py`)

**Create:**

- [ ] **10.1** `title` (required)
- [ ] **10.2** `slug` (or `''`)
- [ ] **10.3** `host` (or `''`)
- [ ] **10.4** `description` if present

- [ ] **10.5** `mappings.environments[source_project][source_id] → target_id`

---

## 11. Shared steps (`migration/create/shared_steps.py`, `migration/extract/shared_steps.py`)

**Source:** paginated `get_shared_steps` (entries with `hash` only).

**Create:**

- [ ] **11.1** `title`
- [ ] **11.2** Steps: each with `action` (non-empty; default `'No action'`), `expected_result` or `expected`

- [ ] **11.3** `mappings.shared_steps[source_project][source_hash] → target_hash`

---

## 12. Suites (`migration/create/suites.py`, `migration/extract/suites.py`)

**Create (recursive tree):**

- [ ] **12.1** `title` (fallback `name` or `"Suite {id}"`)
- [ ] **12.2** `description`
- [ ] **12.3** `preconditions`
- [ ] **12.4** `parent_id` → mapped target parent id

- [ ] **12.5** `mappings.suites[source_project][source_suite_id] → target_suite_id`

---

## 13. Test cases (`migration/create/cases.py`, `migration/extract/cases.py`)

**Source:** paginated `get_cases`; if steps missing, `get_case` per id.

**Transform → bulk create** via `QaseRawApiClient.create_cases_bulk` (not SDK single-case create).

**Scalar / list fields sent (when present after transform):**

- [ ] **13.1** `id` — optional; `preserve_or_hash_id` when `preserve_ids` true (`migrate_cases` flag from CLI/config)
- [ ] **13.2** `title`, `description`, `preconditions`, `postconditions`
- [ ] **13.3** `severity`, `priority`, `type`, `behavior`, `automation`, `status`
- [ ] **13.4** `tags` — normalized to list of strings (from dict `title`/`name` or strings)
- [ ] **13.5** `created_at`, `updated_at` — ISO strings if available
- [ ] **13.6** `author_id` — from `member_id` / `created_by` / `author_id` via `mappings.get_user_id` (default `1`)
- [ ] **13.7** `milestone_id` — mapped
- [ ] **13.8** `suite_id` — mapped when source had suite
- [ ] **13.9** `is_flaky`
- [ ] **13.10** `params` — dict copied when non-empty
- [ ] **13.11** `parameters` — rebuilt: shared refs → `shared_id` mapped; `single` / `group` structures preserved
- [ ] **13.12** `steps` — either `{shared: target_hash}` or inline steps: `action`, `expected_result`, `data`, `position`, `attachments` (hashes mapped)
- [ ] **13.13** `attachments` — list of mapped hashes
- [ ] **13.14** `custom_field` — keys remapped to **target** field ids; string values run through attachment hash replacement in markdown

**Text rewriting:**

- [ ] **13.15** `description`, `preconditions`, `postconditions`, step text fields, custom field string values: `replace_attachment_hashes_in_text` (markdown `/attachment/{hash}/` URLs)

**Batching:**

- [ ] **13.16** Batch size 20 for case migration (list/get)

- [ ] **13.17** `mappings.cases[source_project][source_case_id] → target_case_id`

---

## 14. Test plans (`migration/create/plans.py`, `migration/extract/plans.py`)

**Source:** `get_plans` + `get_plan` for cases.

**Create:**

- [ ] **14.1** `title` (required)
- [ ] **14.2** `cases` — list of **target** case ids (source `case_id` mapped); plan **skipped** if no mappable cases
- [ ] **14.3** `description` if present

- [ ] **14.4** `mappings.plans[source_project][source_plan_id] → target_plan_id`

---

## 15. Test runs (`migration/create/runs.py`, `migration/extract/runs.py`)

**Source runs:** raw GET `/v1/run/{project}` (full payload including `user_id`).

**Case list on run:**

- [ ] **15.1** `extract_run_cases` — `get_run(..., include='cases')` or fallback `get_tests` pagination
- [ ] **15.2** Fallback: `run_dict['cases']` integers/objects mapped via `case_mapping`

**Configurations on run:**

- [ ] **15.3** Each entry resolved to id → `config_mapping`

**Fields passed to create:**

- [ ] **15.4** `title`, `description`
- [ ] **15.5** `author_id` from `user_id` / `created_by` / `author_id` / `member_id` → `get_user_id` (0 → 1)
- [ ] **15.6** `start_time`, `end_time` — `format_datetime` when valid
- [ ] **15.7** `cases` — target ids
- [ ] **15.8** `configurations` — target ids
- [ ] **15.9** `milestone_id` — resolved from run’s `milestone` object **title** → source milestone id → map (or raw `milestone_id` if ever present)
- [ ] **15.10** `plan_id` — mapped when present

**Create path:**

- [ ] **15.11** Raw `QaseRawApiClient.create_run` when `milestone_id` in payload; else SDK `RunCreate`

**Completion:**

- [ ] **15.12** Runs with `is_completed` or `end_time` queued on `mappings._runs_to_complete` for `complete_run` after results

- [ ] **15.13** `mappings.runs[source_project][source_run_id] → target_run_id`

---

## 16. Test results (`migration/create/results.py`, `migration/extract/results.py`, `migration/extract/authors.py`)

**Source results:** raw GET `/v1/result/{project}?run={id}` paginated.

**Target write:** API **v2** `ResultsApi(target_service.client_v2).create_results_v2` with `CreateResultsRequestV2` (same family as Xray → Qase): `testops_id` = target numeric case id; `title`; `execution` (`status`, `duration` ms, `stacktrace`, `start_time`/`end_time` = `null`); `message` (markdown); `attachments` (hashes); `steps` only when non-empty — each step `data.action` / `expected_result` (action `"."` if empty), `execution.comment` for actual + step comment, `execution.attachments` (hashes); nested steps as dicts.

- [ ] **16.1** `extract_authors` — raw GET `/v1/author` (used elsewhere; v2 result create has no author field on payload)
- [ ] **16.2** Chunks of ≤500 → `create_results_v2`
- [ ] **16.3** Refetch v1 `GET /result?run=` on target to map `result_hashes` by `case_id`
- [ ] **16.4** `complete_run` for `mappings._runs_to_complete` when applicable
- [ ] **16.5** `mappings.result_hashes` / `author_uuid_to_id_mapping` updated as before

---

## 17. Defects (`migration/create/defects.py`, `migration/extract/defects.py`)

**Source:** raw GET `/v1/defect/{project}` paginated.

**Create via** `QaseRawApiClient.create_defect`:

- [ ] **17.1** `title`
- [ ] **17.2** `actual_result` — attachment markdown rewritten via `replace_attachment_hashes_in_text`
- [ ] **17.3** `severity` — string → int (`undefined`…`trivial`)
- [ ] **17.4** `author_id` from `author_id` / `member_id` → `get_user_id`
- [ ] **17.5** `milestone_id` if mapped
- [ ] **17.6** `attachments` — mapped hashes when available

**Resolve:**

- [ ] **17.7** `resolve_defect` when source status string in `resolved|closed|invalid|duplicate` or status int `> 0`

**Not done by orchestrator:**

- [ ] **17.8** Linking defects to specific runs/results — `QaseRawApiClient.attach_defect_to_results` exists in `migration/utils.py` but is **never called** from `migrate_workspace.py`

- [ ] **17.9** `mappings.defects[source_project][source_defect_id] → target_defect_id`

---

## 18. `mappings.json` persistence (`MigrationMappings.save_to_file`)

Keys written today:

- [ ] `projects`, `suites`, `cases`, `runs`, `milestones`
- [ ] `configurations`, `configuration_groups`, `environments`
- [ ] `shared_steps`, `shared_parameters`, `custom_fields`
- [ ] `users`, `user_email_mapping`, `user_uuid_mapping`, `author_uuid_to_id_mapping`
- [ ] `attachments`, `plans`, `defects`, `result_hashes`

**Not persisted (runtime only):**

- [ ] `mappings.groups` — updated in memory during group migration, **omitted** from `save_to_file` / `load_from_file`
- [ ] `mappings._runs_to_complete` — consumed during results migration

**Note:** `target_workspace_hash` is **not** in the saved dict in `save_to_file` (load sets it only if key exists in JSON).

---

## 19. Raw API helpers in `QaseRawApiClient` (`migration/utils.py`)

| Method | Used by migration? |
|--------|-------------------|
| `create_cases_bulk` | Yes — cases |
| `create_results_bulk` | **No** — results use v2 `ResultsApi.create_results_v2` (`migration/create/results.py`) |
| `create_run` | Yes — runs with milestone |
| `create_defect` | Yes — defects |
| `resolve_defect` | Yes — defects |
| `attach_defect_to_results` | **No** — not referenced by orchestrator |
| `attach_external_issues` | **No** — not referenced by orchestrator |

---

## 20. SCIM client (`migration/utils/scim_client.py`)

Used when configuring SCIM on services:

- [ ] User listing / creation (via `QaseScimClient` from `qase_service`)
- [ ] Group listing / creation / `add_users_to_group`

---

## 21. Config file knobs that gate behavior (`config.json` + `example_config.json`)

- [ ] `source.scim_token` / `source.scim_host`, `target.scim_token` / `target.scim_host` — required for user migration path
- [ ] `users.migrate`, `users.create`, `users.inactive`, `users.default`
- [ ] `groups.create`

---

## 22. Statistics counters (`MigrationStats.add_entity`)

Entity types accumulated during a run include at least:

`projects`, `users`, `groups`, `custom_fields`, `shared_parameters`, `attachments`, `milestones`, `configuration_groups`, `configurations`, `environments`, `shared_steps`, `suites`, `cases`, `plans`, `runs`, `results`, `defects`

---

## 23. Explicit non-coverage (no code path in this repo)

The following are **not** implemented in `migrate_workspace.py` or its callees: workspace roles, dashboards, saved queries, requirements, traceability, Aiden-specific artifacts/settings, project settings beyond fields on migrated entities, integrations/Jira re-linking, workspace-level shared-step libraries as a separate concept (only per-project shared steps via API), and any use of `attach_external_issues` / `attach_defect_to_results`.

For product-level API limits, see **README.md** → “What is not included”.

---

*Generated from static analysis of the migration codebase. Re-run an audit after major refactors.*
