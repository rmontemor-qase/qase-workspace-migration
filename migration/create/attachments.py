"""
Create attachments in target Qase workspace.
"""
import logging
import re
import os
import sys
import time
import base64
import threading
import requests
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from typing import Dict, Set, List, Any, Optional, Tuple
from tqdm import tqdm
from qase.api_client_v1.api.attachments_api import AttachmentsApi
from qase.api_client_v1.api.cases_api import CasesApi
from qase_service import QaseService
from migration.utils import MigrationMappings, MigrationStats, retry_with_backoff, extract_entities_from_response, to_dict
from migration.extract.attachments import extract_all_attachment_hashes

logger = logging.getLogger(__name__)

# Qase bulk attachment upload limits (POST /v1/attachment/{projectCode}, multipart file[])
MAX_FILES_PER_UPLOAD = 20
MAX_BYTES_PER_UPLOAD = 128 * 1024 * 1024
MAX_BYTES_PER_FILE = 32 * 1024 * 1024
DOWNLOAD_PARALLEL_WORKERS = 8
_BULK_UPLOAD_RETRIES = 5


def _try_download_markdown_url(
    markdown_url: str,
    attachment_hash: str,
    source_service: QaseService,
    filename_hint: Optional[str] = None,
) -> Tuple[Optional[bytes], Optional[str]]:
    """HTTP-only download from a known attachment URL (safe for parallel use)."""
    filename = filename_hint
    if not filename:
        parsed = urlparse(markdown_url)
        url_filename = os.path.basename(parsed.path)
        if url_filename and url_filename != "/":
            filename = url_filename
    try:
        url_response = requests.get(markdown_url, timeout=60)
        if url_response.status_code == 200:
            content = url_response.content
            if not filename:
                parsed = urlparse(markdown_url)
                filename = os.path.basename(parsed.path) or f"attachment_{attachment_hash[:8]}.bin"
            return content, filename
        if url_response.status_code == 403:
            headers = {"Token": source_service.api_token}
            url_response = requests.get(markdown_url, headers=headers, timeout=60)
            if url_response.status_code == 200:
                if not filename:
                    parsed = urlparse(markdown_url)
                    filename = os.path.basename(parsed.path) or f"attachment_{attachment_hash[:8]}.bin"
                return url_response.content, filename
    except Exception:
        pass
    return None, None


def _parse_hashes_from_upload_json(data: Any, expected_n: int) -> List[Optional[str]]:
    """Normalize attachment upload JSON into a list of target hashes."""
    if not isinstance(data, dict) or not data.get("status"):
        return [None] * expected_n
    result = data.get("result")
    if result is None:
        return [None] * expected_n
    if isinstance(result, list):
        out = []
        for item in result:
            if isinstance(item, dict):
                out.append(item.get("hash"))
            elif hasattr(item, "hash"):
                out.append(getattr(item, "hash", None))
            else:
                out.append(None)
        return out
    if isinstance(result, dict):
        if "hash" in result:
            return [result.get("hash")]
        entities = result.get("entities")
        if isinstance(entities, list):
            return [e.get("hash") if isinstance(e, dict) else None for e in entities]
    return [None] * expected_n


def _maybe_capture_workspace_hash_from_upload_result(mappings: MigrationMappings, data: Any) -> None:
    if mappings.target_workspace_hash:
        return

    def scan(obj: Any) -> None:
        if mappings.target_workspace_hash:
            return
        if isinstance(obj, dict):
            for v in obj.values():
                scan(v)
            u = obj.get("url") or obj.get("full_path")
            if isinstance(u, str):
                m = re.search(r"/public/team/([a-f0-9]{32,64})/", u, re.IGNORECASE)
                if m:
                    mappings.target_workspace_hash = m.group(1)
        elif isinstance(obj, list):
            for x in obj:
                scan(x)

    scan(data)


def _upload_files_http(
    target_service: QaseService,
    project_code: str,
    named_files: List[Tuple[str, bytes]],
    mappings: MigrationMappings,
) -> List[Optional[str]]:
    """
    POST multipart file[] to /v1/attachment/{code}. Returns target hashes aligned with named_files.
    """
    n = len(named_files)
    if n == 0:
        return []
    base = target_service.client.configuration.host.rstrip("/")
    url = f"{base}/attachment/{project_code}"
    token = target_service.api_token
    headers = {"Token": token, "accept": "application/json"}
    multipart = [
        ("file[]", (fn or "file.bin", content, "application/octet-stream"))
        for fn, content in named_files
    ]
    last_exc: Optional[Exception] = None
    for attempt in range(_BULK_UPLOAD_RETRIES):
        try:
            r = requests.post(url, headers=headers, files=multipart, timeout=300)
            if r.status_code == 507:
                logger.error("Attachment upload failed: insufficient storage (507) for project %s", project_code)
                return [None] * n
            if r.status_code == 429 or r.status_code >= 500:
                delay = 1.0 * (2**attempt)
                logger.warning(
                    "Attachment upload HTTP %s, retry in %.1fs (%s/%s)",
                    r.status_code,
                    delay,
                    attempt + 1,
                    _BULK_UPLOAD_RETRIES,
                )
                time.sleep(delay)
                continue
            r.raise_for_status()
            payload = r.json()
            _maybe_capture_workspace_hash_from_upload_result(mappings, payload)
            hashes = _parse_hashes_from_upload_json(payload, n)
            if len(hashes) < n:
                hashes.extend([None] * (n - len(hashes)))
            elif len(hashes) > n:
                hashes = hashes[:n]
            if all(h is None for h in hashes) and n > 0:
                logger.warning(
                    "Bulk attachment upload returned no hashes (project=%s body_keys=%s)",
                    project_code,
                    list(payload.keys()) if isinstance(payload, dict) else type(payload),
                )
            return hashes
        except Exception as e:
            last_exc = e
            delay = 1.0 * (2**attempt)
            logger.warning("Attachment upload error: %s; retry in %.1fs", e, delay)
            time.sleep(delay)
    if last_exc:
        logger.error("Attachment upload failed after retries: %s", last_exc)
    return [None] * n


def _take_upload_batch(ready: deque) -> List[Tuple[str, str, bytes]]:
    """Greedy batch: max 20 files, max 128 MiB total; oversized files (>32 MiB) upload alone."""
    if not ready:
        return []
    first = ready[0]
    _, _, first_bytes = first
    if len(first_bytes) > MAX_BYTES_PER_FILE:
        ready.popleft()
        return [first]
    batch: List[Tuple[str, str, bytes]] = []
    total = 0
    while ready:
        h, fn, c = ready[0]
        if len(c) > MAX_BYTES_PER_FILE:
            break
        if len(batch) >= MAX_FILES_PER_UPLOAD:
            break
        if total + len(c) > MAX_BYTES_PER_UPLOAD and batch:
            break
        batch.append(ready.popleft())
        total += len(c)
    return batch


def _log_source_attachment_library_totals(source_service: QaseService, projects: List[Dict[str, Any]]) -> None:
    """Log result.total from GET /v1/attachment/{code} (no bulk download API in Qase)."""
    base = source_service.client.configuration.host.rstrip("/")
    headers = {"Token": source_service.api_token, "accept": "application/json"}
    for project in projects:
        code = project.get("source_code")
        if not code:
            continue
        try:
            r = requests.get(
                f"{base}/attachment/{code}",
                headers=headers,
                params={"limit": 1, "offset": 0},
                timeout=30,
            )
            if not r.ok:
                continue
            data = r.json()
            if not isinstance(data, dict):
                continue
            res = data.get("result")
            if isinstance(res, dict) and res.get("total") is not None:
                logger.info(
                    "Source project %s attachment library: %s file(s) (API total)",
                    code,
                    res.get("total"),
                )
        except Exception:
            pass


def check_existing_attachments_in_target(
    target_service: QaseService,
    projects: List[Dict[str, Any]]
) -> Set[str]:
    """
    Check for existing attachments in target workspace.
    
    Returns:
        Set of existing attachment hashes
    """
    attachments_api_target = AttachmentsApi(target_service.client)
    cases_api_target = CasesApi(target_service.client)
    existing_attachments = set()
    
    for project in projects:
        project_code_target = project['target_code']
        try:
            offset = 0
            limit = 100
            
            while True:
                try:
                    cases_response = retry_with_backoff(
                        cases_api_target.get_cases,
                        code=project_code_target,
                        limit=limit,
                        offset=offset
                    )
                    
                    cases_entities = extract_entities_from_response(cases_response)
                    if not cases_entities:
                        break
                    
                    for case in cases_entities:
                        case_dict = to_dict(case)
                        
                        if case_dict.get('attachments'):
                            for att_item in case_dict['attachments']:
                                att_hash = None
                                if isinstance(att_item, str):
                                    att_hash = att_item
                                elif isinstance(att_item, dict):
                                    if 'hash' in att_item:
                                        att_hash = att_item['hash']
                                    elif 'url' in att_item:
                                        url = att_item['url']
                                        match = re.search(r'/attachment/([a-f0-9]{32,64})/', url, re.IGNORECASE)
                                        if match:
                                            att_hash = match.group(1)
                                if att_hash:
                                    existing_attachments.add(att_hash.lower())
                    
                    if len(cases_entities) < limit:
                        break
                    offset += limit
                except Exception as e:
                    break
        except Exception as e:
            pass
    
    return existing_attachments


def download_attachment(
    attachments_api_source: AttachmentsApi,
    attachment_hash: str,
    markdown_url: Optional[str],
    source_service: QaseService
) -> Tuple[Optional[bytes], Optional[str]]:
    """
    Download attachment content and extract filename.
    
    Returns:
        Tuple of (file_content, filename) or (None, None) if download fails
    """
    file_content = None
    filename = None
    
    if markdown_url:
        parsed = urlparse(markdown_url)
        url_filename = os.path.basename(parsed.path)
        if url_filename and url_filename != '/':
            filename = url_filename
    
    download_response = None
    try:
        download_response = retry_with_backoff(
            attachments_api_source.get_attachment,
            hash=attachment_hash
        )
    except Exception as api_error:
        pass
    
    if download_response and hasattr(download_response, 'status') and download_response.status:
        if hasattr(download_response, 'result') and download_response.result:
            attachment_obj = download_response.result
            
            # Try to get file content from 'file' field
            if hasattr(attachment_obj, 'file') and attachment_obj.file is not None:
                if isinstance(attachment_obj.file, bytes):
                    file_content = attachment_obj.file
                elif isinstance(attachment_obj.file, str):
                    try:
                        file_content = base64.b64decode(attachment_obj.file)
                    except Exception:
                        pass
            
            if not filename:
                if hasattr(attachment_obj, 'filename') and attachment_obj.filename:
                    filename = attachment_obj.filename
                elif hasattr(attachment_obj, 'full_path') and attachment_obj.full_path:
                    filename = os.path.basename(attachment_obj.full_path)
                elif hasattr(attachment_obj, 'extension') and attachment_obj.extension:
                    filename = f"attachment_{attachment_hash[:8]}.{attachment_obj.extension}"
            
            if not file_content:
                url_to_download = None
                if hasattr(attachment_obj, 'url'):
                    url_value = getattr(attachment_obj, 'url', None)
                    if url_value:
                        url_to_download = url_value
                if not url_to_download and hasattr(attachment_obj, 'full_path'):
                    full_path_value = getattr(attachment_obj, 'full_path', None)
                    if full_path_value:
                        url_to_download = full_path_value
                
                if url_to_download:
                    try:
                        url_response = requests.get(url_to_download, timeout=60)
                        if url_response.status_code == 200:
                            file_content = url_response.content
                        elif url_response.status_code == 403:
                            headers = {'Token': source_service.api_token}
                            url_response = requests.get(url_to_download, headers=headers, timeout=60)
                            if url_response.status_code == 200:
                                file_content = url_response.content
                    except Exception as e:
                        pass
    
    if not file_content and markdown_url:
        try:
            url_response = requests.get(markdown_url, timeout=60)
            if url_response.status_code == 200:
                file_content = url_response.content
                if not filename:
                    parsed = urlparse(markdown_url)
                    filename = os.path.basename(parsed.path) or f"attachment_{attachment_hash[:8]}.bin"
            elif url_response.status_code == 403:
                headers = {'Token': source_service.api_token}
                url_response = requests.get(markdown_url, headers=headers, timeout=60)
                if url_response.status_code == 200:
                    file_content = url_response.content
                    if not filename:
                        parsed = urlparse(markdown_url)
                        filename = os.path.basename(parsed.path) or f"attachment_{attachment_hash[:8]}.bin"
        except Exception as e:
            pass
    
    if not filename:
        filename = f"attachment_{attachment_hash[:8]}.bin"
    
    return file_content, filename


def upload_attachment(
    target_service: QaseService,
    project_code_target: str,
    filename: str,
    file_content: bytes,
    mappings: MigrationMappings,
) -> Optional[str]:
    """
    Upload a single attachment via HTTP multipart (same endpoint as bulk).

    Returns:
        Target attachment hash, or None if upload fails
    """
    hashes = _upload_files_http(
        target_service, project_code_target, [(filename, file_content)], mappings
    )
    return hashes[0] if hashes else None


def _download_single_for_migration(
    attachment_hash: str,
    markdown_url: Optional[str],
    attachments_api_source: AttachmentsApi,
    source_service: QaseService,
    sdk_lock: threading.Lock,
) -> Tuple[str, Optional[bytes], Optional[str]]:
    """Try public URL first (parallel-friendly), then SDK get_attachment under lock."""
    if markdown_url:
        content, filename = _try_download_markdown_url(
            markdown_url, attachment_hash, source_service
        )
        if content and filename:
            return attachment_hash, content, filename
    with sdk_lock:
        content, filename = download_attachment(
            attachments_api_source, attachment_hash, markdown_url, source_service
        )
    return attachment_hash, content, filename


def _download_batch_parallel(
    batch: List[Tuple[str, Optional[str]]],
    attachments_api_source: AttachmentsApi,
    source_service: QaseService,
    sdk_lock: threading.Lock,
) -> List[Tuple[str, str, bytes]]:
    if not batch:
        return []
    out: List[Tuple[str, str, bytes]] = []
    if len(batch) == 1:
        h, url = batch[0]
        _, content, filename = _download_single_for_migration(
            h, url, attachments_api_source, source_service, sdk_lock
        )
        if content and filename:
            out.append((h, filename, content))
        return out
    workers = min(DOWNLOAD_PARALLEL_WORKERS, len(batch))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(
                _download_single_for_migration,
                h,
                url,
                attachments_api_source,
                source_service,
                sdk_lock,
            ): h
            for h, url in batch
        }
        for fut in as_completed(future_map):
            src_hash = future_map[fut]
            try:
                _, content, filename = fut.result()
            except Exception as e:
                logger.error("Download failed for attachment %s: %s", src_hash, e)
                continue
            if content and filename:
                out.append((src_hash, filename, content))
    return out


def migrate_attachments_workspace(
    source_service: QaseService,
    target_service: QaseService,
    projects: List[Dict[str, Any]],
    mappings: MigrationMappings,
    stats: MigrationStats
) -> Dict[str, Dict[str, str]]:
    """
    Migrate all attachments at workspace level by collecting from all projects.
    Attachments are project-scoped but we collect them all upfront.
    Deduplicates attachments across projects (same hash = same file).
    
    Args:
        projects: List of project mappings with 'source_code' and 'target_code'
    
    Returns:
        Dictionary mapping project_code -> {source_hash -> target_hash}
    """
    attachments_api_source = AttachmentsApi(source_service.client)

    existing_attachments = check_existing_attachments_in_target(target_service, projects)

    all_project_attachments = extract_all_attachment_hashes(source_service, projects)

    global_attachment_set = set()
    attachment_urls = {}
    all_attachments = {}

    for project_code_source, (hashes, urls) in all_project_attachments.items():
        all_attachments[project_code_source] = hashes
        global_attachment_set.update(hashes)
        attachment_urls.update(urls)

    total_unique_attachments = len(global_attachment_set)

    global_attachment_mapping = {}
    migrated_count = 0
    skipped_existing_count = 0
    skipped_already_migrated_count = 0

    _log_source_attachment_library_totals(source_service, projects)

    upload_queues: Dict[str, deque] = defaultdict(deque)
    work_total = 0

    for attachment_hash_raw in global_attachment_set:
        attachment_hash = str(attachment_hash_raw).lower()

        already_mapped = False
        for project_code in mappings.attachments:
            if attachment_hash in mappings.attachments[project_code]:
                global_attachment_mapping[attachment_hash] = mappings.attachments[project_code][attachment_hash]
                skipped_already_migrated_count += 1
                already_mapped = True
                break
            if attachment_hash_raw in mappings.attachments[project_code]:
                global_attachment_mapping[attachment_hash] = mappings.attachments[project_code][attachment_hash_raw]
                skipped_already_migrated_count += 1
                already_mapped = True
                break

        if already_mapped:
            continue

        if attachment_hash in existing_attachments:
            global_attachment_mapping[attachment_hash] = attachment_hash
            skipped_existing_count += 1
            continue

        source_project = None
        target_project = None
        for project in projects:
            project_code_source = project["source_code"]
            if project_code_source in all_attachments:
                if attachment_hash in all_attachments[project_code_source]:
                    source_project = project_code_source
                    target_project = project["target_code"]
                    break

        if not source_project or not target_project:
            continue

        markdown_url = attachment_urls.get(attachment_hash)
        upload_queues[target_project].append((attachment_hash, markdown_url))
        work_total += 1

    sdk_lock = threading.Lock()
    pbar = tqdm(
        total=work_total,
        desc="Migrating attachments",
        unit="file",
        file=sys.stderr,
        dynamic_ncols=True,
    )

    try:
        for target_code in sorted(upload_queues.keys()):
            q = upload_queues[target_code]
            ready: deque = deque()
            while q or ready:
                while len(ready) < MAX_FILES_PER_UPLOAD * 3 and q:
                    download_batch: List[Tuple[str, Optional[str]]] = []
                    while len(download_batch) < MAX_FILES_PER_UPLOAD and q:
                        download_batch.append(q.popleft())
                    downloaded = _download_batch_parallel(
                        download_batch, attachments_api_source, source_service, sdk_lock
                    )
                    ok_hashes = {t[0] for t in downloaded}
                    for h, _url in download_batch:
                        if h not in ok_hashes:
                            pbar.update(1)
                    for triple in downloaded:
                        ready.append(triple)
                if not ready:
                    break
                upload_batch = _take_upload_batch(ready)
                if not upload_batch:
                    continue
                named_files = [(fn, content) for _h, fn, content in upload_batch]
                target_hashes = _upload_files_http(
                    target_service, target_code, named_files, mappings
                )
                if (
                    len(upload_batch) > 1
                    and target_hashes
                    and all(th is None for th in target_hashes)
                ):
                    for src_hash, fn, content in upload_batch:
                        th = _upload_files_http(target_service, target_code, [(fn, content)], mappings)
                        mapped = th[0] if th else None
                        if mapped:
                            global_attachment_mapping[src_hash] = mapped
                            migrated_count += 1
                        pbar.update(1)
                    continue
                for i, (src_hash, _fn, _c) in enumerate(upload_batch):
                    th = target_hashes[i] if i < len(target_hashes) else None
                    if th:
                        global_attachment_mapping[src_hash] = th
                        migrated_count += 1
                    pbar.update(1)
    finally:
        pbar.close()
    
    for project in projects:
        project_code_source = project['source_code']
        
        if project_code_source not in mappings.attachments:
            mappings.attachments[project_code_source] = {}
        
        if project_code_source in all_attachments:
            for attachment_hash in all_attachments[project_code_source]:
                normalized_hash = attachment_hash.lower()
                target_hash = global_attachment_mapping.get(normalized_hash) or global_attachment_mapping.get(attachment_hash)
                if target_hash:
                    mappings.attachments[project_code_source][normalized_hash] = target_hash
                    if normalized_hash != attachment_hash:
                        mappings.attachments[project_code_source][attachment_hash] = target_hash
    
    stats.add_entity('attachments', total_unique_attachments, migrated_count)
    
    return mappings.attachments
