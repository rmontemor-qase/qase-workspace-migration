"""
Create attachments in target Qase workspace.
"""
import logging
import re
import os
import base64
import requests
from urllib.parse import urlparse
from typing import Dict, Set, List, Any, Optional, Tuple
from qase.api_client_v1.api.attachments_api import AttachmentsApi
from qase.api_client_v1.api.cases_api import CasesApi
from qase.api_client_v1.exceptions import ApiException
from qase_service import QaseService
from migration.utils import MigrationMappings, MigrationStats, retry_with_backoff, extract_entities_from_response, to_dict
from migration.extract.attachments import extract_all_attachment_hashes

logger = logging.getLogger(__name__)


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
    attachments_api_target: AttachmentsApi,
    project_code_target: str,
    filename: str,
    file_content: bytes,
    mappings: MigrationMappings
) -> Optional[str]:
    """
    Upload attachment to target workspace.
    
    Returns:
        Target attachment hash, or None if upload fails
    """
    upload_response = retry_with_backoff(
        attachments_api_target.upload_attachment,
        code=project_code_target,
        file=[(filename, file_content)]
    )
    
    if not upload_response:
        return None
    
    target_hash = None
    upload_url = None
    
    if isinstance(upload_response, list) and len(upload_response) > 0:
        upload_item = upload_response[0]
        if hasattr(upload_item, 'hash'):
            target_hash = upload_item.hash
        elif isinstance(upload_item, dict):
            target_hash = upload_item.get('hash')
        if hasattr(upload_item, 'url'):
            upload_url = upload_item.url
        elif isinstance(upload_item, dict):
            upload_url = upload_item.get('url')
    elif hasattr(upload_response, 'result'):
        result = upload_response.result
        if hasattr(result, 'hash'):
            target_hash = result.hash
        elif isinstance(result, list) and len(result) > 0:
            if hasattr(result[0], 'hash'):
                target_hash = result[0].hash
        if hasattr(result, 'url'):
            upload_url = result.url
        elif isinstance(result, list) and len(result) > 0:
            if hasattr(result[0], 'url'):
                upload_url = result[0].url
    
    if upload_url and not mappings.target_workspace_hash:
        workspace_match = re.search(r'/public/team/([a-f0-9]{32,64})/', upload_url, re.IGNORECASE)
        if workspace_match:
            mappings.target_workspace_hash = workspace_match.group(1)
    
    return target_hash


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
    attachments_api_target = AttachmentsApi(target_service.client)
    
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
    
    for attachment_hash_raw in global_attachment_set:
        attachment_hash = str(attachment_hash_raw).lower()
        
        already_mapped = False
        for project_code in mappings.attachments:
            if attachment_hash in mappings.attachments[project_code]:
                global_attachment_mapping[attachment_hash] = mappings.attachments[project_code][attachment_hash]
                skipped_already_migrated_count += 1
                already_mapped = True
                break
            elif attachment_hash_raw in mappings.attachments[project_code]:
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
        
        attachment_exists_in_target = False
        for project in projects:
            project_code_target = project['target_code']
            try:
                check_response = retry_with_backoff(
                    attachments_api_target.get_attachment,
                    hash=attachment_hash
                )
                if check_response:
                    global_attachment_mapping[attachment_hash] = attachment_hash
                    skipped_existing_count += 1
                    attachment_exists_in_target = True
                    break
            except ApiException as e:
                if e.status == 404:
                    pass
            except Exception:
                pass
        
        if attachment_exists_in_target:
            continue
        
        source_project = None
        target_project = None
        for project in projects:
            project_code_source = project['source_code']
            if project_code_source in all_attachments:
                if attachment_hash in all_attachments[project_code_source]:
                    source_project = project_code_source
                    target_project = project['target_code']
                    break
        
        if not source_project:
            continue
        
        markdown_url = attachment_urls.get(attachment_hash)
        
        try:
            file_content, filename = download_attachment(
                attachments_api_source, attachment_hash, markdown_url, source_service
            )
            
            if not file_content:
                continue
            
            target_hash = upload_attachment(
                attachments_api_target, target_project, filename, file_content, mappings
            )
            
            if target_hash:
                global_attachment_mapping[attachment_hash] = target_hash
                migrated_count += 1
        except Exception as e:
            logger.error(f"Error migrating attachment {attachment_hash}: {e}")
    
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
