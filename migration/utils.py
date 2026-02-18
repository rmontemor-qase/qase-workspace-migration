"""
Migration utilities - Helper functions for data processing, ID mapping, and error handling.
"""
import json
import logging
import time
import hashlib
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime
from qase.api_client_v1.exceptions import ApiException
import requests


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MigrationMappings:
    """Stores mappings between source and target entity IDs."""
    
    def __init__(self):
        self.projects = {}
        self.suites = {}
        self.cases = {}
        self.runs = {}
        self.milestones = {}
        self.configurations = {}
        self.configuration_groups = {}
        self.environments = {}
        self.shared_steps = {}
        self.shared_parameters = {}
        self.custom_fields = {}
        self.users = {}
        self.attachments = {}
        self.plans = {}
        self.target_workspace_hash = None
    
    def save_to_file(self, filepath: str):
        """Save mappings to JSON file."""
        mappings_dict = {
            'projects': self.projects,
            'suites': self.suites,
            'cases': self.cases,
            'runs': self.runs,
            'milestones': self.milestones,
            'configurations': self.configurations,
            'configuration_groups': self.configuration_groups,
            'environments': self.environments,
            'shared_steps': self.shared_steps,
            'shared_parameters': self.shared_parameters,
            'custom_fields': self.custom_fields,
            'users': self.users,
            'attachments': self.attachments,
            'plans': self.plans
        }
        with open(filepath, 'w') as f:
            json.dump(mappings_dict, f, indent=2)
    
    def load_from_file(self, filepath: str):
        """Load mappings from JSON file."""
        try:
            with open(filepath, 'r') as f:
                mappings_dict = json.load(f)
            self.projects = mappings_dict.get('projects', {})
            self.suites = mappings_dict.get('suites', {})
            self.cases = mappings_dict.get('cases', {})
            self.runs = mappings_dict.get('runs', {})
            self.milestones = mappings_dict.get('milestones', {})
            self.configurations = mappings_dict.get('configurations', {})
            self.configuration_groups = mappings_dict.get('configuration_groups', {})
            self.environments = mappings_dict.get('environments', {})
            self.shared_steps = mappings_dict.get('shared_steps', {})
            self.shared_parameters = mappings_dict.get('shared_parameters', {})
            self.custom_fields = mappings_dict.get('custom_fields', {})
            self.users = mappings_dict.get('users', {})
            self.attachments = mappings_dict.get('attachments', {})
            self.plans = mappings_dict.get('plans', {})
            self.target_workspace_hash = mappings_dict.get('target_workspace_hash')
        except FileNotFoundError:
            pass


class MigrationStats:
    """Tracks migration statistics."""
    
    def __init__(self):
        self.entities_processed = {}
        self.entities_created = {}
        self.errors = []
    
    def add_entity(self, entity_type: str, source_count: int, target_count: int):
        """Record entity migration stats. Accumulates counts across multiple calls."""
        if entity_type in self.entities_processed:
            self.entities_processed[entity_type] += source_count
            self.entities_created[entity_type] += target_count
        else:
            self.entities_processed[entity_type] = source_count
            self.entities_created[entity_type] = target_count
    
    def add_error(self, entity_type: str, error: str):
        """Record an error."""
        self.errors.append({
            'entity_type': entity_type,
            'error': error,
            'timestamp': datetime.now().isoformat()
        })
    
    def print_summary(self):
        """Print migration summary."""
        print("\n" + "="*60)
        print("MIGRATION SUMMARY")
        print("="*60)
        for entity_type in self.entities_processed:
            processed = self.entities_processed[entity_type]
            created = self.entities_created.get(entity_type, 0)
            print(f"{entity_type:30s}: {created}/{processed} created")
        print(f"\nTotal Errors: {len(self.errors)}")
        if self.errors:
            print("\nErrors:")
            for error in self.errors[:10]:
                print(f"  - {error['entity_type']}: {error['error']}")
        print("="*60 + "\n")


MAX_SAFE_ID = 2**31 - 1


def preserve_or_hash_id(original_id: int, preserve_ids: bool = True) -> int:
    """
    Preserve ID if within int32 range, otherwise hash it.
    
    Args:
        original_id: Original entity ID
        preserve_ids: Whether to preserve IDs when possible
    
    Returns:
        Preserved or hashed ID
    """
    if original_id <= MAX_SAFE_ID:
        if preserve_ids:
            return original_id
        else:
            return int(time.time() * 1000) % MAX_SAFE_ID
    else:
        hashed = int(hashlib.md5(str(original_id).encode()).hexdigest()[:8], 16)
        return hashed % MAX_SAFE_ID


def safe_api_call(api_method, *args, **kwargs):
    """
    Safely call API method with error handling.
    
    Args:
        api_method: API method to call
        *args: Positional arguments
        **kwargs: Keyword arguments
    
    Returns:
        Result object or None on error
    """
    try:
        response = api_method(*args, **kwargs)
        if response.status:
            return response.result
        else:
            error_msg = getattr(response, 'error', 'Unknown error')
            logger.error(f"API call failed: {error_msg}")
            return None
    except ApiException as e:
        logger.error(f"API exception: {e.status} - {e.reason}")
        if e.body:
            try:
                error_data = json.loads(e.body)
                logger.error(f"Error details: {error_data}")
            except:
                logger.error(f"Error body: {e.body}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def retry_with_backoff(func, max_retries: int = 3, base_delay: float = 1.0, *args, **kwargs):
    """
    Retry function with exponential backoff.
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retries
        base_delay: Base delay in seconds
        *args: Positional arguments for func
        **kwargs: Keyword arguments for func
    
    Returns:
        Function result (even if None)
    """
    last_exception = None
    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            return result
        except ApiException as e:
            last_exception = e
            if e.status == 429 or e.status >= 500:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
            is_attachment_error = False
            if e.status == 404:
                if e.body:
                    try:
                        import json
                        error_data = json.loads(e.body)
                        error_msg = str(error_data).lower()
                        if 'attachment' in error_msg or 'attachment not found' in error_msg:
                            is_attachment_error = True
                    except:
                        if 'attachment' in str(e.body).lower():
                            is_attachment_error = True
                if not is_attachment_error and hasattr(func, '__name__'):
                    func_name = func.__name__.lower()
                    if 'attachment' in func_name:
                        is_attachment_error = True
            
            if is_attachment_error:
                return None
            else:
                logger.error(f"API exception (status {e.status}): {e.reason}")
                if e.body:
                    try:
                        import json
                        error_data = json.loads(e.body)
                        logger.error(f"Error details: {error_data}")
                    except:
                        logger.error(f"Error body: {e.body}")
                raise
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                time.sleep(delay)
                continue
            raise
    
    if last_exception:
        raise last_exception
    return None


def format_datetime(dt: Any) -> Optional[str]:
    """
    Format datetime to Qase format: YYYY-MM-DD HH:MM:SS
    
    Args:
        dt: Datetime object or string
    
    Returns:
        Formatted datetime string or None
    """
    if not dt:
        return None
    
    if isinstance(dt, str):
        try:
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d']:
                try:
                    parsed = datetime.strptime(dt, fmt)
                    return parsed.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue
            return dt
        except:
            return dt
    
    if isinstance(dt, datetime):
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return None


def format_date(dt: Any) -> Optional[str]:
    """
    Format datetime to date format: YYYY-MM-DD
    
    Args:
        dt: Datetime object or string
    
    Returns:
        Formatted date string or None
    """
    if not dt:
        return None
    
    if isinstance(dt, str):
        try:
            for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
                try:
                    parsed = datetime.strptime(dt[:10], '%Y-%m-%d')
                    return parsed.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return dt[:10] if len(dt) >= 10 else dt
        except:
            return dt[:10] if len(dt) >= 10 else dt
    
    if isinstance(dt, datetime):
        return dt.strftime('%Y-%m-%d')
    
    return None


def chunks(lst: List, n: int):
    """
    Split list into chunks of size n.
    
    Args:
        lst: List to chunk
        n: Chunk size
    
    Yields:
        Chunks of the list
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def to_dict(obj: Any) -> Dict[str, Any]:
    """
    Convert object to dictionary.
    
    Args:
        obj: Object to convert
    
    Returns:
        Dictionary representation
    """
    if hasattr(obj, 'to_dict'):
        return obj.to_dict()
    elif isinstance(obj, dict):
        return obj
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    else:
        return {}


def extract_entities_from_response(response: Any) -> Optional[List]:
    """
    Extract entities list from Qase API response.
    Handles different response structures: response.result.entities or response.entities
    
    Args:
        response: API response object
    
    Returns:
        List of entities or None
    """
    if not response:
        return None
    
    if hasattr(response, 'status') and hasattr(response, 'result'):
        if not response.status or not response.result:
            return None
        entities = getattr(response.result, 'entities', None)
    elif hasattr(response, 'entities'):
        entities = response.entities
    else:
        entities = getattr(response, 'entities', None)
    
    return entities if entities else None


def convert_uuids_to_strings(obj: Any) -> Any:
    """
    Recursively convert UUID objects to strings for JSON serialization.
    
    Args:
        obj: Object that may contain UUIDs
    
    Returns:
        Object with UUIDs converted to strings
    """
    if isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, dict):
        return {key: convert_uuids_to_strings(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_uuids_to_strings(item) for item in obj]
    else:
        return obj


class QaseRawApiClient:
    """Raw HTTP API client for operations that SDK doesn't support well."""
    
    def __init__(self, base_url: str, api_token: str):
        """
        Initialize raw API client.
        
        Args:
            base_url: Base API URL (e.g., https://api.qase.io/v1)
            api_token: API token
        """
        self.base_url = base_url.rstrip('/')
        self.headers = {
            'Token': api_token,
            'Content-Type': 'application/json'
        }
    
    def create_cases_bulk(self, project_code: str, cases: List[Dict[str, Any]]) -> Optional[List[int]]:
        """
        Create test cases in bulk using raw HTTP API.
        This bypasses SDK validation which may reject cases with shared steps.
        
        Args:
            project_code: Project code
            cases: List of case dictionaries
        
        Returns:
            List of created case IDs if successful, None otherwise
        """
        url = f"{self.base_url}/case/{project_code}/bulk"
        # Convert UUIDs to strings before JSON serialization
        cases_serializable = convert_uuids_to_strings(cases)
        payload = {"cases": cases_serializable}
        
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=60)
            if response.status_code == 200:
                response_data = response.json()
                if 'result' in response_data:
                    if 'ids' in response_data['result']:
                        return response_data['result']['ids']
                    elif 'id' in response_data['result']:
                        return [response_data['result']['id']]
                return []
            else:
                logger.error(f"Failed to create cases bulk: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Exception creating cases bulk: {e}")
            return None
    
    def attach_external_issues(self, project_code: str, links: List[Dict[str, Any]], issue_type: str = "jira-cloud") -> bool:
        """
        Attach external issues to test cases.
        
        Args:
            project_code: Project code
            links: List of links with case_id and external_issues
            issue_type: Type of external issue system (jira-cloud, jira-server, etc.)
        
        Returns:
            True if successful, False otherwise
        """
        url = f"{self.base_url}/case/{project_code}/external-issue/attach"
        payload = {
            "type": issue_type,
            "links": links
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=60)
            if response.status_code == 200:
                return True
            else:
                logger.error(f"Failed to attach external issues: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Exception attaching external issues: {e}")
            return False
