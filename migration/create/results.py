"""
Create results in target Qase workspace using API v2 bulk create.

V1 bulk + PATCH does not reliably persist comments, step actuals, or attachments.
V2 POST /{project}/run/{id}/results matches the shape used by the Xray → Qase migrator:
ResultCreate with testops_id, execution, message, attachments, and ResultStep with
data (action / expected_result) + execution.comment for actual output.
"""
import logging
import re
import time
from typing import Any, Dict, List, Optional

from qase.api_client_v2.api.results_api import ResultsApi
from qase.api_client_v1.exceptions import ApiException
from qase.api_client_v2.exceptions import ApiException as ApiExceptionV2
from qase.api_client_v2.models.create_results_request_v2 import CreateResultsRequestV2
from qase.api_client_v2.models.result_create import ResultCreate
from qase.api_client_v2.models.result_execution import ResultExecution
from qase.api_client_v2.models.result_step import ResultStep
from qase.api_client_v2.models.result_step_data import ResultStepData
from qase.api_client_v2.models.result_step_execution import ResultStepExecution
from qase.api_client_v2.models.result_step_status import ResultStepStatus

from qase.api_client_v1.api.runs_api import RunsApi
from qase.api_client_v1.api.cases_api import CasesApi
from qase_service import QaseService
from migration.utils import MigrationMappings, MigrationStats, chunks, to_dict
from migration.extract.results import extract_results, fetch_result_detail_json
from migration.extract.authors import extract_authors
from migration.transform.attachments import replace_attachment_hashes_in_text
from migration.trace_log import (
    chunk_results_payload_for_trace,
    summarize_enriched_result,
    summarize_result_create,
)

logger = logging.getLogger(__name__)

# Step fields Qase often nests under `execution` (especially automated / reporter runs).
_STEP_EXECUTION_OVERLAY_KEYS = frozenset(
    {
        "comment",
        "Comment",
        "message",
        "Message",
        "text",
        "Text",
        "actual_result",
        "actualResult",
        "actual",
        "Actual",
        "result",
        "Result",
        "body",
        "Body",
        "content",
        "Content",
        "output",
        "Output",
        "log",
        "Log",
        "stdout",
        "stderr",
        "error",
        "Error",
        "errors",
        "attachments",
        "duration",
        "time_ms",
        "time_spent_ms",
        "stacktrace",
        "start_time",
        "end_time",
        "status",
        "status_id",
    }
)


def _is_nonempty_for_merge(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, (list, dict, set)):
        return len(v) > 0
    return True


_STEP_INT_TO_V2: Dict[int, ResultStepStatus] = {
    1: ResultStepStatus.PASSED,
    2: ResultStepStatus.FAILED,
    3: ResultStepStatus.BLOCKED,
    5: ResultStepStatus.SKIPPED,
    7: ResultStepStatus.IN_PROGRESS,
}


# Qase markdown and API URLs: /attachment/{hash}/ or /attachments/{hash}/; hash may be UUID.
_ATTACHMENT_URL_RE = re.compile(
    r"/attachments?/([a-f0-9]{32,64}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})/",
    re.IGNORECASE,
)


def _looks_like_attachment_hash(s: str) -> bool:
    t = str(s).strip().replace("-", "")
    return len(t) >= 32 and bool(re.fullmatch(r"[a-f0-9]+", t, re.I))


def _candidate_hash_keys(h: str) -> List[str]:
    s = str(h).strip()
    if not s:
        return []
    keys = [s, s.lower(), s.upper()]
    if "-" in s:
        nd = s.replace("-", "")
        keys.extend([nd, nd.lower(), nd.upper()])
    out: List[str] = []
    seen = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _hashes_from_attachment_item(att_item: Any) -> List[str]:
    found: List[str] = []
    if isinstance(att_item, str):
        s = att_item.strip()
        if _looks_like_attachment_hash(s):
            found.append(s.replace("-", "").lower() if "-" in s else s)
        m = _ATTACHMENT_URL_RE.search(s)
        if m:
            g = m.group(1).replace("-", "").lower()
            if g not in found:
                found.append(g)
        return found
    if isinstance(att_item, dict):
        d = to_dict(att_item)
        for k in (
            "hash",
            "attachment_hash",
            "attachmentHash",
            "file_hash",
            "fileHash",
            "uuid",
            "UUID",
        ):
            v = d.get(k)
            if v is not None and _looks_like_attachment_hash(str(v)):
                found.append(str(v).replace("-", "").lower() if "-" in str(v) else str(v).strip())
        vid = d.get("id")
        if vid is not None and _looks_like_attachment_hash(str(vid)):
            found.append(str(vid).replace("-", "").lower())
        for uk in (
            "url",
            "URL",
            "link",
            "href",
            "thumb_url",
            "thumbnail_url",
            "preview_url",
            "src",
            "file_url",
        ):
            u = d.get(uk)
            if u:
                m = _ATTACHMENT_URL_RE.search(str(u))
                if m:
                    hx = m.group(1).replace("-", "").lower()
                    if hx not in found:
                        found.append(hx)
    return found


def _map_attachment_hashes(
    att_items: Any,
    attachment_mapping: Dict[str, str],
) -> List[str]:
    if not att_items or not attachment_mapping:
        return []
    if not isinstance(att_items, list):
        att_items = [att_items]
    out: List[str] = []
    seen = set()
    for att_item in att_items:
        for raw_h in _hashes_from_attachment_item(att_item):
            mapped_val = None
            for key in _candidate_hash_keys(raw_h):
                mapped_val = (
                    attachment_mapping.get(key)
                    or attachment_mapping.get(key.lower())
                    or attachment_mapping.get(key.upper())
                )
                if mapped_val:
                    break
            if mapped_val:
                ms = str(mapped_val).strip()
                if ms and ms not in seen:
                    seen.add(ms)
                    out.append(ms)
    return out


def _coalesce_result_attachments(result_dict: Dict[str, Any]) -> None:
    """
    Automated / reporter runs often store media under files, screenshots, execution.* — not only `attachments`.
    """
    merged: List[Any] = []
    seen_sig: set = set()

    def _add(items: Any) -> None:
        if not isinstance(items, list):
            return
        for it in items:
            sig = repr(it) if isinstance(it, dict) else str(it)
            if sig in seen_sig:
                continue
            seen_sig.add(sig)
            merged.append(it)

    _add(result_dict.get("attachments"))
    for key in ("files", "screenshots", "media", "images", "evidence", "artifacts"):
        _add(result_dict.get(key))
    ex = result_dict.get("execution")
    if isinstance(ex, dict):
        exd = to_dict(ex)
        _add(exd.get("attachments"))
        for key in ("files", "screenshots", "media", "images"):
            _add(exd.get(key))
    if merged:
        result_dict["attachments"] = merged


def _rewrite_text(
    text: Optional[str],
    attachment_mapping: Dict[str, str],
    target_workspace_hash: Optional[str],
) -> Optional[str]:
    if not text or not isinstance(text, str):
        return text
    if not attachment_mapping:
        return text
    return replace_attachment_hashes_in_text(text, attachment_mapping, target_workspace_hash)


def _iter_result_steps(result_dict: Dict[str, Any]) -> Optional[List[Any]]:
    for key in (
        "steps",
        "step_results",
        "elements",
        "result_steps",
        "test_results",
        "substeps",
    ):
        v = result_dict.get(key)
        if isinstance(v, list) and len(v) > 0:
            return v
    return None


def _pick_str(d: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _flatten_step_dict(step: Any) -> Dict[str, Any]:
    """Merge nested `data` and `execution` onto the step dict (manual + automated shapes)."""
    sd = to_dict(step) if not isinstance(step, dict) else dict(step)
    inner = sd.get("data")
    if isinstance(inner, dict):
        inn = to_dict(inner)
        for k, v in inn.items():
            if sd.get(k) in (None, "", [], {}):
                sd[k] = v
    for ex_key in ("execution", "Execution"):
        ex = sd.get(ex_key)
        if not isinstance(ex, dict):
            continue
        inn = to_dict(ex)
        for k, v in inn.items():
            if k in _STEP_EXECUTION_OVERLAY_KEYS:
                if k in ("status", "status_id"):
                    if v is not None:
                        sd[k] = v
                elif _is_nonempty_for_merge(v):
                    sd[k] = v
            elif sd.get(k) in (None, "", [], {}):
                sd[k] = v
    return sd


def _merge_result_top_level_execution(result_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Promote result-level `execution` (automated runs) into flat fields for v2 message/duration."""
    out = dict(result_dict)
    ex = out.get("execution")
    if not isinstance(ex, dict):
        return out
    exd = to_dict(ex)
    if _is_nonempty_for_merge(exd.get("comment")) and not _is_nonempty_for_merge(out.get("comment")):
        out["comment"] = exd["comment"]
    if _is_nonempty_for_merge(exd.get("message")) and not _is_nonempty_for_merge(out.get("message")):
        out["message"] = exd["message"]
    st = exd.get("stacktrace") or exd.get("stack_trace")
    if _is_nonempty_for_merge(st) and not _is_nonempty_for_merge(out.get("stacktrace")):
        out["stacktrace"] = st
    for ex_k, out_k in (
        ("time_spent_ms", "time_spent_ms"),
        ("time_ms", "time_spent_ms"),
        ("duration_ms", "time_spent_ms"),
    ):
        if ex_k in exd and exd[ex_k] is not None and out.get(out_k) in (None, 0, ""):
            try:
                out[out_k] = int(exd[ex_k])
            except (TypeError, ValueError):
                out[out_k] = exd[ex_k]
    dur = exd.get("duration")
    if dur is not None and out.get("time_spent_ms") in (None, 0, ""):
        try:
            out["time_spent_ms"] = int(float(dur))
        except (TypeError, ValueError):
            pass
    if not _iter_result_steps(out) and isinstance(exd.get("steps"), list) and len(exd["steps"]) > 0:
        out["steps"] = exd["steps"]
    return out


def _merge_result_detail_into_summary(summary: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    if not detail:
        return dict(summary)
    out = dict(summary)
    for k in (
        "steps",
        "comment",
        "message",
        "stacktrace",
        "attachments",
        "status",
        "status_id",
        "time_spent_ms",
        "member_id",
        "author_uuid",
        "files",
        "screenshots",
        "media",
        "images",
    ):
        if k not in detail:
            continue
        v = detail[k]
        if v is None:
            continue
        if k == "steps" and isinstance(v, list):
            prev = out.get("steps")
            if not isinstance(prev, list) or len(v) >= len(prev):
                out[k] = v
        elif k in ("files", "screenshots", "media", "images") and isinstance(v, list):
            prev = out.get(k)
            if not isinstance(prev, list) or len(v) > len(prev):
                out[k] = v
        else:
            out[k] = v
    for k, v in detail.items():
        if v is None or k in out:
            continue
        out[k] = v

    if not _pick_str(out, "title", "Title", "case_title", "caseTitle", "name", "Name"):
        ct = _pick_str(detail, "case_title", "caseTitle")
        if ct:
            out["title"] = ct
        else:
            case_d = detail.get("case")
            if isinstance(case_d, dict):
                t = _pick_str(case_d, "title", "Title", "name", "Name")
                if t:
                    out["title"] = t
    return out


def _merge_result_steps_with_case_steps(
    result_steps: List[Any],
    case_steps: List[Any],
    parent_status_id: Any = None,
) -> List[Dict[str, Any]]:
    """
    Overlay case step definitions (action / expected / shared ref) onto result rows.
    Case dict is merged first so result execution fields (status, attachments, comment) win.
    """
    rs = [_flatten_step_dict(x) for x in (result_steps or [])]
    cs = [_flatten_step_dict(x) for x in (case_steps or [])]
    if not cs:
        return rs
    if not rs:
        synth: List[Dict[str, Any]] = []
        for c in cs:
            d = dict(c)
            if d.get("status") is None and d.get("status_id") is None and parent_status_id is not None:
                d["status_id"] = parent_status_id
            synth.append(d)
        return synth

    out: List[Dict[str, Any]] = []
    n = max(len(rs), len(cs))
    for i in range(n):
        r = rs[i] if i < len(rs) else {}
        c = cs[i] if i < len(cs) else {}
        merged = {**c, **r}
        r_act = _pick_str(merged, "action", "Action", "title", "Title", "name", "Name")
        if not r_act or r_act == ".":
            c_act = _pick_str(c, "action", "Action", "title", "Title", "name", "Name")
            if c_act:
                merged["action"] = c_act
        r_exp = _pick_str(merged, "expected_result", "expectedResult", "expected", "Expected")
        if not r_exp:
            c_exp = _pick_str(c, "expected_result", "expectedResult", "expected", "Expected")
            if c_exp:
                merged["expected_result"] = c_exp
        rnest = r.get("steps") if isinstance(r.get("steps"), list) else None
        cnest = c.get("steps") if isinstance(c.get("steps"), list) else None
        if rnest is not None or cnest is not None:
            merged["steps"] = _merge_result_steps_with_case_steps(
                rnest or [],
                cnest or [],
                parent_status_id,
            )
        out.append(merged)
    return out


def _fetch_source_case_metadata(
    source_service: QaseService,
    project_code: str,
    case_id: int,
) -> Dict[str, Any]:
    """
    Single get_case for source workspace. Does not use retry_with_backoff so 404s stay quiet
    (deleted / missing cases still appear in old run results).
    """
    meta: Dict[str, Any] = {"steps": [], "title": None}
    try:
        cases_api = CasesApi(source_service.client)
        resp = cases_api.get_case(code=project_code, id=case_id)
        if resp and getattr(resp, "result", None):
            cd = to_dict(resp.result)
            meta["steps"] = cd.get("steps") or []
            t = _pick_str(cd, "title", "Title", "name", "Name")
            if t:
                meta["title"] = t
    except ApiException as e:
        if e.status == 404:
            logger.debug(
                "get_case 404 (case missing or removed): project=%s case_id=%s",
                project_code,
                case_id,
            )
        else:
            logger.warning("get_case failed project=%s case_id=%s: %s", project_code, case_id, e)
    except Exception as e:
        logger.debug("get_case error project=%s case_id=%s: %s", project_code, case_id, e)
    return meta


def _get_source_case_cached(
    cache: Dict[int, Dict[str, Any]],
    source_service: QaseService,
    project_code: str,
    case_id: int,
) -> Dict[str, Any]:
    if case_id in cache:
        return cache[case_id]
    meta = _fetch_source_case_metadata(source_service, project_code, case_id)
    cache[case_id] = meta
    return meta


def enrich_source_result_for_v2(
    result_dict: Dict[str, Any],
    source_service: QaseService,
    project_code: str,
    case_steps_cache: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    out = dict(result_dict)
    h = out.get("hash")
    if h:
        detail = fetch_result_detail_json(source_service, project_code, str(h))
        if detail:
            out = _merge_result_detail_into_summary(out, detail)

    out = _merge_result_top_level_execution(out)

    steps_list = _iter_result_steps(out)
    sid = out.get("case_id")
    if sid is not None:
        try:
            cid = int(sid)
        except (TypeError, ValueError):
            cid = None
        if cid is not None:
            meta = _get_source_case_cached(case_steps_cache, source_service, project_code, cid)
            case_steps = meta.get("steps") or []
            if meta.get("title") and not _pick_str(out, "title", "Title", "case_title", "caseTitle", "name", "Name"):
                out["_resolved_case_title"] = meta["title"]
            if case_steps:
                merged = _merge_result_steps_with_case_steps(
                    steps_list or [],
                    case_steps,
                    parent_status_id=out.get("status_id"),
                )
                if merged:
                    out["steps"] = merged
    _coalesce_result_attachments(out)
    return out


def _resolve_target_author_id(
    result_dict: Dict[str, Any],
    mappings: MigrationMappings,
    author_uuid_to_id_mapping: Dict[str, int],
) -> int:
    author_uuid = result_dict.get("author_uuid")
    if author_uuid:
        source_author_id = author_uuid_to_id_mapping.get(author_uuid)
        if source_author_id:
            try:
                sid = int(source_author_id)
                if sid == 0:
                    return 1
                return mappings.get_user_id(sid)
            except (ValueError, TypeError):
                pass

    for key in ("member_id", "author_id", "user_id", "created_by"):
        raw = result_dict.get(key)
        if raw is None:
            continue
        try:
            uid = int(raw)
            if uid == 0:
                return 1
            return mappings.get_user_id(uid)
        except (ValueError, TypeError):
            continue
    return 1


def _resolve_result_status_string(result_dict: Dict[str, Any]) -> str:
    status_map = {
        1: "passed",
        2: "blocked",
        3: "skipped",
        4: "retest",
        5: "failed",
    }
    status_id = result_dict.get("status_id")
    status_str = result_dict.get("status", "").lower() if result_dict.get("status") else None

    if status_id is not None and status_id in status_map:
        return status_map[status_id]
    if status_str:
        sl = status_str.lower()
        if sl in ("passed", "pass"):
            return "passed"
        if sl in ("failed", "fail"):
            return "failed"
        if sl in ("blocked", "block"):
            return "blocked"
        if sl in ("skipped", "skip"):
            return "skipped"
        if sl in ("retest", "retry"):
            return "retest"
        if sl in ("invalid",):
            return "invalid"
        if sl in ("in_progress", "in progress", "pending"):
            return "in_progress"
        if sl in ("untested",):
            return "untested"
        return "skipped"
    return "skipped"


def _map_v2_run_execution_status(core: str) -> str:
    """ResultExecution.status — coerce values that commonly 422 on historical runs."""
    c = (core or "skipped").lower()
    if c == "retest":
        return "blocked"
    if c == "in_progress":
        return "blocked"
    if c in ("passed", "failed", "blocked", "skipped", "invalid", "untested"):
        return c
    return "skipped"


def _map_step_status_v2(val: Any) -> ResultStepStatus:
    if val is None:
        return ResultStepStatus.PASSED
    if isinstance(val, str):
        v = val.lower().strip()
        if v in ("passed", "pass"):
            return ResultStepStatus.PASSED
        if v in ("failed", "fail"):
            return ResultStepStatus.FAILED
        if v in ("blocked", "block"):
            return ResultStepStatus.BLOCKED
        if v in ("skipped", "skip"):
            return ResultStepStatus.SKIPPED
        if v in ("in_progress", "in progress", "pending"):
            return ResultStepStatus.IN_PROGRESS
        return ResultStepStatus.PASSED
    if isinstance(val, int):
        return _STEP_INT_TO_V2.get(val, ResultStepStatus.PASSED)
    return ResultStepStatus.PASSED


def _result_duration_ms(result_dict: Dict[str, Any]) -> Optional[int]:
    for key in ("time_spent_ms", "time_ms", "duration_ms"):
        v = result_dict.get(key)
        if v is not None:
            try:
                n = int(v)
                return n if n >= 0 else None
            except (TypeError, ValueError):
                pass
    for key in ("time", "duration"):
        v = result_dict.get(key)
        if v is not None:
            try:
                n = int(v)
                return n * 1000 if n >= 0 else None
            except (TypeError, ValueError):
                pass
    return None


def _build_v2_step(
    step: Any,
    attachment_mapping: Dict[str, str],
    target_workspace_hash: Optional[str],
) -> Optional[ResultStep]:
    sd = _flatten_step_dict(step)
    if not sd:
        return None

    action = _pick_str(sd, "action", "Action", "title", "Title", "name", "Name") or "."
    er = _pick_str(sd, "expected_result", "expectedResult", "expected", "Expected")
    expected_result: Optional[str] = er or None

    data = ResultStepData(action=action, expected_result=expected_result)

    actual = _pick_str(
        sd,
        "actual_result",
        "actualResult",
        "actual",
        "Actual",
        "result",
        "Result",
        "body",
        "Body",
        "content",
        "Content",
        "output",
        "Output",
        "log",
        "Log",
        "stdout",
        "stderr",
        "error",
        "Error",
    ) or None
    extra_comment = _pick_str(sd, "comment", "Comment", "message", "Message", "text", "Text") or None
    parts: List[str] = []
    if actual:
        parts.append(f"**Actual result**\n{actual}")
    if extra_comment:
        parts.append(f"**Comment**\n{extra_comment}")
    comment_md = "\n\n".join(parts) if parts else None
    if comment_md:
        rw = _rewrite_text(comment_md, attachment_mapping, target_workspace_hash)
        comment_md = rw if rw else comment_md

    step_attachments = _map_attachment_hashes(sd.get("attachments"), attachment_mapping)
    step_status_raw = sd.get("status")
    if step_status_raw is None:
        step_status_raw = sd.get("status_id")
    step_exec_kwargs: Dict[str, Any] = {
        "status": _map_step_status_v2(step_status_raw),
        "start_time": None,
        "end_time": None,
    }
    if comment_md:
        step_exec_kwargs["comment"] = comment_md
    if step_attachments:
        step_exec_kwargs["attachments"] = step_attachments
    sdur = sd.get("duration") or sd.get("time_ms") or sd.get("time_spent_ms")
    if sdur is not None:
        try:
            step_exec_kwargs["duration"] = int(sdur)
        except (TypeError, ValueError):
            pass

    execution = ResultStepExecution(**step_exec_kwargs)

    nested = sd.get("steps")
    nested_dicts: Optional[List[Dict[str, Any]]] = None
    if nested and isinstance(nested, list):
        nested_dicts = []
        for ch in nested:
            child_rs = _build_v2_step(ch, attachment_mapping, target_workspace_hash)
            if child_rs:
                nested_dicts.append(child_rs.model_dump(by_alias=True, exclude_none=True))

    if nested_dicts:
        return ResultStep(
            data=data,
            execution=execution,
            steps=nested_dicts,
        )
    return ResultStep(data=data, execution=execution)


def _resolve_result_display_title(result_dict: Dict[str, Any], target_case_id: int) -> str:
    """Qase run result list/detail often omit `title`; use case blob, case_title, or cache."""
    title = _pick_str(
        result_dict,
        "title",
        "Title",
        "case_title",
        "caseTitle",
        "name",
        "Name",
    )
    if title:
        return title
    rt = result_dict.get("_resolved_case_title")
    if isinstance(rt, str) and rt.strip():
        return rt.strip()
    for key in ("case", "test_case", "TestCase"):
        c = result_dict.get(key)
        if isinstance(c, dict):
            title = _pick_str(c, "title", "Title", "name", "Name")
            if title:
                return title
    return f"Case {target_case_id}"


def _source_to_result_create_v2(
    result_dict: Dict[str, Any],
    target_case_id: int,
    mappings: MigrationMappings,
    author_uuid_to_id_mapping: Dict[str, int],
    attachment_mapping: Dict[str, str],
) -> ResultCreate:
    """Build one API v2 ResultCreate from a v1-style result payload."""
    _coalesce_result_attachments(result_dict)
    target_workspace_hash = getattr(mappings, "target_workspace_hash", None)
    core = _resolve_result_status_string(result_dict)

    title = _resolve_result_display_title(result_dict, target_case_id)

    duration_ms = _result_duration_ms(result_dict)
    stack_raw = result_dict.get("stacktrace")
    stacktrace = None
    if stack_raw:
        stacktrace = _rewrite_text(str(stack_raw), attachment_mapping, target_workspace_hash)
        if isinstance(stacktrace, str) and not stacktrace.strip():
            stacktrace = None

    execution = ResultExecution(
        start_time=None,
        end_time=None,
        status=_map_v2_run_execution_status(core),
        duration=duration_ms,
        stacktrace=stacktrace,
    )

    msg_parts: List[str] = []
    for key in ("comment", "message", "text"):
        t = result_dict.get(key)
        if t:
            msg_parts.append(str(t))
    message = "\n\n".join(msg_parts) if msg_parts else None
    if message:
        rw = _rewrite_text(message, attachment_mapping, target_workspace_hash)
        message = rw if rw else message
        if isinstance(message, str) and not message.strip():
            message = None

    res_attachments = _map_attachment_hashes(result_dict.get("attachments"), attachment_mapping)

    steps_src = _iter_result_steps(result_dict)
    steps_models: Optional[List[ResultStep]] = None
    if steps_src:
        built: List[ResultStep] = []
        for s in steps_src:
            rs = _build_v2_step(s, attachment_mapping, target_workspace_hash)
            if rs:
                built.append(rs)
        if built:
            steps_models = built

    params = result_dict.get("param")
    if isinstance(params, dict) and params:
        str_params = {str(k): str(v) if v is not None else "" for k, v in params.items()}
    else:
        str_params = None

    param_groups = result_dict.get("param_groups")
    if not isinstance(param_groups, list) or not param_groups:
        param_groups = None

    defect = result_dict.get("defect")
    defect_b: Optional[bool] = bool(defect) if defect is not None else None

    kwargs: Dict[str, Any] = {
        "title": title,
        "testops_id": int(target_case_id),
        "execution": execution,
    }
    if message:
        kwargs["message"] = message
    if res_attachments:
        kwargs["attachments"] = res_attachments
    if steps_models is not None:
        kwargs["steps"] = steps_models
    if str_params:
        kwargs["params"] = str_params
    if param_groups:
        kwargs["param_groups"] = param_groups
    if defect_b is not None:
        kwargs["defect"] = defect_b

    _resolve_target_author_id(result_dict, mappings, author_uuid_to_id_mapping)
    # v2 ResultCreate has no author_id; attribution is implicit / not migrated on v2 create

    return ResultCreate(**kwargs)


def _complete_target_run_safely(
    runs_api: RunsApi,
    project_code: str,
    run_id: int,
    max_attempts: int = 4,
    trace: Any = None,
    trace_extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Call complete_run without retry_with_backoff (avoids ERROR spam + swallowed failures on 4xx)."""
    delay = 1.0
    extra = dict(trace_extra or {})
    for attempt in range(max_attempts):
        try:
            runs_api.complete_run(code=project_code, id=run_id)
            logger.debug("complete_run ok project=%s run_id=%s", project_code, run_id)
            if trace:
                trace.event(
                    "run_complete_ok",
                    project_target=project_code,
                    target_run_id=run_id,
                    **extra,
                )
            return
        except ApiException as e:
            http_status = getattr(e, "status", None)
            if http_status == 429 and attempt < max_attempts - 1:
                time.sleep(delay)
                delay *= 2
                continue
            body = getattr(e, "body", None) or getattr(e, "reason", None) or str(e)
            logger.warning(
                "complete_run failed project=%s run_id=%s HTTP %s %s",
                project_code,
                run_id,
                http_status,
                (body[:500] if isinstance(body, str) else body),
            )
            if trace:
                trace.event(
                    "run_complete_failed",
                    project_target=project_code,
                    target_run_id=run_id,
                    http_status=http_status,
                    error_body=(body[:1200] if isinstance(body, str) else str(body)[:1200]),
                    **extra,
                )
            return
        except Exception as e:
            logger.warning(
                "complete_run failed project=%s run_id=%s: %s",
                project_code,
                run_id,
                e,
            )
            if trace:
                trace.event(
                    "run_complete_failed",
                    project_target=project_code,
                    target_run_id=run_id,
                    error=str(e),
                    **extra,
                )
            return


def _create_results_v2_with_retry(
    results_api: ResultsApi,
    project_code: str,
    run_id: int,
    request: CreateResultsRequestV2,
    max_attempts: int = 4,
    trace: Any = None,
    trace_ctx: Optional[Dict[str, Any]] = None,
    full_payloads: bool = False,
) -> bool:
    delay = 1.0
    ctx = dict(trace_ctx or {})
    for attempt in range(max_attempts):
        try:
            results_api.create_results_v2(project_code, run_id, request)
            if trace:
                trace.event(
                    "results_v2_api_ok",
                    project_target=project_code,
                    target_run_id=run_id,
                    attempt=attempt + 1,
                    **ctx,
                )
            return True
        except ApiExceptionV2 as e:
            http_status = getattr(e, "status", None)
            if http_status == 429 and attempt < max_attempts - 1:
                time.sleep(delay)
                delay *= 2
                continue
            body = getattr(e, "body", None) or str(e)
            logger.error(
                "create_results_v2 failed run=%s: HTTP %s %s",
                run_id,
                http_status,
                (body[:800] if isinstance(body, str) else body),
            )
            if trace:
                payload = None
                req_results = getattr(request, "results", None)
                if full_payloads and req_results:
                    try:
                        payload = chunk_results_payload_for_trace(
                            list(req_results), full_payloads=True
                        )
                    except Exception:
                        payload = None
                trace.event(
                    "results_v2_api_failed",
                    project_target=project_code,
                    target_run_id=run_id,
                    http_status=http_status,
                    error_body=(body[:2000] if isinstance(body, str) else str(body)[:2000]),
                    payload_full=payload,
                    **ctx,
                )
            return False
        except Exception as e:
            logger.error("create_results_v2 unexpected error run=%s: %s", run_id, e, exc_info=True)
            if trace:
                trace.event(
                    "results_v2_api_error",
                    project_target=project_code,
                    target_run_id=run_id,
                    error=str(e),
                    **ctx,
                )
            return False
    return False


def migrate_results(
    source_service: QaseService,
    target_service: QaseService,
    project_code_source: str,
    project_code_target: str,
    run_mapping: Dict[int, int],
    case_mapping: Dict[int, int],
    mappings: MigrationMappings,
    stats: MigrationStats,
) -> Dict[str, str]:
    """
    Migrate test results via API v2 bulk create, then map hashes using v1 list API.
    """
    runs_api_target = RunsApi(target_service.client)
    result_hash_mapping: Dict[str, str] = {}

    author_uuid_to_id_mapping = extract_authors(source_service)
    mappings.author_uuid_to_id_mapping = author_uuid_to_id_mapping

    attachment_mapping: Dict[str, str] = {}
    if project_code_source in mappings.attachments:
        attachment_mapping = dict(mappings.attachments[project_code_source])
        normalized_mapping: Dict[str, str] = {}
        for key, value in attachment_mapping.items():
            normalized_mapping[str(key).lower()] = value
            normalized_mapping[str(key)] = value
        attachment_mapping = normalized_mapping

    results_api_v2 = ResultsApi(target_service.client_v2)

    total_results = 0
    created_results = 0
    case_steps_cache: Dict[int, Dict[str, Any]] = {}
    trace = getattr(mappings, "trace", None)
    trace_full = bool(getattr(trace, "full_payloads", False)) if trace else False

    if trace:
        trace.event(
            "results_phase_start",
            project_source=project_code_source,
            project_target=project_code_target,
            n_runs_in_mapping=len(run_mapping),
            attachment_mapping_size=len(attachment_mapping),
        )

    for source_run_id, target_run_id in run_mapping.items():
        source_results = extract_results(source_service, project_code_source, source_run_id)
        if not source_results:
            if trace:
                trace.event(
                    "results_run_empty_extract",
                    project_source=project_code_source,
                    source_run_id=source_run_id,
                    target_run_id=target_run_id,
                )
            continue

        if trace:
            trace.event(
                "results_run_extracted",
                project_source=project_code_source,
                source_run_id=source_run_id,
                target_run_id=target_run_id,
                n_raw_results=len(source_results),
            )

        batch_rows: List[Dict[str, Any]] = []

        for raw in source_results:
            total_results += 1
            result_dict = raw if isinstance(raw, dict) else to_dict(raw)
            raw_preview = {
                "case_id": result_dict.get("case_id"),
                "hash": result_dict.get("hash"),
                "status_id": result_dict.get("status_id"),
            }
            result_dict = enrich_source_result_for_v2(
                result_dict,
                source_service,
                project_code_source,
                case_steps_cache,
            )

            source_case_id = result_dict.get("case_id")
            lookup_id: Any = source_case_id
            if source_case_id is not None:
                try:
                    lookup_id = int(source_case_id)
                except (TypeError, ValueError):
                    pass
            target_case_id = case_mapping.get(lookup_id)
            if target_case_id is None and source_case_id is not None:
                target_case_id = case_mapping.get(source_case_id)
            if not target_case_id:
                if trace:
                    trace.event(
                        "result_skipped_no_target_case",
                        project_source=project_code_source,
                        source_run_id=source_run_id,
                        target_run_id=target_run_id,
                        source_case_id=source_case_id,
                        lookup_id=lookup_id,
                        raw_preview=raw_preview,
                        enriched_summary=summarize_enriched_result(result_dict),
                    )
                continue

            try:
                rc = _source_to_result_create_v2(
                    result_dict,
                    target_case_id,
                    mappings,
                    author_uuid_to_id_mapping,
                    attachment_mapping,
                )
                batch_rows.append({"source": result_dict, "create": rc})
                if trace:
                    trace.event(
                        "result_row_built",
                        project_source=project_code_source,
                        source_run_id=source_run_id,
                        target_run_id=target_run_id,
                        source_case_id=source_case_id,
                        target_case_id=target_case_id,
                        raw_preview=raw_preview,
                        enriched=summarize_enriched_result(result_dict),
                        v2_create_summary=summarize_result_create(rc),
                    )
            except Exception as e:
                logger.error("Error building v2 ResultCreate: %s", e, exc_info=True)
                if trace:
                    trace.event(
                        "result_build_failed",
                        project_source=project_code_source,
                        source_run_id=source_run_id,
                        target_run_id=target_run_id,
                        source_case_id=source_case_id,
                        error=str(e),
                        enriched_summary=summarize_enriched_result(result_dict),
                    )
                continue

        if not batch_rows:
            if trace:
                trace.event(
                    "results_run_no_rows_to_send",
                    project_source=project_code_source,
                    source_run_id=source_run_id,
                    target_run_id=target_run_id,
                    n_raw_results=len(source_results),
                )
            continue

        for chunk_idx, chunk_rows in enumerate(chunks(batch_rows, 500)):
            results_list: List[ResultCreate] = [r["create"] for r in chunk_rows]
            request = CreateResultsRequestV2(results=results_list)

            if trace:
                trace.event(
                    "results_chunk_payload",
                    project_source=project_code_source,
                    source_run_id=source_run_id,
                    target_run_id=target_run_id,
                    chunk_index=chunk_idx,
                    chunk_size=len(chunk_rows),
                    payload=chunk_results_payload_for_trace(results_list, trace_full),
                )

            if _create_results_v2_with_retry(
                results_api_v2,
                project_code_target,
                int(target_run_id),
                request,
                trace=trace,
                trace_ctx={
                    "project_source": project_code_source,
                    "source_run_id": source_run_id,
                    "chunk_index": chunk_idx,
                    "chunk_size": len(chunk_rows),
                },
                full_payloads=trace_full,
            ):
                created_results += len(chunk_rows)
                try:
                    target_results = extract_results(
                        target_service, project_code_target, int(target_run_id)
                    )
                    for row in chunk_rows:
                        sd = row["source"]
                        sh = sd.get("hash")
                        tcid = case_mapping.get(sd.get("case_id"))
                        if not tcid:
                            continue
                        for tr in target_results:
                            trd = tr if isinstance(tr, dict) else to_dict(tr)
                            if trd.get("case_id") == tcid:
                                th = trd.get("hash")
                                if sh and th:
                                    result_hash_mapping[str(sh)] = str(th)
                                break
                except Exception as hash_error:
                    logger.warning(
                        "Could not map result hashes after v2 bulk run %s: %s",
                        target_run_id,
                        hash_error,
                    )
            else:
                logger.error(
                    "V2 results bulk failed for run %s (%s results in chunk)",
                    target_run_id,
                    len(chunk_rows),
                )
                if trace:
                    trace.event(
                        "results_chunk_skipped_after_fail",
                        project_source=project_code_source,
                        source_run_id=source_run_id,
                        target_run_id=target_run_id,
                        chunk_index=chunk_idx,
                    )

    stats.add_entity("results", total_results, created_results)

    if hasattr(mappings, "_runs_to_complete") and project_code_source in mappings._runs_to_complete:
        runs_to_complete = mappings._runs_to_complete[project_code_source]
        if trace:
            trace.event(
                "results_complete_run_phase_start",
                project_source=project_code_source,
                n_runs_to_complete=len(runs_to_complete),
                target_run_ids=list(runs_to_complete.keys()),
            )
        for target_run_id, run_info in runs_to_complete.items():
            if run_info.get("is_completed"):
                _complete_target_run_safely(
                    runs_api_target,
                    str(run_info["project_code"]),
                    int(target_run_id),
                    trace=trace,
                    trace_extra={
                        "project_source": project_code_source,
                        "source_flags": {
                            "source_is_completed": run_info.get("source_is_completed"),
                            "has_end_time": run_info.get("has_end_time"),
                        },
                    },
                )

    if trace:
        trace.event(
            "results_phase_end",
            project_source=project_code_source,
            total_results_seen=total_results,
            created_results=created_results,
        )

    if project_code_source not in mappings.result_hashes:
        mappings.result_hashes[project_code_source] = {}
    mappings.result_hashes[project_code_source].update(result_hash_mapping)

    return result_hash_mapping
