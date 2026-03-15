#!/usr/bin/env python3
import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

STRUCTURED_EVENT_KEYS = {
    "client_request_received",
    "client_request_dedup_hit",
    "client_request_pending_duplicate",
    "client_request_rejected_recovering",
    "client_request_append_rejected",
    "client_request_appended",
    "command_decided_seen",
    "command_already_executed",
    "command_applied_in_memory",
    "command_applied_persisted",
    "command_apply_complete",
    "reply_suppressed",
    "client_reply_sent",
    "recovery_mode_entered",
    "recovery_state_ready",
    "recovery_complete",
    "fresh_start",
}

DECIDED_RE = re.compile(r"local decided_idx=(\d+), replay_from=(\d+)")


def load_log_lines(path: Path) -> List[str]:
    return path.read_text(encoding="utf-8", errors="replace").splitlines()



def parse_structured_events(lines: List[str]) -> List[dict]:
    events = []
    for line in lines:
        s = line.strip()
        if not s or not s.startswith("{"):
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict) and obj.get("event") in STRUCTURED_EVENT_KEYS:
            events.append(obj)
    return events



def parse_plain_recovery_markers(lines: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "fresh_start": False,
        "restored_from_storage": False,
        "local_decided_idx": None,
        "replay_from": None,
        "raw_markers": [],
    }
    for line in lines:
        low = line.lower()
        if "fresh start" in low:
            out["fresh_start"] = True
            out["raw_markers"].append(line)
        if "restored paxos storage" in low:
            out["restored_from_storage"] = True
            out["raw_markers"].append(line)
            m = DECIDED_RE.search(line)
            if m:
                out["local_decided_idx"] = int(m.group(1))
                out["replay_from"] = int(m.group(2))
    return out



def summarize_node(path: Path) -> Dict[str, Any]:
    lines = load_log_lines(path)
    events = parse_structured_events(lines)
    markers = parse_plain_recovery_markers(lines)

    counts = Counter(ev.get("event") for ev in events)

    recovery_enter = [ev for ev in events if ev.get("event") == "recovery_mode_entered"]
    recovery_ready = [ev for ev in events if ev.get("event") == "recovery_state_ready"]
    recovery_complete = [ev for ev in events if ev.get("event") == "recovery_complete"]

    decided = [ev for ev in events if ev.get("event") == "command_decided_seen"]
    applied = [ev for ev in events if ev.get("event") == "command_applied_persisted"]
    replies = [ev for ev in events if ev.get("event") == "client_reply_sent"]
    suppress = [ev for ev in events if ev.get("event") == "reply_suppressed"]
    append_rejected = [ev for ev in events if ev.get("event") == "client_request_append_rejected"]
    append_ok = [ev for ev in events if ev.get("event") == "client_request_appended"]

    ready_ts = recovery_ready[-1]["ts_ms"] if recovery_ready else None
    complete_ts = recovery_complete[-1]["ts_ms"] if recovery_complete else None

    ready_recovering_flag = None
    if recovery_ready:
        fields = recovery_ready[-1].get("fields", {})
        if isinstance(fields, dict):
            ready_recovering_flag = fields.get("recovering")

    effective_complete_ts = complete_ts
    implicit_recovery_complete = False
    if effective_complete_ts is None and ready_ts is not None and ready_recovering_flag is False:
        effective_complete_ts = ready_ts
        implicit_recovery_complete = True

    def after_ts(items: List[dict], ts: Optional[int]) -> List[dict]:
        if ts is None:
            return []
        return [x for x in items if isinstance(x.get("ts_ms"), int) and x["ts_ms"] >= ts]

    post_ready_decided = after_ts(decided, ready_ts)
    post_ready_applied = after_ts(applied, ready_ts)
    post_ready_replies = after_ts(replies, ready_ts)
    post_ready_suppress = after_ts(suppress, ready_ts)

    post_complete_decided = after_ts(decided, effective_complete_ts)
    post_complete_applied = after_ts(applied, effective_complete_ts)
    post_complete_replies = after_ts(replies, effective_complete_ts)
    post_complete_suppress = after_ts(suppress, effective_complete_ts)

    has_recovery_cycle = bool(recovery_enter or markers["restored_from_storage"] or recovery_ready)
    caught_up_evidence = bool(post_complete_applied or post_complete_decided or post_complete_replies or post_complete_suppress)
    served_client_after_recovery = bool(post_complete_replies)
    follower_catchup_after_recovery = bool(post_complete_applied or post_complete_decided or post_complete_suppress)

    req_ids_decided = {
        ev.get("fields", {}).get("req_id")
        for ev in decided
        if isinstance(ev.get("fields"), dict)
    }
    req_ids_applied = {
        ev.get("fields", {}).get("req_id")
        for ev in applied
        if isinstance(ev.get("fields"), dict)
    }
    req_ids_replied = {
        ev.get("fields", {}).get("req_id")
        for ev in replies
        if isinstance(ev.get("fields"), dict)
    }

    suspicious_reply_req_ids = sorted(
        rid for rid in req_ids_replied if rid is not None and rid not in req_ids_decided and rid not in req_ids_applied
    )

    node_name = path.stem.replace("docker-", "")
    return {
        "node": node_name,
        "log_file": str(path),
        "markers": markers,
        "event_counts": dict(counts),
        "recovery": {
            "has_recovery_cycle": has_recovery_cycle,
            "recovery_mode_entered": len(recovery_enter) > 0,
            "recovery_state_ready": len(recovery_ready) > 0,
            "recovery_complete": len(recovery_complete) > 0,
            "implicit_recovery_complete": implicit_recovery_complete,
            "ready_recovering_flag": ready_recovering_flag,
            "ready_ts_ms": ready_ts,
            "complete_ts_ms": complete_ts,
            "effective_complete_ts_ms": effective_complete_ts,
            "post_ready": {
                "decided_seen": len(post_ready_decided),
                "applied_persisted": len(post_ready_applied),
                "reply_sent": len(post_ready_replies),
                "reply_suppressed": len(post_ready_suppress),
            },
            "post_complete": {
                "decided_seen": len(post_complete_decided),
                "applied_persisted": len(post_complete_applied),
                "reply_sent": len(post_complete_replies),
                "reply_suppressed": len(post_complete_suppress),
            },
            "served_client_after_recovery": served_client_after_recovery,
            "follower_catchup_after_recovery": follower_catchup_after_recovery,
            "caught_up_evidence": caught_up_evidence,
        },
        "quorum_reply_validation": {
            "reply_events": len(replies),
            "decided_events": len(decided),
            "applied_events": len(applied),
            "append_ok_events": len(append_ok),
            "append_rejected_events": len(append_rejected),
            "suspicious_reply_req_ids": suspicious_reply_req_ids[:50],
            "replys_without_local_decided_or_applied_count": len(suspicious_reply_req_ids),
            "looks_consistent": len(suspicious_reply_req_ids) == 0,
        },
        "samples": {
            "first_recovery_ready": recovery_ready[0] if recovery_ready else None,
            "first_recovery_complete": recovery_complete[0] if recovery_complete else None,
            "first_post_complete_applied": post_complete_applied[0] if post_complete_applied else None,
            "first_post_complete_reply": post_complete_replies[0] if post_complete_replies else None,
            "first_post_complete_suppressed": post_complete_suppress[0] if post_complete_suppress else None,
        },
    }



def build_global_report(nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary = {
        "nodes": [n["node"] for n in nodes],
        "all_restored_from_storage": all(n["markers"]["restored_from_storage"] for n in nodes),
        "all_recovery_complete_logged": all(
            n["recovery"]["recovery_complete"] or n["recovery"]["implicit_recovery_complete"]
            for n in nodes
        ),
        "all_nodes_have_caught_up_evidence": all(n["recovery"]["caught_up_evidence"] for n in nodes),
        "nodes_served_client_after_recovery": [n["node"] for n in nodes if n["recovery"]["served_client_after_recovery"]],
        "nodes_with_follower_catchup_after_recovery": [n["node"] for n in nodes if n["recovery"]["follower_catchup_after_recovery"]],
        "nodes_with_quorum_reply_consistency": [n["node"] for n in nodes if n["quorum_reply_validation"]["looks_consistent"]],
    }
    return {
        "summary": summary,
        "nodes": {n["node"]: n for n in nodes},
    }



def main() -> None:
    ap = argparse.ArgumentParser(description="Parse OmniPaxos node docker logs and summarize recovery/quorum evidence.")
    ap.add_argument("--logs", nargs="+", required=True, help="Paths to docker-n1.log docker-n2.log docker-n3.log ...")
    ap.add_argument("--out", default="recovery_log_summary.json", help="Output JSON path")
    args = ap.parse_args()

    node_reports = [summarize_node(Path(p)) for p in args.logs]
    report = build_global_report(node_reports)

    out_path = Path(args.out)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print("===== Recovery/Quorum Log Summary =====")
    print(f"all_restored_from_storage: {report['summary']['all_restored_from_storage']}")
    print(f"all_recovery_complete_logged: {report['summary']['all_recovery_complete_logged']}")
    print(f"all_nodes_have_caught_up_evidence: {report['summary']['all_nodes_have_caught_up_evidence']}")
    print(f"nodes_served_client_after_recovery: {report['summary']['nodes_served_client_after_recovery']}")
    print(f"nodes_with_follower_catchup_after_recovery: {report['summary']['nodes_with_follower_catchup_after_recovery']}")
    print(f"nodes_with_quorum_reply_consistency: {report['summary']['nodes_with_quorum_reply_consistency']}")
    print(f"written: {out_path}")


if __name__ == "__main__":
    main()
