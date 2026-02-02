import argparse
import json
from typing import Dict, List, Tuple, Optional

import pandas as pd
from deepdiff import DeepDiff
import requests


def load_data(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception as e:
        raise RuntimeError(f"Failed to load {path}: {e}")


def infer_schema(df: pd.DataFrame) -> Dict[str, str]:
    return {c: str(t) for c, t in df.dtypes.to_dict().items()}


def compare_columns(old: pd.DataFrame, new: pd.DataFrame) -> Tuple[List[str], List[str], List[str]]:
    old_cols = set(old.columns)
    new_cols = set(new.columns)
    added = sorted(list(new_cols - old_cols))
    removed = sorted(list(old_cols - new_cols))
    common = sorted(list(old_cols & new_cols))
    return added, removed, common


def compare_types(old_schema: Dict[str, str], new_schema: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    diffs = {}
    for col, old_t in old_schema.items():
        if col in new_schema and new_schema[col] != old_t:
            diffs[col] = {"old": old_t, "new": new_schema[col]}
    return diffs


def null_rate(df: pd.DataFrame) -> Dict[str, float]:
    return (df.isna().sum() / len(df)).to_dict()


def numeric_stats(df: pd.DataFrame, cols: List[str]) -> Dict[str, Dict[str, float]]:
    stats = {}
    for c in cols:
        try:
            s = pd.to_numeric(df[c], errors="coerce")
            stats[c] = {"mean": float(s.mean()), "std": float(s.std())}
        except Exception:
            stats[c] = {"mean": None, "std": None}
    return stats


def send_slack_block_alert(webhook_url: str, report: Dict, report_url: Optional[str] = None, acknowledge_url: Optional[str] = None, interactive: bool = False) -> bool:
    """Send a Slack Block Kit message summarizing drift and linking to the JSON report (if provided).

    `report` should be the dict returned by `detect_drift`.
    Returns True on success.
    """
    try:
        summary = report.get("summary", {})
        added = summary.get("added_columns", [])
        removed = summary.get("removed_columns", [])
        type_changes = summary.get("type_changes", {})

        fields = []
        if added:
            fields.append({"type": "mrkdwn", "text": f"*Added:* {', '.join(added[:10])}"})
        if removed:
            fields.append({"type": "mrkdwn", "text": f"*Removed:* {', '.join(removed[:10])}"})
        if type_changes:
            tc_items = [f"{c}: {v['old']}→{v['new']}" for c, v in type_changes.items()]
            fields.append({"type": "mrkdwn", "text": f"*Type changes:* {', '.join(tc_items[:10])}"})

        # Null-rate and numeric messages (limit to 8 columns)
        null_changes = report.get("null_rate_changes", {})
        if null_changes:
            nitems = [f"{c}: {v['old_null_rate']:.2f}→{v['new_null_rate']:.2f}" for c, v in list(null_changes.items())[:8]]
            fields.append({"type": "mrkdwn", "text": f"*Null-rate changes:* {', '.join(nitems)}"})

        numeric_changes = report.get("numeric_distribution_changes", {})
        if numeric_changes:
            nitems = [f"{c}: {','.join(r['reasons'])}" for c, r in list(numeric_changes.items())[:8]]
            fields.append({"type": "mrkdwn", "text": f"*Numeric changes:* {', '.join(nitems)}"})

        blocks = []
        blocks.append({"type": "header", "text": {"type": "plain_text", "text": "Schema Drift Detected"}})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"Detected schema/statistical drift between `{report.get('old_path')}` and `{report.get('new_path')}`."}})

        if fields:
            # Split fields into two-column layout when many
            blocks.append({"type": "section", "fields": fields})

        if report_url:
            # Add a context block with link to the report
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"Report: <{report_url}|Open JSON report>"}})


        # Action buttons: Open report (always present if report_url).
        # For acknowledgement we support two modes:
        # - interactive=True: send a button with an action_id/value so Slack will POST an action payload to your app's Request URL.
        # - interactive=False and acknowledge_url provided: include a regular URL button that opens the given acknowledge_url.
        actions = {"type": "actions", "elements": []}
        if report_url:
            actions["elements"].append({
                "type": "button",
                "text": {"type": "plain_text", "text": "Open report"},
                "url": report_url,
            })

        if interactive:
            # interactive acknowledgement: use an action_id and include the report path/value
            # value should be a short identifier; include report path for convenience
            val = report.get("new_path", report_url or "")
            actions["elements"].append({
                "type": "button",
                "text": {"type": "plain_text", "text": "Acknowledge"},
                "action_id": "ack_button",
                "value": val,
                "style": "primary",
            })
        elif acknowledge_url:
            actions["elements"].append({
                "type": "button",
                "text": {"type": "plain_text", "text": "Acknowledge"},
                "url": acknowledge_url,
                "style": "primary",
            })

        if actions["elements"]:
            blocks.append(actions)

        payload = {"blocks": blocks}
        resp = requests.post(webhook_url, json=payload, timeout=5)
        return 200 <= resp.status_code < 300
    except Exception:
        return False


def _lookup_user_id_by_email(bot_token: str, email: str) -> Optional[str]:
    url = "https://slack.com/api/users.lookupByEmail"
    headers = {"Authorization": f"Bearer {bot_token}"}
    params = {"email": email}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=5)
        j = r.json()
        if j.get("ok"):
            return j["user"]["id"]
    except Exception:
        pass
    return None


def send_slack_dm(bot_token: str, user_id: Optional[str], user_email: Optional[str], report: Dict, report_url: Optional[str] = None, interactive: bool = False) -> bool:
    """Send the report as a direct message to a Slack user.

    Provide either `user_id` or `user_email`. Returns True on success.
    """
    try:
        if not user_id and user_email:
            user_id = _lookup_user_id_by_email(bot_token, user_email)
            if not user_id:
                return False

        # Open or get an IM channel
        open_url = "https://slack.com/api/conversations.open"
        headers = {"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json; charset=utf-8"}
        payload = {"users": user_id}
        r = requests.post(open_url, headers=headers, json=payload, timeout=5)
        j = r.json()
        if not j.get("ok"):
            return False
        channel = j.get("channel", {}).get("id")
        if not channel:
            return False

        # Build blocks similar to webhook sender
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": "Schema Drift Detected"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"Detected schema/statistical drift between `{report.get('old_path')}` and `{report.get('new_path')}`."}},
        ]
        summary = report.get("summary", {})
        fields = []
        if summary.get("added_columns"):
            fields.append({"type": "mrkdwn", "text": f"*Added:* {', '.join(summary.get('added_columns')[:10])}"})
        if summary.get("removed_columns"):
            fields.append({"type": "mrkdwn", "text": f"*Removed:* {', '.join(summary.get('removed_columns')[:10])}"})
        if summary.get("type_changes"):
            tc_items = [f"{c}: {v['old']}→{v['new']}" for c, v in summary.get("type_changes", {}).items()]
            fields.append({"type": "mrkdwn", "text": f"*Type changes:* {', '.join(tc_items[:10])}"})

        null_changes = report.get("null_rate_changes", {})
        if null_changes:
            nitems = [f"{c}: {v['old_null_rate']:.2f}→{v['new_null_rate']:.2f}" for c, v in list(null_changes.items())[:8]]
            fields.append({"type": "mrkdwn", "text": f"*Null-rate changes:* {', '.join(nitems)}"})

        numeric_changes = report.get("numeric_distribution_changes", {})
        if numeric_changes:
            nitems = [f"{c}: {','.join(r['reasons'])}" for c, r in list(numeric_changes.items())[:8]]
            fields.append({"type": "mrkdwn", "text": f"*Numeric changes:* {', '.join(nitems)}"})

        if fields:
            blocks.append({"type": "section", "fields": fields})

        if report_url:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"Report: <{report_url}|Open JSON report>"}})

        actions = {"type": "actions", "elements": []}
        if report_url:
            actions["elements"].append({"type": "button", "text": {"type": "plain_text", "text": "Open report"}, "url": report_url})
        if interactive:
            # interactive: use action_id/value; Slack will post action payload to your app Request URL
            val = report.get("new_path", report_url or "")
            actions["elements"].append({"type": "button", "text": {"type": "plain_text", "text": "Acknowledge"}, "action_id": "ack_button", "value": val, "style": "primary"})
        if actions["elements"]:
            blocks.append(actions)

        post_url = "https://slack.com/api/chat.postMessage"
        post_payload = {"channel": channel, "blocks": blocks, "text": "Schema drift detected"}
        r2 = requests.post(post_url, headers=headers, json=post_payload, timeout=5)
        j2 = r2.json()
        return j2.get("ok", False)
    except Exception:
        return False


def detect_drift(old_path: str, new_path: str, mean_thresh: float = 0.25, std_thresh: float = 0.4, null_thresh: float = 0.15) -> Dict:
    old = load_data(old_path)
    new = load_data(new_path)

    old_schema = infer_schema(old)
    new_schema = infer_schema(new)

    added_cols, removed_cols, common_cols = compare_columns(old, new)
    type_changes = compare_types(old_schema, new_schema)

    # DeepDiff for a compact schema diff
    schema_diff = DeepDiff(old_schema, new_schema, verbose_level=2)

    old_null = null_rate(old)
    new_null = null_rate(new)

    null_changes = {}
    for c in set(list(old_null.keys()) + list(new_null.keys())):
        old_n = float(old_null.get(c, 0.0))
        new_n = float(new_null.get(c, 0.0))
        if abs(new_n - old_n) >= null_thresh:
            null_changes[c] = {"old_null_rate": old_n, "new_null_rate": new_n}

    # Numeric column drift by mean/std relative change
    numeric_cols = [c for c, t in old_schema.items() if "int" in t or "float" in t]
    numeric_cols = [c for c in numeric_cols if c in common_cols]
    old_stats = numeric_stats(old, numeric_cols)
    new_stats = numeric_stats(new, numeric_cols)

    numeric_changes = {}
    for c in numeric_cols:
        o_mean = old_stats[c]["mean"]
        n_mean = new_stats[c]["mean"]
        o_std = old_stats[c]["std"]
        n_std = new_stats[c]["std"]
        try:
            mean_rel = abs(n_mean - o_mean) / (abs(o_mean) + 1e-9)
        except Exception:
            mean_rel = None
        try:
            std_rel = abs(n_std - o_std) / (abs(o_std) + 1e-9)
        except Exception:
            std_rel = None

        flagged = False
        reasons = []
        if mean_rel is not None and mean_rel >= mean_thresh:
            flagged = True
            reasons.append(f"mean_change={mean_rel:.2f}")
        if std_rel is not None and std_rel >= std_thresh:
            flagged = True
            reasons.append(f"std_change={std_rel:.2f}")

        if flagged:
            numeric_changes[c] = {
                "old": old_stats[c],
                "new": new_stats[c],
                "mean_rel_change": mean_rel,
                "std_rel_change": std_rel,
                "reasons": reasons,
            }

    report = {
        "old_path": old_path,
        "new_path": new_path,
        "summary": {
            "added_columns": added_cols,
            "removed_columns": removed_cols,
            "type_changes": type_changes,
            "schema_diff": schema_diff.to_dict() if hasattr(schema_diff, 'to_dict') else str(schema_diff),
        },
        "null_rate_changes": null_changes,
        "numeric_distribution_changes": numeric_changes,
    }
    return report


def main():
    parser = argparse.ArgumentParser(description="Detect schema and basic statistical drift between two CSV files.")
    parser.add_argument("old", help="Path to reference/old CSV file")
    parser.add_argument("new", help="Path to new CSV file to test")
    parser.add_argument("--out", help="Path to write JSON report (optional)")
    parser.add_argument("--mean-thresh", type=float, default=0.25, help="Relative mean change threshold")
    parser.add_argument("--std-thresh", type=float, default=0.4, help="Relative std change threshold")
    parser.add_argument("--null-thresh", type=float, default=0.15, help="Absolute null-rate change threshold")

    args = parser.parse_args()

    report = detect_drift(args.old, args.new, mean_thresh=args.mean_thresh, std_thresh=args.std_thresh, null_thresh=args.null_thresh)

    print(json.dumps(report, indent=2))

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
