import os
import json
import hmac
import hashlib
import requests
from flask import Flask, request, make_response
from datetime import datetime

app = Flask(__name__)

SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET")
ACK_LOG = os.environ.get("ACK_LOG_PATH", "ack_log.jsonl")


def verify_slack_request(req) -> bool:
    """Verify Slack request using signing secret."""
    if not SIGNING_SECRET:
        return False
    timestamp = req.headers.get("X-Slack-Request-Timestamp", "")
    sig = req.headers.get("X-Slack-Signature", "")
    if not timestamp or not sig:
        return False
    # Prevent replay attacks (allow 5 minutes)
    try:
        req_ts = int(timestamp)
    except ValueError:
        return False
    if abs(datetime.utcnow().timestamp() - req_ts) > 60 * 5:
        return False

    body = req.get_data(as_text=True)
    basestring = f"v0:{timestamp}:{body}".encode("utf-8")
    my_sig = "v0=" + hmac.new(SIGNING_SECRET.encode("utf-8"), basestring, hashlib.sha256).hexdigest()
    return hmac.compare_digest(my_sig, sig)


@app.route("/slack/actions", methods=["POST"])
def slack_actions():
    # Slack sends a form-encoded 'payload' field containing JSON
    try:
        # Verify signature
        if not verify_slack_request(request):
            return make_response("Invalid request signature", 401)

        payload = request.form.get("payload")
        if not payload:
            return make_response("No payload", 400)

        data = json.loads(payload)
        # Write acknowledgement to a local log file
        ack_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "payload": data,
        }
        with open(ACK_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(ack_entry) + "\n")

        # If Slack provided a response_url, update the original message to show acknowledged state
        response_url = data.get("response_url")
        try:
            user = data.get("user", {})
            user_name = user.get("username") or user.get("name") or user.get("id")
            ack_text = f"Acknowledged by {user_name} at {datetime.utcnow().isoformat()}Z"

            if response_url:
                # Build a simple replacement message
                new_blocks = [
                    {"type": "section", "text": {"type": "mrkdwn", "text": ack_text}},
                ]
                # POST to the response_url to update the original message
                requests.post(response_url, json={"replace_original": True, "blocks": new_blocks}, timeout=5)
        except Exception:
            # Non-fatal: proceed to return success to Slack
            pass

        # Respond to Slack quickly
        return make_response("", 200)
    except Exception as e:
        return make_response(str(e), 500)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
