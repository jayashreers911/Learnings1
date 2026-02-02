# Schema Drift Agent

This repository includes a simple single-shot schema drift detector `drift_agent.py`.

Usage:

```
python drift_agent.py <old_csv_path> <new_csv_path> [--out report.json]
```

Example:

```
python drift_agent.py "old_Units.csv" "new_data/Units.csv" --out drift_report.json
```

The detector reports:
- Added / removed columns
- Column type changes
- Null-rate increases above a threshold
- Numeric mean/std relative changes above thresholds

Thresholds are configurable via CLI flags `--mean-thresh`, `--std-thresh`, and `--null-thresh`.

Interactive Slack actions
 If you'd like, I can add an option to upload reports to S3 and use the HTTPS S3 link in the Slack "Open report" button instead of a local file:// link.

Message updates on acknowledge
-----------------------------
The provided `ack_server.py` will not only log the acknowledgement payload but will also attempt to update the original Slack message to reflect the acknowledged state. It uses the `response_url` included in interactive action payloads to replace the original message with a short block that says who acknowledged and when.

Notes:
- Updating the message via `response_url` is simple and does not require a Bot token, but it only works for the original message in the channel where the action occurred.
- If you need to programmatically edit messages outside that context, or post follow-up messages, you'll need a Slack Bot token with `chat:write` and `chat:write.public` scopes and use `chat.update` or `chat.postMessage`.
1. Create a Slack App at https://api.slack.com/apps and add an "Incoming Webhook" (for sending messages) and enable "Interactivity & Shortcuts".
2. In "Interactivity", set the Request URL to an externally reachable endpoint that will receive action payloads (e.g., `https://<your-host>/slack/actions`).
	- For local testing you can run `ack_server.py` (Flask) and expose it with `ngrok`:

```powershell
python ack_server.py
# in another shell
ngrok http 5000
```

3. Copy the app's "Signing Secret" and set it as an environment variable before running the server:

```powershell
$env:SLACK_SIGNING_SECRET = "<your_signing_secret>"
```

4. When posting the alert, run the monitor or detect command with `--slack-webhook` (incoming webhook URL) and `--interactive` to include an Acknowledge action.

Example (monitor, run once, interactive):

```powershell
& "C:/.../.venv/Scripts/python.exe" "main.py" monitor "new_data" "old_Units.csv" --once --slack-webhook "https://hooks.slack.com/services/XXX/YYY/ZZZ" --interactive
```

5. When a user clicks "Acknowledge" in Slack the Slack platform will POST an action payload to the configured Interactivity Request URL. The provided `ack_server.py` will verify the request using `SLACK_SIGNING_SECRET` and append the payload to `ack_log.jsonl`.

Notes:
- Interactive buttons require a Slack App and Interactivity enabled; an incoming webhook alone is not enough.
- For production, host the ack endpoint on a secure HTTPS server reachable by Slack (or use a tunnel for local testing).
- If you'd like, I can add an option to upload reports to S3 and use the HTTPS S3 link in the Slack "Open report" button instead of a local file:// link.
