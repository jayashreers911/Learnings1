import time
import os
import json
from datetime import datetime

from schema_drift_detector import load_data
try:
    from drift_agent import detect_drift, send_slack_block_alert, send_slack_dm
except Exception:
    detect_drift = None
    send_slack_block_alert = None
    send_slack_dm = None

def get_data_files(data_dir):
    """
    Returns a list of all CSV files in the specified directory.
    """
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
    return files

def monitor_pipeline(
    data_dir,
    old_data_file,
    run_once: bool = False,
    slack_webhook: str = None,
    ack_url: str = None,
    interactive: bool = False,
    slack_bot_token: str = None,
    slack_user_email: str = None,
    slack_user_id: str = None,
):
    """
    Monitors the data pipeline, checks for schema drift, and writes a JSON report for each new file.
    If `run_once` is True, the loop runs a single iteration (useful for testing).
    """
    # Ensure reference file exists
    if not os.path.exists(old_data_file):
        print(f"Error: old data file not found: {old_data_file}")
        return

    print(f"Monitoring data pipeline in directory: {data_dir}")

    while True:
        files = get_data_files(data_dir)
        if files:
            # Assuming the most recent file is the new data file
            new_data_file = max(files, key=os.path.getctime)
            print(f"New data file detected: {new_data_file}")

            # Use detect_drift if available (it accepts file paths)
            if detect_drift is None:
                print("detect_drift not available (drift_agent.py missing or failed to import).")
            else:
                print("Running drift detection...")
                try:
                    report = detect_drift(old_data_file, new_data_file)
                except Exception as e:
                    print(f"Error running detect_drift: {e}")
                    report = {"error": str(e)}

                # Save report to `drift_reports` under data_dir with timestamp
                reports_dir = os.path.join(data_dir, "drift_reports")
                os.makedirs(reports_dir, exist_ok=True)
                ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                base_new = os.path.splitext(os.path.basename(new_data_file))[0]
                report_path = os.path.join(reports_dir, f"drift_report_{base_new}_{ts}.json")
                try:
                    with open(report_path, "w", encoding="utf-8") as f:
                        json.dump(report, f, indent=2)
                    print(f"Drift report written: {report_path}")
                except Exception as e:
                    print(f"Failed to write report to {report_path}: {e}")

                # Send a richer Slack Block Kit alert linking to the report (if webhook provided)
                if slack_webhook and send_slack_block_alert:
                    # Convert local path to file:// URL so Slack may render a link (note: host may need access)
                    report_url = f"file://{os.path.abspath(report_path)}"
                    try:
                        ok = send_slack_block_alert(slack_webhook, report, report_url=report_url, acknowledge_url=ack_url, interactive=interactive)
                        print(f"Slack alert sent: {ok}")
                    except Exception as e:
                        print(f"Failed to send Slack alert: {e}")

                # If a Slack bot token was provided, send the report as a DM
                if slack_bot_token and send_slack_dm:
                    report_url = f"file://{os.path.abspath(report_path)}"
                    try:
                        ok = send_slack_dm(slack_bot_token, slack_user_id, slack_user_email, report, report_url=report_url, interactive=interactive)
                        print(f"Slack DM sent: {ok}")
                    except Exception as e:
                        print(f"Failed to send Slack DM: {e}")

            # Optionally update the reference file by replacing it with the new file
            # (This behavior is destructive; keep it commented unless desired)
            # shutil.copyfile(new_data_file, old_data_file)

        if run_once:
            print("Run-once flag set; exiting monitor loop.")
            break

        time.sleep(60)