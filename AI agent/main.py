import sys
import argparse
import json
import os
from data_pipeline_monitor import monitor_pipeline

try:
    from drift_agent import detect_drift, send_slack_block_alert, send_slack_dm
except Exception:
    detect_drift = None
    send_slack_block_alert = None
    send_slack_dm = None


def main():
    parser = argparse.ArgumentParser(description="Data pipeline utilities: monitor or detect schema drift")
    subparsers = parser.add_subparsers(dest="cmd", required=False)

    # Monitor subcommand (existing behavior)
    monitor_parser = subparsers.add_parser("monitor", help="Run continuous pipeline monitor")
    monitor_parser.add_argument("data_dir", help="Directory to watch for CSV files")
    monitor_parser.add_argument("old_data_file", help="Path to reference/old CSV file")
    monitor_parser.add_argument("--once", action="store_true", help="Run monitor only once and exit")
    monitor_parser.add_argument("--slack-webhook", help="Slack incoming webhook URL to notify when drift is detected")
    monitor_parser.add_argument("--ack-url", help="Acknowledgement URL to include as a button in Slack alerts")
    monitor_parser.add_argument("--interactive", action="store_true", help="Use Slack interactive actions (requires Slack App interactivity configured)")
    monitor_parser.add_argument("--slack-bot-token", help="Slack bot token to send direct messages to a user (optional)")
    monitor_parser.add_argument("--slack-user-email", help="User email to DM (used with --slack-bot-token)")
    monitor_parser.add_argument("--slack-user-id", help="Slack user id to DM (used with --slack-bot-token)")

    # Detect subcommand (single-shot detection)
    detect_parser = subparsers.add_parser("detect", help="Run single-shot schema/statistical drift detection")
    detect_parser.add_argument("old", help="Path to reference/old CSV file")
    detect_parser.add_argument("new", help="Path to new CSV file to test")
    detect_parser.add_argument("--out", help="Path to write JSON report (optional)")
    detect_parser.add_argument("--mean-thresh", type=float, default=0.25, help="Relative mean change threshold")
    detect_parser.add_argument("--std-thresh", type=float, default=0.4, help="Relative std change threshold")
    detect_parser.add_argument("--null-thresh", type=float, default=0.15, help="Absolute null-rate change threshold")
    detect_parser.add_argument("--slack-webhook", help="Slack incoming webhook URL to notify when drift is detected")
    detect_parser.add_argument("--ack-url", help="Acknowledgement URL to include as a button in Slack alerts")
    detect_parser.add_argument("--interactive", action="store_true", help="Use Slack interactive actions (requires Slack App interactivity configured)")
    detect_parser.add_argument("--slack-bot-token", help="Slack bot token to send direct messages to a user (optional)")
    detect_parser.add_argument("--slack-user-email", help="User email to DM (used with --slack-bot-token)")
    detect_parser.add_argument("--slack-user-id", help="Slack user id to DM (used with --slack-bot-token)")

    args = parser.parse_args()

    # Backwards-compatible: if no subcommand provided, show help
    if args.cmd is None:
        parser.print_help()
        sys.exit(1)

    if args.cmd == "monitor":
        monitor_pipeline(
            args.data_dir,
            args.old_data_file,
            run_once=args.once,
            slack_webhook=args.slack_webhook,
            ack_url=args.ack_url,
            interactive=args.interactive,
            slack_bot_token=args.slack_bot_token,
            slack_user_email=args.slack_user_email,
            slack_user_id=args.slack_user_id,
        )

    elif args.cmd == "detect":
        if detect_drift is None:
            print("`drift_agent.py` not found or failed to import. Ensure it exists and dependencies are installed.")
            sys.exit(1)

        # Run detection
        report = detect_drift(args.old, args.new, mean_thresh=args.mean_thresh, std_thresh=args.std_thresh, null_thresh=args.null_thresh)

        # Print and optionally save report
        print(json.dumps(report, indent=2))
            if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)

            # If a Slack webhook was provided, send a Block Kit alert linking to the report file
            if args.slack_webhook and send_slack_block_alert:
                report_url = f"file://{os.path.abspath(args.out)}"
                try:
                    ok = send_slack_block_alert(args.slack_webhook, report, report_url=report_url, acknowledge_url=args.ack_url, interactive=args.interactive)
                    print(f"Slack alert sent: {ok}")
                except Exception as e:
                    print(f"Failed to send Slack alert: {e}")

                # If a Slack bot token was provided, send the report as a DM to the specified user
                if args.slack_bot_token and send_slack_dm:
                    report_url = f"file://{os.path.abspath(args.out)}"
                    try:
                        ok = send_slack_dm(args.slack_bot_token, args.slack_user_id, args.slack_user_email, report, report_url=report_url, interactive=args.interactive)
                        print(f"Slack DM sent: {ok}")
                    except Exception as e:
                        print(f"Failed to send Slack DM: {e}")


if __name__ == "__main__":
    main()