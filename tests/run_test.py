import json
import os
import re
import sys
import boto3

QUEUE_URL = os.environ.get("EVENTGUARDIAN_QUEUE_URL")

if not QUEUE_URL:
    print("❌ EVENTGUARDIAN_QUEUE_URL environment variable is not set.")
    print("   Set it with:")
    print('   export EVENTGUARDIAN_QUEUE_URL=$(terraform -chdir=../terraform output -raw event_queue_url)')
    sys.exit(1)

if len(sys.argv) != 2:
    print("Usage: python run_test.py <json_file>")
    sys.exit(1)

file_path = sys.argv[1]

try:
    with open(file_path, "r") as f:
        message = json.load(f)
except json.JSONDecodeError as e:
    print(f"❌ Invalid JSON: {e}")
    sys.exit(1)
except FileNotFoundError:
    print(f"❌ File not found: {file_path}")
    sys.exit(1)

# Dynamically extract region from SQS Queue URL
match = re.search(r"sqs\.([a-z0-9-]+)\.amazonaws\.com", QUEUE_URL)
region = match.group(1) if match else "ap-south-1"

sqs = boto3.client("sqs", region_name=region)

if isinstance(message, list):
    print(f"Detected list of {len(message)} messages. Sending individually...")
    for idx, msg in enumerate(message, 1):
        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(msg)
        )
        print(f"  [{idx}] Sent: {msg.get('event_id')} -> MessageId: {response['MessageId']}")
    print("✅ All messages sent successfully!")
else:
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message)
    )
    print("✅ Message sent successfully!")
    print("MessageId:", response["MessageId"])
