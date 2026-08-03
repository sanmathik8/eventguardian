# EventGuardian

## Project Overview
EventGuardian is a **server‑less event processing pipeline** built on AWS. Clients publish JSON events to an Amazon SQS queue. A Lambda function consumes the messages, ensures **idempotent processing** using DynamoDB, stores the raw event payload in an S3 bucket, and logs detailed execution information to CloudWatch. Failed or poison messages are automatically routed to a dead‑letter queue (DLQ) for later analysis.

## Architecture Diagram
```mermaid
flowchart TD
    Client[Client/Application] -->|Send JSON| SQS[Amazon SQS (eventguardian‑events)]
    SQS -->|Trigger| Lambda[Lambda (eventguardian‑processor)]
    Lambda -->|Idempotency check| DynamoDB[Amazon DynamoDB (idempotency table)]
    Lambda -->|Store event| S3[Amazon S3 (processed bucket)]
    Lambda -->|Log| CloudWatch[CloudWatch Logs]
    Lambda -->|DLQ on failure| DLQ[Amazon SQS (eventguardian‑dlq)]
    DLQ -->|Alert| SNS[Amazon SNS (DLQ alerts)]
    SNS -->|Email| Email[Stakeholder Email]
```

## AWS Services Used
| Service | Why it was chosen |
|--------|-------------------|
| **Amazon SQS** | Decouples producers and consumers, provides at‑least‑once delivery, built‑in visibility timeout & DLQ support. |
| **AWS Lambda** | Fully managed compute that scales automatically with queue depth; ideal for short‑lived processing. |
| **Amazon DynamoDB** | Server‑less key‑value store for idempotency tokens; low latency and TTL for automatic cleanup. |
| **Amazon S3** | Cheap, durable object storage for persisting raw events; enables downstream analytics. |
| **Amazon CloudWatch** | Centralised logging and metrics for observability, alerts, and troubleshooting. |
| **AWS IAM** | Fine‑grained least‑privilege permissions for Lambda to interact with the above services. |
| **AWS SNS** (optional) | Sends email alerts when messages land in the DLQ. |

## Features
- **Idempotent processing** using `aws-lambda-powertools` idempotency library.
- **Automatic DLQ handling** with SNS email alerts.
- **Server‑less, infrastructure‑as‑code** with Terraform.
- **Structured logging** via `aws-lambda-powertools` logger.
- **Configurable batch size** (10 messages per Lambda invocation).
- **TTL‑based cleanup** of idempotency records.

## Project Structure
```
.
├── lambda_processor/          # Lambda source code (app.py)
├── terraform/                # IaC – SQS, Lambda, DynamoDB, S3, IAM, etc.
├── tests/                    # Integration test script & sample events
├── .gitignore                # Ignored files and directories
├── README.md                 # This document
├── lambda_function.zip       # Pre‑built Lambda deployment package (ignored)
└── ...
```

## Setup Instructions
1. **Prerequisites**
   - AWS CLI configured with appropriate credentials.
   - Terraform >= 1.3.
   - Python 3.13 (runtime used by the Lambda).
2. **Clone the repo**
   ```bash
   git clone https://github.com/sanmathik8/eventguardian.git
   cd eventguardian
   ```
3. **Create a virtual environment & install dependencies**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # on Windows: .venv\Scripts\activate
   pip install -r lambda_processor/requirements.txt
   ```
4. **Build the Lambda deployment package**
   ```bash
   cd lambda_processor
   zip -r ../lambda_function.zip .
   cd ..
   ```
5. **Deploy the infrastructure**
   ```bash
   cd terraform
   terraform init
   terraform apply -var='budget_alert_email=you@example.com'
   ```
   The output will include the `event_queue_url` needed for the test script.

## Testing
Run the provided test script to push a sample event:
```bash
export EVENTGUARDIAN_QUEUE_URL=$(terraform -chdir=terraform output -raw event_queue_url)
python tests/run_test.py tests/events.json
```
Check CloudWatch logs for the Lambda execution and the S3 bucket for the stored event.

## Failure Handling
- **Message processing errors** → Lambda returns a failure; after `maxReceiveCount` (3) the message moves to the DLQ.
- **DLQ alerts** → SNS triggers an email to the address defined in `budget_alert_email`.
- **Idempotency conflicts** → Duplicate messages are ignored by the DynamoDB‑backed idempotency layer.
- **Infrastructure drift** → Re‑run `terraform plan` to detect changes.

## Future Improvements
- Add **Step Functions** for multi‑stage processing (validation → enrichment → storage).
- Implement **event schema validation** using `jsonschema`.
- Enable **S3 event notifications** to trigger downstream analytics.
- Add **CI/CD pipeline** (GitHub Actions) to automatically lint, test, and deploy.
- Expand DLQ monitoring with **CloudWatch metric filters** and dashboards.
```
