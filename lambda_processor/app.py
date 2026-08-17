import json
import os

import boto3

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.idempotency import (
    DynamoDBPersistenceLayer,
    IdempotencyConfig,
    idempotent_function,
)

logger = Logger(service="eventguardian")

TABLE_NAME = os.environ["IDEMPOTENCY_TABLE"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]

s3 = boto3.client("s3")

persistence_layer = DynamoDBPersistenceLayer(
    table_name=TABLE_NAME
)

idempotency_config = IdempotencyConfig(
    event_key_jmespath="[tenant_id, client_request_id]",
    payload_validation_jmespath="[event_type, payload]",
    expires_after_seconds=3600,
    raise_on_no_idempotency_key=True,
)

processor = BatchProcessor(event_type=EventType.SQS)


@idempotent_function(
    data_keyword_argument="event_data",
    persistence_store=persistence_layer,
    config=idempotency_config,
)
def process_event(event_data: dict):
    required_fields = [
        "event_id",
        "event_type",
        "tenant_id",
        "client_request_id",
        "timestamp",
        "payload",
    ]

    missing = [
        field
        for field in required_fields
        if field not in event_data
    ]

    if missing:
        logger.error(
            "Validation failed",
            extra={
                "missing_fields": missing,
                "event": event_data,
            },
        )
        raise ValueError(f"Missing fields: {missing}")

    if event_data["event_type"] == "POISON_EVENT":
        logger.error(
            "Poison event detected",
            extra={
                "event_id": event_data["event_id"],
                "tenant_id": event_data["tenant_id"],
            },
        )
        raise RuntimeError("Controlled poison event")

    event_id = event_data["event_id"]

    logger.info(
        "Processing event",
        extra={
            "event_id": event_id,
            "event_type": event_data["event_type"],
            "tenant_id": event_data["tenant_id"],
        },
    )

    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=f"events/{event_id}.json",
        Body=json.dumps(event_data).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "Event processed successfully",
        extra={
            "event_id": event_id,
            "event_type": event_data["event_type"],
            "bucket": OUTPUT_BUCKET,
        },
    )

    return {
        "status": "COMPLETED",
        "event_id": event_id,
        "event_type": event_data["event_type"],
    }


def record_handler(record: SQSRecord):
    event_data = json.loads(record.body)
    return process_event(event_data=event_data)


@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, context):
    idempotency_config.register_lambda_context(context)

    return process_partial_response(
        event=event,
        record_handler=record_handler,
        processor=processor,
        context=context,
    )
