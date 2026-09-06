import json
import os
import pytest
import boto3
from moto import mock_aws

# Set test environment variables before importing app
os.environ["AWS_DEFAULT_REGION"] = "ap-south-1"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["IDEMPOTENCY_TABLE"] = "test-idempotency-table"
os.environ["OUTPUT_BUCKET"] = "test-output-bucket"

from lambda_processor import app
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyValidationError,
    IdempotencyAlreadyInProgressError,
)


class MockContext:
    function_name = "eventguardian-processor"
    memory_limit_in_mb = 256
    invoked_function_arn = "arn:aws:lambda:ap-south-1:123456789012:function:eventguardian-processor"
    aws_request_id = "test-request-id"

    def get_remaining_time_in_millis(self):
        return 30000


@pytest.fixture(autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_DEFAULT_REGION"] = "ap-south-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def test_env():
    """Setup mock DynamoDB and S3 environments."""
    with mock_aws():
        # Setup S3
        s3 = boto3.client("s3", region_name="ap-south-1")
        s3.create_bucket(
            Bucket=os.environ["OUTPUT_BUCKET"],
            CreateBucketConfiguration={"LocationConstraint": "ap-south-1"}
        )

        # Setup DynamoDB
        dynamodb = boto3.client("dynamodb", region_name="ap-south-1")
        dynamodb.create_table(
            TableName=os.environ["IDEMPOTENCY_TABLE"],
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST"
        )

        # Update app clients to use the mocked context
        app.s3 = s3
        app.persistence_layer.client = dynamodb
        app.persistence_layer.table_name = os.environ["IDEMPOTENCY_TABLE"]
        app.idempotency_config.register_lambda_context(MockContext())

        yield {"s3": s3, "dynamodb": dynamodb}


def create_sqs_record(body_dict, message_id):
    return {
        "messageId": message_id,
        "receiptHandle": f"receipt-{message_id}",
        "body": json.dumps(body_dict),
        "attributes": {
            "ApproximateReceiveCount": "1",
            "SentTimestamp": "1545082649183",
            "SenderId": "AIDAIENQZJOLO23YVJ4VO",
            "ApproximateFirstReceiveTimestamp": "1545082649185"
        },
        "messageAttributes": {},
        "md5OfBody": "098f6bcd4621d373cade4e832627b4f6",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:ap-south-1:123456789012:eventguardian-events",
        "awsRegion": "ap-south-1"
    }


# Test 1: Successful event processing
def test_1_successful_event_processing(test_env):
    event_payload = {
        "event_id": "evt-101",
        "event_type": "ORDER_CREATED",
        "tenant_id": "tenant-01",
        "client_request_id": "req-001",
        "timestamp": "2026-09-01T10:00:00Z",
        "payload": {"order_id": "ord-1", "amount": 2500}
    }

    result = app.process_event(event_data=event_payload)
    assert result["status"] == "COMPLETED"
    assert result["event_id"] == "evt-101"

    # Verify object exists in S3
    s3 = test_env["s3"]
    obj = s3.get_object(Bucket=os.environ["OUTPUT_BUCKET"], Key="events/evt-101.json")
    saved_data = json.loads(obj["Body"].read().decode("utf-8"))
    assert saved_data["event_id"] == "evt-101"


# Test 2: Duplicate event idempotency
def test_2_duplicate_event_idempotency(test_env):
    event_payload = {
        "event_id": "evt-102",
        "event_type": "ORDER_CREATED",
        "tenant_id": "tenant-01",
        "client_request_id": "req-002",
        "timestamp": "2026-09-01T10:00:00Z",
        "payload": {"order_id": "ord-2", "amount": 5000}
    }

    # First call
    res1 = app.process_event(event_data=event_payload)
    assert res1["status"] == "COMPLETED"

    # Second call (duplicate)
    res2 = app.process_event(event_data=event_payload)
    assert res2["status"] == "COMPLETED"
    assert res1 == res2


# Test 3: Concurrent duplicate event simulation (IN_PROGRESS lock)
def test_3_concurrent_duplicate_in_progress(test_env):
    event_payload = {
        "event_id": "evt-103-concurrent",
        "event_type": "ORDER_CREATED",
        "tenant_id": "tenant-01",
        "client_request_id": "req-concurrent",
        "timestamp": "2026-09-01T10:00:00Z",
        "payload": {"order_id": "ord-conc", "amount": 100}
    }

    # Configure persistence layer and save an IN_PROGRESS lock
    app.persistence_layer.configure(config=app.idempotency_config, function_name="lambda_processor.app.process_event")
    app.persistence_layer.save_inprogress(
        data=event_payload,
        remaining_time_in_millis=30000
    )

    # Calling process_event while in-progress should raise IdempotencyAlreadyInProgressError
    with pytest.raises(IdempotencyAlreadyInProgressError):
        app.process_event(event_data=event_payload)


# Test 4: Partial batch failure (Batch of 5: A, B, C, D, E where C fails processing, e.g. S3 failure)
def test_4_partial_batch_failures(test_env, monkeypatch):
    messages = [
        {"event_id": "evt-A", "event_type": "ORDER", "tenant_id": "t1", "client_request_id": "r-A", "timestamp": "t", "payload": {"item": "A"}},
        {"event_id": "evt-B", "event_type": "ORDER", "tenant_id": "t1", "client_request_id": "r-B", "timestamp": "t", "payload": {"item": "B"}},
        {"event_id": "evt-C", "event_type": "ORDER", "tenant_id": "t1", "client_request_id": "r-C", "timestamp": "t", "payload": {"item": "C"}},
        {"event_id": "evt-D", "event_type": "ORDER", "tenant_id": "t1", "client_request_id": "r-D", "timestamp": "t", "payload": {"item": "D"}},
        {"event_id": "evt-E", "event_type": "ORDER", "tenant_id": "t1", "client_request_id": "r-E", "timestamp": "t", "payload": {"item": "E"}},
    ]

    real_put_object = app.s3.put_object

    def mock_put_object(*args, **kwargs):
        if kwargs.get("Key") == "events/evt-C.json":
            raise RuntimeError("Simulated S3 failure for evt-C")
        return real_put_object(*args, **kwargs)

    monkeypatch.setattr(app.s3, "put_object", mock_put_object)

    records = [create_sqs_record(msg, f"msg-{chr(65+i)}") for i, msg in enumerate(messages)]
    sqs_event = {"Records": records}

    context = MockContext()
    response = app.lambda_handler(sqs_event, context)

    # Only msg-C should be reported as failed in the partial batch response!
    assert "batchItemFailures" in response
    failed_ids = [item["itemIdentifier"] for item in response["batchItemFailures"]]
    assert failed_ids == ["msg-C"]

    # Verify A, B, D, E were stored in S3
    s3 = test_env["s3"]
    for letter in ["A", "B", "D", "E"]:
        obj = s3.get_object(Bucket=os.environ["OUTPUT_BUCKET"], Key=f"events/evt-{letter}.json")
        assert obj is not None

    # Verify C was NOT stored in S3
    with pytest.raises(Exception):
        s3.get_object(Bucket=os.environ["OUTPUT_BUCKET"], Key="events/evt-C.json")


# Test 5: Processing failure error propagation (simulated S3 failure raises RuntimeError)
def test_5_processing_failure_error(test_env, monkeypatch):
    event_payload = {
        "event_id": "evt-105",
        "event_type": "ORDER_CREATED",
        "tenant_id": "tenant-01",
        "client_request_id": "req-005",
        "timestamp": "2026-09-01T10:00:00Z",
        "payload": {"data": "corrupt"}
    }

    def mock_put_object(*args, **kwargs):
        raise RuntimeError("Simulated S3 failure")

    monkeypatch.setattr(app.s3, "put_object", mock_put_object)

    with pytest.raises(RuntimeError) as exc_info:
        app.process_event(event_data=event_payload)
    assert "Simulated S3 failure" in str(exc_info.value)


# Test 6: Missing required fields (validation failure)
def test_6_missing_required_fields(test_env):
    malformed_event = {
        "event_id": "evt-106",
        "event_type": "ORDER_CREATED",
        "tenant_id": "tenant-01",
        # missing client_request_id
    }

    with pytest.raises(Exception):
        app.process_event(event_data=malformed_event)

    # Empty string check for required technical contract field
    empty_field_event = {
        "event_id": "   ",
        "event_type": "ORDER_CREATED",
        "tenant_id": "tenant-01",
        "client_request_id": "req-006",
    }
    with pytest.raises(ValueError) as exc_info:
        app.process_event(event_data=empty_field_event)
    assert "Missing or empty required field: event_id" in str(exc_info.value)


# Test 7: Same client_request_id with modified payload raises IdempotencyValidationError
def test_7_payload_mutation_conflict(test_env):
    original_payload = {
        "event_id": "evt-107",
        "event_type": "ORDER_CREATED",
        "tenant_id": "tenant-01",
        "client_request_id": "req-007",
        "timestamp": "2026-09-01T10:00:00Z",
        "payload": {"order_id": "ord-7", "amount": 100}
    }

    mutated_payload = {
        "event_id": "evt-107-diff",
        "event_type": "ORDER_CREATED",
        "tenant_id": "tenant-01",
        "client_request_id": "req-007",  # Same key
        "timestamp": "2026-09-01T10:00:00Z",
        "payload": {"order_id": "ord-7", "amount": 9999}  # Different payload
    }

    # First succeeds
    app.process_event(event_data=original_payload)

    # Second with same key but different payload must raise IdempotencyValidationError
    with pytest.raises(IdempotencyValidationError):
        app.process_event(event_data=mutated_payload)


# Test 8: Malformed JSON syntax in SQS record body is caught and isolated
def test_8_invalid_json_body_partial_batch_failure(test_env):
    record_valid = create_sqs_record(
        {"event_id": "evt-ok", "event_type": "O", "tenant_id": "t1", "client_request_id": "r-ok", "timestamp": "ts", "payload": {}},
        "msg-valid"
    )
    record_invalid = {
        "messageId": "msg-corrupted",
        "receiptHandle": "receipt-corrupted",
        "body": "{ invalid json without closing bracket: ",
        "attributes": {"ApproximateReceiveCount": "1"},
        "messageAttributes": {},
        "md5OfBody": "xxx",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:ap-south-1:123456789012:eventguardian-events",
        "awsRegion": "ap-south-1"
    }

    sqs_event = {"Records": [record_valid, record_invalid]}
    context = MockContext()
    response = app.lambda_handler(sqs_event, context)

    # Corrupted JSON record must be reported as batch failure
    assert "batchItemFailures" in response
    failed_ids = [item["itemIdentifier"] for item in response["batchItemFailures"]]
    assert failed_ids == ["msg-corrupted"]

    # Valid record must have succeeded
    s3 = test_env["s3"]
    obj = s3.get_object(Bucket=os.environ["OUTPUT_BUCKET"], Key="events/evt-ok.json")
    assert obj is not None

