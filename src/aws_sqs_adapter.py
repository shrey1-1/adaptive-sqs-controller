from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class AWSValidationConfig:
    queue_url: str
    region: str
    max_messages: int = 10
    wait_time_seconds: int = 2


class Boto3SQSAdapter:
    """Small opt-in SQS validation adapter; never creates resources automatically."""

    def __init__(self, config: AWSValidationConfig) -> None:
        if not config.queue_url:
            raise ValueError("A pre-created test queue URL is required")
        import boto3

        self.config = config
        # Boto3's normal credential chain supports environment variables,
        # ~/.aws, IAM roles, and credentials injected from Colab Secrets.
        self.client = boto3.client("sqs", region_name=config.region)

    @classmethod
    def from_environment(cls) -> "Boto3SQSAdapter":
        queue_url = os.environ.get("ADAPTIVE_SQS_TEST_QUEUE_URL", "")
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        return cls(AWSValidationConfig(queue_url=queue_url, region=region))

    def send_job(self, job: dict[str, Any]) -> str:
        response = self.client.send_message(
            QueueUrl=self.config.queue_url,
            MessageBody=json.dumps(job),
        )
        return str(response["MessageId"])

    def receive_one(self, visibility_timeout: int) -> dict[str, Any] | None:
        response = self.client.receive_message(
            QueueUrl=self.config.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=self.config.wait_time_seconds,
            VisibilityTimeout=int(visibility_timeout),
            AttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
        )
        messages = response.get("Messages", [])
        return messages[0] if messages else None

    def extend(self, receipt_handle: str, timeout: int) -> None:
        self.client.change_message_visibility(
            QueueUrl=self.config.queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=int(timeout),
        )

    def delete(self, receipt_handle: str) -> None:
        self.client.delete_message(
            QueueUrl=self.config.queue_url,
            ReceiptHandle=receipt_handle,
        )

    def validate_one(self, job: dict[str, Any], timeout: int) -> dict[str, Any]:
        """Budget-conscious real-time validation for one short job."""
        self.send_job(job)
        received = self.receive_one(timeout)
        if received is None:
            return {"received": False}
        started = time.monotonic()
        time.sleep(float(job["processing_duration"]))
        elapsed = time.monotonic() - started
        self.delete(received["ReceiptHandle"])
        return {
            "received": True,
            "elapsed_seconds": elapsed,
            "receive_count": int(received.get("Attributes", {}).get("ApproximateReceiveCount", "1")),
        }

