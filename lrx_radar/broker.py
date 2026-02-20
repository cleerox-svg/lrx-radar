from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class BrokerMessage:
    payload: dict[str, Any]
    receipt: Any


class AbstractBroker:
    async def publish_many(self, payloads: list[dict[str, Any]]) -> None:
        raise NotImplementedError

    async def get(self, timeout_seconds: float = 1.0) -> BrokerMessage | None:
        raise NotImplementedError

    async def ack(self, message: BrokerMessage) -> None:
        raise NotImplementedError

    async def requeue(self, message: BrokerMessage, payload: dict[str, Any], delay_seconds: float) -> None:
        raise NotImplementedError

    async def depth(self) -> int:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError


class MemoryBroker(AbstractBroker):
    def __init__(self) -> None:
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def publish_many(self, payloads: list[dict[str, Any]]) -> None:
        for payload in payloads:
            self._queue.put_nowait(payload)

    async def get(self, timeout_seconds: float = 1.0) -> BrokerMessage | None:
        try:
            payload = await asyncio.wait_for(self._queue.get(), timeout=timeout_seconds)
            return BrokerMessage(payload=payload, receipt=None)
        except TimeoutError:
            return None

    async def ack(self, message: BrokerMessage) -> None:
        self._queue.task_done()

    async def requeue(
        self, message: BrokerMessage, payload: dict[str, Any], delay_seconds: float = 0.0
    ) -> None:
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)
        self._queue.put_nowait(payload)
        self._queue.task_done()

    async def depth(self) -> int:
        return self._queue.qsize()

    async def close(self) -> None:
        return


class SqsBroker(AbstractBroker):
    def __init__(self, *, queue_url: str, region: str | None = None) -> None:
        try:
            import boto3  # type: ignore
        except ImportError as exc:  # pragma: no cover - exercised only in SQS mode
            raise RuntimeError("SQS broker requires boto3 to be installed") from exc
        self._client = boto3.client("sqs", region_name=region) if region else boto3.client("sqs")
        self._queue_url = queue_url

    async def publish_many(self, payloads: list[dict[str, Any]]) -> None:
        for idx in range(0, len(payloads), 10):
            batch = payloads[idx : idx + 10]
            entries = [
                {"Id": str(inner_idx), "MessageBody": json.dumps(item)}
                for inner_idx, item in enumerate(batch)
            ]
            response = await asyncio.to_thread(
                self._client.send_message_batch,
                QueueUrl=self._queue_url,
                Entries=entries,
            )
            if response.get("Failed"):
                raise RuntimeError(f"SQS batch send failed: {response['Failed']}")

    async def get(self, timeout_seconds: float = 1.0) -> BrokerMessage | None:
        wait_seconds = max(0, min(20, int(round(timeout_seconds))))
        response = await asyncio.to_thread(
            self._client.receive_message,
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait_seconds,
            VisibilityTimeout=30,
        )
        messages = response.get("Messages", [])
        if not messages:
            return None
        raw_message = messages[0]
        return BrokerMessage(
            payload=json.loads(raw_message["Body"]),
            receipt=raw_message["ReceiptHandle"],
        )

    async def ack(self, message: BrokerMessage) -> None:
        await asyncio.to_thread(
            self._client.delete_message,
            QueueUrl=self._queue_url,
            ReceiptHandle=message.receipt,
        )

    async def requeue(
        self, message: BrokerMessage, payload: dict[str, Any], delay_seconds: float = 0.0
    ) -> None:
        await asyncio.to_thread(
            self._client.send_message,
            QueueUrl=self._queue_url,
            MessageBody=json.dumps(payload),
            DelaySeconds=max(0, min(900, int(round(delay_seconds)))),
        )
        await self.ack(message)

    async def depth(self) -> int:
        response = await asyncio.to_thread(
            self._client.get_queue_attributes,
            QueueUrl=self._queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )
        attrs = response.get("Attributes", {})
        visible = int(attrs.get("ApproximateNumberOfMessages", 0))
        not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        return visible + not_visible

    async def close(self) -> None:
        return


def build_broker(
    *,
    backend: str,
    sqs_queue_url: str | None = None,
    aws_region: str | None = None,
) -> AbstractBroker:
    normalized = backend.strip().lower()
    if normalized in {"memory", "in-memory", "in_memory"}:
        return MemoryBroker()
    if normalized == "sqs":
        if not sqs_queue_url:
            raise RuntimeError("SQS broker selected but SQS_QUEUE_URL is not configured.")
        return SqsBroker(queue_url=sqs_queue_url, region=aws_region)
    if normalized in {"rabbitmq", "kafka"}:
        raise RuntimeError(
            f"Broker backend '{normalized}' is planned but not wired yet. "
            "Use 'memory' or 'sqs' for now."
        )
    raise RuntimeError(f"Unsupported broker backend '{backend}'.")
