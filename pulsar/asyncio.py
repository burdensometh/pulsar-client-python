#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# pylint: disable=no-name-in-module,c-extension-no-member,protected-access

"""
The Pulsar Python client APIs that work with the asyncio module.
"""

import asyncio
import functools
from collections import deque
from datetime import timedelta
from typing import Any, Callable, Deque, List, Optional, Union

import _pulsar
from _pulsar import (
    InitialPosition,
    CompressionType,
    PartitionsRoutingMode,
    BatchingType,
    ProducerAccessMode,
    RegexSubscriptionMode,
    ConsumerCryptoFailureAction,
)
import pulsar
from pulsar import _check_type

#: Default capacity of the per-consumer staging queue for results whose waiter
#: was cancelled. It is bounded so a consumer that stops reading cannot grow
#: memory without limit; an overflowing message is explicitly returned to the
#: broker for redelivery (see ``Consumer._handle_staging_overflow``). Readers
#: have no redelivery mechanism and use an unbounded queue instead, so an
#: advanced reader position can never skip data.
_MAX_STAGED_RESULTS = 1000

class PulsarException(Exception):
    """
    The exception that wraps the Pulsar error code
    """

    def __init__(self, result: pulsar.Result, msg: str | None = None) -> None:
        """
        Create the Pulsar exception.

        Parameters
        ----------
        result: pulsar.Result
            The error code of the underlying Pulsar APIs.
        msg: str | None
            An optional error message providing more details.
        """
        self._result = result
        self._msg = msg

    def error(self) -> pulsar.Result:
        """
        Returns the Pulsar error code.
        """
        return self._result

    def __str__(self):
        """
        Convert the exception to string.
        """
        if self._msg:
            return f'{self._result.value} {self._result.name}: {self._msg}'
        return f'{self._result.value} {self._result.name}'


class _AsyncReadable:
    """
    Lifecycle manager for the asynchronous message-delivery objects
    (``Consumer.receive`` and ``Reader.read_next``).

    The C++ client does not support cancelling a pending ``receiveAsync`` /
    ``readNextAsync`` call: the callback is always invoked eventually, even if
    the asyncio task that initiated the read was cancelled (for example by
    ``asyncio.wait_for`` timing out). This class makes that contract safe:

    * **Single-flight** — at most one underlying read is registered at any
      time, so repeated timeouts cannot pile up an unbounded number of pending
      reads inside the client.
    * **Bounded staging** — results that arrive after their waiters were
      cancelled are queued FIFO and handed to later reads, so a message that
      already left the client's receiver queue is never silently lost. The
      queue is capped (``max_staged``); consumers return an overflowing message
      to the broker for redelivery, while readers — which have no
      acknowledgement/redelivery mechanism — use an unbounded queue so the
      advanced position never skips data.
    * **Bounded backlog of waiters** — concurrent callers wait on a single
      in-flight read instead of each registering their own.
    * **Deterministic shutdown** — closing fails every pending waiter with
      ``Result.AlreadyClosed`` and absorbs any callback that arrives later.
    """

    def __init__(self, schema: pulsar.schema.Schema,
                 max_staged: Optional[int] = _MAX_STAGED_RESULTS) -> None:
        self._schema = schema
        self._loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        # Results (messages or errors) delivered by an underlying read whose
        # waiters had already been cancelled, kept for subsequent reads.
        # ``maxlen=None`` means an unbounded FIFO (used by readers).
        self._staged: Deque[Any] = deque(maxlen=max_staged)
        # Asyncio tasks currently blocked in the public read coroutine.
        self._waiters: Deque[asyncio.Future] = deque()
        # Maps a waiter to the staged message it was handed, so that if the
        # waiter's task is cancelled before it can consume the message it can
        # be returned to the staging queue rather than lost.
        self._delivered: dict = {}
        # Whether an underlying read is currently registered.
        self._reading = False
        self._closed = False

    def _start_underlying_read(self, callback: Callable[..., None]) -> None:
        """Register one asynchronous read with the C++ client."""
        raise NotImplementedError

    def _wrap_message(self, message: _pulsar.Message) -> pulsar.Message:
        m = pulsar.Message()
        m._message = message
        m._schema = self._schema
        return m

    def _staging_is_full(self) -> bool:
        maxlen = self._staged.maxlen
        return maxlen is not None and len(self._staged) >= maxlen

    def _handle_staging_overflow(self, message: _pulsar.Message) -> None:
        """
        Deal with a late message that cannot be staged because the bounded
        staging queue is full.

        Consumers give the message back to the broker for redelivery via a
        negative acknowledgement. Readers use an unbounded queue, so this
        branch is unreachable for them.
        """

    def _on_underlying_read_done(self, *args: Any) -> None:
        """
        Callback invoked by the C++ client (from a non-asyncio thread) when the
        single in-flight read completes.
        """
        self._loop.call_soon_threadsafe(self._handle_read_done, args)

    def _handle_read_done(self, args: tuple) -> None:
        self._reading = False

        if self._closed:
            # The object is shutting down. Fail any waiters explicitly and do
            # not let late results resurrect state or escape as unhandled
            # errors.
            self._fail_waiters(PulsarException(_pulsar.Result.AlreadyClosed))
            return

        result = self._decode_result(args)

        if isinstance(result, PulsarException):
            # Errors are never staged: a late error belongs to the read that
            # was cancelled, not to the next caller. Surface it to a current
            # waiter, otherwise discard it.
            waiter = self._pop_waiter()
            if waiter is not None and not waiter.done():
                waiter.set_exception(result)
            # Other concurrent callers still need a read of their own.
            self._pump()
            return

        # Successful message delivery.
        waiter = self._pop_waiter()
        if waiter is not None and not waiter.done():
            # Record ownership so the message is reclaimed into staging if the
            # receiving task is cancelled before it consumes the result.
            waiter.set_result(result)
            self._delivered[waiter] = result
        elif self._staging_is_full():
            # No live waiter and the bounded queue is full. Do not silently
            # drop the message: let the concrete class return it for
            # redelivery (consumer) or keep it (unbounded reader queue).
            self._handle_staging_overflow(result)
        else:
            # The originating read was cancelled: retain the message FIFO for
            # the next read instead of dropping it.
            self._staged.append(result)

        # Re-arm a read for any remaining waiters, and match a staged result
        # with a read that was started while the callback was in flight.
        self._pump()

    def _decode_result(self, args: tuple) -> Union[_pulsar.Message, PulsarException]:
        # Both the consumer and reader C++ callbacks have the shape
        # (Result, Message). The _pulsar.Error form is kept defensively for
        # v2-style bindings that report failures as a single error object.
        if len(args) == 2 and isinstance(args[0], _pulsar.Result):
            result, value = args
            if result == _pulsar.Result.Ok:
                return value
            return PulsarException(result)
        value = args[0]
        if isinstance(value, _pulsar.Error):
            return PulsarException(value.error, value.message)
        return value

    def _pop_waiter(self) -> Optional[asyncio.Future]:
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                return waiter
        return None

    def _deliver_staged(self) -> None:
        """
        Hand staged results to live waiters in FIFO order.

        A cancelled waiter is skipped (it removes itself from the queue in its
        cancellation handler) and the result stays staged for a later read.
        The hand-off is recorded in ``_delivered`` keyed by the waiter, whose
        lifecycle is independent of the calling task's cancellation, so the
        message can always be reclaimed.
        """
        while self._staged and self._waiters:
            waiter = self._waiters[0]
            if waiter.done():
                self._waiters.popleft()
                self._delivered.pop(waiter, None)
                continue
            message = self._staged.popleft()
            waiter.set_result(message)
            self._waiters.popleft()
            self._delivered[waiter] = message

    def _return_to_staging(self, message: _pulsar.Message) -> None:
        """
        Put a message back at the front of the staging queue, preserving FIFO
        order. If a bounded queue is full, the oldest tail entry is displaced
        and handled through the overflow hook instead of being dropped.
        """
        if self._staging_is_full():
            displaced = self._staged.pop()
            self._handle_staging_overflow(displaced)
        self._staged.appendleft(message)

    def _fail_waiters(self, exc: Exception) -> None:
        while True:
            waiter = self._pop_waiter()
            if waiter is None:
                break
            if not waiter.done():
                waiter.set_exception(exc)
            self._delivered.pop(waiter, None)

    def _pump(self) -> None:
        """
        Match staged results with waiters and keep at most one underlying read
        in flight.

        First, any staged result is handed directly to a waiting caller without
        touching the client. A new underlying read is then registered only when
        there is an unsatisfied waiter and none is pending. Reads are therefore
        lazy: nothing is pre-fetched without a caller, which keeps side-effect
        sensitive APIs such as ``Reader.has_message_available`` accurate. When a
        read's waiter is cancelled its late result is staged, and the next read
        drains that staging queue and re-arms the client here.
        """
        if self._closed:
            return

        self._deliver_staged()

        if self._waiters and not self._reading:
            self._reading = True
            try:
                self._start_underlying_read(self._on_underlying_read_done)
            except Exception as exc:  # registration itself failed
                self._reading = False
                waiter = self._pop_waiter()
                if waiter is not None and not waiter.done():
                    waiter.set_exception(exc)
                else:
                    raise

    async def _read(self) -> pulsar.Message:
        """
        Common implementation of ``Consumer.receive`` / ``Reader.read_next``.
        """
        if self._closed:
            raise PulsarException(_pulsar.Result.AlreadyClosed)

        # The waiter is an internal future resolved by the pump; it is never
        # cancelled directly, so a result stored on it always stays retrievable
        # even when the calling task is cancelled. The gate merely bridges the
        # task's cancellation into this coroutine while mirroring the waiter's
        # outcome once it resolves.
        waiter = self._loop.create_future()
        gate = self._loop.create_future()
        self._waiters.append(waiter)

        def _mirror(_: asyncio.Future) -> None:
            if gate.done():
                return
            if waiter.cancelled():
                gate.cancel()
            else:
                exc = waiter.exception()
                if exc is not None:
                    gate.set_exception(exc)
                else:
                    gate.set_result(None)

        waiter.add_done_callback(_mirror)
        self._pump()
        try:
            await gate
        except asyncio.CancelledError:
            # A pump triggered during cancellation processing may only now
            # hand a staged result to this waiter. Run it while the waiter is
            # still registered so the hand-off is recorded, then detach.
            self._pump()
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            # The waiter is never cancelled (only the gate is), so any result
            # recorded for it is still retrievable. Reclaim it into staging
            # for the next read instead of losing it with this task.
            delivered = self._delivered.pop(waiter, None)
            if delivered is not None:
                self._return_to_staging(delivered)
                self._pump()
            raise

        self._delivered.pop(waiter, None)
        raw_message = waiter.result()
        # A staged result may have satisfied this read while a previously
        # armed read is blocked further down the stream. Now that the slot is
        # free again, re-arm so subsequent messages keep flowing.
        self._pump()
        return self._wrap_message(raw_message)

    async def _close_readable(self, close_async: Callable[[Callable], None]) -> None:
        """
        Shared close sequence: fail pending waiters, run the underlying close,
        and neutralise any callback still in flight.
        """
        if self._closed:
            raise PulsarException(_pulsar.Result.AlreadyClosed)
        self._closed = True
        self._fail_waiters(PulsarException(_pulsar.Result.AlreadyClosed))

        future = self._loop.create_future()
        close_async(functools.partial(_set_future, future, value=None))
        try:
            await future
        finally:
            # Any read that completes from now on is absorbed in
            # _handle_read_done; drain retained state so it is not mistaken for
            # deliverable data after shutdown.
            self._staged.clear()
            self._delivered.clear()
            self._fail_waiters(PulsarException(_pulsar.Result.AlreadyClosed))


class Producer:
    """
    The Pulsar message producer, used to publish messages on a topic.
    """

    def __init__(self, producer: _pulsar.Producer, schema: pulsar.schema.Schema) -> None:
        """
        Create the producer.
        Users should not call this constructor directly. Instead, create the
        producer via `Client.create_producer`.

        Parameters
        ----------
        producer: _pulsar.Producer
            The underlying Producer object from the C extension.
        schema: pulsar.schema.Schema
            The schema of the data that will be sent by this producer.
        """
        self._producer = producer
        self._schema = schema

    # pylint: disable=too-many-arguments,too-many-locals,too-many-positional-arguments
    async def send(self, content: Any,
                   properties: dict | None = None,
                   partition_key: str | None = None,
                   ordering_key: str | None = None,
                   sequence_id: int | None = None,
                   replication_clusters: List[str] | None = None,
                   disable_replication: bool | None = None,
                   event_timestamp: int | None = None,
                   deliver_at: int | None = None,
                   deliver_after: timedelta | None = None) -> pulsar.MessageId:
        """
        Send a message asynchronously.

        parameters
        ----------
        content: Any
            The message payload, whose type should respect the schema defined in
            `Client.create_producer`.
        properties: dict | None
            A dict of application-defined string properties.
        partition_key: str | None
            Sets the partition key for the message routing. A hash of this key is
            used to determine the message's topic partition.
        ordering_key: str | None
            Sets the ordering key for the message routing.
        sequence_id: int | None
            Specify a custom sequence id for the message being published.
        replication_clusters: List[str] | None
            Override namespace replication clusters. Note that it is the caller's responsibility
            to provide valid cluster names and that all clusters have been previously configured
            as topics. Given an empty list, the message will replicate per the namespace
            configuration.
        disable_replication: bool | None
            Do not replicate this message.
        event_timestamp: int | None
            Timestamp in millis of the timestamp of event creation
        deliver_at: int | None
            Specify the message should not be delivered earlier than the specified timestamp.
        deliver_after: timedelta | None
            Specify a delay in timedelta for the delivery of the messages.

        Returns
        -------
        pulsar.MessageId
            The message id that represents the persisted position of the message.

        Raises
        ------
        PulsarException
        """
        builder = _pulsar.MessageBuilder()
        builder.content(self._schema.encode(content))

        if properties is not None:
            for k, v in properties.items():
                builder.property(k, v)
        if partition_key is not None:
            builder.partition_key(partition_key)
        if ordering_key is not None:
            builder.ordering_key(ordering_key)
        if sequence_id is not None:
            builder.sequence_id(sequence_id)
        if replication_clusters is not None:
            builder.replication_clusters(replication_clusters)
        if disable_replication is not None:
            builder.disable_replication(disable_replication)
        if event_timestamp is not None:
            builder.event_timestamp(event_timestamp)
        if deliver_at is not None:
            builder.deliver_at(deliver_at)
        if deliver_after is not None:
            builder.deliver_after(deliver_after)

        future = asyncio.get_running_loop().create_future()
        self._producer.send_async(builder.build(), functools.partial(_set_future, future))
        msg_id = await future
        return pulsar.MessageId(
            msg_id.partition(),
            msg_id.ledger_id(),
            msg_id.entry_id(),
            msg_id.batch_index(),
        )

    async def flush(self) -> None:
        """
        Flush all the messages buffered in the producer asynchronously.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._producer.flush_async(functools.partial(_set_future, future, value=None))
        await future

    async def close(self) -> None:
        """
        Close the producer.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._producer.close_async(functools.partial(_set_future, future, value=None))
        await future

    def topic(self):
        """
        Return the topic which producer is publishing to
        """
        return self._producer.topic()

    def producer_name(self):
        """
        Return the producer name which could have been assigned by the
        system or specified by the client
        """
        return self._producer.producer_name()

    def last_sequence_id(self):
        """
        Return the last sequence id that was published and acknowledged by this producer.

        The sequence id can be either automatically assigned or custom set on the message.
        After recreating a producer with the same name, this will return the sequence id
        of the last message that was published in the previous session, or -1 if no
        message was ever published.
        """
        return self._producer.last_sequence_id()
    
    def is_connected(self) -> bool:
        """
        Check if the producer is connected or not.
        """
        return self._producer.is_connected()

class Consumer(_AsyncReadable):
    """
    The Pulsar message consumer, used to subscribe to messages from a topic.
    """

    def __init__(self, consumer: _pulsar.Consumer, schema: pulsar.schema.Schema) -> None:
        """
        Create the consumer.
        Users should not call this constructor directly. Instead, create the
        consumer via `Client.subscribe`.

        Parameters
        ----------
        consumer: _pulsar.Consumer
            The underlying Consumer object from the C extension.
        schema: pulsar.schema.Schema
            The schema of the data that will be received by this consumer.
        """
        super().__init__(schema)
        self._consumer = consumer

    def _start_underlying_read(self, callback: Callable[..., None]) -> None:
        self._consumer.receive_async(callback)

    def _handle_staging_overflow(self, message: _pulsar.Message) -> None:
        # The bounded staging queue is full. Explicitly give this late message
        # back to the broker for redelivery instead of silently dropping it.
        # negative_acknowledge is a blocking client call, so run it off the
        # event loop thread.
        async def redeliver() -> None:
            await asyncio.to_thread(self._consumer.negative_acknowledge, message)

        task = self._loop.create_task(redeliver())
        task.add_done_callback(_consume_task_exception)

    async def receive(self) -> pulsar.Message:
        """
        Receive a single message asynchronously.

        Cancelling the returned coroutine (for example via
        ``asyncio.wait_for`` timing out) does not cancel the underlying read,
        which is not cancellable. If the message arrives afterwards it is kept
        in an internal bounded queue and returned by a later ``receive`` call;
        if that queue is already full the message is negatively acknowledged
        for broker redelivery. A message is therefore never silently lost, and
        never delivered to a task that already gave up.

        Returns
        -------
        pulsar.Message
            The message received.

        Raises
        ------
        PulsarException
        """
        return await self._read()

    async def acknowledge(
        self,
        message: Union[pulsar.Message, pulsar.MessageId,
                       _pulsar.Message, _pulsar.MessageId]
    ) -> None:
        """
        Acknowledge the reception of a single message asynchronously.

        Parameters
        ----------
        message : Message, MessageId, _pulsar.Message, _pulsar.MessageId
            The received message or message id.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        if isinstance(message, pulsar.Message):
            msg = message._message
        elif isinstance(message, pulsar.MessageId):
            msg = message._msg_id
        else:
            msg = message
        self._consumer.acknowledge_async(msg, functools.partial(_set_future, future, value=None))
        await future

    async def acknowledge_cumulative(
        self,
        message: Union[pulsar.Message, pulsar.MessageId,
                       _pulsar.Message, _pulsar.MessageId]
    ) -> None:
        """
        Acknowledge the reception of all the messages in the stream up to (and
        including) the provided message asynchronously.

        Parameters
        ----------
        message : Message, MessageId, _pulsar.Message, _pulsar.MessageId
            The received message or message id.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        if isinstance(message, pulsar.Message):
            msg = message._message
        elif isinstance(message, pulsar.MessageId):
            msg = message._msg_id
        else:
            msg = message
        self._consumer.acknowledge_cumulative_async(
            msg, functools.partial(_set_future, future, value=None)
        )
        await future

    async def negative_acknowledge(
        self,
        message: Union[pulsar.Message, pulsar.MessageId, _pulsar.Message, _pulsar.MessageId]
    ) -> None:
        """
        Acknowledge the failure to process a single message asynchronously.

        When a message is "negatively acked" it will be marked for redelivery after
        some fixed delay. The delay is configurable when constructing the consumer
        with {@link ConsumerConfiguration#setNegativeAckRedeliveryDelayMs}.
        This call is not blocking.
        
        Parameters
        ----------
        message:
            The received message or message id.
        """
        if isinstance(message, pulsar.Message):
            msg = message._message
        elif isinstance(message, pulsar.MessageId):
            msg = message._msg_id
        else:
            msg = message
        await asyncio.to_thread(self._consumer.negative_acknowledge, msg)

    async def unsubscribe(self) -> None:
        """
        Unsubscribe the current consumer from the topic asynchronously.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.unsubscribe_async(functools.partial(_set_future, future, value=None))
        await future

    async def seek(self, messageid: Union[pulsar.MessageId, int]) -> None:
        """
        Reset the subscription associated with this consumer to a specific
        message id or publish timestamp asynchronously.

        The message id can either be a specific message or represent the first
        or last messages in the topic.

        Parameters
        ----------
        messageid : MessageId or int
            The message id for seek, OR an integer event time (timestamp) to
            seek to

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        if isinstance(messageid, pulsar.MessageId):
            msg_id = messageid._msg_id
        elif isinstance(messageid, int):
            msg_id = messageid
        else:
            raise ValueError(f"invalid messageid type {type(messageid)}")
        self._consumer.seek_async(
            msg_id, functools.partial(_set_future, future, value=None)
        )
        await future

    async def close(self) -> None:
        """
        Close the consumer asynchronously.

        Pending ``receive`` calls fail with a ``PulsarException`` carrying
        ``Result.AlreadyClosed``. Messages whose underlying read completes
        during or after closing are no longer delivered; unacknowledged
        messages remain tracked by the subscription and are redelivered by the
        broker according to its redelivery policy.

        Raises
        ------
        PulsarException
        """
        await self._close_readable(self._consumer.close_async)

    async def get_last_message_id(self) -> _pulsar.MessageId:
        """
        Asynchronously get the last message id.
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.get_last_message_id_async(functools.partial(_set_future, future))
        id = await future
        return id

    def redeliver_unacknowledged_messages(self):
        """
        Redelivers all the unacknowledged messages. In failover mode, the
        request is ignored if the consumer is not active for the given topic. In
        shared mode, the consumer's messages to be redelivered are distributed
        across all the connected consumers. This is a non-blocking call and
        doesn't throw an exception. In case the connection breaks, the messages
        are redelivered after reconnect.
        """
        self._consumer.redeliver_unacknowledged_messages()

    def topic(self) -> str:
        """
        Return the topic this consumer is subscribed to.
        """
        return self._consumer.topic()

    def subscription_name(self) -> str:
        """
        Return the subscription name.
        """
        return self._consumer.subscription_name()

    def consumer_name(self) -> str:
        """
        Return the consumer name.
        """
        return self._consumer.consumer_name()

class Reader(_AsyncReadable):
    """
    The Pulsar topic reader, used to read messages from a topic.
    """

    def __init__(self, reader: _pulsar.Reader, schema: pulsar.schema.Schema) -> None:
        """
        Create the reader.
        Users should not call this constructor directly. Instead, create the
        reader via ``Client.create_reader``.

        Parameters
        ----------
        reader: _pulsar.Reader
            The underlying Reader object from the C extension.
        schema: pulsar.schema.Schema
            The schema of the data that will be received by this reader.
        """
        super().__init__(schema, max_staged=None)
        self._reader = reader

    def _start_underlying_read(self, callback: Callable[..., None]) -> None:
        self._reader.read_next_async(callback)

    async def read_next(self) -> pulsar.Message:
        """
        Read a single message asynchronously.

        Cancelling the returned coroutine (for example via
        ``asyncio.wait_for`` timing out) does not cancel the underlying read,
        which is not cancellable. A reader has no acknowledgement mechanism,
        so a message that arrives after the read was cancelled is retained and
        returned by the next ``read_next`` call: the reader's advanced
        position is never skipped over.

        Returns
        -------
        pulsar.Message
            The message received.

        Raises
        ------
        PulsarException
        """
        return await self._read()

    async def has_message_available(self) -> bool:
        """
        Check if there is any message available to read from the current
        position.
        """
        future = asyncio.get_running_loop().create_future()
        self._reader.has_message_available_async(functools.partial(_set_future, future))
        return await future

    async def seek(self, messageid: Union[pulsar.MessageId, int]) -> None:
        """
        Reset this reader to a specific message id or publish timestamp
        asynchronously.

        Parameters
        ----------
        messageid : MessageId or int
            The message id for seek, OR an integer event time (timestamp) to
            seek to.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        if isinstance(messageid, pulsar.MessageId):
            msg_id = messageid._msg_id
        elif isinstance(messageid, int):
            msg_id = messageid
        else:
            raise ValueError(f"invalid messageid type {type(messageid)}")
        self._reader.seek_async(msg_id, functools.partial(_set_future, future, value=None))
        await future

    async def close(self) -> None:
        """
        Close the reader asynchronously.

        Pending ``read_next`` calls fail with a ``PulsarException`` carrying
        ``Result.AlreadyClosed`` and any late callback is absorbed.

        Raises
        ------
        PulsarException
        """
        await self._close_readable(self._reader.close_async)

    def topic(self) -> str:
        """
        Return the topic this reader is reading from.
        """
        return self._reader.topic()

    def is_connected(self) -> bool:
        """
        Check if the reader is connected or not.
        """
        return self._reader.is_connected()


class Client:
    """
    The asynchronous version of `pulsar.Client`.
    """

    def __init__(self, service_url, **kwargs) -> None:
        """
        See `pulsar.Client.__init__`
        """
        self._client: _pulsar.Client = pulsar.Client(service_url, **kwargs)._client

    # pylint: disable=too-many-arguments,too-many-locals,too-many-positional-arguments
    async def create_producer(self, topic: str,
                              producer_name: str | None = None,
                              schema: pulsar.schema.Schema | None = None,
                              initial_sequence_id: int | None = None,
                              send_timeout_millis: int = 30000,
                              compression_type: CompressionType = CompressionType.NONE,
                              max_pending_messages: int = 1000,
                              max_pending_messages_across_partitions: int = 50000,
                              block_if_queue_full: bool = False,
                              batching_enabled: bool = True,
                              batching_max_messages: int = 1000,
                              batching_max_allowed_size_in_bytes: int = 128*1024,
                              batching_max_publish_delay_ms: int = 10,
                              chunking_enabled: bool = False,
                              message_routing_mode: PartitionsRoutingMode =
                              PartitionsRoutingMode.RoundRobinDistribution,
                              lazy_start_partitioned_producers: bool = False,
                              properties: dict | None = None,
                              batching_type: BatchingType = BatchingType.Default,
                              encryption_key: str | None = None,
                              crypto_key_reader: pulsar.CryptoKeyReader | None = None,
                              access_mode: ProducerAccessMode = ProducerAccessMode.Shared,
                              message_router: Callable[[pulsar.Message, int], int] | None = None,
                              ) -> Producer:
        """
        Create a new producer on a given topic

        Parameters
        ----------
        topic: str
            The topic name
        producer_name: str | None
            Specify a name for the producer. If not assigned, the system will generate a globally
            unique name which can be accessed with `Producer.producer_name()`. When specifying a
            name, it is up to the user to ensure that, for a given topic, the producer name is
            unique across all Pulsar's clusters.
        schema: pulsar.schema.Schema | None, default=None
            Define the schema of the data that will be published by this producer.
        initial_sequence_id: int | None, default=None
            Set the baseline for the sequence ids for messages published by
            the producer.
        send_timeout_millis: int, default=30000
            If a message is not acknowledged by the server before the
            send_timeout expires, an error will be reported.
        compression_type: CompressionType, default=CompressionType.NONE
            Set the compression type for the producer.
        max_pending_messages: int, default=1000
            Set the max size of the queue holding the messages pending to
            receive an acknowledgment from the broker.
        max_pending_messages_across_partitions: int, default=50000
            Set the max size of the queue holding the messages pending to
            receive an acknowledgment across partitions.
        block_if_queue_full: bool, default=False
            Set whether send operations should block when the outgoing
            message queue is full.
        batching_enabled: bool, default=True
            Enable automatic message batching. Note that, unlike the synchronous producer API in
            ``pulsar.Client.create_producer``, batching is enabled by default for the asyncio
            producer.
        batching_max_messages: int, default=1000
            Maximum number of messages in a batch.
        batching_max_allowed_size_in_bytes: int, default=128*1024
            Maximum size in bytes of a batch.
        batching_max_publish_delay_ms: int, default=10
            The batch interval in milliseconds.
        chunking_enabled: bool, default=False
            Enable chunking of large messages.
        message_routing_mode: PartitionsRoutingMode,
            default=PartitionsRoutingMode.RoundRobinDistribution
            Set the message routing mode for the partitioned producer.
        lazy_start_partitioned_producers: bool, default=False
            Start partitioned producers lazily on demand.
        properties: dict | None, default=None
            Sets the properties for the producer.
        batching_type: BatchingType, default=BatchingType.Default
            Sets the batching type for the producer.
        encryption_key: str | None, default=None
            The key used for symmetric encryption.
        crypto_key_reader: pulsar.CryptoKeyReader | None, default=None
            Symmetric encryption class implementation.
        access_mode: ProducerAccessMode, default=ProducerAccessMode.Shared
            Set the type of access mode that the producer requires on the topic.
        message_router: Callable[[pulsar.Message, int], int] | None, default=None
            A custom message router function that takes a Message and the
            number of partitions and returns the partition index.

        Returns
        -------
        Producer
            The producer created

        Raises
        ------
        PulsarException
        """
        if batching_enabled and chunking_enabled:
            raise ValueError("Batching and chunking of messages can't be enabled together.")

        if schema is None:
            schema = pulsar.schema.BytesSchema()
        schema.attach_client(self._client)

        future = asyncio.get_running_loop().create_future()
        conf = _pulsar.ProducerConfiguration()
        if producer_name is not None:
            conf.producer_name(producer_name)
        conf.schema(schema.schema_info())
        if initial_sequence_id is not None:
            conf.initial_sequence_id(initial_sequence_id)
        conf.send_timeout_millis(send_timeout_millis)
        conf.compression_type(compression_type)
        conf.max_pending_messages(max_pending_messages)
        conf.max_pending_messages_across_partitions(max_pending_messages_across_partitions)
        conf.block_if_queue_full(block_if_queue_full)
        conf.batching_enabled(batching_enabled)
        conf.batching_max_messages(batching_max_messages)
        conf.batching_max_allowed_size_in_bytes(batching_max_allowed_size_in_bytes)
        conf.batching_max_publish_delay_ms(batching_max_publish_delay_ms)
        conf.chunking_enabled(chunking_enabled)
        conf.partitions_routing_mode(message_routing_mode)
        conf.lazy_start_partitioned_producers(lazy_start_partitioned_producers)
        if properties is not None:
            for k, v in properties.items():
                conf.property(k, v)
        conf.batching_type(batching_type)
        if encryption_key is not None:
            conf.encryption_key(encryption_key)
        if crypto_key_reader is not None:
            conf.crypto_key_reader(crypto_key_reader.cryptoKeyReader)
        conf.access_mode(access_mode)
        if message_router is not None:
            def underlying_router(msg: _pulsar.Message, num_partitions: int) -> int:
                return message_router(pulsar.Message._wrap(msg), num_partitions)
            conf.message_router(underlying_router)

        self._client.create_producer_async_v2(
            topic, conf, functools.partial(_set_future_v2, future)
        )
        return Producer(await future, schema)

    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-positional-arguments
    async def subscribe(self, topic: Union[str, List[str]],
                        subscription_name: str,
                        consumer_type: pulsar.ConsumerType =
                        pulsar.ConsumerType.Exclusive,
                        schema: pulsar.schema.Schema | None = None,
                        receiver_queue_size: int = 1000,
                        max_total_receiver_queue_size_across_partitions: int =
                        50000,
                        consumer_name: str | None = None,
                        unacked_messages_timeout_ms: int | None = None,
                        broker_consumer_stats_cache_time_ms: int = 30000,
                        negative_ack_redelivery_delay_ms: int = 60000,
                        is_read_compacted: bool = False,
                        properties: dict | None = None,
                        initial_position: InitialPosition = InitialPosition.Latest,
                        crypto_key_reader: pulsar.CryptoKeyReader | None = None,
                        replicate_subscription_state_enabled: bool = False,
                        max_pending_chunked_message: int = 10,
                        auto_ack_oldest_chunked_message_on_queue_full: bool = False,
                        start_message_id_inclusive: bool = False,
                        batch_receive_policy: pulsar.ConsumerBatchReceivePolicy | None =
                        None,
                        key_shared_policy: pulsar.ConsumerKeySharedPolicy | None =
                        None,
                        batch_index_ack_enabled: bool = False,
                        regex_subscription_mode: RegexSubscriptionMode =
                        RegexSubscriptionMode.PersistentOnly,
                        dead_letter_policy: pulsar.ConsumerDeadLetterPolicy | None =
                        None,
                        crypto_failure_action: ConsumerCryptoFailureAction =
                        ConsumerCryptoFailureAction.FAIL,
                        is_pattern_topic: bool = False) -> Consumer:
        """
        Subscribe to the given topic and subscription combination.

        Parameters
        ----------
        topic: str, List[str], or regex pattern
            The name of the topic, list of topics or regex pattern.
            When `is_pattern_topic` is True, `topic` is treated as a regex.
        subscription_name: str
            The name of the subscription.
        consumer_type: pulsar.ConsumerType, default=pulsar.ConsumerType.Exclusive
            Select the subscription type to be used when subscribing to the topic.
        schema: pulsar.schema.Schema | None, default=None
            Define the schema of the data that will be received by this consumer.
        receiver_queue_size: int, default=1000
            Sets the size of the consumer receive queue.
        max_total_receiver_queue_size_across_partitions: int, default=50000
            Set the max total receiver queue size across partitions.
        consumer_name: str | None, default=None
            Sets the consumer name.
        unacked_messages_timeout_ms: int | None, default=None
            Sets the timeout in milliseconds for unacknowledged messages.
        broker_consumer_stats_cache_time_ms: int, default=30000
            Sets the time duration for which the broker-side consumer stats
            will be cached in the client.
        negative_ack_redelivery_delay_ms: int, default=60000
            The delay after which to redeliver the messages that failed to be
            processed.
        is_read_compacted: bool, default=False
            Selects whether to read the compacted version of the topic.
        properties: dict | None, default=None
            Sets the properties for the consumer.
        initial_position: InitialPosition, default=InitialPosition.Latest
            Set the initial position of a consumer when subscribing to the topic.
        crypto_key_reader: pulsar.CryptoKeyReader | None, default=None
            Symmetric encryption class implementation.
        replicate_subscription_state_enabled: bool, default=False
            Set whether the subscription status should be replicated.
        max_pending_chunked_message: int, default=10
            Consumer buffers chunk messages into memory until it receives all the chunks.
        auto_ack_oldest_chunked_message_on_queue_full: bool, default=False
            Automatically acknowledge oldest chunked messages on queue
            full.
        start_message_id_inclusive: bool, default=False
            Set the consumer to include the given position of any reset
            operation.
        batch_receive_policy: pulsar.ConsumerBatchReceivePolicy | None, default=None
            Set the batch collection policy for batch receiving.
        key_shared_policy: pulsar.ConsumerKeySharedPolicy | None, default=None
            Set the key shared policy for use when the ConsumerType is
            KeyShared.
        batch_index_ack_enabled: bool, default=False
            Enable the batch index acknowledgement.
        regex_subscription_mode: RegexSubscriptionMode,
            default=RegexSubscriptionMode.PersistentOnly
            Set the regex subscription mode for use when the topic is a regex
            pattern.
        dead_letter_policy: pulsar.ConsumerDeadLetterPolicy | None, default=None
            Set dead letter policy for consumer.
        crypto_failure_action: ConsumerCryptoFailureAction,
            default=ConsumerCryptoFailureAction.FAIL
            Set the behavior when the decryption fails.
        is_pattern_topic: bool, default=False
            Whether `topic` is a regex pattern. If it's True when `topic` is a list, a ValueError
            will be raised.

        Returns
        -------
        Consumer
            The consumer created

        Raises
        ------
        PulsarException
        """
        if schema is None:
            schema = pulsar.schema.BytesSchema()

        future = asyncio.get_running_loop().create_future()
        conf = _pulsar.ConsumerConfiguration()
        conf.consumer_type(consumer_type)
        conf.regex_subscription_mode(regex_subscription_mode)
        conf.read_compacted(is_read_compacted)
        conf.receiver_queue_size(receiver_queue_size)
        conf.max_total_receiver_queue_size_across_partitions(
            max_total_receiver_queue_size_across_partitions
        )
        if consumer_name:
            conf.consumer_name(consumer_name)
        if unacked_messages_timeout_ms:
            conf.unacked_messages_timeout_ms(unacked_messages_timeout_ms)

        conf.negative_ack_redelivery_delay_ms(negative_ack_redelivery_delay_ms)
        conf.broker_consumer_stats_cache_time_ms(broker_consumer_stats_cache_time_ms)
        if properties:
            for k, v in properties.items():
                conf.property(k, v)
        conf.subscription_initial_position(initial_position)

        conf.schema(schema.schema_info())

        if crypto_key_reader:
            conf.crypto_key_reader(crypto_key_reader.cryptoKeyReader)

        conf.replicate_subscription_state_enabled(replicate_subscription_state_enabled)
        conf.max_pending_chunked_message(max_pending_chunked_message)
        conf.auto_ack_oldest_chunked_message_on_queue_full(
            auto_ack_oldest_chunked_message_on_queue_full
        )
        conf.start_message_id_inclusive(start_message_id_inclusive)
        if batch_receive_policy:
            conf.batch_receive_policy(batch_receive_policy.policy())

        if key_shared_policy:
            conf.key_shared_policy(key_shared_policy.policy())
        conf.batch_index_ack_enabled(batch_index_ack_enabled)
        if dead_letter_policy:
            conf.dead_letter_policy(dead_letter_policy.policy())
        conf.crypto_failure_action(crypto_failure_action)

        if isinstance(topic, str):
            if is_pattern_topic:
                topics = _pulsar.TopicRegex(topic)
            else:
                topics = topic
        elif isinstance(topic, list):
            if is_pattern_topic:
                raise ValueError(
                    "Argument 'topic' must be a string when "
                    "'is_pattern_topic' is True; lists of topics do not "
                    "support pattern subscriptions"
                )
            topics = topic
        else:
            raise ValueError( "Argument 'topic' is expected to be of type 'str' or 'list'")
        self._client.subscribe_async_v2(
            topics, subscription_name, conf,
            functools.partial(_set_future_v2, future)
        )

        schema.attach_client(self._client)
        return Consumer(await future, schema)
    
    # pylint: disable=too-many-arguments,too-many-locals,too-many-positional-arguments
    async def create_reader(self, topic: str,
                            start_message_id: Union[pulsar.MessageId, _pulsar.MessageId],
                            schema: pulsar.schema.Schema | None = None,
                            receiver_queue_size: int = 1000,
                            reader_name: str | None = None,
                            subscription_role_prefix: str | None = None,
                            is_read_compacted: bool = False,
                            crypto_key_reader: pulsar.CryptoKeyReader | None = None,
                            start_message_id_inclusive: bool = False,
                            crypto_failure_action: ConsumerCryptoFailureAction =
                            ConsumerCryptoFailureAction.FAIL,
                            ) -> Reader:
        """
        Create a reader on a particular topic.

        Parameters
        ----------
        topic: str
            The name of the topic.
        start_message_id: MessageId or _pulsar.MessageId
            The initial reader positioning is done by specifying a message id.
            The options are:

            * ``MessageId.earliest``: Start reading from the earliest message
              available in the topic.
            * ``MessageId.latest``: Start reading from the end topic, only
              getting messages published after the reader was created.
            * ``MessageId``: When passing a particular message id, the reader
              will position itself on that specific position.
        schema: pulsar.schema.Schema | None, default=None
            Define the schema of the data that will be received by this reader.
        receiver_queue_size: int, default=1000
            Sets the size of the reader receive queue.
        reader_name: str | None, default=None
            Sets the reader name.
        subscription_role_prefix: str | None, default=None
            Sets the subscription role prefix.
        is_read_compacted: bool, default=False
            Selects whether to read the compacted version of the topic.
        crypto_key_reader: pulsar.CryptoKeyReader | None, default=None
            Symmetric encryption class implementation.
        start_message_id_inclusive: bool, default=False
            Set the reader to include the startMessageId or given position of
            any reset operation like Reader.seek.
        crypto_failure_action: ConsumerCryptoFailureAction, \
            default=ConsumerCryptoFailureAction.FAIL
            Set the behavior when the decryption fails.

        Returns
        -------
        Reader
            The reader created

        Raises
        ------
        PulsarException
        """
        if schema is None:
            schema = pulsar.schema.BytesSchema()

        if isinstance(start_message_id, pulsar.MessageId):
            start_message_id = start_message_id._msg_id

        _check_type(_pulsar.MessageId, start_message_id, 'start_message_id')

        conf = _pulsar.ReaderConfiguration()
        conf.receiver_queue_size(receiver_queue_size)
        if reader_name is not None:
            conf.reader_name(reader_name)
        if subscription_role_prefix is not None:
            conf.subscription_role_prefix(subscription_role_prefix)
        conf.schema(schema.schema_info())
        conf.read_compacted(is_read_compacted)
        if crypto_key_reader is not None:
            conf.crypto_key_reader(crypto_key_reader.cryptoKeyReader)
        conf.start_message_id_inclusive(start_message_id_inclusive)
        conf.crypto_failure_action(crypto_failure_action)

        future = asyncio.get_running_loop().create_future()
        self._client.create_reader_async_v2(
            topic, start_message_id, conf, functools.partial(_set_future_v2, future)
        )
        reader = await future
        schema.attach_client(self._client)
        return Reader(reader, schema)

    def shutdown(self) -> None:
        """
        Shutdown the client and all the associated producers and consumers

        Raises
        ------
        PulsarException
        """
        self._client.shutdown()

    async def get_topic_partitions(self, topic: str) -> List[str]:
        """
        Get the list of partitions for a given topic in asynchronous mode.

        If the topic is partitioned, this will return a list of partition names. If the topic is not partitioned, the returned list will contain the topic name itself.

        This can be used to discover the partitions and create Reader, Consumer or Producer instances directly on a particular partition.

        Parameters
        ----------

        topic: str
            the topic name to lookup

        Returns
        -------
        list
            a list of partition names
        """
        _check_type(str, topic, 'topic')
        future = asyncio.get_running_loop().create_future()
        self._client.get_topic_partitions_async(topic, functools.partial(_set_future, future))
        id = await future
        return id

    async def close(self) -> None:
        """
        Close the client and all the associated producers and consumers

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._client.close_async(
            functools.partial(_set_future, future, value=None)
        )
        await future

def _consume_task_exception(task: asyncio.Task) -> None:
    """Retrieve a background task's exception so it never surfaces as an
    unhandled-exception warning."""
    try:
        task.exception()
    except (asyncio.CancelledError, asyncio.InvalidStateError):
        pass

def _set_future(future: asyncio.Future, result: _pulsar.Result, value: Any):
    def complete():
        if future.done():
            return
        if result == _pulsar.Result.Ok:
            future.set_result(value)
        else:
            future.set_exception(PulsarException(result))
    future.get_loop().call_soon_threadsafe(complete)

def _set_future_v2(future: asyncio.Future, value: Any):
    def callback():
        if future.done():
            return
        if isinstance(value, _pulsar.Error):
            exc = PulsarException(value.error, value.message)
            future.get_loop().call_soon_threadsafe(future.set_exception, exc)
        else:
            future.get_loop().call_soon_threadsafe(future.set_result, value)
    future.get_loop().call_soon_threadsafe(callback)
