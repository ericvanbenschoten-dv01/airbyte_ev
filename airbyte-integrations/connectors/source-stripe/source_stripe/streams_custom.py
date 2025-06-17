import time
import math
import psutil
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import pendulum
import requests
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed

from airbyte_cdk import StreamSlice
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.core import StreamData
from source_stripe.utils import safe_stream_state
from source_stripe.streams import IncrementalStripeStream, UpdatedCursorIncrementalRecordExtractor, StripeStream, IRecordExtractor, Events, EventRecordExtractor, CreatedCursorIncrementalStripeStream, ParentIncrementalStripeSubStream, IStreamSelector, StripeSubStream
from concurrent.futures import ThreadPoolExecutor, as_completed
from source_stripe.streams import StripeStream
class ThreadedIncrementalStripeStream(IncrementalStripeStream):
    is_resumable = True
    """
    This class combines both normal incremental sync and event based sync. For initial full refresh sync mode we are using the Search API with custom filters
    and incremental syncs we are using the event based sync with post processing filtering.
    """

    def __init__(
        self,
        *args,
        path: Optional[str] = None,
        cursor_field: str = "updated",
        legacy_cursor_field: Optional[str] = "created",
        event_types: Optional[List[str]] = None,
        response_filter: Optional[Callable] = None,
        expand_items: Optional[List[str]] = None,
        max_workers: int = 20,
        extra_request_params: Optional[Union[Mapping[str, Any], Callable]] = None,
        inject_subscription_cancellations: bool = False,
        **kwargs,
    ):
        self._cursor_field = cursor_field
        self._path = path
        is_search_api = path and "search" in path
        super().__init__(*args, **kwargs)
        created_cursor_stream = BoundedThreadedCreatedCursorIncrementalStripeStream(
            *args,
            path=path,
            cursor_field=cursor_field,
            lookback_window_days=0,
            record_extractor=UpdatedCursorIncrementalRecordExtractor(cursor_field, legacy_cursor_field),
            expand_items=expand_items,
            extra_request_params=extra_request_params,
            max_workers=max_workers,
            is_search_api=is_search_api,
            inject_subscription_cancellations=inject_subscription_cancellations,
            **kwargs,
        )
        updated_cursor_stream = ThreadedUpdatedCursorIncrementalStripeStream(
            *args,
            path=path,
            cursor_field=cursor_field,
            legacy_cursor_field=legacy_cursor_field,
            event_types=event_types,
            expand_items=expand_items,
            response_filter=response_filter,
            max_workers=max_workers,
            **kwargs,
        )
        self._parent_stream = None
        self.stream_selector = ThreadedIncrementalStripeStreamSelector(created_cursor_stream, updated_cursor_stream)

    @property
    def path(self) -> Optional[str]:
        return self._path
class ThreadedParentIncrementalStripeSubStream(ParentIncrementalStripeSubStream):
    """
    A substream that:
    - Runs the parent stream in the same sync mode
    - Batches parent records into fixed-size groups
    - Fetches parent records concurrently from different slices
    """
    is_resumable = True

    @property
    def cursor_field(self) -> str:
        return self._cursor_field

    def __init__(self, *args, **kwargs):
        self._cursor_field = kwargs.pop("cursor_field")
        self.max_workers = kwargs.pop("max_workers", 20)
        self.queue_size = 10000
        #self.memory_log_interval = 5000

        super().__init__(cursor_field=self._cursor_field, *args, **kwargs)

    def _fetch_slice(self, slice_, sync_mode, cursor_field, stream_state):
        records = []
        for record in self.parent.read_records(sync_mode, cursor_field, slice_, stream_state):
            minimal_record = {
                "id": record["id"],
                "created": record.get("created"),
                "updated": record.get("updated"),
            }
            records.append(minimal_record)
        mem = psutil.Process().memory_info().rss / 1024 / 1024
        self.logger.info(f"[ThreadedParentIncrementalStripeSubStream Generate Slices] Memory usage: {mem:.2f}MB")
        return records

    def stream_slices(self, sync_mode, cursor_field=None, stream_state=None):
        stream_state = safe_stream_state(stream_state, self.cursor_field) or {}
        if stream_state:
            stream_state = {self.parent.cursor_field: stream_state.get(self.cursor_field, 0)}

        parent_slices = list(self.parent.stream_slices(sync_mode=sync_mode, cursor_field=cursor_field, stream_state=stream_state))
        self.logger.info(f"[stream_slices] parent slices {parent_slices}")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._fetch_slice, s, sync_mode, cursor_field, stream_state) for s in parent_slices]
            for future in as_completed(futures):
                records = future.result()
                yield {"batched_parents": records}

    def _process_parent(self, parent_record, sync_mode, cursor_field, stream_state):
        slice_data = StreamSlice(partition={"parent": parent_record}, cursor_slice={})
        return super().read_records(sync_mode, cursor_field, slice_data, stream_state)

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None):
        stream_state = stream_state or {}
        batched_parents = stream_slice["batched_parents"]

        q = queue.Queue(maxsize=self.queue_size)
        finished = 0
        processed = 0

        def child_worker(parent_record):
            for record in self._process_parent(parent_record, sync_mode, cursor_field, stream_state):
                q.put(record)
            q.put(None)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(child_worker, parent) for parent in batched_parents]
            while finished < len(batched_parents):
                item = q.get()
                if item is None:
                    finished += 1
                else:
                    yield item
                    processed += 1

class ThreadedUpdatedCursorIncrementalStripeStream(StripeStream):
    is_resumable = True
    """
    `CreatedCursorIncrementalStripeStream` does not provide a way to read updated data since given date because the API does not allow to do this.
    It only returns newly created entities since given date. So to have all the updated data as well we need to make use of the Events API,
    which allows to retrieve updated data since given date for a number of predefined events which are associated with the corresponding
    entities.
    """

    @property
    def cursor_field(self):
        return self._cursor_field

    @property
    def legacy_cursor_field(self):
        return self._legacy_cursor_field

    @property
    def event_types(self) -> Iterable[str]:
        """A list of event types that are associated with entity."""
        return self._event_types

    def __init__(
        self,
        *args,
        cursor_field: str = "updated",
        legacy_cursor_field: Optional[str] = "created",
        event_types: Optional[List[str]] = None,
        record_extractor: Optional[IRecordExtractor] = None,
        response_filter: Optional[Callable] = None,
        max_workers: int = 20,
        **kwargs,
    ):
        self._event_types = event_types
        self._cursor_field = cursor_field
        self.max_workers = max_workers
        self._legacy_cursor_field = legacy_cursor_field
        record_extractor = record_extractor or UpdatedCursorIncrementalRecordExtractor(
            self.cursor_field, self.legacy_cursor_field, response_filter
        )
        super().__init__(*args, record_extractor=record_extractor, **kwargs)
        self.events_stream = Events(
            authenticator=kwargs.get("authenticator"),
            lookback_window_days=0,
            start_date_max_days_from_now=30,
            account_id=self.account_id,
            start_date=self.start_date,
            slice_range=self.slice_range,
            event_types=self.event_types,
            cursor_field=self.cursor_field,
            record_extractor=EventRecordExtractor(cursor_field=self.cursor_field, response_filter=response_filter),
        )

    def update_cursor_field(self, stream_state: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        if not self.legacy_cursor_field:
            # Streams that used to support only full_refresh mode.
            # Now they support event-based incremental syncs but have a cursor field only in that mode.
            return stream_state
        # support for both legacy and new cursor fields
        current_stream_state_value = stream_state.get(self.cursor_field, stream_state.get(self.legacy_cursor_field, 0))
        return {self.cursor_field: current_stream_state_value}

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_record_value = latest_record.get(self.cursor_field)
        current_stream_state = self.update_cursor_field(current_stream_state)
        current_state_value = current_stream_state.get(self.cursor_field)
        if current_state_value:
            return {self.cursor_field: max(latest_record_value, current_state_value)}
        return {self.cursor_field: latest_record_value}

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        # When reading from a stream, a `read_records` is called once per slice.
        # We yield a single slice here because we don't want to make duplicate calls for event based incremental syncs.
        yield StreamSlice(partition={}, cursor_slice={})

    def read_event_increments(
        self, cursor_field: Optional[List[str]] = None, stream_state: Optional[Mapping[str, Any]] = None
    ) -> Iterable[StreamData]:
        """
        Runs all event slices concurrently instead of serially
        """
        stream_state = self.update_cursor_field(stream_state or {})
        slices = list(self.events_stream.stream_slices(
            sync_mode=SyncMode.incremental,
            cursor_field=cursor_field,
            stream_state=stream_state
        ))

        def process_slice(event_slice):
            return list(self.events_stream.read_records(
                SyncMode.incremental,
                cursor_field=cursor_field,
                stream_slice=event_slice,
                stream_state=stream_state
            ))

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_slice = {executor.submit(process_slice, s): s for s in slices}
            for future in as_completed(future_to_slice):
                for record in future.result():
                    yield record

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        if not stream_state:
            # both full refresh and initial incremental sync should use usual endpoints
            yield from super().read_records(sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
            return
        yield from self.read_event_increments(cursor_field=cursor_field, stream_state=stream_state)
class BoundedThreadedCreatedCursorIncrementalStripeStream(CreatedCursorIncrementalStripeStream):
    is_resumable = True
    state_checkpoint_interval = math.inf

    def __init__(
        self,
        *args,
        lookback_window_days: int = 0,
        start_date_max_days_from_now: Optional[int] = None,
        cursor_field: str = "created",
        is_search_api: bool = False,
        max_workers: int = 20,
        queue_size=10000,
        inject_subscription_cancellations: bool = False,
        **kwargs,
    ):
        self._cursor_field = cursor_field
        self.is_search_api = is_search_api
        self.max_workers = max_workers
        self.queue_size = queue_size
        super().__init__(*args, **kwargs)
        self.lookback_window_days = lookback_window_days
        self.start_date_max_days_from_now = start_date_max_days_from_now
        self.inject_subscription_cancellations = inject_subscription_cancellations

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        if self.is_search_api:
            params = {
                **(self.extra_request_params(
                    stream_state=stream_state,
                    stream_slice=stream_slice,
                    next_page_token=next_page_token
                ) or {}),
                "limit": 100
            }
            if next_page_token:
                params["page"] = next_page_token["page"]
            if self.expand_items:
                params["expand[]"] = self.expand_items
            return params

        else:
            params = super(BoundedThreadedCreatedCursorIncrementalStripeStream, self).request_params(
                stream_state, stream_slice, next_page_token
            )
            return {
                "created[gte]": stream_slice["created[gte]"],
                "created[lte]": stream_slice["created[lte]"],
                **params
            }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        json_resp = response.json()

        if self.is_search_api:
            if json_resp.get("has_more") and "next_page" in json_resp:
                return {"page": json_resp["next_page"]}
            return None
        else:
            if json_resp.get("has_more") and json_resp.get("data"):
                return {"starting_after": json_resp["data"][-1]["id"]}
            return None

    def stream_slices(self, sync_mode, cursor_field=None, stream_state=None):
        stream_state = stream_state or {}
        start_ts = self.get_start_timestamp(stream_state)
        if start_ts >= pendulum.now().int_timestamp:
            return []
        slices = [
            {"created[gte]": start, "created[lte]": end}
            for start, end in self.chunk_dates(start_ts)
        ]
        return [{"batched_slices": slices}]

    def _read_slice(self, stream_slice, sync_mode, cursor_field, stream_state):
        return list(super().read_records(
            sync_mode=sync_mode,
            cursor_field=cursor_field,
            stream_slice=stream_slice,
            stream_state=stream_state,
        ))

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None):
        stream_state = stream_state or {}
        slices = stream_slice["batched_slices"]
        q = queue.Queue(maxsize=self.queue_size)

        def worker(slice_):
            for record in self._read_slice(slice_, sync_mode, cursor_field, stream_state):
                if (self.inject_subscription_cancellations and
                    self.name == "subscriptions" and
                    record.get("status") == "canceled" and
                    record.get("cancel_at_period_end") is False and
                    (record.get("cancel_at") or record.get("canceled_at"))):
                    # Calculate the new updated with cancellation timestamp
                    updated_cancel_ts = max(record.get("cancel_at", 0) or 0, record.get("canceled_at", 0) or 0)
                    updated_synthetic = min(record.get("updated", updated_cancel_ts), updated_cancel_ts - 86400)
                    # 1. Update the original canceled record
                    updated_record = record.copy()
                    updated_record.update({
                        "created": updated_synthetic,
                        "updated": updated_cancel_ts
                    })
                    q.put(updated_record)
                    # 2. Create synthetic "active" record (1 day before cancellation)
                    synthetic_record = record.copy()
                    synthetic_record.update({
                        "status": "active",
                        "created": updated_synthetic,
                        "updated": updated_synthetic,
                        "cancel_at": None,
                        "canceled_at": None
                    })
                    q.put(synthetic_record)
                else:
                    q.put(record)
            q.put(None)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for s in slices:
                executor.submit(worker, s)

            finished = 0
            while finished < len(slices):
                item = q.get()
                if item is None:
                    finished += 1
                else:
                    yield item
class ThreadedIncrementalStripeStreamSelector(IStreamSelector):
    def __init__(
        self,
        created_cursor_incremental_stream: CreatedCursorIncrementalStripeStream,
        updated_cursor_incremental_stream: ThreadedUpdatedCursorIncrementalStripeStream
    ):
        self._created_cursor_stream = created_cursor_incremental_stream
        self._updated_cursor_stream = updated_cursor_incremental_stream

    def get_parent_stream(self, stream_state: Mapping[str, Any]) -> StripeStream:
        return self._updated_cursor_stream if stream_state else self._created_cursor_stream
class CustomerBalanceTransactions(ParentIncrementalStripeSubStream):
    """
    Optimized threaded version of CustomerBalanceTransactions stream.
    Uses multiple parent streams (e.g., invoices, customers), threads parent record reading,
    deduplicates customer IDs, and applies a lookback window for safe incremental syncs.
    """

    def __init__(self, cursor_field: str, customerStream: StripeSubStream, invoiceStream: StripeSubStream, max_workers: int = 20, *args, **kwargs):
        super().__init__(cursor_field=cursor_field, parent=None, *args, **kwargs)
        self.customerStream = customerStream
        self.invoiceStream = invoiceStream
        self.max_workers = max_workers
        self.batch_size = 500
        self.queue_size: int = 10000
        self._balance_filter_enabled = True
        self.parent_streams = [self.customerStream, self.invoiceStream]

    def normalize_state(self, state: Optional[Mapping[str, Any]]) -> dict:
        """Unifies legacy and new state format."""
        state = (state or {}).get("data", state or {})
        normalized = {
            "cursor": state.get(f"{self.name}_cursor_created") or state.get("created") or state.get("cursor") or self.start_date,
            "parents": {
                p.name: state.get(f"{p.name}_cursor_updated") or state.get("updated") or state.get("created") or state.get("parents", {}).get(p.name) or self.start_date
                for p in self.parent_streams
            }
        }
        return normalized

    def get_updated_state(self, current_state: Mapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:

        previous_cursor_minus_1 = (current_state or {}).get("cursor", self.start_date) - int(timedelta(days=1).total_seconds())
        latest_cursor_minus_1 = latest_record.get(self.cursor_field) - int(timedelta(days=1).total_seconds())
        today_minus_3 = int(datetime.utcnow().timestamp()) - int(timedelta(days=3).total_seconds())
        new_cursor = max(latest_cursor_minus_1, today_minus_3, previous_cursor_minus_1)
        updated_parents = {
            p.name: new_cursor
            for p in self.parent_streams
        }

        return {
            "cursor": new_cursor,
            "parents": updated_parents
        }

    def stream_slices(self, sync_mode: SyncMode, cursor_field=None, stream_state=None):
        normalized_state = self.normalize_state(stream_state) if stream_state else {}

        if stream_state:
            self.logger.info(f"[CBT] Running in FullRefresh mode")
            self._balance_filter_enabled = False
            self.parent_streams = [self.customerStream]
        else:
            self.logger.info(f"[CBT] Running in Incremental mode")
            self._balance_filter_enabled = True
            self.parent_streams = [self.customerStream, self.invoiceStream]

        seen = set()
        any_records = False
        buffer = []

        self.logger.info(f"[CBT] Running parent streams")
        for parent in self.parent_streams:
            slices = parent.stream_slices(sync_mode=sync_mode, cursor_field=parent.cursor_field, stream_state=normalized_state)
            for stream_slice in slices:
                parent_records = parent.read_records(
                    sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=normalized_state
                )
                for record in parent_records:
                    parent_id = record.get("customer") or record.get("id")
                    balance_filter = ((record.get("balance") or record.get("total", 0)) != 0) if self._balance_filter_enabled else True
                    if parent_id and parent_id not in seen and balance_filter:
                        seen.add(parent_id)
                        any_records = True
                        buffer.append({"id": parent_id})
                        if len(buffer) >= self.batch_size:
                            yield {"batched_parents": buffer}
                            buffer = []
                        #yield {"parent": {"id": parent_id}}

            if not any_records:
                yield {"batched_parents": {"id": "empty_slice"}}
            if buffer:
                yield {"batched_parents": buffer}

    def _process_parent(self, parent_record: Mapping[str, Any], sync_mode, cursor_field, stream_state):
        sl = StreamSlice(partition={"parent": parent_record}, cursor_slice={})
        yield from super().read_records(sync_mode, cursor_field, sl, stream_state)

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[str] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None
    ) -> Iterable[Mapping[str, Any]]:
        state = stream_state or {}
        batch = stream_slice["batched_parents"]

        q = queue.Queue(maxsize=self.queue_size)
        finished = 0

        def worker(pr):
            for out in self._process_parent(pr, sync_mode, cursor_field, state):
                q.put(out)
            q.put(None)

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            for pr in batch:
                pool.submit(worker, pr)

            while finished < len(batch):
                item = q.get()
                if item is None:
                    finished += 1
                else:
                    yield item
