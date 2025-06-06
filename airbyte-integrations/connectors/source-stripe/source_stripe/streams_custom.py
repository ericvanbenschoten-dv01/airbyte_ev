import copy
import hashlib
import math
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from itertools import chain
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import pendulum
import requests

from airbyte_cdk import BackoffStrategy, StreamSlice
from airbyte_cdk.models import SyncMode, AirbyteMessage, AirbyteStateBlob, AirbyteStreamState, AirbyteStateType, AirbyteStateMessage, StreamDescriptor, Type as MessageType
#from airbyte_cdk.sources.declarative.requesters.error_handlers.backoff_strategies import ExponentialBackoffStrategy
#from airbyte_cdk.sources.streams.checkpoint import Cursor
#from airbyte_cdk.sources.streams.checkpoint.resumable_full_refresh_cursor import ResumableFullRefreshCursor
#from airbyte_cdk.sources.streams.checkpoint.substream_resumable_full_refresh_cursor import SubstreamResumableFullRefreshCursor
from airbyte_cdk.sources.streams.core import StreamData
#from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
#from airbyte_cdk.sources.streams.http.error_handlers import ErrorHandler
#from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
#from source_stripe.error_handlers import ParentIncrementalStripeSubStreamErrorHandler, StripeErrorHandler
#from source_stripe.error_mappings import PARENT_INCREMENTAL_STRIPE_SUB_STREAM_ERROR_MAPPING
from source_stripe.streams import IncrementalStripeStream, UpdatedCursorIncrementalRecordExtractor, StripeStream, IRecordExtractor, Events, EventRecordExtractor, CreatedCursorIncrementalStripeStream, ParentIncrementalStripeSubStream, IStreamSelector, StripeSubStream
from concurrent.futures import ThreadPoolExecutor, as_completed


################################################################ HUBIFI ################################################################
class IncrementalSearchStripeStream(IncrementalStripeStream):
    is_resumable = True
    """
    This class combines both normal incremental sync and event based sync. For initial full refresh sync mode we are using the Search API with custom filters
    and incremental syncs we are using the event based sync with post processing filtering.
    """

    def __init__(
        self,
        *args,
        cursor_field: str = "updated",
        legacy_cursor_field: Optional[str] = "created",
        event_types: Optional[List[str]] = None,
        response_filter: Optional[Callable] = None,
        expand_items: Optional[List[str]] = None,
        max_workers: int = 20,
        extra_request_params: Optional[Union[Mapping[str, Any], Callable]] = None,
        **kwargs,
    ):
        self._cursor_field = cursor_field
        super().__init__(*args, **kwargs)
        created_cursor_stream = SearchStripeStream(
            *args,
            cursor_field=cursor_field,
            lookback_window_days=0,
            record_extractor=UpdatedCursorIncrementalRecordExtractor(cursor_field, legacy_cursor_field),
            expand_items=expand_items,
            extra_request_params=extra_request_params,
            max_workers=max_workers,
            **kwargs,
        )
        updated_cursor_stream = ThreadedUpdatedCursorIncrementalStripeStream( #UpdatedCursorIncrementalStripeStream
            *args,
            cursor_field=cursor_field,
            legacy_cursor_field=legacy_cursor_field,
            event_types=event_types,
            expand_items=expand_items,
            response_filter=response_filter,
            #max_workers=max_workers,
            **kwargs,
        )
        self._parent_stream = None
        self.stream_selector = IncrementalSearchStripeStreamSelector(created_cursor_stream, updated_cursor_stream)
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
class SearchStripeStream(CreatedCursorIncrementalStripeStream):
    def __init__(
        self,
        *args,
        lookback_window_days: int = 0,
        start_date_max_days_from_now: Optional[int] = None,
        cursor_field: str = "created",
        max_workers: int = 20,
        **kwargs,
    ):
        self._cursor_field = cursor_field
        self.max_workers = max_workers
        super().__init__(*args, **kwargs)
        self.lookback_window_days = lookback_window_days
        self.start_date_max_days_from_now = start_date_max_days_from_now

    def path(self, *args, **kwargs) -> str:
        return f"{self.name}/search"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            **self.extra_request_params(
                stream_state=stream_state,
                stream_slice=stream_slice,
                next_page_token=next_page_token,
            ),
        }

        if self.expand_items:
            params["expand[]"] = self.expand_items

        if next_page_token:
            params["page"] = next_page_token["page"]
        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        json_resp = response.json()
        if json_resp.get("has_more") and "next_page" in json_resp:
            return {"page": json_resp["next_page"]}
        return None

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_state = stream_state or {}
        start_ts = self.get_start_timestamp(stream_state)
        self.logger.info(f"[SearchStripeStream - stream_slices]")
        if start_ts >= pendulum.now().int_timestamp:
            self.logger.info(f"[SearchStripeStream - return none?]")
            return []
        slices = [
            {"created[gte]": start, "created[lte]": end}
            for start, end in self.chunk_dates(start_ts)
        ]
        self.logger.info(f"[SearchStripeStream - returning slices: {slices} ]")

        return [{"batched_slices": slices}]

    def chunk_dates(self, start_date_ts: int) -> Iterable[Tuple[int, int]]:
        now = pendulum.now().int_timestamp
        self.logger.info(f"[SearchStripeStream - Chunk Dates] SLICE RANGE: {self.slice_range}")
        step = int(pendulum.duration(days=1).total_seconds())
        #step = int(pendulum.duration(days=self.slice_range).total_seconds()) #TODO: need to figure out slice_ranges
        self.logger.info(f"[SearchStripeStream - Chunk Dates] STEP: {step}")
        after_ts = start_date_ts
        while after_ts < now:
            before_ts = min(now, after_ts + step)
            yield after_ts, before_ts
            after_ts = before_ts + 1

    def _read_slice(self, stream_slice, sync_mode, cursor_field, stream_state):
        return list(super().read_records(
            sync_mode=sync_mode,
            cursor_field=cursor_field,
            stream_slice=stream_slice,
            stream_state=stream_state,
        ))

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        """
        Run all stream slices concurrently during full refresh or initial sync.
        Ignore Airbyte's default slice-by-slice invocation pattern.
        """
        stream_state = stream_state or {}
        slices = stream_slice["batched_slices"]
        max_workers =  min(len(slices), self.max_workers)
        self.logger.info(f"{len(slices)} slices to process with {self.max_workers} threads!!!")

        with ThreadPoolExecutor(max_workers=self.max_workers) as thread_pool:
            tasks = {
                thread_pool.submit(self._read_slice, s, sync_mode, cursor_field, stream_state): s for s in slices
            }
            for task in as_completed(tasks):
                yield from task.result()

class IncrementalSearchStripeStreamSelector(IStreamSelector):
    def __init__(
        self,
        created_cursor_incremental_stream: SearchStripeStream,
        updated_cursor_incremental_stream: ThreadedUpdatedCursorIncrementalStripeStream
    ):
        self._created_cursor_stream = created_cursor_incremental_stream
        self._updated_cursor_stream = updated_cursor_incremental_stream

    def get_parent_stream(self, stream_state: Mapping[str, Any]) -> StripeStream:
        return self._updated_cursor_stream if stream_state else self._created_cursor_stream

class CustomerBalanceTransactions(ParentIncrementalStripeSubStream):
    """
    Custom connector that incrementally collects the id from customers and customer from invoices.
    It collects these customer IDs and makes a call to retrieve the customer balance transactions.
    It implements a 2 day window to catch transactions created during previous run or right before
    the invoice/customer event. To move the cursor along it will also cap to 7 days ago after initial
    sync. API docs: https://stripe.com/docs/api/customer_balance_transactions/list
    """
    def __init__(self, cursor_field: str, parents: List[StripeSubStream], *args, **kwargs):
        super().__init__(cursor_field=cursor_field, parent=parents[0], *args, **kwargs)
        self.parent_streams = parents

    @property
    def state_checkpoint_interval(self) -> int:
        return 1  # force state write

    def stream_slices(self, sync_mode: SyncMode, cursor_field=None, stream_state=None):
        if stream_state:
            normalized_state = self.normalize_state(stream_state)
        else:
            stream_state = {}
            normalized_state = stream_state

        seen = set()
        any_records = False

        for parent in self.parent_streams:
            self.logger.info(f"Starting parent stream {parent.name} with state {normalized_state}")
            slices = parent.stream_slices(sync_mode=sync_mode, cursor_field=parent.cursor_field, stream_state=normalized_state)

            for stream_slice in slices:
                records = parent.read_records(sync_mode=sync_mode, cursor_field=parent.cursor_field, stream_slice=stream_slice, stream_state=normalized_state)
                for r in records:
                    parent_id = r.get("customer") or r.get("id")
                    balance = r.get("balance") or r.get("total", 0)
                    if parent_id and parent_id not in seen and balance != 0:
                        seen.add(parent_id)
                        any_records = True
                        yield {"parent": {"id": parent_id}}

        if not any_records:
            yield {"parent": {"id": "empty_slice"}}

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None):
        state = self.normalize_state(stream_state)
        lookback = state["cursor"] - int(timedelta(days=2).total_seconds())

        for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
            if record.get("created", 0) > lookback:
                yield record

    def read(self, *args, **kwargs):
        read_count = 0

        for record in super().read(*args, **kwargs):
            read_count += 1
            yield record

        if read_count == 1:
            now = int(datetime.utcnow().timestamp())
            synthetic = {self.cursor_field: now}
            state = self.normalize_state(kwargs.get("stream_state"))
            new_state = self.get_updated_state(state, synthetic)
            self.logger.info(f"Persisting synthetic state: {new_state}")
            yield AirbyteMessage(
                type=MessageType.STATE,
                state=AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(name=self.name),
                        stream_state=AirbyteStateBlob(data=new_state)
                    )
                )
            )

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

        previous_cursor_minus_2 = (current_state or {}).get("cursor", self.start_date) - int(timedelta(days=2).total_seconds())
        latest_cursor_minus_2 = latest_record.get(self.cursor_field) - int(timedelta(days=2).total_seconds())
        today_minus_7 = int(datetime.utcnow().timestamp()) - int(timedelta(days=7).total_seconds())
        new_cursor = max(latest_cursor_minus_2, today_minus_7, previous_cursor_minus_2)
        updated_parents = {
            p.name: new_cursor
            for p in self.parent_streams
        }

        return {
            "cursor": new_cursor,
            "parents": updated_parents
        }
