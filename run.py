#!/usr/bin/env python3

import asyncio
import logging
import pickle
import os.path
import sys
from dataclasses import dataclass
from googleapiclient.discovery import build
from typing import Any, Dict, List, Optional, Union

from auth import create_or_get_credentials


def get_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(process)d - %(name)s - " "%(levelname)s - %(message)s",
    )
    return logging.getLogger("gmail-size")


LOGGER = get_logger()

MESSAGE_FETCHING_CONCURRENCY = 25


class LabelData:
    def __init__(self, id: str, name: str, typ: str) -> None:
        self.id = id
        self.name = name
        self.type = typ
        self.size = 0
        self.count = 0

    def is_relevant(self) -> bool:
        return self.count > 0 and self.type != "system"

    def __str__(self) -> str:
        size_mb = int(self.size / 1024 / 1024)
        return f"{self.name} {self.count} {size_mb}"


@dataclass
class StateSnapshot:
    done: bool
    labels: Dict[str, LabelData]
    message_per_page: int
    next_page_token: Optional[str] = None
    messages_processed: int = 0


@dataclass
class State:
    path: str
    done: bool
    labels: Dict[str, LabelData]
    message_per_page: int
    next_page_token: Optional[str] = None
    messages_processed: int = 0

    @classmethod
    def load_or_create(cls, path: str, message_per_page: int) -> Optional["State"]:
        if os.path.exists(path):
            with open(path, "rb") as f:
                snapshot = pickle.load(f)
                return State(
                    path=path,
                    done=snapshot.done,
                    labels=snapshot.labels,
                    message_per_page=snapshot.message_per_page,
                    next_page_token=snapshot.next_page_token,
                    messages_processed=snapshot.messages_processed,
                )
        else:
            return State(path, False, {}, message_per_page)

    def save(self) -> None:
        with open(self.path, "wb") as f:
            snapshot = StateSnapshot(
                done=self.done,
                labels=self.labels,
                message_per_page=self.message_per_page,
                next_page_token=self.next_page_token,
                messages_processed=self.messages_processed,
            )
            pickle.dump(snapshot, f)
        label_cnt = len(self.labels.values())
        total_size = int(sum(l.size for l in self.labels.values()) / 1024)
        LOGGER.info(
            f"State saved: {label_cnt} labels, "
            f"{self.messages_processed} messages processed, "
            f"{total_size} KB"
        )


async def gen_service() -> int:
    creds = create_or_get_credentials()
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


async def gen_labels(service) -> Dict[str, LabelData]:
    LOGGER.info("Getting labels")
    label_response = service.users().labels().list(userId="me").execute()
    label_list = label_response.get("labels", [])
    return {
        label["id"]: LabelData(label["id"], label["name"], label["type"])
        for label in label_list
    }


class MessageFetcher:
    def __init__(self) -> None:
        self._messages_svc = None

    def get_message(self, message_id) -> Dict[str, Any]:
        return self._messages_svc.get(
            userId="me", id=message_id, format="minimal"
        ).execute()

    async def gen_message(self, message_id) -> Dict[str, Any]:
        if self._messages_svc is None:
            service = await gen_service()
            self._messages_svc = service.users().messages()
        return await asyncio.get_event_loop().run_in_executor(
            None, self.get_message, message_id
        )


MESSAGE_FETCHERS = [MessageFetcher() for i in range(MESSAGE_FETCHING_CONCURRENCY)]


async def gen_stat_for_messages(
    labels: Dict[str, LabelData], messages: List[Dict[str, Union[str, int]]]
) -> None:
    remaining = messages
    current_batch = None

    while remaining:
        current_batch = remaining[:MESSAGE_FETCHING_CONCURRENCY]
        remaining = remaining[MESSAGE_FETCHING_CONCURRENCY:]

        message_responses = await asyncio.gather(
            *[
                MESSAGE_FETCHERS[i].gen_message(current_batch[i]["id"])
                for i in range(min(len(current_batch), MESSAGE_FETCHING_CONCURRENCY))
            ]
        )

        for message_response in message_responses:
            size = message_response.get("sizeEstimate", 0)
            for label_id in message_response.get("labelIds", []):
                labels[label_id].count += 1
                labels[label_id].size += size


async def gen_stat(service, state: State) -> None:
    prev_request = None
    response = None
    while True:
        if prev_request is None:
            request = (
                service.users()
                .messages()
                .list(
                    userId="me",
                    includeSpamTrash=False,
                    maxResults=state.message_per_page,
                    pageToken=state.next_page_token,
                )
            )
        else:
            request = (
                service.users()
                .messages()
                .list_next(previous_request=prev_request, previous_response=response)
            )
            prev_request = request
        response = request.execute()
        messages = response.get("messages", [])

        if not messages:
            break

        await gen_stat_for_messages(state.labels, messages)
        state.messages_processed += len(messages)
        state.next_page_token = response.get("nextPageToken")
        state.save()


async def gen_print_stat(labels: Dict[str, LabelData]) -> None:
    labels = [label for label in labels.values() if label.is_relevant()]
    labels = sorted(labels, key=lambda label_data: label_data.size)
    for label in labels:
        LOGGER.info(str(label))


async def main():
    if len(sys.argv) != 3:
        LOGGER.warn(f"Usage: {sys.argv[0]} <state_backup> <message_per_page>")
        sys.exit(1)

    state_backup = sys.argv[1]
    message_per_page = int(sys.argv[2])

    service = await gen_service()
    state = State.load_or_create(state_backup, message_per_page)

    if state.done:
        LOGGER.info("Stat loaded (done):")
        await gen_print_stat(state.labels)
        return

    if state.labels:
        LOGGER.info("Labels loaded from saved state")
    else:
        state.labels = await gen_labels(service)
        state.save()

    if state.next_page_token:
        LOGGER.info("Stat loaded:")
        await gen_print_stat(state.labels)

    await gen_stat(service, state)
    state.done = True
    state.save()
    await gen_print_stat(state.labels)


if __name__ == "__main__":
    asyncio.run(main())

"""
20000

10 -> 8:50
15 -> 6:50
20 -> 6:00
25 -> 4:55
"""
