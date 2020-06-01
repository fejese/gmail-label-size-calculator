#!/usr/bin/env python3

import asyncio
import pickle
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union

import click
from lib.auth import CredentialsProvider
from lib.logging import format_number, get_logger
from lib.service import ServiceProvider


LOGGER = get_logger()
MESSAGE_FETCHING_CONCURRENCY = 25


@dataclass
class LabelData:
    id: str
    name: str
    type: str
    size: int = 0
    count: int = 0

    def is_relevant(self) -> bool:
        return self.count > 0 and self.type != "system"

    def __str__(self) -> str:
        return f"{self.name} {self.count} {format_number(self.size)}"


class MessageFetcher:
    def __init__(self, service_provider: ServiceProvider) -> None:
        self._service_provider = service_provider
        self._messages_svc = None

    def get_message(self, message_id) -> Dict[str, Union[str, int, List[str]]]:
        return self._messages_svc.get(
            userId="me", id=message_id, format="minimal"
        ).execute()

    async def gen_message(self, message_id) -> Dict[str, Union[str, int, List[str]]]:
        self._messages_svc = await self._service_provider.create_messages_service(self)
        return await asyncio.get_event_loop().run_in_executor(
            None, self.get_message, message_id
        )


@dataclass
class State:
    done: bool
    labels: Dict[str, LabelData]
    message_per_page: int
    next_page_token: Optional[str] = None
    messages_processed: int = 0

    @property
    def stat_string(self) -> str:
        label_cnt = len(self.labels.values())
        total_size = sum(l.size for l in self.labels.values())

        return ", ".join(
            [
                f"{label_cnt} labels",
                f"{self.messages_processed} messages processed",
                f"{format_number(total_size)} total size",
                f"next page token: {self.next_page_token}",
            ]
        )

    def print_stat(self) -> None:
        LOGGER.info(self.stat_string)

        labels = [label for label in self.labels.values() if label.is_relevant()]
        labels = sorted(labels, key=lambda label_data: label_data.size)
        for label in labels:
            LOGGER.info(str(label))


@dataclass
class Calculator:
    def __init__(
        self, service_provider: ServiceProvider, snapshot: Path, state: State
    ) -> None:
        self.service_provider = service_provider
        self.snapshot = snapshot
        self.state = state

        self.message_fetchers = [
            MessageFetcher(service_provider)
            for i in range(MESSAGE_FETCHING_CONCURRENCY)
        ]

    @classmethod
    def load_or_create(
        cls, service_provider: ServiceProvider, snapshot: Path, message_per_page: int
    ) -> Optional["Calculator"]:
        if snapshot.exists():
            LOGGER.info(f"Loading state from snapshot")
            with open(snapshot, "rb") as f:
                state = pickle.load(f)
                if state.message_per_page != message_per_page:
                    LOGGER.warning(
                        f"Ignoring specified message_per_page {message_per_page}, "
                        f"using {state.message_per_page} from snapshot"
                    )
                return Calculator(
                    service_provider=service_provider, snapshot=snapshot, state=state
                )
        else:
            return Calculator(
                service_provider=service_provider,
                snapshot=snapshot,
                state=State(done=False, labels={}, message_per_page=message_per_page),
            )

    def save(self) -> None:
        with open(self.snapshot, "wb") as f:
            pickle.dump(self.state, f)
        LOGGER.info(f"Calculator saved: {self.state.stat_string}")

    async def gen_labels(self) -> None:
        LOGGER.info("Getting labels")
        labels_service = await self.service_provider.create_labels_service()
        label_response = labels_service.list(userId="me").execute()
        label_list = label_response.get("labels", [])
        self.state.labels = {
            label["id"]: LabelData(label["id"], label["name"], label["type"])
            for label in label_list
        }
        self.save()

    async def gen_stat_for_messages(
        self, messages: List[Dict[str, Union[str, int]]]
    ) -> None:
        remaining = messages
        current_batch = None

        while remaining:
            current_batch = remaining[:MESSAGE_FETCHING_CONCURRENCY]
            remaining = remaining[MESSAGE_FETCHING_CONCURRENCY:]

            message_responses = await asyncio.gather(
                *[
                    self.message_fetchers[i].gen_message(current_batch[i]["id"])
                    for i in range(
                        min(len(current_batch), MESSAGE_FETCHING_CONCURRENCY)
                    )
                ]
            )

            for message_response in message_responses:
                size = message_response.get("sizeEstimate", 0)
                for label_id in message_response.get("labelIds", []):
                    self.state.labels[label_id].count += 1
                    self.state.labels[label_id].size += size

    async def gen_stat(self) -> None:
        message_service = await self.service_provider.create_messages_service()
        prev_request = None
        response = None
        while True:
            if prev_request is None:
                request = message_service.list(
                    userId="me",
                    includeSpamTrash=False,
                    maxResults=self.state.message_per_page,
                    pageToken=self.state.next_page_token,
                )
            else:
                request = message_service.list_next(
                    previous_request=prev_request, previous_response=response
                )
                prev_request = request

            response = request.execute()

            next_page_token = response.get("nextPageToken")
            if next_page_token == self.state.next_page_token:
                break

            messages = response.get("messages", [])
            if not messages:
                break

            await self.gen_stat_for_messages(messages)
            self.state.messages_processed += len(messages)
            self.state.next_page_token = next_page_token
            self.save()

        self.state.done = True
        self.save()


async def main(calculator: Calculator) -> None:
    if calculator.state.done:
        LOGGER.info("Stat loaded (done):")
        calculator.state.print_stat()
        return

    if calculator.state.labels:
        LOGGER.info("Labels loaded from saved state")
    else:
        await calculator.gen_labels()

    if calculator.state.messages_processed > 0:
        LOGGER.info("Stat loaded from saved state:")
        calculator.state.print_stat()

    await calculator.gen_stat()
    LOGGER.info("Processing done, stat loaded:")
    calculator.state.print_stat()


@click.command()
@click.option(
    "-s", "--snapshot", type=Path, required=True, help="Path to snapshot file."
)
@click.option(
    "-p",
    "--messages-per-page",
    type=int,
    required=True,
    help=(
        "How many messages to query at once."
        "Snapshots will be taken after processing this many messages."
    ),
)
@click.option(
    "-c",
    "--credentials",
    "credentials_file",
    type=Path,
    default=Path("credentials.json"),
    help="Path to snapshot file.",
    show_default=True,
)
@click.option(
    "-t",
    "--token",
    "token_file",
    type=Path,
    default=Path("token.pickle"),
    help="Path to file for caching token.",
    show_default=True,
)
@click.option(
    "--scopes",
    type=str,
    multiple=True,
    default=["https://www.googleapis.com/auth/gmail.readonly"],
    help="List of scopes used for authentication.",
    show_default=True,
)
def cli(
    snapshot: Path,
    messages_per_page: int,
    credentials_file: Path,
    token_file: Path,
    scopes: List[str],
):
    credentials_provider = CredentialsProvider(
        credentials_file=credentials_file, token_file=token_file, scopes=scopes
    )
    calculator = Calculator.load_or_create(
        ServiceProvider(credentials_provider), snapshot, messages_per_page
    )

    try:
        asyncio.run(main(calculator))
    except KeyboardInterrupt:
        LOGGER.info("Processing cancelled, stat loaded so far:")
        calculator.state.print_stat()


if __name__ == "__main__":
    cli()
