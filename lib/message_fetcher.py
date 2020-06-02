import asyncio
from typing import Dict, List, Union

from lib.service import ServiceProvider


Message = Dict[str, Union[str, int, List[str]]]


class MessageFetcher:
    def __init__(self, service_provider: ServiceProvider) -> None:
        self._service_provider = service_provider
        self._messages_svc = None

    def get_message(self, message_id) -> Message:
        return self._messages_svc.get(
            userId="me", id=message_id, format="minimal"
        ).execute()

    async def gen_message(self, message_id) -> Message:
        self._messages_svc = await self._service_provider.create_messages_service(self)
        return await asyncio.get_event_loop().run_in_executor(
            None, self.get_message, message_id
        )


class BatchedMessageFetcher:
    def __init__(self, service_provider: ServiceProvider, pool_size: int) -> None:
        self._pool_size = pool_size
        self._message_fetchers = [
            MessageFetcher(service_provider) for i in range(pool_size)
        ]

    async def fetch_messages(
        self, message_headers: List[Dict[str, Union[str, int]]]
    ) -> List[Message]:
        messages: List[Message] = []

        remaining = message_headers
        current_batch = None
        while remaining:
            current_batch = remaining[: self._pool_size]
            remaining = remaining[self._pool_size :]

            message_responses = await asyncio.gather(
                *[
                    self._message_fetchers[i].gen_message(current_batch[i]["id"])
                    for i in range(min(len(current_batch), self._pool_size))
                ]
            )

            messages += message_responses

        return messages
