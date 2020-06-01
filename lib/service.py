import asyncio
from enum import Enum, auto
from functools import partial
from pprint import pprint
from typing import Any, Dict, Optional, Tuple

from googleapiclient.discovery import Resource, build
from lib.auth import CredentialsProvider


class ServiceType(Enum):
    LABELS = auto()
    MESSAGES = auto()


class ServiceProvider:
    def __init__(self, credentials_provider: CredentialsProvider) -> None:
        self._credentials_provider = credentials_provider
        self._cache: Dict[Tuple[ServiceType, Any], Resource] = {}

    async def _create_base_service(self) -> Resource:
        creds = await self._credentials_provider.create_or_get_credentials()
        return await asyncio.get_event_loop().run_in_executor(
            None,
            partial(build, "gmail", "v1", credentials=creds, cache_discovery=False),
        )

    def _get_from_cache(
        self, cache_type: ServiceType, cache_key: Any
    ) -> Optional[Resource]:
        return self._cache.get((cache_type, cache_key))

    def _save_to_cache(
        self, cache_type: ServiceType, cache_key: Any, service: Resource
    ) -> Optional[Resource]:
        self._cache[(cache_type, cache_key)] = service

    def _get_service(
        self, base_service: Resource, service_type: ServiceType
    ) -> Resource:
        if service_type == ServiceType.LABELS:
            return base_service.users().labels()
        if service_type == ServiceType.MESSAGES:
            return base_service.users().messages()
        raise NotImplementedError(service_type)

    async def _create_service(
        self, service_type: ServiceType, cache_key: Optional[Any] = None
    ) -> Resource:
        service: Optional[Resource] = None
        if cache_key is not None:
            service = self._get_from_cache(service_type, cache_key)
        if not service:
            base_service = await self._create_base_service()
            service = await asyncio.get_event_loop().run_in_executor(
                None, partial(self._get_service, base_service, service_type)
            )
            if cache_key is not None:
                self._save_to_cache(service_type, cache_key, service)

        return service

    async def create_labels_service(self, cache_key: Optional[Any] = None) -> Resource:
        return await self._create_service(ServiceType.LABELS, cache_key)

    async def create_messages_service(
        self, cache_key: Optional[Any] = None
    ) -> Resource:
        return await self._create_service(ServiceType.MESSAGES, cache_key)
