import asyncio
import os.path
import pickle
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import List, Optional

import aiofiles
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow


class MismatchedScopesException(Exception):
    def __init__(self, existing: List[str], required: List[str]) -> None:
        super(MismatchedScopesException, self).__init__(
            f"Existing scopes: {existing}, required scopes: {required}"
        )


@dataclass
class Token:
    credentials: Credentials
    scopes: List[str]


class CredentialsProvider:
    def __init__(self, credentials_file: Path, token_file: Path, scopes: List[str]):
        self._credentials_file = credentials_file
        self._token_file = token_file
        self._scopes = scopes

        self._credentials: Optional[Credentials] = None

    async def _save_token(self, token: Token) -> None:
        async with aiofiles.open(self._token_file, "wb") as f:
            await f.write(pickle.dumps(token))

    async def _read_token(self) -> Optional[Token]:
        if not os.path.exists(self._token_file):
            return None

        async with aiofiles.open(self._token_file, "rb") as f:
            content = await f.read()
            stored = pickle.loads(content)
            if isinstance(stored, Credentials):
                # Backward compatibility
                stored_creds = stored
                token = Token(credentials=stored_creds, scopes=self._scopes)
                await self._save_token(token)
                return token
            elif isinstance(stored, Token):
                stored_token = stored
                stored_creds = stored_token.credentials
                if set(stored.scopes) != set(self._scopes):
                    raise MismatchedScopesException(
                        existing=stored.scopes, required=self._scopes
                    )
                return stored_token

    async def _create_or_get_credentials(self) -> Credentials:
        stored_token: Optional[Token] = await self._read_token()
        if stored_token and stored_token.credentials.valid:
            return stored_token.credentials
        stored_creds: Optional[
            Credentials
        ] = stored_token.credentials if stored_token else None
        creds: Credentials

        # If there are no (valid) credentials available, let the user log in.
        if stored_creds and stored_creds.expired and stored_creds.refresh_token:
            await asyncio.get_event_loop().run_in_executor(
                None, stored_creds.refresh, Request()
            )
            creds = stored_creds
        else:
            flow = await asyncio.get_event_loop().run_in_executor(
                None,
                InstalledAppFlow.from_client_secrets_file,
                self._credentials_file,
                self._scopes,
            )
            creds = await asyncio.get_event_loop().run_in_executor(
                None, partial(flow.run_local_server, port=0)
            )

        await self._save_token(Token(credentials=creds, scopes=self._scopes))

        return creds

    async def create_or_get_credentials(self) -> Credentials:
        if self._credentials is None:
            self._credentials = await self._create_or_get_credentials()

        return self._credentials
