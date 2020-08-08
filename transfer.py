from contextvars import ContextVar
from dataclasses import dataclass
from decimal import Decimal
from logging import getLogger
from typing import Dict, List
from uuid import UUID, uuid4

from asyncpg.pool import Pool, create_pool
from asyncpg.protocol.protocol import Record
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from pydantic import BaseSettings, BaseModel

logger = getLogger('transfer')

app = FastAPI()

"""Environment settings.

Pydantic models (derived from BaseSettings and BaseModel) will be used for request-response schema too.
Config establishes loading params from .env file first, overridden only by real env variables.
If appropriate environment variables not found, then service just won't start.
"""


class Settings(BaseSettings):
    database_url: str
    database_min_pool_size: int
    database_max_pool_size: int

    class Config:
        env_file = '.env'


settings = Settings()

"""Database initialization.

The data layer in the application is just a pooled connection interface + set of functions.
Init database and close it on appropriate events of the FastAPI lifecycle.
If it helps, Intellij (PyCharm) does have language injections (highlighting) for multi-line SQL queries.
"""

pool: Pool


@app.on_event('startup')
async def on_startup():
    global pool
    pool = await create_pool(dsn=settings.database_url,
                             min_size=settings.database_min_pool_size,
                             max_size=settings.database_max_pool_size)


@app.on_event('shutdown')
async def on_shutdown():
    await pool.close()


async def fetch_accounts_meta(account_ids: List[UUID]) -> Dict[UUID, Record]:
    res = await pool.fetch("""
                           SELECT id, balance, next_transfer_index
                           FROM account_metadata($1::UUID[])
                           """,
                           account_ids)

    return {row['id']: row for row in res}


async def init_account(account_id: UUID, balance: Decimal):
    await pool.execute("""
                       SELECT init_account($1, $2)
                       """,
                       account_id,
                       balance)


async def check_account_exists(account_id: UUID) -> bool:
    return await pool.fetchval("""
                               SELECT EXISTS (SELECT id
                                              FROM account
                                              WHERE id = $1)
                               """,
                               account_id)


async def insert_transfer(source_id: UUID, index: int, destination_id: UUID, amount: Decimal):
    await pool.execute("""
                       INSERT INTO transfer (source, index, destination, amount)
                       VALUES ($1, $2, $3, $4)
                       """,
                       source_id,
                       index,
                       destination_id,
                       amount)


"""Marker exceptions.

Short-circuit (marker) exceptions to properly serve errors to the client.
Ensures separation of business-logic errors from generic exceptions.
"""


@dataclass
class BadRequest(RuntimeError):
    message: str


@dataclass
class NotFound(RuntimeError):
    message: str


"""Request identification.

Context-aware variable to bind and retrieve unique Request-ID.
Exists for the sole purpose of logging and user-friendly error processing.
Note that generator of request IDs needed for test mocking.
"""

ctx_request_id: ContextVar[UUID] = ContextVar('request_id')


def gen_request_id() -> UUID:
    return uuid4()


def logging_extra() -> dict:
    return {'request_id': ctx_request_id.get()}


@app.middleware('http')
async def bind_request_id(request: Request, call_next):
    request_id = ctx_request_id.set(request.headers.get('X-Request-ID', gen_request_id()))
    response = await call_next(request)
    ctx_request_id.reset(request_id)
    return response


"""Exception handlers.

Grouping is done from more generic handler to more concrete.
Note that generic handler does not return exception message as-is.
"""


def error_response(status_code: int, error: str):
    return JSONResponse(status_code=status_code,
                        content={'request_id': str(ctx_request_id.get()),
                                 'error': error})


@app.exception_handler(Exception)
async def generic_exception_handler(_: Request, exc: Exception):
    logger.error(exc, exc_info=True, extra=logging_extra())
    return error_response(500, error='Internal Server Error')


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError):
    logger.info(exc, extra=logging_extra())
    return error_response(400, error=str(exc.errors()))


@app.exception_handler(BadRequest)
async def bad_request_handler(_: Request, exc: NotFound):
    logger.warning(exc, extra=logging_extra())
    return error_response(400, error=exc.message)


@app.exception_handler(NotFound)
async def not_found_handler(_: Request, exc: NotFound):
    logger.info(exc, extra=logging_extra())
    return error_response(404, error=exc.message)


"""Health-check.

Fast dummy route to see if the server is up.
"""


@app.get('/health', status_code=204)
async def health_check():
    pass


"""Create a new account.

Deriving AccountBalance from BaseModel (like Settings), but now for auto-documenting API and structure validity checks.
Note that Config has an extra schema with an example that is added to OpenAPI spec.

Note service account, the special account to keep database consistent.
With the help of it, we can ensure that the total of all account balances (including service one) equals to 0.
Ideally, it should be in the database, not hardcoded in the application itself.
All initial funds come as a transfer from the service account to the new account.
"""

SERVICE_ACCOUNT_ID = UUID('00000000-0000-0000-0000-000000000000')


class AccountBalance(BaseModel):
    account_id: UUID
    balance: Decimal

    class Config:
        schema_extra = {'example': {'account_id': 'db6008f2-eb47-432b-8977-340bfe029744',
                                    'balance': 100.0}}


@app.post('/accounts', status_code=204)
async def create_new_account(request: AccountBalance):
    if request.balance < 0:
        raise BadRequest('a new account balance should be greater or equal to 0')

    account_exists = await check_account_exists(request.account_id)

    if account_exists:
        raise BadRequest(f'account already exists')

    await init_account(account_id=request.account_id,
                       balance=request.balance)


"""Get account balance.

Using established earlier AccountBalance as return schema.
"""


@app.get('/accounts/{account_id}/balance', response_model=AccountBalance)
async def get_account_balance(account_id: UUID):
    metadata = await fetch_accounts_meta([account_id])

    if account_id not in metadata:
        raise NotFound(f'account not found')

    return AccountBalance(account_id=account_id,
                          balance=metadata[account_id]['balance'])


"""Make transfer.

The core of service: transfer route and its processing.
Only a single write (INSERT) operation is done.
"""


class Transfer(BaseModel):
    source: UUID
    destination: UUID
    amount: Decimal

    class Config:
        schema_extra = {'example': {'source': 'db6008f2-eb47-432b-8977-340bfe029744',
                                    'destination': '6d412386-8f3a-4b43-96d4-95d2a67ab430',
                                    'amount': 50.0}}


@app.post('/transfers', status_code=204)
async def make_transfer(request: Transfer):
    if request.amount <= 0:
        raise BadRequest('transfer amount must be greater than 0')

    if request.source == SERVICE_ACCOUNT_ID:
        raise BadRequest('the service account cannot be use as a source account')

    if request.source == request.destination:
        raise BadRequest('source account must not be equal to the destination account')

    metadata = await fetch_accounts_meta([request.source, request.destination])

    if request.destination not in metadata:
        raise BadRequest(f'destination account not found')

    if request.source not in metadata:
        raise BadRequest(f'source account not found')

    if metadata[request.source]['balance'] < request.amount:
        raise BadRequest(f'not enough funds on the source account')

    await insert_transfer(source_id=request.source,
                          index=metadata[request.source]['next_transfer_index'],
                          destination_id=request.destination,
                          amount=request.amount)


"""The End.

Note, we do not initialize service here.
This ensures the availability of quick REPL development and testing.
"""
