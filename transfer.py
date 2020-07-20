from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from logging import getLogger
from typing import Union
from uuid import UUID, uuid4

from asyncpg import Connection
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
Config establishes loading local development params from .env file.
If appropriate environment variable not found, than service just won't start. 
"""


class Settings(BaseSettings):
    database_url: str
    database_min_pool_size: int
    database_max_pool_size: int

    class Config:
        env_file = '.env'


settings = Settings()

"""Database initialization.

Data layer in application is just a pooled connection interface + set of functions.
Init database and close it on appropriate events of FastAPI lifecycle.
If it helps, Intellij (PyCharm) does have language injections (highlighting) for multi-line SQL queries.
Note DbConn union to simplify usage of pool or specific connection inside transaction for account creation.
"""

pool: Pool

DbConn = Union[Pool, Connection]


@app.on_event('startup')
async def on_startup():
    global pool
    pool = await create_pool(dsn=settings.database_url,
                             min_size=settings.database_min_pool_size,
                             max_size=settings.database_max_pool_size)


@app.on_event('shutdown')
async def on_shutdown():
    await pool.close()


async def fetch_account_meta(account_id: UUID, timestamp: datetime = None) -> Record:
    if timestamp is None:
        timestamp = datetime.now()

    return await pool.fetchrow("""
                               SELECT balance, next_transfer_index
                               FROM account_metadata($1, $2)
                               """,
                               account_id,
                               timestamp)


async def insert_account(account_id: UUID, conn: DbConn):
    await conn.execute("""
                       INSERT INTO account (id)
                       VALUES ($1)
                       """,
                       account_id)


async def check_account_exists(account_id: UUID) -> bool:
    return await pool.fetchval("""
                               SELECT EXISTS (SELECT id
                                              FROM account
                                              WHERE id = $1)
                               """,
                               account_id)


async def insert_transfer(source_id: UUID, index: int, destination_id: UUID, amount: Decimal, conn: DbConn) -> datetime:
    return await conn.fetchval("""
                               INSERT INTO transfer (source, index, destination, amount)
                               VALUES ($1, $2, $3, $4)
                               RETURNING created_at
                               """,
                               source_id,
                               index,
                               destination_id,
                               amount)


"""Marker exceptions.

Short-circuit (marker) exceptions to properly serve errors to client.
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
Exists for sole purpose of logging and user-friendly error processing.
Note that generator of request ids needed for test mocking.
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

Deriving from BaseModel (like Settings), but now for auto-documenting API and structure validity checks.
Note that Config has extra schema with example which is added to OpenAPI spec.
"""


class HealthCheck(BaseModel):
    status: str

    class Config:
        schema_extra = {'example': {'status': 'alive'}}


@app.get('/health', response_model=HealthCheck)
async def health_check():
    return HealthCheck(status='alive')


"""Create new account.

Note service account, the special account to keep database consistent.
By help of it, we can ensure that total of all account balances (including service one) equals to 0.
Ideally, it should be in database, not hardcoded in the application itself.
All initial funds come as transfer from service account to new account.
"""

SERVICE_ACCOUNT_ID = UUID('00000000-0000-0000-0000-000000000000')


class AccountBalance(BaseModel):
    account_id: UUID
    balance: Decimal

    class Config:
        schema_extra = {'example': {'account_id': 'db6008f2-eb47-432b-8977-340bfe029744',
                                    'balance': 100.0}}


async def create_user_and_transfer_funds(account_id: UUID, amount: Decimal) -> datetime:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await insert_account(account_id=account_id,
                                 conn=conn)

            if amount > 0:
                service_meta = await fetch_account_meta(SERVICE_ACCOUNT_ID)
                return await insert_transfer(source_id=SERVICE_ACCOUNT_ID,
                                             index=service_meta['next_transfer_index'],
                                             destination_id=account_id,
                                             amount=amount,
                                             conn=conn)

            return datetime.now()


@app.post('/accounts', response_model=AccountBalance)
async def create_new_account(request: AccountBalance):
    if request.balance < 0:
        raise BadRequest('new account balance should be greater or equal to 0')

    account_exists = await check_account_exists(request.account_id)

    if account_exists:
        raise BadRequest(f'account with id {request.account_id} already exists')

    timestamp = await create_user_and_transfer_funds(account_id=request.account_id,
                                                     amount=request.balance)

    new_account_meta = await fetch_account_meta(account_id=request.account_id,
                                                timestamp=timestamp)

    return AccountBalance(account_id=request.account_id,
                          balance=new_account_meta['balance'])


"""Get account balance.

Using established earlier AccountBalance as return schema.
"""


@app.get('/accounts/{account_id}/balance', response_model=AccountBalance)
async def get_account_balance(account_id: UUID):
    account_meta = await fetch_account_meta(account_id)

    if account_meta is None:
        raise NotFound(f'account with id {account_id} not found')

    return AccountBalance(account_id=account_id,
                          balance=account_meta['balance'])


"""Make transfer.

Core of service: transfer route and its processing.
Mostly naive, read-heavy impl with multiple round-trips to database.
Only single, simple write (INSERT) operation is done, though.
"""


class TransferRequest(BaseModel):
    source: UUID
    destination: UUID
    amount: Decimal

    class Config:
        schema_extra = {'example': {'source': 'db6008f2-eb47-432b-8977-340bfe029744',
                                    'destination': '6d412386-8f3a-4b43-96d4-95d2a67ab430',
                                    'amount': 50.0}}


class TransferResult(BaseModel):
    source_balance: Decimal
    destination_balance: Decimal

    class Config:
        schema_extra = {'example': {'source_balance': 50.0,
                                    'destination_balance': 50.0}}


@app.post('/transfers', response_model=TransferResult)
async def make_transfer(request: TransferRequest):
    if request.amount <= 0:
        raise BadRequest('transfer amount must be greater than 0')

    if request.source == SERVICE_ACCOUNT_ID:
        raise BadRequest('service account cannot be used as source account')

    if request.source == request.destination:
        raise BadRequest('source account must not be equal to destination account')

    destination_exists = await check_account_exists(request.destination)

    if not destination_exists:
        raise BadRequest(f'destination account {request.destination} not found')

    source_meta = await fetch_account_meta(request.source)

    if not source_meta:
        raise BadRequest(f'source account {request.source} not found')

    if source_meta['balance'] < request.amount:
        raise BadRequest(f'transfer amount {request.amount} is more than account total of {source_meta["balance"]}')

    timestamp = await insert_transfer(source_id=request.source,
                                      index=source_meta['next_transfer_index'],
                                      destination_id=request.destination,
                                      amount=request.amount,
                                      conn=pool)

    new_source_meta = await fetch_account_meta(account_id=request.source,
                                               timestamp=timestamp)

    new_destination_meta = await fetch_account_meta(account_id=request.destination,
                                                    timestamp=timestamp)

    return TransferResult(source_balance=new_source_meta['balance'],
                          destination_balance=new_destination_meta['balance'])


"""The End.

Note, we do not initialize service here.
This ensures availability of quick REPL development and testing.
"""
