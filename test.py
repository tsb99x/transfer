from contextlib import contextmanager
from typing import Tuple
from unittest.mock import patch
from uuid import uuid4, UUID

from fastapi.testclient import TestClient
from requests import Session, Response

import transfer

"""Utility methods, for mocking"""


def error_response(msg: str) -> dict:
    return {'request_id': str(ZERO_UUID),
            'error': msg}


ZERO_UUID = UUID('00000000-0000-0000-0000-000000000000')


def mock_gen_request_id():
    return ZERO_UUID


@contextmanager
def test_context_client():
    with patch.object(transfer, 'gen_request_id', wraps=mock_gen_request_id):
        with TestClient(transfer.app) as client:
            yield client


"""Health check route"""


def test_should_answer_on_health():
    with test_context_client() as client:
        res = client.get('/health')
        assert res.status_code == 204


"""Create account route"""


def create_account(client: Session, account_id: UUID, balance: int) -> Tuple[dict, Response]:
    req = {'account_id': str(account_id),
           'balance': balance}
    res = client.post('/accounts', json={'account_id': str(account_id),
                                         'balance': balance})
    return req, res


def test_should_create_account_properly():
    with test_context_client() as client:
        req, res = create_account(client=client,
                                  account_id=uuid4(),
                                  balance=1)
        assert res.status_code == 204


def test_should_create_account_but_do_no_transfers_on_zero_init_balance():
    with test_context_client() as client:
        req, res = create_account(client=client,
                                  account_id=uuid4(),
                                  balance=0)
        assert res.status_code == 204


def test_should_not_create_account_with_negative_balance():
    with test_context_client() as client:
        req, res = create_account(client=client,
                                  account_id=uuid4(),
                                  balance=-1)
        assert res.status_code == 400
        assert res.json() == error_response('a new account balance should be greater or equal to 0')


def test_should_not_create_account_if_already_exists():
    with test_context_client() as client:
        account_id = uuid4()
        req, res = create_account(client=client,
                                  account_id=account_id,
                                  balance=0)
        assert res.status_code == 204

        req, res = create_account(client=client,
                                  account_id=account_id,
                                  balance=0)
        assert res.status_code == 400
        assert res.json() == error_response(f'account already exists')


"""Account balance route"""


def test_should_get_balance_properly():
    with test_context_client() as client:
        account_id = uuid4()
        req, _ = create_account(client=client,
                                account_id=account_id,
                                balance=0)

        res = client.get(f'/accounts/{account_id}/balance')
        assert res.status_code == 200
        assert res.json() == req


def test_should_not_get_balance_if_account_does_not_exist():
    with test_context_client() as client:
        account_id = uuid4()

        res = client.get(f'/accounts/{account_id}/balance')
        assert res.status_code == 404
        assert res.json() == error_response(f'account not found')


"""Make transfer route"""


def create_accounts(client: Session, first_balance: int = 100, second_balance: int = 0) -> Tuple[UUID, UUID]:
    first_id = uuid4()
    req, _ = create_account(client=client,
                            account_id=first_id,
                            balance=first_balance)

    second_id = uuid4()
    req, _ = create_account(client=client,
                            account_id=second_id,
                            balance=second_balance)

    return first_id, second_id


def make_transfer(client: Session, source: UUID, destination: UUID, amount: int) -> Tuple[dict, Response]:
    req = {'source': str(source),
           'destination': str(destination),
           'amount': amount}
    res = client.post('/transfers', json=req)
    return req, res


def test_should_make_transfer_properly():
    with test_context_client() as client:
        first_id, second_id = create_accounts(client)

        _, res = make_transfer(client=client,
                               source=first_id,
                               destination=second_id,
                               amount=1)
        assert res.status_code == 204


def test_should_not_make_transfer_if_amount_is_less_or_equal_to_zero():
    with test_context_client() as client:
        first_id, second_id = create_accounts(client)

        _, res = make_transfer(client=client,
                               source=first_id,
                               destination=second_id,
                               amount=-1)
        assert res.status_code == 400
        assert res.json() == error_response('transfer amount must be greater than 0')


def test_should_not_make_transfer_if_source_account_is_service_one():
    with test_context_client() as client:
        first_id, _ = create_accounts(client)

        _, res = make_transfer(client=client,
                               source=transfer.SERVICE_ACCOUNT_ID,
                               destination=first_id,
                               amount=1)
        assert res.status_code == 400
        assert res.json() == error_response('the service account cannot be use as a source account')


def test_should_not_make_transfer_if_source_and_destination_are_the_same():
    with test_context_client() as client:
        first_id, _ = create_accounts(client)

        _, res = make_transfer(client=client,
                               source=first_id,
                               destination=first_id,
                               amount=1)
        assert res.status_code == 400
        assert res.json() == error_response('source account must not be equal to the destination account')


def test_should_not_make_transfer_if_destination_account_does_not_exist():
    with test_context_client() as client:
        first_id, _ = create_accounts(client)
        random_id = uuid4()

        _, res = make_transfer(client=client,
                               source=first_id,
                               destination=random_id,
                               amount=1)
        assert res.status_code == 400
        assert res.json() == error_response(f'destination account not found')


def test_should_not_make_transfer_if_source_account_does_not_exist():
    with test_context_client() as client:
        first_id, _ = create_accounts(client)
        random_id = uuid4()

        _, res = make_transfer(client=client,
                               source=random_id,
                               destination=first_id,
                               amount=1)
        assert res.status_code == 400
        assert res.json() == error_response(f'source account not found')


def test_should_not_make_transfer_if_amount_is_more_than_source_balance():
    with test_context_client() as client:
        balance = 100
        first_id, second_id = create_accounts(client=client,
                                              first_balance=balance)

        amount = balance + 1
        _, res = make_transfer(client=client,
                               source=first_id,
                               destination=second_id,
                               amount=amount)
        assert res.status_code == 400
        assert res.json() == error_response(f'not enough funds on the source account')
