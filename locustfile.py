import random
from uuid import uuid4

from locust import task, between
from locust.contrib.fasthttp import FastHttpUser

"""Performance Test.

Accounts set is a storage for all accounts, used in the test.
Useful, when modeling transfer from some account to another.
Note that if we cannot create a user, raise an exception and stop modeling that specific user.
"""

accounts = set()


class TransferUser(FastHttpUser):
    wait_time = between(1, 3)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id = uuid4()

    @task(3)
    def check_balance(self):
        self.client.get(f'/accounts/{self.id}/balance')

    @task
    def make_transfer(self):
        destination = random.sample(accounts - {self.id}, 1)[0]
        self.client.post('/transfers', json={'source': str(self.id),
                                             'destination': str(destination),
                                             'amount': 1})

    def on_start(self):
        res = self.client.post('/accounts', json={'account_id': str(self.id),
                                                  'balance': 1_000_000})
        if not res.status_code == 204:
            raise RuntimeError('failed to create user')
        accounts.add(self.id)
