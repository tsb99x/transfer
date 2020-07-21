========
Transfer
========

This is a sample implementation to demonstrate **deadlock-free** system for **bank transfers**.

Structure
---------

Every part is written in somewhat-literal style, i.e. read code from top to bottom:

* `transfer.py <transfer.py>`_ -- represents main code
* `init.sql <init.sql>`_ -- establishing schema and system integrity checks
* `test.py <test.py>`_ -- main operational testing script
* `locustfile <locustfile.py>`_ -- performance testing script

Immutable Design
----------------

To get rid of *mean deadlocks*, system design can be based on *immutability* principles.
Thus, system operation can be modeled just as a *series of transfers* between *accounts* on *specific date and time*.
This ensures that a total *value*, represented by transfers, wouldn't be lost at some point in time.
Very much alike to *double-entry bookkeeping*, where same principles are applied to *real-world accounting*.
Relevant data is calculated from *recorded transfers*, including *account balance* on specific *time in past* or *now*.

Transfers
---------

To be sure that data will be consistent at all times, each transfer represented as:

* source account id
* index of transfer from source account
* destination account id
* amount

Index of transfer ensures that two concurrent processes cannot record transfers from same account at the same time.
But ordered indexing is used for accounts separately, allowing parallel insertion of transfers from different accounts.
Amount must be less that total balance of source account right before transaction and greater than 0.
Transfers where source and destination are the same are not allowed, as they don't make any sense.

Dependencies
------------

Minimal number of external dependencies used.

Production (`requirements.txt <requirements.txt>`_):

* fastapi
* uvicorn
* asyncpg

Development (`requirements-dev.txt <requirements-dev.txt>`_):

* pytest & cov
* locust

Deployment
----------

To run, just use:

  docker-compose up

Everything should be up and running in no time!

* PostgreSQL: user:pass@localhost:5432/db
* Service: http://localhost:8000
* Locust Web-GUI: http://localhost:8089

OpenAPI:

* Swagger: http://localhost:8000/docs
* ReDoc: http://localhost:8000/redoc

To shut everything down and cleanup (flag -v is for database volume removal):

  docker-compose down -v

Integration Testing & Code Coverage
-----------------------------------

To launch test suite and get code coverage report in HTML format, use:

  pytest --cov-report html --cov transfer test.py

Performance Testing
-------------------

Locust is used to model a distinct user that creating an account.
After that, user can check account balance or do a transfer to some other account (picked randomly).
Balance check done more often than transfers (3 to 1 ratio).
Action is performed by user in 1 to 3 seconds, randomly.
With this parameters, a group of *1000 users* should be doing, on average, a *500 RPS* load.
