========
Transfer
========

This is a sample implementation to demonstrate the **deadlock-free** system for **bank transfers**.

Structure
---------

Every part is written in a somewhat-literal style, i.e. read code from top to bottom:

* `transfer.py <transfer.py>`_ -- represents the main code
* `init.sql <init.sql>`_ -- establishing schema and system integrity checks
* `test.py <test.py>`_ -- main operational testing script
* `locustfile.py <locustfile.py>`_ -- performance testing script

Immutable Design
----------------

To get rid of *mean deadlocks*, system design can be based on *immutability* principles.
Thus, system operation can be modeled just like a *series of transfers* between *accounts* on *specific date and time*.
This ensures that a total *value*, represented by transfers, wouldn't be lost at some point in time.
Very much alike to *double-entry bookkeeping*, where the same principles are applied to *real-world accounting*.
Relevant data is calculated from *recorded transfers*, including *account balance* on a specific time *in the past* or *now*.

Transfers
---------

To be sure that data will be consistent at all times, each transfer represented as:

* source account id
* index of transfer from the source account
* destination account id
* amount

Index of transfer ensures that two concurrent processes cannot record transfers from the same account at the same time.
But ordered indexing is used for accounts separately, allowing parallel insertion of transfers from different accounts.
The amount must be less than the total balance of source account right before the transaction and greater than 0.
Transfers, where source and destination, are the same are not allowed, as they don't make any sense.

Dependencies
------------

A minimal number of external dependencies used.

Production (`requirements.txt <requirements.txt>`_):

* `fastapi <https://github.com/tiangolo/fastapi>`_
* `uvicorn <https://github.com/encode/uvicorn>`_
* `asyncpg <https://github.com/MagicStack/asyncpg>`_

Development (`requirements-dev.txt <requirements-dev.txt>`_):

* `pytest <https://github.com/pytest-dev/pytest>`_ & `pytest-cov <https://github.com/pytest-dev/pytest-cov>`_
* `locust <https://github.com/locustio/locust>`_

Deployment
----------

To run, just use:

::

  docker-compose up -d

Everything should be up and running in no time!

* PostgreSQL: postgres://user:pass@localhost:5432/db
* Service: http://localhost:8000
* Locust Web-GUI: http://localhost:8089

OpenAPI:

* Swagger: http://localhost:8000/docs
* ReDoc: http://localhost:8000/redoc

To shut everything down and clean up (flag -v is for database volume removal):

::

  docker-compose down -v

Integration Testing & Code Coverage
-----------------------------------

To launch DB, test suite and get code coverage report in HTML format, use:

::

  docker-compose up -d db
  pytest --cov-report html --cov transfer test.py

Performance Testing
-------------------

Locust is used for modeling a distinct user that creating an account.
After that, the user can check the account balance or do a transfer to some other account (picked randomly).
A balance check is done more often than transfers (3 to 1 ratio).
Action is performed by the user in 1 to 3 seconds, randomly.
With these parameters, a group of *1024 users* should be doing, on average, a *512 RPS* load.

Digital Ocean droplets were used to test real-world performance.
Infrastructure was based on the cheapest one-click Docker droplets.
Characteristics for nodes (*3x*) are: **1 vCPU**, **1 GB RAM**, **25 GB SSD** (total of *15$/month* or *$0.021/hour*).
One node was used as Locust host, one node for service itself, one node for PostgreSQL.
The additional configuration includes *the removal of access logs* from the service and usage of asyncpg *pool size in [5, 25]*.

.. image:: locust_data_2020-07-05.png

As can be seen, even at *1024 users*, service works as expected, without dropping requests or failing badly.
Although it can be seen, that RPS becomes a little volatile, as well as percentile 95 (yellow line on the second graph).

Best RPS can be seen at a *505.5* mark.
Worst (max) median response time is recorded at *150 ms*.
Testing was started when the count of transfer records in the database was *514k*.
After testing there were *634k* records, i.e. *120k* records was created.

Peak load for nodes:

* Database - 74% CPU, 62% MEM, 13% DISK
* Service - 90% CPU
* Locust - 48% CPU
