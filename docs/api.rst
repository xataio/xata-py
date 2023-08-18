.. _api:

Xata Python SDK API Reference
=============================

Xata Client
-----------

.. py:module:: xata
.. autoclass:: XataClient
   :members:

.. py:module:: xata.namespace
.. autoclass:: Namespace
   :members:

Data API
--------
.. py:module:: xata.namespace.workspace.search_and_filter
.. autoclass:: SearchAndFilter
   :members:

.. py:module:: xata.namespace.workspace.records
.. autoclass:: Records
   :members:

.. py:module:: xata.namespace.files
.. autoclass:: Files
   :members:

.. py:module:: xata.api.sql
.. autoclass:: SQL
   :members:

Management API
--------------


.. py:module:: xata.namespace.core.databases
.. autoclass:: Databases
   :members:

.. py:module:: xata.namespace.workspace.branch
.. autoclass:: Branch
   :members:

.. py:module:: xata.namespace.workspace.table
.. autoclass:: Table
   :members:

.. py:module:: xata.namespace.core.authentication
.. autoclass:: Authentication
   :members:

.. py:module:: xata.namespace.core.users
.. autoclass:: Users
   :members:

.. py:module:: xata.namespace.core.workspaces
.. autoclass:: Workspaces
   :members:

.. py:module:: xata.namespace.core.invites
.. autoclass:: Invites
   :members:

Helpers
-------

.. py:module:: xata.helpers
.. autoclass:: BulkProcessor
   :members:
.. autoclass:: Transaction
   :members:

Errors
------

.. py:module:: xata.errors
.. autoclass:: BadRequestException
   :members:
.. autoclass:: ServerErrorException
   :members:
