.. xata-py documentation master file, created by
   sphinx-quickstart on Sat Oct  1 12:34:50 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Python SDK for Xata
===================

This is a the official low-level Python client for the Xata_ service.
Xata is a Serverless Database that is as easy to use as a spreadsheet, has the
data integrity of PostgreSQL, and the search and analytics functionality of
Elasticsearch.


Installation
------------

Install the `xata` package from PyPI:

.. code-block:: bash

    $ pip install xata

While not strictly required, we recommend installing the Xata_CLI_ and initializing
your project with the `xata init` command:

.. code-block:: bash

    $ xata init

This will then interactively ask you to select a Database, and store the required
connection information in `.xatarc` and `.env`. The Python SDK automatically reads those
files, but it's also possible to pass the connection information directly to the
`xata.Client` constructor.

See more information about the installation in the Xata_Python_SDK_Install_Docs_.

Example Usage
-------------
.. code-block:: python

    from xata.client import XataClient
 
    resp = xata.data().query("Avengers", {
        "columns": ["name", "thumbnail"],  # the columns we want returned
        "filter": { "job": "spiderman" },  # optional filters to apply
        "sort": { "name": "desc" }  # optional sorting key and order (asc/desc)
    })
    assert resp.is_success()
    print(resp["records"])
    # [{"id": "spidey", "name": "Peter Parker", "job": "spiderman"}]
    # Note it will be an array, even if there is only one record matching the filters

See more examples in the Xata_Python_SDK_Examples_Docs_.

.. toctree::
   :maxdepth: 3
   :caption: Contents:

   api



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Xata: https://xata.io
.. _Xata_CLI: https://xata.io/docs/getting-started/cli
.. _Xata_Python_SDK_Install_Docs: https://xata.io/docs/python-sdk/overview
.. _Xata_Python_SDK_Examples_Docs: https://xata.io/docs/python-sdk/examples
