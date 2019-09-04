set-maintenance
===============

Set Maintenance On
-------------------

.. argparse::
   :module: pubtools._pulp.tasks.set_maintenance.set_maintenance_on
   :func: doc_parser
   :prog: pubtools-pulp-maintenance-on

Example
.......

A typical invocation of set maintenance would look like this:

.. code-block::

  pubtools-pulp-maintenance-on \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --repo-ids example-repo

In this example, the repository with id 'example-repo' will be set to
maintenance mode.

Set Maintenance Off
-------------------

.. argparse::
   :module: pubtools._pulp.tasks.set_maintenance.set_maintenance_off
   :func: doc_parser
   :prog: pubtools-pulp-maintenance-off


Example
.......

A typical invocation of unset maintenance would look like this:

.. code-block::

  pubtools-pulp-maintenance-off \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --repo-ids example-repo

In this example, the repository with id 'example-repo' will be removed
from maintenance mode.