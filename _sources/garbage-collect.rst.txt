garbage-collect
===============

.. argparse::
   :module: pubtools._pulp.tasks.garbage_collect
   :func: doc_parser
   :prog: pubtools-pulp-garbage-collect

Example
.......

A typical invocation of garbage-collect would look like this:

.. code-block::

  pubtools-pulp-garbage-collect \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --gc-threshold 14

In this example, temporary repositories older than 2 weeks would be
deleted from the remote Pulp server.
