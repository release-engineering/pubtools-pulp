garbage-collect
===============

.. argparse::
   :module: pubtools.pulp.tasks.garbage_collect
   :func: doc_parser
   :prog: garbage-collect

Example
.......

A typical invocation of garbage-collect would look like this:

.. code-block::

  garbage-collect \
    --url https://pulp.example.com/ \
    --user admin \
    --password XXXXX \
    --gc-threshold 14

In this example, temporary repositories older than 2 weeks would be
deleted from the remote Pulp server.
