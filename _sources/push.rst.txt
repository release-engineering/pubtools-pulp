push
====

.. argparse::
   :module: pubtools._pulp.tasks.push
   :func: doc_parser
   :prog: pubtools-pulp-push


Example
.......

A typical invocation to push a single advisory would look like this:

.. code-block::

  pubtools-pulp-push \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --source errata:https://errata.example.com?errata=RHBA-2020:1234

Note the ```--source``` argument accepts any source of content supported
by the `pushsource library <https://release-engineering.github.io/pushsource/>`_.
