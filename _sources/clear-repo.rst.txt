clear-repo
==========

.. argparse::
   :module: pubtools._pulp.tasks.clear_repo
   :func: doc_parser
   :prog: pubtools-pulp-clear-repo


Example
.......

A typical invocation of clear-repo would look like this:

.. code-block::

  pubtools-pulp-clear-repo \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    my-repo1 my-repo2 ...

All content of the mentioned repositories would be removed.


Example: skipping publish
.........................

If you know that there's no point in publishing Pulp repositories
(for example, you're about to push new content into repos after
clearing them), you can speed up the task by skipping publish:

.. code-block::

  pubtools-pulp-clear-repo \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --skip publish \
    my-repo1 my-repo2 ...


Example: with cache flush
.........................

If your Pulp server is configured to publish to an Akamai CDN,
usage of the Akamai FastPurge API for cache flushing may be enabled
to flush cache after publishing repositories.

This is enabled by providing a value for ``--fastpurge-root-url``.
FastPurge credentials may also be provided; if omitted, the command
will attempt to use a local
`edgerc <https://developer.akamai.com/introduction/Conf_Client.html>`_
file for authentication.

.. code-block::

  pubtools-pulp-clear-repo \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --fastpurge-root-url https://cdn.example.com/ \
    my-repo1 my-repo2 ...
