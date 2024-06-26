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

All content of the mentioned repositories would be removed, published
and the cache will be flushed on publish.


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
