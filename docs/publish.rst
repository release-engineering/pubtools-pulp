publish
=======

.. argparse::
   :module: pubtools._pulp.tasks.publish
   :func: doc_parser
   :prog: pubtools-pulp-publish


Example
.......

A typical invocation of publish would look like this:

.. code-block::

  pubtools-pulp-publish \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --repo-ids my-repo1,my-repo2 ...

Mentioned repositories will be published to the defined
endpoints in the distributors.


Example: applying filters
.........................

Instead of providing the repositories explicitly, you can
use filters i.e. url-regex and published-before to fetch
the repositories and publish.

.. code-block::

  pubtools-pulp-publish \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --published-before 2019-09-10
    --repo-url-regex /some/url/to/match

These filters can be applied on the provided repos too and
only the repos matching those filters are published.

.. code-block::

  pubtools-pulp-publish \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --published-before 2019-09-10
    --repo-url-regex /some/url/to/match
    --repo-ids my-repo1,my-repo2 ...


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

  pubtools-pulp-publish \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --fastpurge-root-url https://cdn.example.com/ \
    --repo-ids my-repo1,my-repo2 ...
