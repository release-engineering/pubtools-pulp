fix-cves
========

.. argparse::
   :module: pubtools._pulp.tasks.fix_cves
   :func: doc_parser
   :prog: pubtools-pulp-fix-cves


Example
.......

A typical invocation of fix-cves would look like this:

.. code-block::

  pubtools-pulp-fix-cves \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --advisory RHXA-123:45 \
    --cves CVE-123,CVE-345

Mentioned CVEs will be updated in the advisory and uploaded
to one of the randomly picked repo from the list of repos the
advisory belongs to. All those repos will then be published.


Example: with cache flush
.........................

If the repo is listed for Unified Downloads,UD cache flush may be
enabled with --udcache-url.

.. code-block::

  pubtools-pulp-fix-cves \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --udcache-url https://ud.example.com/ \
    --advisory RHXA-123:45 \
    --cves CVE-123,CVE-345

Once the advisory is updated and the related repos are published,
caches will be cleared for the provided urls.
