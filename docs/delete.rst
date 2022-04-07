delete
======

.. argparse::
   :module: pubtools._pulp.tasks.delete
   :func: doc_parser
   :prog: pubtools-pulp-delete


Example: File
.............

A typical invocation of delete file would look like this:

.. code-block::

  pubtools-pulp-delete \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --file some-pkg-1.3-1.el8_x86_64.rpm \
    --signing-key aabbcc \
    --repo some-yumrepo

Mentioned file with the signing key will be removed from the
repo. If there are multiple files and repos, each file will
be removed from the repos they belong to. All those repos will
then be published.

Example: Mix file types
.......................

File provided in ```--file``` could either be a RPM(rpm name),
ISO(filename) or ModuleMD(N:S:V:C:A) file. Different file types
could be included in the same command.

.. code-block::

  pubtools-pulp-delete \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --file some-pkg-1.3-1.el8_x86_64.rpm \
    --file some-file.iso,some-modmd:s1:123:a1c2:s390x \
    --signing-key aabbcc \
    --repo some-yumrepo \
    --repo some-filerepo

Signing key will be used wherever applicable.

Example: Advisory
.................

Packages from an advisory could be removed by providing the advisory ID and the
repos to be removed from. All the packages and the modules that were pushed to
the provided repos as part of the advisory will be removed.

.. code-block::

  pubtools-pulp-delete \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --advisory RHBA-2020:1234 \
    --repo some-yumrepo \
    --repo some-filerepo

Example: Mix files and advisory
...............................

Removing files and packages from the advisory could be requested in the same request.
They will be removed from the provided applicable repos.

.. code-block::

  pubtools-pulp-delete \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --advisory RHBA-2020:1234 \
    --file some-pkg-1.3-1.el8_x86_64.rpm \
    --file some-file.iso,some-modmd:s1:123:a1c2:s390x \
    --signing-key aabbcc \
    --repo some-yumrepo
    --repo some-filerepo

Example: With cache flush
.........................

If the Pulp server is configured to publish to an Akamai CDN,
usage of the Akamai FastPurge API for cache flushing may be enabled
to flush cache after publishing repositories.

This is enabled by providing a value for ``--fastpurge-root-url``.
FastPurge credentials may also be provided; if omitted, the command
will attempt to use a local
`edgerc <https://developer.akamai.com/introduction/Conf_Client.html>`_
file for authentication.

.. code-block::

  pubtools-pulp-delete \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --fastpurge-root-url https://cdn.example.com/ \
    --file some-pkg-1.3-1.el8_x86_64.rpm \
    --signing-key aabbcc \
    --repo some-yumrepo

Example: Skip publish
.....................

The repos are published and the caches are cleaned(provided required arguments)
by default. This can skipped by providing ```--skip``` option with the publish
step name i.e. ```--skip publish```

.. code-block::

  pubtools-pulp-delete \
    --pulp-url https://pulp.example.com/ \
    --pulp-user admin \
    --pulp-password XXXXX \
    --file some-pkg-1.3-1.el8_x86_64.rpm \
    --signing-key aabbcc \
    --repo some-yumrepo \
    --skip publish
