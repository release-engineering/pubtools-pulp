copy-repo
=========

.. argparse::
   :module: pubtools._pulp.tasks.copy_repo
   :func: doc_parser
   :prog: pubtools-pulp-copy-repo


Examples
........

Copy content from one repository to another:

.. code-block::

   pubtools-pulp-copy-repo \
      --content-type rpm,srpm \
      repo-A,repo-B

This command copies RPM and SRPM content from repository `repo-A` to `repo-B`.
If the user provides a non existing repo, the command fails.

Provide multiple repository pairs to copy them in one command:

.. code-block::

   pubtools-pulp-copy-repo \
      --content-type iso \
      repo-1,repo-2 repo-3,repo-4

