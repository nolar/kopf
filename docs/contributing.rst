============
Contributing
============

In a nutshell, to contribute, follow this scenario:

* Fork the repo in GitHub.
* Clone the fork.
* Check out a feature branch.
* **Implement the changes.**
  * Lint with ``pre-commit run``.
  * Test with ``pytest``.
* Sign off your commits.
* Create a pull request.
* Ensure all required checks pass.
* Wait for a review by the project maintainers.


Git workflow
============

Kopf uses a Git Forking Workflow. This means all development should happen
in individual forks, not in feature branches of the main repo.

The recommended setup:

* Fork a repo on GitHub and clone the fork (not the original repo).
* Configure the ``upstream`` remote in addition to ``origin``:

.. code-block:: bash

    git remote add upstream git@github.com:nolar/kopf.git
    git fetch upstream

* Sync your ``main`` branch with the upstream regularly:

.. code-block:: bash

    git checkout main
    git pull upstream main --ff
    git push origin main

Work in feature branches of your fork, not in the upstream's branches:

* Create a feature branch in the fork:

.. code-block:: bash

    git checkout -b feature-x
    git push origin feature-x

* Once the feature is ready, create a pull request
  from your fork to the main repo.

.. seealso::

    * `Overview of the Forking Workflow. <https://gist.github.com/Chaser324/ce0505fbed06b947d962>`_
    * `GitHub's manual on forking <https://help.github.com/en/articles/fork-a-repo>`_
    * `GitHub's manual on syncing the fork <https://help.github.com/en/articles/syncing-a-fork>`_


Git conventions
===============

The more rules you have, the less they are followed.

Kopf tries to avoid written rules and to follow human habits
and intuitive expectations where possible. Therefore:

* Write clear and explanatory commit messages and PR titles.
  Read `How to Write a Git Commit Message <https://chris.beams.io/posts/git-commit/>`_
  for examples.
* Avoid prefixes or suffixes in commit messages or PR titles for issues or change types.
  In general, keep the git log clean --- it will later go into the changelogs.
* Sign off your commits for DCO (see below).

No other rules.


DCO sign-off
============

All contributions (including pull requests) must agree
to the Developer Certificate of Origin (DCO) version 1.1.
This is the same one created and used by the Linux kernel developers,
posted at http://developercertificate.org/.

This is a developer's certification that they have the right to submit
the patch for inclusion in the project.

Simply submitting a contribution implies this agreement.
However, please include a "Signed-off-by" tag in every patch
(this tag is a conventional way to confirm that you agree to the DCO).

The sign-off can be written manually or added with ``git commit -s``.
If you contribute often, you can automate this in Kopf's repo with
a `Git hook <https://stackoverflow.com/a/46536244/857383>`_.


Code style
==========

Common sense is the best code formatter.
Blend your code into the surrounding code's style.

Kopf does not use and will never use strict code formatters
(at least until they acquire common sense and context awareness).
When in doubt, adhere to PEP 8 and the
`Google Python Style Guide <https://google.github.io/styleguide/pyguide.html>`_.

The line length is 100 characters for code, 80 for docstrings and RST files.
Long URLs can exceed this length.

For linting, minor code styling, import sorting, and layered module checks, run:

.. code-block:: bash

    pre-commit run


Tests
=====

If possible, run the unit-tests locally before submitting
(this will save you some time, but is not mandatory):

.. code-block:: bash

    pytest

If possible, run the functional tests with a realistic local cluster
(for example, with k3s/k3d on macOS; Kind and Minikube are also fine):

.. code-block:: bash

    brew install k3d
    k3d cluster create
    pytest --only-e2e

If that is not possible, create a draft PR instead,
check the GitHub Actions results for unit and functional tests,
fix as needed, and promote the draft PR to a full PR once everything is ready.


Reviews
=======

If possible, reference the issue the PR addresses in the PR's body.
You can use one of the existing or closed issues that best matches your topic.

PRs can be reviewed and commented on by anyone,
but can be approved only by the project maintainers.
