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
* Sign-off your commits.
* Create a pull request.
* Ensure all required checks are passed.
* Wait for a review by the project maintainers.


Git workflow
============

Kopf uses Git Forking Workflow. It means, all the development should happen
in the individual forks, not in the feature branches of the main repo.

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

Work in the feature branches of your fork, not in the upstream's branches:

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

Kopf tries to avoid any written rules and to follow human habits
and intuitive expectations where possible. Therefore:

* Write clear and explanatory commit messages and PR titles.
  Read `How to Write a Git Commit Message <https://chris.beams.io/posts/git-commit/>`_
  for examples.
* Avoid commits' or PRs' prefixes/suffixes with the issues or change types.
  In general, keep the git log clean -- this will later go to the changelogs.
* Sign-off your commits for DCO (see below).

No more other rules.


DCO sign-off
============

All contributions (including pull requests) must agree
to the Developer Certificate of Origin (DCO) version 1.1.
This is the same one created and used by the Linux kernel developers
and posted on http://developercertificate.org/.

This is a developer's certification that they have the right to submit
the patch for inclusion into the project.

Simply submitting a contribution implies this agreement.
However, please include a "Signed-off-by" tag in every patch
(this tag is a conventional way to confirm that you agree to the DCO):

The sign-off can be either written manually or added with ``git commit -s``.
If you contribute often, you can automate this in Kopf's repo with
a [Git hook](https://stackoverflow.com/a/46536244/857383).


Code style
==========

Common sense is the best code formatter.
Blend your code into the surrounding code style.

Kopf does not use and will never use strict code formatters
(at least until they acquire common sense and context awareness).
In case of doubt, adhere to PEP-8 and
[Google Python Style Guide](https://google.github.io/styleguide/pyguide.html).

The line length is 100 characters for code, 80 for docstrings and RsT files.
Long URLs can exceed this length.

For linting, minor code styling, import sorting, layered modules checks, run:

.. code-block:: bash

    pre-commit run


Tests
=====

If possible, run the unit-tests locally before submitting
(this will save you some time, but is not mandatory):

.. code-block:: bash

    pytest

If possible, run the functional tests with a realistic local cluster
(for examples, with k3s/k3d on MacOS; Kind and Minikube are also fine):

.. code-block:: bash

    brew install k3d
    k3d cluster create
    pytest --only-e2e

If not possible, create a PR draft instead of a PR,
and check the GitHub Actions' results for unit- & functional tests,
fix as needed, and promote the PR draft into a PR once everything is ready.


Reviews
=======

If possible, refer to an issue for which the PR is created in the PR's body.
You can use one of the existing or closed issues that match your topic best.

The PRs can be reviewed and commented by anyone,
but can be approved only by the project maintainers.
