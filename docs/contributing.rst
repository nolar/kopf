============
Contributing
============

.. highlight:: bash

Git workflow
============

Kopf uses Git Forking Workflow. It means, all the development should happen
in the individual forks, not in the feature branches of the main repo.

The recommended setup:

* Fork a repo on GitHub and clone the fork (not the original repo).
* Configure the ``upstream`` remote in addition to ``origin``::

        git remote add upstream git@github.com:zalando-incubator/kopf.git
        git fetch upstream

* Sync your ``master`` branch with the upstream regularly::

        git checkout master
        git pull upstream master --ff
        git push origin master

Work in the feature branches of your fork, not in the upstream's branches:

* Create a feature branch in the fork::

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

Kopf tries to avoid any written rules, and to follow the human habits
and intuitive expectations where possible. Therefore:

* Write clear and explanatory commit messages and PR titles.
  Read `How to Write a Git Commit Message <https://chris.beams.io/posts/git-commit/>`_
  for examples.
* Branch names can be anything explanatory (1-2-3 words).
  It is good if they start with an issue number, though it is not required.
* No commit or PR prefixes/suffixes with the issue numbers.
  Keep the git log clean. This will later go to the changelogs.

Yet some rules have to be followed -- read below.


Sign your code
==============

Create GPG key:

* MacOS: https://gpgtools.org/
* Ubuntu: `Create a key with GNUPG <https://help.github.com/en/articles/generating-a-new-gpg-key>`_ (``apt-get install``);
  then `add another uid for additional emails <https://superuser.com/questions/293184/one-gnupg-pgp-key-pair-two-emails>`_.
* Or use any other tool of preference (no strict requirements).

Add the GPG key to your GitHub settings.

The straightforward way to sign git commits::

    git commit -s -m "adding X to change Y"

You can also configure the auto-signing of all commits (recommended)::

    git config --global user.signingKey 0123456789ABCDEF...
    git config --global commit.gpgSign true


Sign DCO
========

All contributions (including pull requests) must agree
to the Developer Certificate of Origin (DCO) version 1.1.
This is exactly the same one created and used by the Linux kernel developers
and posted on http://developercertificate.org/.
This is a developer's certification that he or she has the right to submit
the patch for inclusion into the project.
Simply submitting a contribution implies this agreement,
however, please include a "Signed-off-by" tag in every patch
(this tag is a conventional way to confirm that you agree to the DCO) -
you can automate this with
a [Git hook](https://stackoverflow.com/questions/15015894/git-add-signed-off-by-line-using-format-signoff-not-working)


Code reviews
============

As Kopf is part of the Zalando's OpenSource initiative,
we have to follow some rules of play for compliance:

* Always have an issue for which the PR is created.
  Put a link to that issue in the PR body.
  You can use one of the existing or closed issues that matches your topic best.
  PRs without a referred issue will not be approved
  (not silently though: you will be asked to add a reference).
* The PRs can be reviewed and commented by anyone,
  but can be approved only by the Zalando employees.
  Two approvals are needed at least
  (the PR author automatically counts if they are a Zalando employee).


Private CI/CD
=============

The existing setup runs the Travis CI builds on every push
to the existing pull requests of the upstream repository.

In case you do not want to create a pull request yet,
but want to run the builds for your branch,
enable Travis CI for your own fork:

* Create a `Travis CI <https://travis-ci.org/>`_ account.
* Find your fork in the list of repos.
* Click the toggle.
* Push a feature branch to ``origin`` (see above).
* Observe how Travis runs the tests in Travis CI in your account.

When ready, create a PR to the upstream repository.
This will run the tests in the upstream's Travis account.
