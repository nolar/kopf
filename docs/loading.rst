Loading and importing
=====================

Kopf requires the source files with the handlers on the command line.
It does not do any attempts to guess the user's intentions
or to introduce any conventions (at least, now).

There are two way to specify them (both mimicking the Python's own way):

* Direct script files::

    kopf run file1.py file2.py

* Importable modules::

    kopf run -m package1.module1 -m package2.module2

* Or mixed::

    kopf run file1.py file2.py -m package1.module1 -m package2.module2

Which way to use depends on how the source code is structured,
and is out of scope of Kopf.

Each of the mentioned files and modules will be imported.
The handlers should be registered during the import.
This is usually done by using the function decorators --- see :doc:`/handlers`.
