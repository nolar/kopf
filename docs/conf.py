# Configuration file for the Sphinx documentation builder.
# http://www.sphinx-doc.org/en/master/config
import os

###############################################################################
# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

project = 'Kopf'
copyright = '2019, Zalando SE'
author = 'Sergey Vasilyev'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx_autodoc_typehints',
    'sphinx.ext.todo',
    'sphinx.ext.extlinks',
    'sphinx.ext.linkcode',
    'sphinx.ext.intersphinx',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
html_static_path = ['_static']
html_theme = 'sphinx_rtd_theme'

default_role = 'py:obj'

todo_include_todos = False
todo_emit_warnings = True

extlinks = {
    'issue': ('https://github.com/zalando-incubator/kopf/issues/%s', 'issue '),
}

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
}


def linkcode_resolve(domain, info):
    if domain != 'py':
        return None
    if not info['module']:
        return None
    filename = info['module'].replace('.', '/')
    return "https://github.com/zalando-incubator/kopf/blob/master/%s.py" % filename


###############################################################################
# Ensure the apidoc is always built as part of the build process,
# especially in ReadTheDocs build environment.
# See: https://github.com/rtfd/readthedocs.org/issues/1139
###############################################################################

def run_apidoc(_):
    ignore_paths = [
    ]

    docs_path = os.path.relpath(os.path.dirname(__file__))
    root_path = os.path.relpath(os.path.dirname(os.path.dirname(__file__)))

    argv = [
        '--force',
        '--no-toc',
        '--separate',
        '--module-first',
        '--output-dir', os.path.join(docs_path, 'packages'),
        os.path.join(root_path, 'kopf'),
    ] + ignore_paths

    try:
        # Sphinx 1.7+
        from sphinx.ext import apidoc
        apidoc.main(argv)
    except ImportError:
        # Sphinx 1.6 (and earlier)
        from sphinx import apidoc
        argv.insert(0, apidoc.__file__)
        apidoc.main(argv)


def setup(app):
    app.connect('builder-inited', run_apidoc)
