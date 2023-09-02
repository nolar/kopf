import os.path

from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
    LONG_DESCRIPTION = f.read()
    DESCRIPTION = LONG_DESCRIPTION.splitlines()[0].lstrip('#').strip()

PROJECT_URLS = {
    'Documentation': 'https://kopf.readthedocs.io',
    'Bug Tracker': 'https://github.com/nolar/kopf/issues',
    'Source Code': 'https://github.com/nolar/kopf',
}

setup(
    name='kopf',
    use_scm_version=True,

    url=PROJECT_URLS['Source Code'],
    project_urls=PROJECT_URLS,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    author='Sergey Vasilyev',
    author_email='nolar@nolar.info',
    maintainer='Sergey Vasilyev',
    maintainer_email='nolar@nolar.info',
    keywords=['kubernetes', 'operator', 'framework', 'python', 'k8s'],
    license='MIT',
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries',
    ],

    zip_safe=True,
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'kopf = kopf.cli:main',
        ],
    },

    python_requires='>=3.7',
    setup_requires=[
        'setuptools_scm',
    ],
    install_requires=[
        'typing_extensions',    # 0.20 MB
        'python-json-logger',   # 0.05 MB
        'iso8601',              # 0.07 MB
        'click',                # 0.60 MB
        'aiohttp<4.0.0',        # 7.80 MB
        'pyyaml',               # 0.90 MB
        'attrs>=20.1.0',        # 1.00 MB, comes from the dependencies anyway
    ],
    extras_require={
        'full-auth': [
            'pykube-ng',        # 4.90 MB
            'kubernetes',       # 40.0 MB (!)
        ],
        'uvloop': [
            'uvloop',           # 9.00 MB
        ],
        'dev': [
            'pyngrok',          # 1.00 MB + downloaded binary
            'oscrypto',         # 2.80 MB (smaller than cryptography: 8.7 MB)
            'certbuilder',      # +0.1 MB (2.90 MB if alone)
            'certvalidator',    # +0.1 MB (2.90 MB if alone)
        ],
    },
    package_data={"kopf": ["py.typed"]},
)
