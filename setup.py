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
        'typing_extensions',
        'python-json-logger',
        'click',
        'iso8601',
        'aiohttp<4.0.0',
        'aiojobs',
        'pykube-ng>=0.27',  # used only for config parsing
    ],
)
