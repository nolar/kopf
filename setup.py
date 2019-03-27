import os.path

from setuptools import setup, find_packages

LONG_DESCRIPTION = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()
DESCRIPTION = LONG_DESCRIPTION.splitlines()[0].lstrip('#').strip()
PROJECT_URLS = {
    'Documentation': 'https://github.com/zalando-incubator/kopf/blob/master/README.md',
    'Bug Tracker': 'https://github.com/zalando-incubator/kopf/issues',
    'Source Code': 'https://github.com/zalando-incubator/kopf',
}

setup(
    name='kopf',
    use_scm_version=True,

    url=PROJECT_URLS['Source Code'],
    project_urls=PROJECT_URLS,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author='Sergey Vasilyev',
    author_email='sergey.vasilyev@zalando.de',
    maintainer='Sergey Vasilyev, Chiara Mezzavilla',
    maintainer_email='sergey.vasilyev@zalando.de, chiara.mezzavilla@zalando.de',
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
        'click',
        'iso8601',
        'aiojobs',
        'kubernetes',
    ],
)
