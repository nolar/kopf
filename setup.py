from setuptools import setup, find_packages

setup(
    name='kopf',
    use_scm_version=True,
    url='https://pypi.org/project/kopf/',

    author='Sergey Vasilyev',
    author_email='sergey.vasilyev@zalando.de',

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
