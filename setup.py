from setuptools import setup, find_packages

setup(
    name='kopf',
    use_scm_version=True,

    packages=find_packages(),
    include_package_data=True,

    setup_requires=[
        'setuptools_scm',
    ],
    install_requires=[
    ],
)
