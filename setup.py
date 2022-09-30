from setuptools import find_packages, setup
setup(
    name='xata',
    packages=find_packages(include=['xata']),
    version='0.1.0',
    description='Python client for Xata.io',
    author='Tudor Golubenco',
    license='Apache-2.0',
    python_requires=">=3.6, <4",
    install_requires=["requests", "python-dotenv"],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
)