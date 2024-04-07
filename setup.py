from setuptools import find_packages, setup
setup(
    name='oss_cli',
    version='1.0',
    author='InkCoderYmc',
    description='a oss cli tool',
    packages=find_packages(),
    install_requires=[
        'setuptools',
        'pyyaml',
        'boto3',
    ],
    python_requires='>=3.7',
    entry_points={
        'console_scripts': [
            'oss_cli=oss_cli.oss_cli:main',
        ]
    }
)