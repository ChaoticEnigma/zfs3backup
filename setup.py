import os
import sys
from setuptools import setup, find_packages

dir_path = os.path.dirname(os.path.realpath(__file__))

VERSION = open(os.path.join(dir_path, 'VERSION')).read()

setup()
sys.exit(0)

setup(
    name="zfs3backup",
    version=VERSION,
    platforms='any',
    packages=find_packages(),
    include_package_data=True,
    install_requires=["boto3","ConfigParser"],
    author="Marco Montagna",
    author_email="marcojoemontagna@gmail.com",
    url="https://github.com/mmontagna/zfs3backup",
    entry_points={
        'console_scripts': [
            'zfs3backup = zfs3backup.snap:main',
            'zfs3backup_get = zfs3backup.get:main',
            'zfs3backup_put = zfs3backup.put:main',
            'zfs3backup_ssh_sync = zfs3backup.ssh_sync:main'
        ]
    },
    keywords='ZFS backup',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: Utilities",
    ],
)
