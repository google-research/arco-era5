# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Setup arco_era5.

This uses Apache Beam's recommended way to install non-python dependencies. This lets us install ecCodes for cfgrib on
Dataflow workers.

Please see the following documentation and examples for more:
- https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#nonpython
- https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/juliaset/setup.py
"""
import subprocess
from distutils.command.build import build as _build  # type: ignore

from setuptools import setup, find_packages, Command

"""Install the ecCodes binary from ECMWF."""
CUSTOM_COMMANDS = [
    cmd.split() for cmd in [
        'apt-get update',
        'apt-get --assume-yes install libeccodes-dev'
    ]
]


# This class handles the pip install mechanism.
class build(_build):  # pylint: disable=invalid-name
    """A build command class that will be invoked during package install.
    The package built using the current setup.py will be staged and later
    installed in the worker using `pip install package'. This class will be
    instantiated during install for this specific scenario and will trigger
    running the custom commands specified.
    """
    sub_commands = _build.sub_commands + [('CustomCommands', None)]


class CustomCommands(Command):
    """A setuptools Command class able to run arbitrary commands."""

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def RunCustomCommand(self, command_list):
        print('Running command: %s' % command_list)
        p = subprocess.Popen(
            command_list,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        # Can use communicate(input='y\n'.encode()) if the command run requires
        # some confirmation.
        stdout_data, _ = p.communicate()
        print('Command output: %s' % stdout_data)
        if p.returncode != 0:
            raise RuntimeError(
                'Command %s failed: exit code: %s' % (command_list, p.returncode))

    def run(self):
        for command in CUSTOM_COMMANDS:
            self.RunCustomCommand(command)


setup(
    name='arco_era5',
    packaging=find_packages('src'),
    author_email='anthromet-core+era5@google.com',
    description="Analysis-Ready & Cloud-Optimized ERA5.",
    platforms=['darwin', 'linux'],
    python_requires='>=3.7, <3.10',
    install_requires=[
        'apache_beam[gcp]',
        'pangeo-forge-recipes==0.8.3',
        'pandas',
        'gcsfs',
        'cfgrib',
        'google-weather-tools>=0.3.1',
    ],
    tests_require=['pytest'],
    cmdclass={
        # Command class instantiated and run during pip install scenarios.
        'build': build,
        'CustomCommands': CustomCommands,
    }
)
