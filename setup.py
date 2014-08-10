#########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

__author__ = 'Oleksandr_Raskosov'


from setuptools import setup


setup(
    zip_safe=True,
    name='cloudify-libcloud-plugin',
    version='1.0',
    author='Oleksandr_Raskosov',
    author_email='Oleksandr_Raskosov@epam.com',
    packages=[
        'libcloud_plugin_common',
        'server_plugin',
        'security_group_plugin',
        'floating_ip_plugin',
    ],
    license='LICENSE',
    description='Cloudify plugin for Libcloud infrastructure.',
    install_requires=[
        'cloudify-plugins-common>=3.0',
        'apache-libcloud==0.15.1',
    ]
)
