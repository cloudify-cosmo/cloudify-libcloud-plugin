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


import json
import os
import cloudify
from cloudify.exceptions import NonRecoverableError, RecoverableError
from functools import wraps
import sys
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
import abc

class LibcloudProviderContext(object):

    def __init__(self, provider_context):
        self._provider_context = provider_context or {}
        self._resources = self._provider_context.get('resources', {})
    def __repr__(self):
        info = json.dumps(self._provider_context)
        return '<' + self.__class__.__name__ + ' ' + info + '>'


def provider(ctx):
    config = _get_connection_config(ctx)
    mapper = Mapper(
        transfer_cloud_provider_name(config['cloud_provider_name']))
    return mapper.get_provider_context(ctx.provider_context)


def transfer_cloud_provider_name(provider_name):
    return provider_name.replace('-', '_')


def transform_resource_name(res, ctx):

    if isinstance(res, basestring):
        res = {'name': res}

    if not isinstance(res, dict):
        raise ValueError("transform_resource_name() expects either string or "
                         "dict as the first parameter")

    pfx = ctx.bootstrap_context.resources_prefix

    if not pfx:
        return res['name']

    name = res['name']
    res['name'] = pfx + name

    if name.startswith(pfx):
        ctx.logger.warn("Prefixing resource '{0}' with '{1}' but it "
                        "already has this prefix".format(name, pfx))
    else:
        ctx.logger.info("Transformed resource name '{0}' to '{1}'".format(
                        name, res['name']))

    return res['name']


class LibcloudClient(object):

    def get(self, mapper, config, *args, **kw):
        ret = self.connect(config, mapper)
        ret.format = 'json'
        return ret

    def connect(self, cfg, mapper):
        self.driver = mapper.connect(cfg)
        return self


class LibcloudServerClient(LibcloudClient):

    @abc.abstractmethod
    def create(self, name, ctx, server_context, provider_context):
        return

    @abc.abstractmethod
    def get_by_id(self, server_id):
        return

    @abc.abstractmethod
    def get_by_name(self, server_name):
        return

    @abc.abstractmethod
    def start_server(self, server):
        return

    @abc.abstractmethod
    def stop_server(self, server):
        return

    @abc.abstractmethod
    def delete_server(self, server):
        return

    @abc.abstractmethod
    def wait_for_server_to_be_deleted(self, server, timeout, sleep_time):
        return

    @abc.abstractmethod
    def wait_for_server_to_be_running(self, server, timeout, sleep_time):
        return

    @abc.abstractmethod
    def connect_floating_ip(self, server, ip):
        return

    @abc.abstractmethod
    def disconnect_floating_ip(self, ip):
        return

    @abc.abstractmethod
    def get_image_by_name(self, image_name):
        return

    @abc.abstractmethod
    def get_size_by_name(self, size_name):
        return

    @abc.abstractmethod
    def is_server_active(self, server):
        return


class LibcloudFloatingIPClient(LibcloudClient):

    @abc.abstractmethod
    def delete(self, ip):
        return

    @abc.abstractmethod
    def create(self, **kwargs):
        return

    @abc.abstractmethod
    def get_by_ip(self, ip):
        return


class LibcloudSecurityGroupClient(LibcloudClient):

    @abc.abstractmethod
    def create(self, security_group):
        return

    @abc.abstractmethod
    def delete(self, id):
        return

    @abc.abstractmethod
    def get_list_by_name(self, name):
        return

    @abc.abstractmethod
    def get_description(self, sg):
        return

    @abc.abstractmethod
    def get_id(self, sg):
        return

    @abc.abstractmethod
    def get_rules(self, sg):
        return

    @abc.abstractmethod
    def create_security_group_rule(self, rule):
        return


# Decorators
def _find_instanceof_in_kw(cls, kw):
    ret = [v for v in kw.values() if isinstance(v, cls)]
    if not ret:
        return None
    if len(ret) > 1:
        raise NonRecoverableError(
            "Expected to find exactly one instance of {0} in "
            "kwargs but found {1}".format(cls, len(ret)))
    return ret[0]


def _find_context_in_kw(kw):
    return _find_instanceof_in_kw(cloudify.context.CloudifyContext, kw)


def _get_connection_config(ctx):
    def _get_static_config():
        which = 'connection'
        env_name = which.upper() + '_CONFIG_PATH'
        default_location_tpl = '~/' + which + '_config.json'
        default_location = os.path.expanduser(default_location_tpl)
        config_path = os.getenv(env_name, default_location)
        try:
            with open(config_path) as f:
                cfg = json.loads(f.read())
        except IOError:
            raise NonRecoverableError(
                "Failed to read {0} configuration from file '{1}'."
                "The configuration is looked up in {2}. If defined, "
                "environment variable "
                "{3} overrides that location.".format(
                    which, config_path, default_location_tpl, env_name))
        return cfg
    static_config = _get_static_config()
    cfg = {}
    cfg.update(static_config)
    config = ctx.node.properties.get('connection_config')
    if config:
        cfg.update(config)
    return cfg


def with_server_client(f):
    @wraps(f)
    def wrapper(*args, **kw):
        ctx = _find_context_in_kw(kw)
        config = _get_connection_config(ctx)
        mapper = Mapper(
            transfer_cloud_provider_name(config['cloud_provider_name']))
        kw['server_client'] = mapper.get_server_client(config)
        return f(*args, **kw)
    return wrapper


def with_floating_ip_client(f):
    @wraps(f)
    def wrapper(*args, **kw):
        ctx = _find_context_in_kw(kw)
        config = _get_connection_config(ctx)
        mapper = Mapper(
            transfer_cloud_provider_name(config['cloud_provider_name']))
        kw['floating_ip_client'] = mapper.get_floating_ip_client(config)
        return f(*args, **kw)
    return wrapper


def get_floating_ip_client(ctx):
    config = _get_connection_config(ctx)
    mapper = Mapper(
        transfer_cloud_provider_name(config['cloud_provider_name']))
    return mapper.get_floating_ip_client(config)


def with_security_group_client(f):
    @wraps(f)
    def wrapper(*args, **kw):
        ctx = _find_context_in_kw(kw)
        config = _get_connection_config(ctx)
        mapper = Mapper(
            transfer_cloud_provider_name(config['cloud_provider_name']))
        kw['security_group_client'] = mapper.get_security_group_client(config)
        return f(*args, **kw)
    return wrapper


_non_recoverable_error_codes = [400, 401, 403, 404, 409]


def _re_raise(e, recoverable, retry_after=None):
    exc_type, exc, traceback = sys.exc_info()
    if recoverable:
        if retry_after == 0:
            retry_after = None
        raise RecoverableError(
            message=e.message,
            retry_after=retry_after), None, traceback
    else:
        raise NonRecoverableError(e.message), None, traceback


class Mapper(object):

    def __init__(self, provider_name):
        if provider_name == Provider.EC2_AP_NORTHEAST:
            self.core_provider = Provider.EC2
            self.provider = Provider.EC2_AP_NORTHEAST
        elif provider_name == Provider.EC2_AP_SOUTHEAST:
            self.core_provider = Provider.EC2
            self.provider = Provider.EC2_AP_SOUTHEAST
        elif provider_name == Provider.EC2_AP_SOUTHEAST2:
            self.core_provider = Provider.EC2
            self.provider = Provider.EC2_AP_SOUTHEAST2
        elif provider_name == Provider.EC2_EU:
            self.core_provider = Provider.EC2
            self.provider = Provider.EC2_EU
        elif provider_name == Provider.EC2_EU_WEST:
            self.core_provider = Provider.EC2
            self.provider = Provider.EC2_EU_WEST
        elif provider_name == Provider.EC2_SA_EAST:
            self.core_provider = Provider.EC2
            self.provider = Provider.EC2_SA_EAST
        elif provider_name == Provider.EC2_US_EAST:
            self.core_provider = Provider.EC2
            self.provider = Provider.EC2_US_EAST
        elif provider_name == Provider.EC2_US_WEST:
            self.core_provider = Provider.EC2
            self.provider = Provider.EC2_US_WEST
        elif provider_name == Provider.EC2_US_WEST_OREGON:
            self.core_provider = Provider.EC2
            self.provider = Provider.EC2_US_WEST_OREGON
        elif provider_name == Provider.VCLOUD:
            self.provider = Provider.VCLOUD
            self.core_provider = Provider.VCLOUD
        else:
            raise NonRecoverableError('Error during trying to choose'
                                      ' the Libcloud provider,'
                                      ' provider name: {0}'
                                      .format(provider_name))

    def connect(self, connection_config):
        if self.core_provider == Provider.EC2:
            return get_driver(self.provider)(connection_config['access_id'],
                                             connection_config['secret_key'])
        elif self.core_provider == Provider.VCLOUD:
            return get_driver(self.provider)(connection_config['access_id'],
                                             connection_config['secret_key'],
                                             host=connection_config['host'],
                                             api_version='1.5')

    def get_server_client(self, config):
        if self.core_provider == Provider.EC2:
            from ec2 import EC2LibcloudServerClient
            return EC2LibcloudServerClient().get(mapper=self, config=config)
        elif self.core_provider == Provider.VCLOUD:
            from vcloud import VCloudLibcloudServerClient
            return VCloudLibcloudServerClient().get(mapper=self, config=config)

    def get_floating_ip_client(self, config):
        if self.core_provider == Provider.EC2:
            from ec2 import EC2LibcloudFloatingIPClient
            return EC2LibcloudFloatingIPClient()\
                .get(mapper=self, config=config)
        elif self.core_provider == Provider.VCLOUD:
            from vcloud import VCLOUDLibcloudFloatingIPClient
            return VCloudLibcloudFloatingIPClient()\
                .get(mapper=self, config=config)

    def get_security_group_client(self, config):
        if self.core_provider == Provider.EC2:
            from ec2 import EC2LibcloudSecurityGroupClient
            return EC2LibcloudSecurityGroupClient()\
                .get(mapper=self, config=config)
        elif self.core_provider == Provider.VCLOUD:
            from vcloud import VCloudLibcloudSecurityGroupClient
            return VCloudLibcloudSecurityGroupClient()\
                .get(mapper=self, config=config)

    def get_provider_context(self, context):
        if self.core_provider == Provider.EC2:
            from ec2 import EC2LibcloudProviderContext
            return EC2LibcloudProviderContext(context)
        elif self.core_provider == Provider.VCLOUD:
            from vcloud import VCloudLibcloudProviderContext
            return VCloudLibcloudProviderContext(context)
