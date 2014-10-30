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


import copy
from cloudify.decorators import operation
from libcloud_plugin_common import (with_server_client,
                                    get_floating_ip_client,
                                    provider,
                                    transform_resource_name)


LIBCLOUD_SERVER_ID_PROPERTY = 'libcloud_server_id'
TIMEOUT = 120
SLEEP_TIME = 5


def start_new_server(ctx, server_client, **kwargs):
    provider_context = provider(ctx)

    server = {
        'name': ctx.instance.id
    }
    server.update(copy.deepcopy(ctx.node.properties['server']))
    transform_resource_name(server, ctx)

    ctx.logger.info("Creating VM")

    server = server_client.create(ctx.instance.id, ctx, server, provider_context)
    server_client.wait_for_server_to_be_running(server, TIMEOUT, SLEEP_TIME)

    ctx.instance.runtime_properties[LIBCLOUD_SERVER_ID_PROPERTY] = server.id


@operation
@with_server_client
def start(ctx, server_client, **kwargs):
    server = get_server_by_context(server_client, ctx.instance)
    if server is not None:
        server_client.start_server(server)
        return

    start_new_server(ctx, server_client, **kwargs)


@operation
@with_server_client
def stop(ctx, server_client, **kwargs):
    server = get_server_by_context(server_client, ctx.instance)
    if server is None:
        raise RuntimeError(
            "Cannot stop server - server doesn't exist for node: {0}"
            .format(ctx.instance.id))
    server_client.stop_server(server)


@operation
@with_server_client
def delete(ctx, server_client, **kwargs):
    server = get_server_by_context(server_client, ctx.instance)
    if server is None:
        return
    server_client.delete_server(server)
    server_client.wait_for_server_to_be_deleted(server, TIMEOUT, SLEEP_TIME)


@operation
@with_server_client
def get_state(ctx, server_client, **kwargs):
    ctx.logger.info("Try to get server state")
    server = get_server_by_context(server_client, ctx.instance)
    if server_client.is_server_active(server):
        ctx.logger.info("Server \'{0}\' is active".format(server.name))
        ips = {}
        ips['private'] = server.private_ips
        ips['public'] = server.public_ips
        ctx.instance.runtime_properties['networks'] = ips
        ctx.instance.runtime_properties['ip'] = server.private_ips[0]
        return True
    return False


@operation
@with_server_client
def connect_floating_ip(ctx, server_client, **kwargs):
    ctx.logger.info("Try to connect floating IP")
    server = get_server_by_context(server_client, ctx.instance)
    if server is None:
        raise RuntimeError(
            "Cannot connect floating IP to the server"
            " - server doesn't exist for node: {0}"
            .format(ctx.instance.id))
    floating_ip_client = get_floating_ip_client(ctx)
    ip = ctx.target.instance.runtime_properties['floating_ip_address']
    floating_ip = floating_ip_client.get_by_ip(ip)
    if floating_ip is None:
        raise RuntimeError(
            "Cannot connect floating IP to the server"
            " - floating IP doesn't exist: {0}"
            .format(ip))
    ctx.logger.info("Connect floating IP method called:"
                    " server - {0}, IP - {1}"
                    .format(server.name, floating_ip.ip))
    server_client.connect_floating_ip(server, floating_ip)


@operation
@with_server_client
def disconnect_floating_ip(ctx, server_client, **kwargs):
    floating_ip_client = get_floating_ip_client(ctx)
    ip = ctx.related.runtime_properties['floating_ip_address']
    floating_ip = floating_ip_client.get_by_ip(ip)
    if floating_ip is None:
        raise RuntimeError(
            "Cannot connect floating IP to the server"
            " - floating IP doesn't exist: {0}"
            .format(ip))
    server_client.disconnect_floating_ip(floating_ip)


def get_server_by_context(server_client, node_instance):
    if LIBCLOUD_SERVER_ID_PROPERTY in node_instance.runtime_properties:
         return server_client.get_by_id(
            node_instance.runtime_properties[LIBCLOUD_SERVER_ID_PROPERTY])
    return server_client.get_by_name(node_instance.id)
