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


from cloudify.decorators import operation
from libcloud_plugin_common import with_floating_ip_client
from cloudify.exceptions import NonRecoverableError


@operation
@with_floating_ip_client
def create(ctx, floating_ip_client, **kwargs):
    # Already acquired?
    if ctx.instance.runtime_properties.get('ip_address'):
        ctx.logger.debug("Using already allocated Floating IP {0}".format(
            ctx.instance.runtime_properties['ip_address']))
        return

    floatingip = {
        # No defaults
    }
    floatingip.update(ctx.node.properties['floatingip'])

    # Sugar: ip -> (copy as is) -> floating_ip_address
    if 'ip' in floatingip:
        floatingip['ip_address'] = floatingip['ip']
        del floatingip['ip']

    if 'ip_address' in floatingip:
        ctx.instance.runtime_properties['ip_address'] = \
            floatingip['ip_address']
        # Not acquired here
        ctx.instance.runtime_properties['enable_deletion'] = False
        return

    fip = floating_ip_client.create()
    ctx.instance.runtime_properties['external_id'] = fip.ip
    ctx.instance.runtime_properties['floating_ip_address'] = fip.ip
    # Acquired here -> OK to delete
    ctx.instance.runtime_properties['enable_deletion'] = True
    ctx.logger.info(
        "Allocated floating IP {0}".format(fip.ip))


@operation
@with_floating_ip_client
def delete(ctx, floating_ip_client, **kwargs):
    do_delete = bool(ctx.instance.runtime_properties.get('enable_deletion'))
    op = ['Not deleting', 'Deleting'][do_delete]
    ctx.logger.debug("{0} floating IP {1}".format(
        op, ctx.instance.runtime_properties['floating_ip_address']))
    if do_delete:
        ip_address = ctx.instance.runtime_properties['external_id']
        ip = floating_ip_client.get_by_ip(ip_address)
        if not ip:
            raise NonRecoverableError('Floating IP can\'t be found for IP: {}'
                                      .format(ip_address))
        floating_ip_client.delete(ip)
