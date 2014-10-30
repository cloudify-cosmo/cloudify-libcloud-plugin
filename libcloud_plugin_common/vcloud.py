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


import time
from cloudify.exceptions import NonRecoverableError
from libcloud.compute.types import NodeState
from libcloud_plugin_common import (LibcloudServerClient,
                                    LibcloudFloatingIPClient,
                                    LibcloudSecurityGroupClient,
                                    transform_resource_name,
                                    LibcloudProviderContext)
import libcloud.security

class VCloudLibcloudServerClient(LibcloudServerClient):

    def get_by_name(self, server_name):
	libcloud.security.VERIFY_SSL_CERT = False
        nodes = self.driver.list_nodes()
        for node in nodes:
            if node.name == server_name:
                return node

    def get_by_id(self, server_id):
        nodes = self.driver.list_nodes()
        for node in nodes:
            if node.id == server_id:
                return node

    def start_server(self, server):
        self.driver.ex_power_on_node(server)

    def stop_server(self, server):
        self.driver.ex_shutdown_node(server)

    def delete_server(self, server):
        self.driver.destroy_node(server)

    def wait_for_server_to_be_deleted(self, server, timeout, sleep_time):
        self._wait_for_server_to_obtaine_state(server,
                                               timeout,
                                               sleep_time,
                                               NodeState.TERMINATED)

    def wait_for_server_to_be_running(self, server, timeout, sleep_time):
        self._wait_for_server_to_obtaine_state(server,
                                               timeout,
                                               sleep_time,
                                               NodeState.RUNNING)

    def _wait_for_server_to_obtaine_state(self,
                                          server,
                                          timeout,
                                          sleep_time,
                                          state):
        while server.state is not state:
            timeout -= 5
            if timeout <= 0:
                raise RuntimeError('Server {} has not been deleted.'
                                   ' Waited for {} seconds'
                                   .format(server.id, timeout))
            time.sleep(sleep_time)
            server = self.get_by_id(server.id)

#    def connect_floating_ip(self, server, ip):
 #       self.driver.ex_associate_address_with_node(server, ip)

 #   def disconnect_floating_ip(self, ip):
  #      self.driver.ex_disassociate_address(ip)

    def get_image_by_name(self, image_name):
        images = self.driver.list_images()
        if images:
            for image in images:
                if image.name == image_name:
                    return image

    def get_size_by_ram(self, ram):
        sizes = self.driver.list_sizes()
        if sizes:
            for item in sizes:
                if item.ram == ram:
                    return item

    def is_server_active(self, server):
        return server.state == NodeState.RUNNING

    def create(
            self,
            name,
            ctx,
            server_context,
            provider_context):

        def rename(name):
            return transform_resource_name(name, ctx)

        if 'image_name' in server_context:
            image = self.get_image_by_name(server_context['image_name'])
        else:
            raise NonRecoverableError("Image is a required parameter")
        if 'ram' in server_context:
            size = self.get_size_by_ram(int(server_context['ram']))
        else:
            raise NonRecoverableError("ram is a required parameter")
        if 'vcpus' in server_context:
            vcpus = int(server_context['vcpus'])
        else:
            vcpus = 1
        if 'management_network_name' in server_context:
            management_network_name = server_context['management_network_name']
        else:
            raise NonRecoverableError("management_network_name is a required parameter")

# For now it is disabled
#        security_groups = map(rename,
#                              server_context.get('security_groups', []))
#        if provider_context.agents_security_group:
#            asg = provider_context.agents_security_group['name']
#            if asg not in security_groups:
#                security_groups.append(asg)

#        if 'key_name' in server_context:
#            key_name = rename(server_context['key_name'])
#        else:
#            if provider_context.agents_keypair:
#                key_name = provider_context.agents_keypair['name']
#            else:
#                raise NonRecoverableError("Key is a required parameter")

#        ctx.logger.error(security_groups)

        node = self.driver.create_node(name=name,
                                       image=image,
                                       ex_vm_memory=size.ram,
                                       ex_vm_cpu=vcpus,
                                       ex_network=management_network_name,
                                       ex_vm_fench='bridged' )
        return node


class VCloudLibcloudFloatingIPClient(LibcloudFloatingIPClient):

    def delete(self, ip):
        self.driver.ex_disassociate_address(ip)
        self.driver.ex_release_address(ip)

    def create(self, **kwargs):
        return self.driver.ex_allocate_address()

    def get_by_ip(self, ip):
        addresses = self.driver.ex_describe_all_addresses()
        for address in addresses:
            if address.ip == ip:
                return address


class VCloudLibcloudSecurityGroupClient(LibcloudSecurityGroupClient):

    def create(self, security_group):
        sg = self.driver.ex_create_security_group(
            security_group['name'], security_group['description'])
        return sg

    def delete(self, id):
        self.driver.ex_delete_security_group_by_id(id)

    def get_list_by_name(self, name):
        try:
            return self.driver.ex_get_security_groups(group_names=[name])
        except:
            return None

    def get_description(self, sg):
        return sg.extra['description']

    def get_id(self, sg):
        if isinstance(sg, dict):
            return sg['group_id']
        else:
            return sg.id

    def get_rules(self, sg):
        result = []
        for rule in sg.ingress_rules:
            sgr = {
                'direction': 'ingress',
                'port_range_max': rule['to_port'],
                'port_range_min': rule['from_port'],
                'protocol': rule['protocol'],
                'remote_group_id': None,
                'remote_ip_prefix': '0.0.0.0/0',
            }
            if 'group_pairs' in rule:
                if len(rule['group_pairs']) > 0:
                    sgr['remote_group_id'] = rule['group_pairs'][0]['group_id']
                    del sgr['remote_ip_prefix']
            if 'cidr_ips' in rule:
                if len(rule['cidr_ips']) > 0:
                    sgr['remote_ip_prefix'] = rule['cidr_ips'][0]
                    del sgr['remote_group_id']
            result.append(sgr)
        return result

    def create_security_group_rule(self, rule):
        if 'remote_ip_prefix' in rule:
            self.driver.ex_authorize_security_group_ingress(
                rule['security_group_id'],
                rule['port_range_min'],
                rule['port_range_max'],
                cidr_ips=[rule['remote_ip_prefix']])
        elif 'group_id' in rule:
            self.driver.ex_authorize_security_group_ingress(
                rule['security_group_id'],
                rule['port_range_min'],
                rule['port_range_max'],
                group_pairs=[{'group_id': rule['remote_group_id']}])


class VCloudLibcloudProviderContext(LibcloudProviderContext):

    @property
    def agents_security_group(self):
        return self._resources.get('agents_security_group')

    @property
    def agents_keypair(self):
        return self._resources.get('agents_keypair')
