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

import re
import copy
import json

from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError
from libcloud_plugin_common import (with_security_group_client,
                                    transform_resource_name)


NODE_NAME_RE = re.compile('^(.*)_.*$')  # Anything before last underscore


@operation
@with_security_group_client
def create(ctx, security_group_client, **kwargs):
    security_group = {
        'description': None,
        'name': ctx.node_id,
    }

    security_group.update(ctx.properties['security_group'])
    transform_resource_name(security_group, ctx)

    existing_sg = _find_existing_sg(ctx,
                                    security_group_client,
                                    security_group['name'])
    if existing_sg:
        ctx.logger.error('!!!!' + security_group_client.get_description(existing_sg) + '!!!!!' + security_group['description'] + '!!!!!')
        if security_group_client.get_description(existing_sg)\
                != security_group['description']:
            raise NonRecoverableError("Descriptions of existing security group"
                                      " and the security group to be created "
                                      "do not match while the names do match."
                                      " Security group name: {0}".format(
                                          security_group['name']))

    rules_to_apply = ctx.properties['rules']

    security_group_rules = []
    for rule in rules_to_apply:
        ctx.logger.debug(
            "security_group.create() rule before transformations: {0}".format(
                rule))
        sgr = {
            'direction': 'ingress',
            'port_range_max': rule.get('port', 65535),
            'port_range_min': rule.get('port', 1),
            'protocol': 'tcp',
            'remote_group_id': None,
            'remote_ip_prefix': '0.0.0.0/0',
        }
        sgr.update(rule)

        if 'port' in sgr:
            del sgr['port']

        if ('remote_group_node' in sgr) and sgr['remote_group_node']:
            _, remote_group_node = _capabilities_of_node_named(
                sgr['remote_group_node'], ctx)
            sgr['remote_ip_prefix'] = remote_group_node.ip
            del sgr['remote_group_node']
            del sgr['remote_ip_prefix']

        if ('remote_group_name' in sgr) and sgr['remote_group_name']:
            sgroups = security_group_client\
                .get_list_by_name(sgr['remote_group_name'])
            sg_count = len(sgroups)
            if sg_count > 1:
                raise NonRecoverableError('More than one security group found'
                                          ' for remote_group_name: {0}'
                                          .format(sgr['remote_group_name']))
            elif sg_count < 1:
                raise NonRecoverableError('None security group found'
                                          ' for remote_group_name: {0}'
                                          .format(sgr['remote_group_name']))
            sgr['remote_group_id'] = security_group_client.get_id(sgroups[0])
            del sgr['remote_group_name']
            del sgr['remote_ip_prefix']

        ctx.logger.debug(
            "security_group.create() rule after transformations: {0}".format(
                sgr))
        security_group_rules.append(sgr)

    if existing_sg:
        r1 = security_group_client.get_rules(existing_sg)
        r2 = security_group_rules
        if _sg_rules_are_equal(r1, r2):
            ctx.logger.info("Using existing security group named '{0}' with "
                            "id {1}".format(
                                security_group['name'],
                                existing_sg['id']))
            ctx.runtime_properties['external_id'] =\
                security_group_client.get_id(existing_sg)
            return
        else:
            raise RulesMismatchError("Rules of existing security group"
                                     " and the security group to be created "
                                     "or used do not match while the names "
                                     "do match. Security group name: '{0}'. "
                                     "Existing rules: {1}. "
                                     "Requested/expected rules: {2} "
                                     "".format(
                                         security_group['name'],
                                         r1,
                                         r2))

    sg = security_group_client.create(security_group)
    sg_id = security_group_client.get_id(sg)

    for sgr in security_group_rules:
        sgr['security_group_id'] = sg_id
        security_group_client.create_security_group_rule(sgr)
    ctx.runtime_properties['external_id'] = sg_id


@operation
@with_security_group_client
def delete(ctx, security_group_client, **kwargs):
    sg_id = ctx.runtime_properties['external_id']
    try:
        security_group_client.delete(sg_id)
    except Exception, e:
        raise NonRecoverableError("Security group client error: " + str(e))


def _find_existing_sg(ctx, security_group_client, name):
    existing_sgs = security_group_client.get_list_by_name(name)
    if existing_sgs:
        existing_sgs = list(existing_sgs)
        if len(existing_sgs) > 1:
            raise NonRecoverableError("Multiple security groups with name '{0}' "
                                      "already exist while trying to create "
                                      "security group with same name"
                                      .format(name))

        ctx.logger.info("Found existing security group "
                        "with name '{0}'".format(name))
        return existing_sgs[0]

    return None


def _capabilities_of_node_named(node_name, ctx):
    result = None
    caps = ctx.capabilities.get_all()
    for node_id in caps:
        match = NODE_NAME_RE.match(node_id)
        if match:
            candidate_node_name = match.group(1)
            if candidate_node_name == node_name:
                if result:
                    raise NonRecoverableError(
                        "More than one node named '{0}' "
                        "in capabilities".format(node_name))
                result = (node_id, caps[node_id])
    if not result:
        raise NonRecoverableError(
            "Could not find node named '{0}' "
            "in capabilities".format(node_name))
    return result


def _sg_rules_are_equal(r1, r2):
    s1 = map(_serialize_sg_rule_for_comparison, r1)
    s2 = map(_serialize_sg_rule_for_comparison, r2)
    return set(s1) == set(s2)


def _serialize_sg_rule_for_comparison(security_group_rule):
    r = copy.deepcopy(security_group_rule)
    for excluded_field in ('id', 'security_group_id', 'tenant_id'):
        if excluded_field in r:
            del r[excluded_field]
    return json.dumps(r, sort_keys=True)


class RulesMismatchError(NonRecoverableError):
    pass
