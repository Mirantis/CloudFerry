# Copyright (c) 2016 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the License);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an AS IS BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and#
# limitations under the License.
import cStringIO
import hashlib
import inspect
import logging
import os
import random
import threading
import time
import uuid

import cloudferry
from cloudferry import config
from cloudferry import discover
from cloudferry import model
from cloudferry.model import compute
from cloudferry.model import identity
from cloudferry.model import image
from cloudferry.model import network
from cloudferry.model import storage
from cloudferry.lib.os import clients
from cloudferry.lib.os.migrate import base
from cloudferry.lib.utils import remote

from cinderclient import exceptions as cinder_exceptions
from novaclient import exceptions as nova_exceptions
from keystoneclient import exceptions as keystone_exceptions
from glanceclient import exc as glance_exceptions
from neutronclient.common import exceptions as neutron_exceptions
import netaddr
import paramiko

LOG = logging.getLogger(__name__)
LOCK = threading.Lock()
IMAGE_FILENAME = 'alpine_vol_tx.qcow2'
RSA1024_KEY = paramiko.RSAKey.generate(1024)

_image_md5 = None
_ip_counter = 2  # 0 is for network and 1 is for compute node
_used_ports = {}


class BaseSymmetricTask(base.MigrationTask):
    def __init__(self, cfg, migration, obj, location, **kwargs):
        self.location = location
        super(BaseSymmetricTask, self).__init__(cfg, migration, obj,
                                                name_suffix=location, **kwargs)

    @property
    def cloud(self):
        return self.config.clouds[getattr(self.migration, self.location)]


class BaseSymmetricSingletonTask(BaseSymmetricTask,
                                 base.SingletonMigrationTask):
    def __init__(self, cfg, migration, obj, location, **kwargs):
        super(BaseSymmetricSingletonTask, self).__init__(
            cfg, migration, obj, location, **kwargs)
        self.destructor = None

    def get_singleton_key(self, *args, **kwargs):
        return self.location,

    def rollback(self, *args, **kwargs):
        if self.destructor is not None:
            self.destructor.run(self.config, self.migration)

    def save_internal_state(self):
        serialized_destructor = None
        if self.destructor is not None:
            serialized_destructor = self.destructor.dump()
        return {'serialized_destructor': serialized_destructor}

    def restore_internal_state(self, internal_state):
        serialized_destructor = internal_state['serialized_destructor']
        if serialized_destructor is None:
            self.destructor = None
        else:
            self.destructor = base.Destructor.load(serialized_destructor)


class CreateVolume(base.MigrationTask):
    default_provides = ['dst_object']

    def migrate(self, source_obj, *args, **kwargs):
        dst_tenant_id = _get_object_tenant_id(self.dst_cloud, source_obj)
        volume_client = clients.volume_client(self.dst_cloud,
                                              _scope(dst_tenant_id))
        ovr_source_obj = self.override(source_obj)
        zone = ovr_source_obj.availability_zone
        vol = clients.retry(volume_client.volumes.create,
                            size=ovr_source_obj.size,
                            display_name=ovr_source_obj.name,
                            display_description=ovr_source_obj.description,
                            volume_type=ovr_source_obj.volume_type,
                            availability_zone=zone,
                            metadata=ovr_source_obj.metadata)
        try:
            self.created_object = clients.wait_for(
                _object_status_is, volume_client, 'volumes', vol.id,
                'available')
        except clients.Timeout:
            self._delete_volume(vol)
            raise base.AbortMigration('Volume didn\'t become active')
        result = self.load_from_cloud(
            storage.Volume, self.dst_cloud, self.created_object)
        return dict(dst_object=result)

    def rollback(self, *args, **kwargs):
        super(CreateVolume, self).rollback(*args, **kwargs)
        if self.created_object is not None:
            self._delete_volume(self.created_object)
            self.created_object = None

    def _delete_volume(self, vol):
        tenant_id = getattr(vol, 'os-vol-tenant-attr:tenant_id')
        volume_client = clients.volume_client(self.dst_cloud,
                                              _scope(tenant_id))
        try:
            volume = clients.retry(
                volume_client.volumes.get, vol.id,
                expected_exceptions=[cinder_exceptions.NotFound])
            if volume.status not in ('available', 'in-use', 'error',
                                     'error_restoring'):
                clients.retry(
                    volume_client.volumes.reset_state, volume, 'error',
                    expected_exceptions=[cinder_exceptions.NotFound])
            clients.retry(volume_client.volumes.delete, volume,
                          expected_exceptions=[cinder_exceptions.NotFound])
        except cinder_exceptions.NotFound:
            LOG.warning('Can not delete cinder volume: already deleted')

    def save_internal_state(self):
        tenant_id = volume_id = None
        if self.created_object is not None:
            volume = self.created_object
            volume_id = volume.id
            tenant_id = getattr(volume, 'os-vol-tenant-attr:tenant_id')

        return {
            'tenant_id': tenant_id,
            'volume_id': volume_id,
        }

    def restore_internal_state(self, internal_state):
        tenant_id = internal_state['tenant_id']
        volume_id = internal_state['volume_id']
        self.created_object = None
        if tenant_id is not None and volume_id is not None:
            volume_client = clients.volume_client(self.dst_cloud,
                                                  _scope(tenant_id))
            try:
                self.created_object = clients.retry(
                    volume_client.volumes.get, volume_id,
                    expected_exceptions=[cinder_exceptions.NotFound])
            except cinder_exceptions.NotFound:
                LOG.warning('Failed to find volume with id %s when restoring '
                            'task state', volume_id)


class BootTransferVm(BaseSymmetricTask):
    def __init__(self, cfg, migration, obj, location):
        self.var_name = location + '_vm'
        self.image_var_name = location + '_image_id'
        self.flavor_var_name = location + '_flavor'
        self.net_var_name = location + '_net'
        super(BootTransferVm, self).__init__(cfg, migration, obj, location,
                                             requires=[self.image_var_name,
                                                       self.flavor_var_name,
                                                       self.net_var_name],
                                             provides=[self.var_name])

    def migrate(self, source_obj, *args, **kwargs):
        int_ip_address = _allocate_ip_address(self.cloud)
        nova_client = clients.compute_client(self.cloud)
        self.created_object = nova_client.servers.create(
            image=kwargs[self.image_var_name],
            flavor=kwargs[self.flavor_var_name].flavor_id,
            name='trans_vol_{}'.format(source_obj.object_id.id),
            config_drive=True,
            nics=[{'net-id': kwargs[self.net_var_name].object_id.id}],
            meta=dict(cidr=str(int_ip_address),
                      internal_address=str(int_ip_address.ip),
                      access_key=RSA1024_KEY.get_base64()))
        try:
            self.created_object = clients.wait_for(
                _object_status_is, nova_client, 'servers',
                self.created_object.id, 'active')
        except clients.Timeout:
            self._delete_vm()
            raise base.AbortMigration(
                'Timeout waiting for VM %s to start on %s',
                self.created_object.id, self.location)
        result = self.load_from_cloud(
            compute.Server, self.cloud, self.created_object)
        return {self.var_name: result}

    def rollback(self, *args, **kwargs):
        super(BootTransferVm, self).rollback(*args, **kwargs)
        if self.created_object is not None:
            self._delete_vm()

    def _delete_vm(self):
        _delete_vm(self.cloud, self.created_object.id)
        self.created_object = None

    def save_internal_state(self):
        vm_id = None
        if self.created_object is not None:
            vm_id = self.created_object.id

        return {
            'vm_id': vm_id,
        }

    def restore_internal_state(self, internal_state):
        vm_id = internal_state['vm_id']
        self.created_object = None
        if vm_id is not None:
            compute_client = clients.compute_client(self.cloud)
            try:
                self.created_object = clients.retry(
                    compute_client.servers.get, vm_id,
                    expected_exceptions=[nova_exceptions.NotFound])
            except nova_exceptions.NotFound:
                LOG.warning('Failed to find VM with id %s when restoring '
                            'task state', vm_id)


class AttachNodeLocalInterface(BaseSymmetricTask):
    def __init__(self, cfg, migration, obj, location):
        self.var_name = location + '_vm'
        super(AttachNodeLocalInterface, self).__init__(
            cfg, migration, obj, location, requires=[self.var_name])

    def migrate(self, **kwargs):
        target_vm = kwargs.get(self.var_name)
        with remote.RemoteExecutor(self.cloud,
                                   target_vm.compute_node) as rexec:
            br_name = 'cn_local'
            rexec.sudo('brctl addbr {bridge} || true', bridge=br_name)
            rexec.sudo('ip addr add {cidr} dev {bridge} || true',
                       cidr=_first_unused_address(self.cloud), bridge=br_name)
            rexec.sudo('ip link set dev {bridge} up', bridge=br_name)
            rexec.sudo('virsh attach-interface {instance} --type bridge '
                       '--source {bridge} --mac {mac_address} '
                       '--model virtio',
                       instance=target_vm.instance_name, bridge=br_name,
                       mac_address=_random_mac())


class TransferVolumeData(base.MigrationTask):
    def __init__(self, *args, **kwargs):
        super(TransferVolumeData, self).__init__(*args, **kwargs)
        self.session_name = None
        self.started_on_src_host = False
        self.started_on_dst_host = False

    def migrate(self, source_obj, source_vm, destination_vm, *args, **kwargs):
        self.session_name = 'vol_{}_{}'.format(
            source_obj.object_id.cloud, source_obj.object_id.id)
        port = _allocate_port(source_vm.hypervisor_hostname, self.src_cloud)
        src_ip = source_vm.metadata['internal_address']
        dst_ip = destination_vm.metadata['internal_address']
        listen_ip = _first_unused_address(self.src_cloud).ip
        dst_private_key = self.dst_cloud.ssh_settings.private_key
        agent = remote.SSHAgent()

        try:
            if dst_private_key is not None:
                agent.start()
                agent.add_key(dst_private_key)

            with remote.RemoteExecutor(
                    self.dst_cloud, destination_vm.compute_node) as dst_re:
                _wait_ip_accessible(self.dst_cloud, dst_re, dst_ip)
                key_path = _deploy_pkey(dst_re)
                dst_re.run('screen -S {session} -d -m '
                           'ssh -o UserKnownHostsFile=/dev/null '
                           '-o StrictHostKeyChecking=no -i {key_path} '
                           'root@{dst_ip} /bin/sh -c '
                           '"\'nc -l {dst_ip} 11111 | '
                           '/usr/local/bin/zstd -d | '
                           'dd of=/dev/vdb bs=512k\'"; sleep 1',
                           session=self.session_name, key_path=key_path,
                           dst_ip=dst_ip)
                self.started_on_dst_host = True

            with remote.RemoteExecutor(self.src_cloud,
                                       source_vm.compute_node) as src_re:
                _wait_ip_accessible(self.src_cloud, src_re, src_ip)
                key_path = _deploy_pkey(src_re)
                # Port forwarding to remote machine
                src_re.run('screen -S {session} -d -m ssh -N '
                           '-o UserKnownHostsFile=/dev/null '
                           '-o StrictHostKeyChecking=no '
                           '-L {listen_ip}:{listen_port}:{forward_ip}:11111 '
                           '{dst_user}@{dst_address}; sleep 1',
                           agent=agent, session=self.session_name,
                           listen_ip=listen_ip, listen_port=port,
                           forward_ip=dst_ip, dst_address=dst_re.hostname,
                           dst_user=self.dst_cloud.ssh_settings.username)
                self.started_on_src_host = True

                LOG.info('Starting to transfer %dGb volume %s',
                         source_obj.size, source_obj.object_id)
                data_transfer_start = time.time()
                src_re.run('ssh -t -o UserKnownHostsFile=/dev/null '
                           '-o StrictHostKeyChecking=no -i {key_path} '
                           'root@{src_ip} /bin/sh -c '
                           '"\'dd if=/dev/vdb bs=512k | pv -r -i 30 | '
                           '/usr/local/bin/zstd | '
                           'nc -w 5 {listen_ip} {listen_port}\'"',
                           session=self.session_name, key_path=key_path,
                           listen_port=port, listen_ip=listen_ip,
                           src_ip=src_ip)
                data_transfer_dur = time.time() - data_transfer_start

                LOG.info('Transferred %dGb volume in %.1f seconds '
                         '(avg. speed: %.2fMb/s)', source_obj.size,
                         data_transfer_dur,
                         source_obj.size * 1024 / data_transfer_dur)
        finally:
            self._cleanup(source_vm, destination_vm)
            agent.terminate()

    def rollback(self, source_vm, destination_vm, *args, **kwargs):
        super(TransferVolumeData, self).rollback(*args, **kwargs)
        self._cleanup(source_vm, destination_vm)

    def _close_screen_session(self, rexec):
        rexec.run('screen -S {session} -x -X quit || true',
                  session=self.session_name)

    def _cleanup(self, source_vm, destination_vm):
        if self.started_on_src_host:
            with remote.RemoteExecutor(self.src_cloud,
                                       source_vm.compute_node) as rexec:
                self._close_screen_session(rexec)
        if self.started_on_dst_host:
            with remote.RemoteExecutor(self.dst_cloud,
                                       destination_vm.compute_node) as rexec:
                self._close_screen_session(rexec)


class CleanupVms(base.MigrationTask):
    def migrate(self, source_vm, destination_vm, *args, **kwargs):
        self._delete_vm_obj(source_vm)
        self._delete_vm_obj(destination_vm)

    def _delete_vm_obj(self, vm):
        cloud = self.config.clouds[vm.object_id.cloud]
        _delete_vm(cloud, vm.object_id.id)


class BaseAttachmentTask(base.MigrationTask):
    def _attach_volume(self, cloud, volume, vm_id):
        volume_id = volume.object_id.id
        nova_client = clients.compute_client(cloud)
        cinder_client = clients.volume_client(
            cloud, _scope(volume.tenant.object_id.id))
        if _object_status_is(cinder_client, 'volumes', volume_id, 'available'):
            nova_client.volumes.create_server_volume(vm_id, volume_id,
                                                     '/dev/vdb')
            try:
                clients.wait_for(
                    _object_status_is, cinder_client, 'volumes',
                    volume_id, 'in-use')
            except clients.Timeout:
                raise base.AbortMigration(
                    'Volume %s in cloud %s couldn\'t attach',
                    volume_id, cloud.name)
        else:
            raise base.AbortMigration(
                'Volume %s in cloud %s is not available for attachment',
                volume_id, cloud.name)

    def _detach_volume(self, cloud, volume, vm_id, abort_migration=False):
        volume_id = volume.object_id.id
        nova_client = clients.compute_client(cloud)
        cinder_client = clients.volume_client(
            cloud, _scope(volume.tenant.object_id.id))
        if _object_is_deleted(cinder_client, 'volumes', volume_id,
                              cinder_exceptions.NotFound):
            return
        if _object_status_is(cinder_client, 'volumes', volume_id, 'in-use'):
            nova_client.volumes.delete_server_volume(vm_id, volume_id)
            try:
                clients.wait_for(_object_status_is, cinder_client, 'volumes',
                                 volume_id, 'available')
            except clients.Timeout:
                if abort_migration:
                    raise base.AbortMigration(
                        'Volume %s in cloud %s couldn\'t attach',
                        volume_id, cloud.name)


class AttachSourceVolume(BaseAttachmentTask):
    def migrate(self, source_obj, source_vm):
        self._attach_volume(self.src_cloud, source_obj, source_vm.object_id.id)

    def rollback(self, source_obj, source_vm, **kwargs):
        self._detach_volume(self.src_cloud, source_obj, source_vm.object_id.id)


class AttachDestinationVolume(BaseAttachmentTask):
    def migrate(self, dst_object, destination_vm):
        self._attach_volume(self.dst_cloud, dst_object,
                            destination_vm.object_id.id)

    def rollback(self, dst_object, destination_vm, **kwargs):
        self._detach_volume(self.dst_cloud, dst_object,
                            destination_vm.object_id.id)


class DetachSourceVolume(BaseAttachmentTask):
    def migrate(self, source_obj, source_vm):
        self._detach_volume(self.src_cloud, source_obj, source_vm.object_id.id,
                            abort_migration=True)


class DetachDestinationVolume(BaseAttachmentTask):
    def migrate(self, dst_object, destination_vm):
        self._detach_volume(self.dst_cloud, dst_object,
                            destination_vm.object_id.id, abort_migration=True)


class DetachMigratedVolume(BaseAttachmentTask):
    default_provides = ['attached_vm_id']

    def __init__(self, cfg, migration, obj):
        super(DetachMigratedVolume, self).__init__(cfg, migration, obj)
        self.detached_vm_id = None

    def migrate(self, source_obj, *args, **kwargs):
        cinder_client = clients.volume_client(
            self.src_cloud, _scope(source_obj.tenant.object_id.id))
        raw_volume = clients.retry(cinder_client.volumes.get,
                                   source_obj.object_id.id)
        if raw_volume.attachments:
            nova_client = clients.compute_client(self.src_cloud)
            assert len(raw_volume.attachments) == 1
            detached_vm_id = raw_volume.attachments[0]['server_id']
            shutoff_vm(nova_client, detached_vm_id)
            self._detach_volume(self.src_cloud, source_obj, detached_vm_id,
                                abort_migration=True)
            self.detached_vm_id = detached_vm_id
        return dict(attached_vm_id=self.detached_vm_id)

    def rollback(self, source_obj, *args, **kwargs):
        if self.detached_vm_id is not None:
            self._attach_volume(self.src_cloud, source_obj,
                                self.detached_vm_id)


class ReattachMigratedVolume(BaseAttachmentTask):
    def migrate(self, source_obj, attached_vm_id, *args, **kwargs):
        if attached_vm_id is None:
            return None
        self._attach_volume(self.src_cloud, source_obj, attached_vm_id)


class ImageDestructor(base.Destructor):
    def __init__(self, location, image_id):
        self.location = location
        self.image_id = image_id

    def get_signature(self):
        return self.location, self.image_id

    def run(self, cfg, migration):
        cloud = cfg.clouds[getattr(migration, self.location)]
        image_client = clients.image_client(cloud)
        try:
            with model.Session() as session:
                object_id = model.ObjectId(self.image_id, cloud.name)
                session.delete(image.Image, object_id=object_id)
            clients.retry(image_client.images.delete, self.image_id,
                          expected_exceptions=[glance_exceptions.NotFound])
        except glance_exceptions.NotFound:
            pass


class FindOrUploadImage(BaseSymmetricSingletonTask):
    def __init__(self, cfg, migration, obj, location):
        self.var_name = location + '_image_id'
        super(FindOrUploadImage, self).__init__(
            cfg, migration, obj, location, provides=[self.var_name])

    def migrate(self, *args, **kwargs):
        with model.Session() as session:
            image_id = self._find_supported_cirros_image(session)
            if image_id is None:
                try:
                    img = self._upload_cirros_image(session)
                except clients.Timeout:
                    raise base.AbortMigration(
                        'Failed to upload transfer VM image')
                image_obj = self.load_from_cloud(image.Image, self.cloud, img)
                session.store(image_obj)
                image_id = img.id
                self.destructor = ImageDestructor(self.location, image_id)

            return {self.var_name: image_id,
                    self.destructor_var: self.destructor}

    def _find_supported_cirros_image(self, session):
        image_client = clients.image_client(self.cloud)
        for img in session.list(image.Image, self.cloud):
            if img.checksum.lower() == _get_image_md5():
                # Test if image is good
                image_id = img.object_id.id
                try:
                    next(image_client.images.data(image_id))
                except Exception:
                    LOG.debug('Failed to download part of image %s from %s',
                              image_id, self.location)
                    continue
                return image_id
        return None

    def _upload_cirros_image(self, session):
        image_client = clients.image_client(self.cloud)
        with open(_get_image_location(), 'r') as f:
            img = image_client.images.create(
                data=f, name=IMAGE_FILENAME,
                container_format='bare',
                disk_format='qcow2',
                is_public=False, protected=False,
                owner=_get_admin_tenant_id(self.cloud, session))
            return clients.wait_for(_object_status_is, image_client, 'images',
                                    img.id, 'active')


class FlavorDestructor(base.Destructor):
    def __init__(self, location, flavor_id, object_id):
        self.location = location
        self.flavor_id = flavor_id
        self.object_id = object_id

    def get_signature(self):
        return self.object_id

    def run(self, cfg, migration):
        cloud = cfg.clouds[getattr(migration, self.location)]
        nova_client = clients.compute_client(cloud)
        try:
            with model.Session() as session:
                session.delete(compute.Flavor, object_id=self.object_id)
            clients.retry(nova_client.flavors.delete, self.flavor_id,
                          expected_exceptions=[nova_exceptions.NotFound])
        except nova_exceptions.NotFound:
            pass


class FindOrCreateFlavor(BaseSymmetricSingletonTask):
    def __init__(self, cfg, migration, obj, location):
        self.var_name = location + '_flavor'
        super(FindOrCreateFlavor, self).__init__(
            cfg, migration, obj, location,
            provides=[self.var_name])

    def migrate(self, *args, **kwargs):
        with model.Session() as session:
            flavor = self._find_existing_flavor(session)
            if flavor is None:
                flavor = self._create_flavor()
                self.destructor = FlavorDestructor(
                    self.location, flavor.flavor_id, flavor.object_id)
            return {self.var_name: flavor,
                    self.destructor_var: self.destructor}

    def _find_existing_flavor(self, session):
        for flavor in session.list(compute.Flavor, self.cloud):
            if not flavor.is_disabled \
                    and not flavor.is_deleted \
                    and flavor.vcpus == 1 \
                    and 48 <= flavor.memory_mb <= 64 \
                    and flavor.root_gb == 0 \
                    and flavor.ephemeral_gb == 0 \
                    and flavor.swap_mb == 0:
                return flavor

    def _create_flavor(self):
        nova_client = clients.compute_client(self.cloud)
        flavor_id = str(uuid.uuid4())
        clients.retry(nova_client.flavors.create, 'tmp.vol_tx', 64, 1, 0,
                      flavorid=flavor_id, is_public=False)
        flavor_discoverer = discover.get_discoverer(self.config, self.cloud,
                                                    compute.Flavor)
        flavor = flavor_discoverer.discover_by_flavor_id(flavor_id)
        return flavor


class NetworkDestructor(base.Destructor):
    def __init__(self, location, network_id, subnet_id):
        self.location = location
        self.network_id = network_id
        self.subnet_id = subnet_id

    def get_signature(self):
        return self.location, self.network_id

    def run(self, cfg, migration):
        cloud = cfg.clouds[getattr(migration, self.location)]
        network_client = clients.network_client(cloud)
        try:
            with model.Session() as session:
                net_obj_id = model.ObjectId(self.network_id, cloud.name)
                subnet_obj_id = model.ObjectId(self.subnet_id, cloud.name)
                session.delete(network.Network, object_id=net_obj_id)
                session.delete(network.Subnet, object_id=subnet_obj_id)
            clients.retry(network_client.delete_network, self.network_id,
                          expected_exceptions=[neutron_exceptions.NotFound])
        except neutron_exceptions.NotFound:
            pass


class FindOrCreateNetwork(BaseSymmetricSingletonTask):
    def __init__(self, cfg, migration, obj, location):
        self.var_name = location + '_net'
        super(FindOrCreateNetwork, self).__init__(
            cfg, migration, obj, location,
            provides=[self.var_name])

    def migrate(self, *args, **kwargs):
        with model.Session() as session:
            net = self._find_existing_network(session)
            if net is None:
                net, net_id, subnet_id = self._create_net(session)
                self.destructor = NetworkDestructor(
                    self.location, net_id, subnet_id)
            return {self.var_name: net, self.destructor_var: self.destructor}

    def _find_existing_network(self, session):
        for net in session.list(network.Network, self.cloud):
            if net.name == 'tmp_vol_tx' and len(net.subnets) == 1:
                return net
        return None

    def _create_net(self, session):
        network_client = clients.network_client(self.cloud)
        raw_net = network_client.create_network({
            'network': {
                'name': 'tmp_vol_tx',
                'shared': False,
            },
        })
        raw_subnet = network_client.create_subnet({
            'subnet': {
                'cidr': '128.0.0.0/1',
                'ip_version': 4,
                'gateway_ip': None,
                'network_id': raw_net['network']['id']
            },
        })
        net = self.load_from_cloud(network.Network, self.cloud,
                                   raw_net['network'])
        session.store(net)
        subnet = self.load_from_cloud(network.Subnet, self.cloud,
                                      raw_subnet['subnet'])
        session.store(subnet)
        return net, raw_net['network']['id'], raw_subnet['subnet']['id']


class EnsureAdminRoleDestructor(base.Destructor):
    def __init__(self, location, user_id, role_id, tenant_id):
        self.location = location
        self.user_id = user_id
        self.role_id = role_id
        self.tenant_id = tenant_id

    def get_signature(self):
        return self.location, self.user_id, self.role_id, self.tenant_id

    def run(self, cfg, migration):
        cloud = cfg.clouds[getattr(migration, self.location)]
        identity_client = clients.identity_client(cloud)
        try:
            clients.retry(identity_client.roles.remove_user_role,
                          user=self.user_id, role=self.role_id,
                          tenant=self.tenant_id,
                          expected_exceptions=[
                              keystone_exceptions.NotFound])
        except keystone_exceptions.NotFound:
            pass


class EnsureAdminRole(BaseSymmetricSingletonTask):
    def __init__(self, cfg, migration, obj, location):
        super(EnsureAdminRole, self).__init__(cfg, migration, obj, location)
        self.already_member = False
        self.user_id = None
        self.role_id = None
        self.tenant_id = None

    def get_singleton_key(self, source_obj, *args, **kwargs):
        return self.location, _get_object_tenant_id(self.cloud, source_obj)

    def _user_id(self, username):
        with model.Session() as session:
            for user in session.list(identity.User, self.cloud):
                if user.name.lower() == username.lower():
                    return user.object_id.id
        raise base.AbortMigration('User % not found in cloud %s', username,
                                  self.cloud.name)

    def _role_id(self, rolename):
        with model.Session() as session:
            for role in session.list(identity.Role, self.cloud):
                if role.name.lower() == rolename.lower():
                    return role.object_id.id
        raise base.AbortMigration('Role % not found in cloud %s', rolename,
                                  self.cloud.name)

    def migrate(self, source_obj, *args, **kwargs):
        cloud = self.cloud
        identity_client = clients.identity_client(cloud)
        destructor_var = self.destructor_var
        try:
            self.user_id = self._user_id(cloud.credential.username)
            self.role_id = self._role_id(cloud.admin_role)
            self.tenant_id = _get_object_tenant_id(self.cloud, source_obj)
            clients.retry(
                identity_client.roles.add_user_role,
                user=self.user_id, role=self.role_id, tenant=self.tenant_id,
                expected_exceptions=[keystone_exceptions.Conflict])
            self.destructor = EnsureAdminRoleDestructor(
                    self.location, self.user_id, self.role_id, self.tenant_id)
        except keystone_exceptions.Conflict:
            pass
        return {
            destructor_var: self.destructor
        }


class RestoreQuotas(base.Destructor):
    def __init__(self, location, admin_tenant_id, obj_tenant_id,
                 net_quota, compute_quota, storage_quota):
        self.location = location
        self.admin_tenant_id = admin_tenant_id
        self.obj_tenant_id = obj_tenant_id
        self.net_quota = net_quota
        self.compute_quota = compute_quota
        self.storage_quota = storage_quota

    def get_signature(self):
        return self.location, self.admin_tenant_id, self.obj_tenant_id

    def run(self, cfg, migration):
        cloud = cfg.clouds[getattr(migration, self.location)]

        network_client = clients.network_client(cloud)
        compute_client = clients.compute_client(cloud)
        storage_client = clients.volume_client(cloud)
        try:
            if self.net_quota is None:
                clients.retry(network_client.delete_quota,
                              self.admin_tenant_id)
            else:
                clients.retry(
                    network_client.update_quota, self.admin_tenant_id, {
                        'quota': {
                            'network': self.net_quota['network'],
                            'subnet': self.net_quota['subnet'],
                            'port': self.net_quota['port'],
                        }
                    })
        except neutron_exceptions.NotFound:
            pass
        if self.compute_quota:
            clients.retry(compute_client.quotas.update, self.admin_tenant_id,
                          **self.compute_quota)
        if self.storage_quota:
            clients.retry(storage_client.quotas.update, self.obj_tenant_id,
                          **self.storage_quota)


class SetUnlimitedQuotas(BaseSymmetricSingletonTask):
    def __init__(self, cfg, migration, obj, location):
        super(SetUnlimitedQuotas, self).__init__(cfg, migration, obj, location)
        self.obj_tenant_id = None
        with model.Session() as session:
            self.admin_tenant_id = _get_admin_tenant_id(self.cloud, session)

    def get_singleton_key(self, source_obj, *args, **kwargs):
        return self.location, _get_object_tenant_id(self.cloud, source_obj)

    def migrate(self, source_obj, *args, **kwargs):
        self.obj_tenant_id = _get_object_tenant_id(self.cloud, source_obj)
        net_quotas = self._set_network_quotas(self.admin_tenant_id)
        compute_quotas = self._set_compute_quotas(self.admin_tenant_id)
        storage_quotas = self._set_cinder_quotas(self.obj_tenant_id)
        self.destructor = RestoreQuotas(
                self.location, self.admin_tenant_id, self.obj_tenant_id,
                net_quotas, compute_quotas, storage_quotas)
        return {
            self.destructor_var: self.destructor
        }

    def _set_network_quotas(self, tenant_id):
        network_client = clients.network_client(self.cloud)
        for quota in network_client.list_quotas(tenant_id=tenant_id)['quotas']:
            if quota['tenant_id'] == tenant_id:
                break
        else:
            quota = None
        network_client.update_quota(tenant_id, {
            'quota': {
                'network': -1,
                'subnet': -1,
                'port': -1,
            }
        })
        return quota

    def _set_compute_quotas(self, tenant_id):
        compute_client = clients.compute_client(self.cloud)
        return self._set_quotas(compute_client, tenant_id, cores=-1, ram=-1,
                                injected_file_content_bytes=-1, instances=-1,
                                fixed_ips=-1)

    def _set_cinder_quotas(self, tenant_id):
        storage_client = clients.volume_client(self.cloud)
        return self._set_quotas(storage_client, tenant_id, gigabytes=-1,
                                snapshots=-1, volumes=-1)

    @staticmethod
    def _set_quotas(client, tenant_id, **kwargs):
        quotas = getattr(clients.retry(client.quotas.get, tenant_id), '_info')
        original = {}
        for item, value in kwargs.items():
            if quotas[item] != value:
                original[item] = quotas[item]
        clients.retry(client.quotas.update, tenant_id, **kwargs)
        return original


class VolumeMigrationFlowFactory(base.MigrationFlowFactory):
    migrated_class = storage.Volume

    def create_flow(self, cfg, migration, obj):
        return [
            SetUnlimitedQuotas(cfg, migration, obj, 'source'),
            SetUnlimitedQuotas(cfg, migration, obj, 'destination'),
            EnsureAdminRole(cfg, migration, obj, 'source'),
            EnsureAdminRole(cfg, migration, obj, 'destination'),
            FindOrCreateNetwork(cfg, migration, obj, 'source'),
            FindOrCreateNetwork(cfg, migration, obj, 'destination'),
            FindOrCreateFlavor(cfg, migration, obj, 'source'),
            FindOrCreateFlavor(cfg, migration, obj, 'destination'),
            FindOrUploadImage(cfg, migration, obj, 'source'),
            FindOrUploadImage(cfg, migration, obj, 'destination'),
            DetachMigratedVolume(cfg, migration, obj),
            CreateVolume(cfg, migration, obj),
            BootTransferVm(cfg, migration, obj, 'source'),
            BootTransferVm(cfg, migration, obj, 'destination'),
            AttachNodeLocalInterface(cfg, migration, obj, 'source'),
            AttachNodeLocalInterface(cfg, migration, obj, 'destination'),
            AttachSourceVolume(cfg, migration, obj),
            AttachDestinationVolume(cfg, migration, obj),
            TransferVolumeData(cfg, migration, obj),
            DetachSourceVolume(cfg, migration, obj),
            DetachDestinationVolume(cfg, migration, obj),
            CleanupVms(cfg, migration, obj),
            ReattachMigratedVolume(cfg, migration, obj),
            base.RememberMigration(cfg, migration, obj),
        ]


def _random_mac():
    mac = [0x00, 0x16, 0x3e,
           random.randint(0x00, 0x7f),
           random.randint(0x00, 0xff),
           random.randint(0x00, 0xff)]
    return ':'.join("%02x" % x for x in mac)


def _first_unused_address(cloud):
    result = netaddr.IPNetwork(cloud.unused_network)
    result.value += 1
    return result


def _allocate_ip_address(cloud):
    global _ip_counter
    with LOCK:
        result = netaddr.IPNetwork(cloud.unused_network)
        result.value += _ip_counter
        _ip_counter += 1

    assert result in cloud.unused_network
    return result


def _allocate_port(host, cloud):
    with LOCK:
        min_port, max_port = cloud.unused_port_range
        used_host_ports = _used_ports.setdefault(host, set())
        while True:
            port = random.randint(min_port, max_port)
            if port not in used_host_ports:
                used_host_ports.add(port)
                return port
            else:
                LOG.warning('Port %d already used on host %s in cloud %s, '
                            'generating new one', port, host, cloud.name)


def _get_private_key(rsa_key):
    pkey = cStringIO.StringIO()
    rsa_key.write_private_key(pkey)
    return pkey.getvalue()


def _deploy_pkey(rexec):
    key_path = rexec.run('mktemp').strip()
    rexec.run('echo "{private_key}" > {key_path}; chmod 600 {key_path}',
              private_key=_get_private_key(RSA1024_KEY),
              key_path=key_path)
    return key_path


def _wait_ip_accessible(cloud, rexec, ip_address):
    waited = 0.0
    while waited <= cloud.operation_timeout:
        before = time.time()
        try:
            rexec.run('ping -c 1 -W 1 {ip_address}', ip_address=ip_address)
            return
        except remote.RemoteFailure:
            after = time.time()
            delta = after - before
            if delta < 1.0:
                delta = 1.0
                time.sleep(1.0)
            waited += delta
    raise base.AbortMigration('VM couldn\'t be reached through %s', ip_address)


def _object_status_is(client, manager_name, obj_id, status):
    manager = getattr(client, manager_name)
    obj = clients.retry(manager.get, obj_id)
    LOG.debug('Checking object %s is in status \'%s\': actual status \'%s\'',
              obj_id, status.lower(), obj.status.lower())
    if obj.status.lower() == status.lower():
        return obj
    elif obj.status.lower() == 'error':
        raise base.AbortMigration('Object %s ended up in ERROR state', obj_id)
    else:
        return None


def _object_is_deleted(client, manager, obj_id, expected_exception):
    try:
        manager_obj = getattr(client, manager)
        clients.retry(manager_obj.get, obj_id,
                      expected_exceptions=[expected_exception])
        return False
    except expected_exception:
        return True


def _scope(tenant_id):
    return config.Scope(project_id=tenant_id,
                        project_name=None,
                        domain_id=None)


def _get_admin_tenant_id(cloud, session):
    scope = cloud.scope
    project_name = scope.project_name
    if scope.project_id is not None:
        return scope.project_id
    elif project_name is not None:
        for tenant in session.list(identity.Tenant, cloud):
            if tenant.name.lower() == project_name.lower():
                return tenant.object_id.id
    raise base.AbortMigration(
        'Unable to upload image: no admin tenant.')


def _get_object_tenant_id(cloud, obj):
    tenant = obj.tenant
    if tenant.object_id.cloud != cloud.name:
        return tenant.find_link(cloud).primary_key.id
    else:
        return tenant.primary_key.id


def _delete_vm(cloud, vm_id):
    nova_client = clients.compute_client(cloud)
    for do_reset in (False, True):
        try:
            if do_reset:
                clients.retry(
                    nova_client.servers.reset_state, vm_id,
                    expected_exceptions=[nova_exceptions.NotFound])
            try:
                clients.retry(
                    nova_client.servers.delete, vm_id,
                    expected_exceptions=[nova_exceptions.NotFound])
            except nova_exceptions.NotFound:
                raise
            except nova_exceptions.ClientException:
                LOG.error('Failed to delete VM %s from cloud %s',
                          vm_id, cloud.name, exc_info=True)
                continue
            if clients.wait_for(_object_is_deleted, nova_client, 'servers',
                                vm_id, nova_exceptions.NotFound):
                return True
        except nova_exceptions.NotFound:
            return True
        except clients.Timeout:
            continue
    LOG.error('Timeout waiting for VM %s from cloud %s to be deleted',
              vm_id, cloud.name, exc_info=True)
    return False


def shutoff_vm(nova_client, instace_id):
    # TODO: make general-purpose utility function

    instance = clients.retry(nova_client.servers.get, instace_id)
    current = instance.status.lower()

    def wait_status(status):
        return clients.wait_for(
            _object_status_is, nova_client, 'servers', instace_id, status)

    try:
        if current == 'paused':
            nova_client.servers.unpause(instance)
            wait_status('active')
            nova_client.servers.stop(instance)
            wait_status('shutoff')
        elif current == 'suspended':
            nova_client.servers.resume(instance)
            wait_status('active')
            nova_client.servers.stop(instance)
            wait_status('shutoff')
        elif current == 'active':
            nova_client.servers.stop(instance)
            wait_status('shutoff')
        elif current == 'verify_resize':
            nova_client.servers.confirm_resize(instance)
            wait_status('active')
            nova_client.servers.stop(instance)
            wait_status('shutoff')
        elif current != 'shutoff':
            raise base.AbortMigration('Invalid state change: %s -> shutoff',
                                      current)
    except clients.Timeout:
        LOG.debug("Failed to change state from '%s' to 'shutoff' for VM "
                  "'%s'", current, instace_id)


def _get_image_location():
    cf_init_path = inspect.getfile(cloudferry)
    return os.path.join(os.path.dirname(cf_init_path),
                        'static', IMAGE_FILENAME)


def _get_image_md5():
    # We don't care about race condition here since MD5 will always return
    # same results
    global _image_md5
    if _image_md5 is None:
        location = _get_image_location()
        hash_md5 = hashlib.md5()
        with open(location, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hash_md5.update(chunk)
        _image_md5 = hash_md5.hexdigest().lower()
    return _image_md5
