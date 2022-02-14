import logging
import re
import os
from time import sleep
import uuid
import grpc
from spdk.rpc.client import JSONRPCException
from google.protobuf import wrappers_pb2 as wrap
from .subsystem import Subsystem, SubsystemException
from ..proto import sma_pb2
from ..proto import nvmf_vfio_pb2
from ..qmp import QMPClient, QMPError

log = logging.getLogger(__name__)


class NvmfVfioException(SubsystemException):
    def __init__(self, code, message, args=None):
        self.args = repr(args)
        super().__init__(code, message)


class NvmfVfioSubsystem(Subsystem):
    def __init__(self, client):
        super().__init__('nvmf_vfio', client)
        self._trtype = 'vfiouser'
        self._root_path = '/var/run/vfio-user/sma'
        self._controllers = {}
        self._has_transport = self._create_transport()

    def _get_name(self):
        return self.name
    
    def _get_trtype(self):
        return self._trtype

    def _prefix_add(self, nqn):
        return f'{self._get_name()}:{nqn}'

    def _prefix_rem(self, nqn):
        return nqn.removeprefix(f'{self._get_name()}:')

    def _get_id_from_nqn(self, nqn):
        return re.sub("[^0-9a-zA-Z]+", "0", nqn)

    def _get_path_from_id(self, id):
        return os.path.join(self._root_path, id)

    def _get_path_from_nqn(self, nqn):
        id = self._get_id_from_nqn(nqn)
        return self._get_path_from_id(id)

    def _create_socket_path(self, id):
        socket_pth = self._get_path_from_id(id)
        try:
            if not os.path.exists(socket_pth):
                os.makedirs(socket_pth)
            return socket_pth
        except OSError as e:
            raise NvmfVfioException(
                        grpc.StatusCode.INTERNAL,
                        'Path creation failed.', socket_pth) from e

    def _remove_socket_path(self, id):
        socket_pth = self._get_path_from_id(id)
        bar = os.path.join(socket_pth, 'bar0')
        cntrl = os.path.join(socket_pth, 'cntrl')
        try:
            if os.path.exists(bar):
                os.remove(bar)
            if os.path.exists(cntrl):
                os.remove(cntrl)
        except OSError as e:
            raise NvmfVfioException(
                        grpc.StatusCode.INTERNAL,
                        'Path deletion failed.', socket_pth) from e

    def _create_transport(self):
        try:
            with self._client() as client:
                transports = client.call('nvmf_get_transports')
                for transport in transports:
                    if transport['trtype'].lower() == self._get_trtype():
                        return True
                # TODO: take the transport params from config
                return client.call('nvmf_create_transport',
                                   {'trtype': self._get_trtype()})
        except JSONRPCException:
            logging.error(f'Transport query NVMe/{self._get_trtype()} failed')
            return False

    def _add_volume(self, ctrlr_name, volume_guid):
        volumes = self._controllers.get(ctrlr_name, [])
        if volume_guid in volumes:
            return
        self._controllers[ctrlr_name] = volumes + [volume_guid]

    def _get_vfio_controllers(self, controllers):
        for controller in controllers:
            if next(filter(lambda c: c.get('trid', {}).get('trtype', '').lower() == self._get_trtype(),
                           controller['ctrlrs']), None) is not None:
                yield controller

    def _cache_controllers(self, controllers):
        for controller in self._get_vfio_controllers(controllers):
            cname = controller['name']
            # If a controller was connected outside of our knowledge (e.g. via discovery),
            # we'll never want to disconnect it.  To prevent from doing that, add NULL GUID
            # acting as an extra reference.
            if cname not in self._controllers:
                logging.debug(f'Found external controller: {cname}')
                self._add_volume(cname, str(uuid.UUID(int=0)))

        # Now go over our cached list and remove controllers that were disconnected in the
        # meantime, without our knowledge
        for cname in [*self._controllers.keys()]:
            if cname not in [c['name'] for c in self._get_vfio_controllers(controllers)]:
                logging.debug(f'Removing disconnected controller: {cname}')
                self._controllers.pop(cname)

    def _unpack_request(self, params, request):
        if not request.params.Unpack(params):
                raise NvmfVfioException(
                            grpc.StatusCode.INTERNAL,
                            'Failed to unpack request', request)
        return params

    def _check_params(self, request, params):
        for param in params:
            if not request.HasField(param):
                raise NvmfVfioException(
                            grpc.StatusCode.INTERNAL,
                            'Could not find param', request)

    def _to_low_case_set(self, dict_in) -> set:
        '''
        Function for creating set from a dictionary with all value
        keys made a lower case string. Designed for address compaction

        :param dict_in is a dictionary to work on
        :return set of pairs with values converted to lower case string
        '''
        return {(K, str(V).lower()) for K, V in dict_in.items()}

    def _check_addr(self, addr, addr_list):
        '''
        Function for transport comparration without known variable set.
        Comparration is made based on inclusion of address set.
        Use with caution for small dictionaries (like 1-2 elements)

        :param dict_in is a dictionary to work on
        :return True is addr was found in addr_list
                False is addr is not a subset of addr_list
        '''
        low_case = self._to_low_case_set(addr)
        return bool(list(filter(lambda i: (low_case.issubset(
                                self._to_low_case_set(i))), addr_list)))

    def _get_bdev_by_uuid(self, client, uuid):
        bdevs = client.call('bdev_get_bdevs')
        for bdev in bdevs:
            if bdev['uuid'] == uuid:
                return bdev
        return None

    def _get_subsystem_by_nqn(self, client, nqn):
        subsystems = client.call('nvmf_get_subsystems')
        for subsystem in subsystems:
            if subsystem['nqn'] == nqn:
                return subsystem
        return None

    def _check_create_subsystem(self, client, nqn):
        '''
        NVMe-oF create NQN subsystem is one does not exists

        :param client is a JSONRPCClient socket
        :param nqn is subsystem NQN we are probing for

        :raise NvmfVfioException if result is unexpected
        :raise JSONRPCException for socket related errors
        :return True if subsys was created
                False when subsys already exists
        '''
        if self._get_subsystem_by_nqn(client, nqn) is None:
            args = {'nqn': nqn, 'allow_any_host': True}
            result = client.call('nvmf_create_subsystem', args)
            if not result:
                raise NvmfVfioException(
                            grpc.StatusCode.INTERNAL,
                            'Failed to create subsystem', args)
            return True
        return False

    def _check_listener(self, client, nqn, addr):
        subsystem = self._get_subsystem_by_nqn(client, nqn)
        if subsystem is None:
            raise NvmfVfioException(
                        grpc.StatusCode.INTERNAL,
                        f'Failed check for {self.getName()} listener', addr)
        return self._check_addr(addr, subsystem['listen_addresses'])

    def _create_listener(self, client, nqn, addr, clean_on_fail=False):
        args = {'nqn': nqn, 'listen_address': addr}
        result = client.call('nvmf_subsystem_add_listener', args)
        if not result:
            if clean_on_fail:
                client.call('nvmf_delete_subsystem', nqn)
            raise NvmfVfioException(
                    grpc.StatusCode.INTERNAL,
                    "Failed to create listener", args)

    def create_device(self, request):
        params = nvmf_vfio_pb2.CreateDeviceParameters()
        params = self._unpack_request(params, request)
        self._check_params(params, ['trbus', 'qtraddr', 'qtrsvcid'])
        nqn = params.subnqn.value
        id = self._get_id_from_nqn(nqn)
        traddr = self._create_socket_path(id)
        addr = { 'traddr': traddr,
                 'trtype': self._get_trtype() }

        trbus = params.trbus.value
        qaddress = (params.qtraddr.value, int(params.qtrsvcid.value))
        try:
            with self._client() as client:
                subsys_created = self._check_create_subsystem(client, nqn)
                if not self._check_listener(client, nqn, addr):
                    self._create_listener(client, nqn, addr, subsys_created)
            # TODO: after couple of add/delete QEMU reports memory leak and crush
            with QMPClient(qaddress) as qclient:
                if not qclient.exec_device_list_properties(id):
                    qclient.exec_device_add(addr['traddr'], trbus, id)
        except JSONRPCException as e:
            raise NvmfVfioException(
                grpc.StatusCode.INTERNAL,
                "JSONRPCException failed to create device", params) from e
        except QMPError as e:
            # TODO: subsys and listener cleanup
            raise NvmfVfioException(
                grpc.StatusCode.INTERNAL,
                "QMPClient failed to create device", params) from e
        return sma_pb2.CreateDeviceResponse(id=wrap.StringValue(
                    value=self._prefix_add(nqn)))

    def remove_device(self, request):
        try:
            with self._client() as client:
                nqn = self._prefix_rem(request.id.value)
                id = self._get_id_from_nqn(nqn)
                if self._get_subsystem_by_nqn(client, nqn) is not None:
                    with QMPClient() as qclient:
                        if qclient.exec_device_list_properties(id):
                            qclient.exec_device_del(id)
                            # TODO: add wait for event device deleted instead sleep
                            sleep(3)
                    if not client.call('nvmf_delete_subsystem', {'nqn': nqn}):
                        raise NvmfVfioException(
                            grpc.StatusCode.INTERNAL,
                            "Failed to remove device", id)
                    self._remove_socket_path(id)
                else:
                    logging.info(f'Tried to remove a non-existing device: {nqn}')
        except JSONRPCException as e:
            raise NvmfVfioException(
                grpc.StatusCode.INTERNAL,
                "JSONRPCException failed to delete device", request) from e
        except QMPError as e:
            raise NvmfVfioException(
                grpc.StatusCode.INTERNAL,
                "QMPClient failed to delete device", request) from e

    def connect_volume(self, request):
        params = nvmf_vfio_pb2.ConnectVolumeParameters()
        params = self._unpack_request(params, request)
        self._check_params(params, ['trbus', 'qtraddr', 'qtrsvcid'])
        print("connecting ")
        print(params)
        try:
            with self._client() as client:
                print("entered ")
                nqn = params.subnqn.value
                print(f"nqn {nqn} ")
                traddr = self._get_path_from_nqn(nqn)
                addr = {'traddr': traddr,
                        'trtype': self._get_trtype() }
                existing = False
                controllers = client.call('bdev_nvme_get_controllers')
                print(f"controllers {controllers} ")

                # First update the controller cache
                self._cache_controllers(controllers)

                for controller in self._get_vfio_controllers(controllers):
                    for path in controller['ctrlrs']:
                        trid = path['trid']
                        if self._check_addr(addr, (trid,)):
                            cname = controller['name']
                            bdevs = client.call('bdev_get_bdevs')
                            nbdevs = [(b['name'], b['driver_specific']['nvme'])
                                      for b in bdevs if b.get(
                                        'driver_specific', {}).get('nvme') is not None]
                            names = [name for name, nvme in nbdevs if
                                     self._check_addr(addr, [n['trid'] for n in nvme])]
                            break
                    else:
                        continue
                    existing = True
                    break
                else:
                    cname = str(uuid.uuid1())
                    print(f"else {cname} ")
                    prms = {'name': cname,
                            'trtype': 'rdma',
                            'traddr': traddr,
                            'subnqn': nqn}
                    print(f"prms {prms} ")
                    names = client.call('bdev_nvme_attach_controller',
                                        prms)
                    print(f"names {names} ")
                    bdevs = client.call('bdev_get_bdevs')
                    print(f"bdev_get_bdevs {bdevs} ")
                # Check if the controller contains specified volume
                for name in names:
                    bdev = next(filter(lambda b: b['name'] == name, bdevs), None)
                    print(f"bdev {bdev} name {name}")
                    if bdev is not None and request.guid.value == bdev['uuid']:
                        break
                else:
                    # Detach the controller only if we've just connected it
                    if not existing:
                        try:
                            client.call('bdev_nvme_detach_controller',
                                        {'name': cname})
                        except JSONRPCException:
                            pass
                    raise SubsystemException(grpc.StatusCode.INVALID_ARGUMENT,
                                             'Volume couldn\'t be found')
                self._add_volume(cname, request.guid.value)
                return sma_pb2.ConnectVolumeResponse()
        except JSONRPCException:
            # TODO: parse the exception's error
            raise SubsystemException(grpc.StatusCode.INTERNAL,
                                     'Failed to connect the volume')

    def disconnect_volume(self, request):
        try:
            with self._client() as client:
                controllers = client.call('bdev_nvme_get_controllers')
                # First update the controller cache
                self._cache_controllers(controllers)

                disconnect, cname = self._remove_volume(request.guid.value)
                if not disconnect:
                    return cname is not None

                for controller in controllers:
                    if controller['name'] == cname:
                        result = client.call('bdev_nvme_detach_controller',
                                             {'name': cname})
                        if not result:
                            raise SubsystemException(grpc.StatusCode.INTERNAL,
                                                     'Failed to disconnect the volume')
                        return True
                else:
                    logging.info('Tried to disconnect volume fron non-existing ' +
                                 f'controller: {cname}')
            return False
        except JSONRPCException:
            # TODO: parse the exception's error
            raise SubsystemException(grpc.StatusCode.INTERNAL,
                                     'Failed to disconnect the volume')

    def attach_volume(self, request):
        self._check_params(request, ['volume_guid'])
        nqn = self._prefix_rem(request.device_id.value)
        try:
            with self._client() as client:
                bdev = self._get_bdev_by_uuid(request.volume_guid.value)
                if bdev is None:
                    raise NvmfVfioException(grpc.StatusCode.NOT_FOUND,
                                            'Invalid volume GUID', request)
                subsystem = self._get_subsystem_by_nqn(nqn)
                if subsystem is None:
                    raise NvmfVfioException(grpc.StatusCode.NOT_FOUND,
                                            'Invalid device ID', request)
                if bdev['name'] not in [ns['name'] for ns in subsystem['namespaces']]:
                    params = {'nqn': nqn, 'namespace': {'bdev_name': bdev['name']}}
                    result = client.call('nvmf_subsystem_add_ns', params)
                    if not result:
                        raise NvmfVfioException(grpc.StatusCode.INTERNAL,
                                                'Failed to attach volume', params)
        except JSONRPCException as e:
            raise NvmfVfioException(grpc.StatusCode.INTERNAL,
                                    'Failed to attach volume', request) from e

    def detach_volume(self, request):
        self._check_params(request, ['volume_guid', 'device_id'])
        nqn = self._prefix_rem(request.id.value) 
        volume = request.volume_guid.value
        try:
            with self._client() as client:
                bdev = self._get_bdev_by_uuid(volume)
                if bdev is None:
                    logging.info(f'Tried to detach non-existing volume: {volume}')
                    return
                subsystem = self._get_subsystem_by_nqn(nqn)
                if subsystem is None:
                    logging.info(f'Tried detaching: {volume} from non-existing: {nqn}')
                    return
                for ns in subsystem['namespaces']:
                    if ns['name'] != bdev['name']:
                        continue
                    params = {'nqn': nqn, 'nsid': ns['nsid']}
                    result = client.call('nvmf_subsystem_remove_ns', params)
                    if not result:
                        raise NvmfVfioException(grpc.StatusCode.INTERNAL,
                                                'Failed to detach volume', request)
                    break
        except JSONRPCException as e:
            raise NvmfVfioException(grpc.StatusCode.INTERNAL,
                                    'Failed to detach volume', request) from e

    def owns_device(self, id):
        return id.startswith(self._get_name())

    def owns_controller(self, id):
        raise NotImplementedError()
