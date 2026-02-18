# Copyright 2024-2025 NetCracker Technology Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import ssl
import time
import pika
import requests
from pika import BaseConnection
from pika.adapters.blocking_connection import BlockingChannel
from pika.connection import Parameters
from pika.frame import Method as FrameMethod
from pika.spec import Basic, BasicProperties, Connection
from pika.exceptions import ChannelClosed, IncompatibleProtocolError
import urllib3
import utils
from robot.api import logger
import os
from typing import Any, Dict, List, Optional, Tuple, Type, Union
from robot.utils import ConnectionCache
from robot.libraries.BuiltIn import BuiltIn
from socket import gaierror, error
from PlatformLibrary import PlatformLibrary

RabbitMqMessage = Union[Tuple[Dict[str, Any], Dict[str, Any], str], Tuple[None, None, None]]  # noqa: 993


def _strtobool(val):
    """Replace distutils.util.strtobool (removed in Python 3.12+). Returns 1 for true, 0 for false."""
    v = (val or "").lower()
    if v in ("y", "yes", "t", "true", "1", "on"):
        return 1
    if v in ("n", "no", "f", "false", "0", "off"):
        return 0
    raise ValueError(f"invalid truth value {val!r}")


CA_CERT_PATH = '/tls/ca.crt'
TLS_CERT_PATH = '/tls/tls.crt'
TLS_KEY_PATH = '/tls/tls.key'
SSL_ENABLED = _strtobool(os.environ.get('RABBITMQ_ENABLE_SSL', 'false'))
EXTERNAL_ENABLED = _strtobool(os.environ.get('EXTERNAL_ENABLED', 'false'))


class RequestConnection(object):
    """This class contains settings to connect to RabbitMQ via HTTP."""
    def __init__(self, host: str, port: Union[int, str], username: str, password: str, timeout: int) -> None:
        """
        Initialization.

        *Args:*\n
        _host_ - server host name;\n
        _port_ - port number;\n
        _username_ - user name;\n
        _password_ - user password;\n
        _timeout_ - connection timeout;\n

        """
        self.host = host
        self.port = port
        if SSL_ENABLED:
            self.url = f'https://{host}:{port}/api'
            self.verify = CA_CERT_PATH
        else:
            self.url = f'http://{host}:{port}/api'
            self.verify = False
        if EXTERNAL_ENABLED:
            self.url = f'{host}/api'
        self.auth = (username, password)
        self.timeout = timeout

    def close(self) -> None:
        """Close connection."""
        pass


class BlockedConnection(pika.BlockingConnection):
    """
    Wrapper over standard connection to RabbitMQ
    Allows to register connection lock events of the server
    """
    def __init__(self, parameters: Parameters = None, impl_class: Type[BaseConnection] = None) -> None:
        """Constructor arguments are supplemented with
        callbacks to register blocking events

        Args:
            parameters: connection parameters instance or non-empty sequence of them;
            impl_class: implementation class (for test/debugging only).
        """
        super(BlockedConnection, self).__init__(parameters=parameters, _impl_class=impl_class)
        self.add_on_connection_blocked_callback(self.on_blocked)
        self.add_on_connection_unblocked_callback(self.on_unblocked)
        self._blocked = False

    def on_blocked(self, method: Connection.Blocked) -> None:
        """
        Set connection blocking flag.

        Args:
            method: the method frame's `method` member is of type `pika.spec.Connection.Blocked`.
        """
        self._blocked = True

    def on_unblocked(self, method: Connection.Unblocked) -> None:
        """
        Unset connection blocking flag.

        Args:
            method: the method frame's `method` member is of type `pika.spec.Connection.Unblocked`.
        """
        self._blocked = False

    @property
    def blocked(self) -> bool:
        """
        *Returns:*\n
            Connection blocking flag.
        """
        return self._blocked

    def close(self, reply_code: int = 200, reply_text: str = 'Normal shutdown') -> None:
        """Close AMQP connection.

        Args:
            reply_code: the code number for the close.
            reply_text: the text reason for the close.
        """
        if self.is_open:
            super().close(reply_code=reply_code, reply_text=reply_text)
        else:
            logger.debug("Connection is already closed.")


class NCRabbitMQLibrary(object):

    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    def __init__(self, host: str, port: str, rmquser: str, rmqpassword: str, timeout: str, backuper_host: str, managed_by_operator="true"):

        urllib3.disable_warnings()
        if SSL_ENABLED:
            self._rabbitmq_url = f'https://{host}:{port}'
            self._backuper_url = f'https://{backuper_host}:8443'
            self.verify = CA_CERT_PATH
        else:
            self._rabbitmq_url = f'http://{host}:{port}'
            self._backuper_url = f'http://{backuper_host}:8080'
            self.verify = False
        if EXTERNAL_ENABLED:
            self._rabbitmq_url = f'{host}'
        self._user = rmquser
        self._password = rmqpassword
        self._timeout = int(timeout)
        """ AMQP Initialization. """
        self._http_connection: Optional[RequestConnection] = None
        self._http_cache = ConnectionCache()
        if not EXTERNAL_ENABLED:
            self._amqp_connection: Optional[BlockedConnection] = None
            self._amqp_cache = ConnectionCache()
            self._channel: Optional[BlockingChannel] = None
        """ BDI connect """
        self.k8s_lib = PlatformLibrary(managed_by_operator)

    @property
    def http_connection(self) -> RequestConnection:
        """Get current http connection to RabbitMQ.

        *Raises:*\n
            RuntimeError: if there isn't any open connection.

        *Returns:*\n
            Current http connection to to RabbitMQ.
        """
        if self._http_connection is None:
            raise RuntimeError('There is no open http connection to RabbitMQ.')
        return self._http_connection

    @property
    def amqp_connection(self) -> BlockedConnection:
        """Get current ampq connection to RabbitMQ.

        *Raises:*\n
            RuntimeError: if there isn't any open connection.

        *Returns:*\n
            Current ampq connection to to RabbitMQ.
        """
        if self._amqp_connection is None:
            raise RuntimeError('There is no open ampq connection to RabbitMQ.')
        return self._amqp_connection

    @staticmethod
    def _prepare_request_headers(body: Dict[str, Any] = None) -> Dict[str, str]:
        """
        Headers definition for HTTP-request.
        Args:*\n
            _body_: HTTP-request body.

        *Returns:*\n
            Dictionary with headers for HTTP-request.
        """
        headers = {}
        if body:
            headers["Content-Type"] = "application/json"
        return headers

    def create_rabbitmq_connection(self, host: str, http_port: Union[int, str], amqp_port: Union[int, str],
                                   username: str, password: str, alias: str, vhost: str) -> None:
        """
        Connect to RabbitMq server.

        *Args:*\n
        _host_ - server host name;\n
        _http_port_ - port number of http-connection \n
        _amqp_port_ - port number of amqp-connection \n
        _username_ - user name;\n
        _password_ - user password;\n
        _alias_ - connection alias;\n
        _vhost_ - virtual host name;\n

        *Returns:*\n
        Current connection index.

        *Raises:*\n
        socket.error if connection cannot be created.

        *Example:*\n
        | Create Rabbitmq Connection | my_host_name | 15672 | 5672 | guest | guest | alias=rmq | vhost=/ |
        """

        self._connect_to_http(host=host, port=http_port, username=username, password=password, alias=alias + "_http")
        if not EXTERNAL_ENABLED:
            self._connect_to_amqp(host=host, port=amqp_port, username=username, password=password, alias=alias + "_amqp",
                                  virtual_host=vhost)

    def _connect_to_amqp(self, host: str, port: Union[int, str], username: str = 'guest', password: str = 'guest',
                         alias: str = None, virtual_host: str = '/', socket_timeout: int = 15,
                         heartbeat_timeout: int = 600, blocked_timeout: int = 300) -> int:
        """ Connect to server via AMQP.

        *Args*:\n
            _host_: server host name.\n
            _port_: port number.\n
            _username_: user name.\n
            _password_: user password.\n
            _alias_: connection alias.\n
            _virtual_host_: virtual host name;\n
            _socket_timeout_: socket connect timeout;\n
            _heartbeat_timeout_: AMQP heartbeat timeout negotiation during connection tuning;\n
            _blocked_timeout_: timeout for the connection to remain blocked.\n

        *Returns:*\n
            Server connection index.
        """
        if port is None:
            BuiltIn().fail(msg="RabbitMq: port for connect is None")
        port = int(port)
        if virtual_host is None:
            BuiltIn().fail(msg="RabbitMq: virtual host for connect is None")
        virtual_host = str(virtual_host)

        parameters_for_connect = \
            f"host={host}, port={port}, username={username}, timeout={socket_timeout}, alias={alias}"

        logger.debug(f'Connecting using : {parameters_for_connect}')

        credentials = pika.PlainCredentials(username=username, password=password)
        if SSL_ENABLED:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(CA_CERT_PATH)
            ssl_options = pika.SSLOptions(context)
            conn_params = pika.ConnectionParameters(host=host, port=port,
                                                    credentials=credentials,
                                                    virtual_host=virtual_host,
                                                    socket_timeout=socket_timeout,
                                                    blocked_connection_timeout=blocked_timeout,
                                                    heartbeat=heartbeat_timeout,
                                                    ssl_options=ssl_options)
        else:
            conn_params = pika.ConnectionParameters(host=host, port=port,
                                                    credentials=credentials,
                                                    virtual_host=virtual_host,
                                                    socket_timeout=socket_timeout,
                                                    blocked_connection_timeout=blocked_timeout,
                                                    heartbeat=heartbeat_timeout)
        try:
            self._amqp_connection = BlockedConnection(parameters=conn_params)
        except (gaierror, error, IOError, IncompatibleProtocolError):
            BuiltIn().fail(msg=f"RabbitMq: Could not connect with following parameters: {parameters_for_connect}")
        self._channel = None
        return self._amqp_cache.register(self._amqp_connection, alias)

    def _connect_to_http(self, host: str, port: Union[int, str], username: str, password: str, alias: str) -> int:
        """ Connect to server via HTTP.

        *Args*:\n
            _host_: server host name.\n
            _port_: port number.\n
            _username_: user name.\n
            _password_: user password.\n
            _alias_: connection alias.\n

        *Returns:*\n
            Server connection index.
        """
        if port is None:
            BuiltIn().fail(msg="RabbitMq: port for connect is None")
        port = int(port)
        timeout = 15
        parameters_for_connect = f"host={host}, port={port}, username={username}, timeout={timeout}, alias={alias}"

        logger.debug('Connecting using : {params}'.format(params=parameters_for_connect))
        try:
            self._http_connection = RequestConnection(host, port, username, password, timeout)
        except (gaierror, error, IOError):
            BuiltIn().fail(msg=f"RabbitMq: Could not connect with following parameters: {parameters_for_connect}")
        return self._http_cache.register(self._http_connection, alias)

    def close_all_rabbitmq_connections(self) -> None:
        """
        Close all RabbitMq connections.

        This keyword is used to close all connections only in case if there are several open connections.
        Do not use keywords [#Disconnect From Rabbitmq|Disconnect From Rabbitmq] and
        [#Close All Rabbitmq Connections|Close All Rabbitmq Connections] together.\n

        After this keyword is executed the index returned by [#Create Rabbitmq Connection|Create Rabbitmq Connection]
        starts at 1.\n

        *Example:*\n
        | Create Rabbitmq Connection | my_host_name | 15672 | 5672 | guest | guest | alias=rmq |
        | Close All Rabbitmq Connections |
        """
        self._http_cache.close_all()
        self._http_connection = None
        self._amqp_cache.close_all()
        self._amqp_connection = None
        self._channel = None

    def overview(self) -> Dict[str, Any]:
        """ Information about RabbitMq server.

        *Returns:*\n
        Dictionary with information about the server.

        *Raises:*\n
        raise HTTPError if the HTTP request returned an unsuccessful status code.

        *Example:*\n
        | ${overview}=  |  Overview |
        | Log Dictionary | ${overview} |
        | ${version}= | Get From Dictionary | ${overview}  |  rabbitmq_version |
        =>\n
        Dictionary size is 14 and it contains following items:
        | cluster_name | rabbit@primary |
        | contexts | [{'node': 'rabbit@primary', 'path': '/', 'description': 'RabbitMQ Management', 'port': 15672}, {'node': 'rabbit@primary', 'path': '/web-stomp-examples', 'description': 'WEB-STOMP: examples', 'port': 15670}] |
        | erlang_full_version | Erlang R16B03 (erts-5.10.4) [source] [64-bit] [async-threads:30] [kernel-poll:true] |
        | erlang_version | R16B03 |
        | exchange_types | [{'enabled': True, 'name': 'fanout', 'description': 'AMQP fanout exchange, as per the AMQP specification'}, {'internal_purpose': 'federation', 'enabled': True, 'name': 'x-federation-upstream', 'description': 'Federation upstream helper exchange'}, {'enabled': True, 'name': 'direct', 'description': 'AMQP direct exchange, as per the AMQP specification'}, {'enabled': True, 'name': 'headers', 'description': 'AMQP headers exchange, as per the AMQP specification'}, {'enabled': True, 'name': 'topic', 'description': 'AMQP topic exchange, as per the AMQP specification'}, {'enabled': True, 'name': 'x-consistent-hash', 'description': 'Consistent Hashing Exchange'}] |
        | listeners | [{'node': 'rabbit@primary', 'ip_address': '::', 'protocol': 'amqp', 'port': 5672}, {'node': 'rabbit@primary', 'ip_address': '::', 'protocol': 'clustering', 'port': 25672}, {'node': 'rabbit@primary', 'ip_address': '::', 'protocol': 'mqtt', 'port': 1883}, {'node': 'rabbit@primary', 'ip_address': '::', 'protocol': 'stomp', 'port': 61613}] |
        | management_version | 3.3.0 |
        | message_stats | {'publish_details': {'rate': 0.0}, 'confirm': 85, 'deliver_get': 85, 'publish': 85, 'confirm_details': {'rate': 0.0}, 'get_no_ack': 85, 'get_no_ack_details': {'rate': 0.0}, 'deliver_get_details': {'rate': 0.0}} |
        | node | rabbit@primary |
        | object_totals | {'connections': 0, 'channels': 0, 'queues': 2, 'consumers': 0, 'exchanges': 10} |
        | queue_totals | {'messages_details': {'rate': 0.0}, 'messages': 0, 'messages_ready': 0, 'messages_ready_details': {'rate': 0.0}, 'messages_unacknowledged': 0, 'messages_unacknowledged_details': {'rate': 0.0}} |
        | rabbitmq_version | 3.3.0 |
        | statistics_db_node | rabbit@primary |
        | statistics_level | fine |

        ${version} = 3.3.0
        """
        url = self.http_connection.url + '/overview'
        response = requests.get(url, auth=self.http_connection.auth,
                                headers=self._prepare_request_headers(),
                                timeout=self.http_connection.timeout,
                                verify=self.http_connection.verify)
        response.raise_for_status()
        return response.json()

    def _get_channel(self) -> BlockingChannel:
        """ Get channel from current connection.

        *Returns:*\n
            Channel.
        """
        if self._channel is None:
            self._channel = self.amqp_connection.channel()
        if self.amqp_connection.blocked:
            raise Exception('Connection is blocked')
        return self._channel

    def is_queue_exist(self, name: str) -> bool:
        """
        Check if queue exists

        *Args:*\n
        _name_ - queue name

        *Example:*\n
        | ${exist}= | Is Queue Exist | name='queue' |
        | Should Be True | ${exist} |

        *Returns:*\n
        True if queue exists otherwise False
        """
        try:
            self._get_channel().queue_declare(queue=name, passive=True)
            return True
        except ChannelClosed:
            return False

    @utils.timeout()
    def is_rabbit_alive(self):
        # Use explicit timeout so connection attempts fail fast and decorator can retry
        r = requests.get(
            url=f'{self._rabbitmq_url}/api',
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        )
        r.raise_for_status()

        if r.status_code == requests.codes.ok:
            return True

        raise Exception(f'Expected the {requests.codes.ok} http status code')

    @utils.timeout()
    def is_rabbit_alive_with_password(self, password: str):
        r = requests.get(
            url=f'{self._rabbitmq_url}/api',
            auth=(self._user, password),
            verify=self.verify,
            timeout=30
        )
        r.raise_for_status()

        if r.status_code == requests.codes.ok:
            return True
        raise Exception(f'Expected the {requests.codes.ok} http status code')

    @utils.timeout()
    def is_cluster_alive(self, wait_for):
        r = requests.get(
            url=f'{self._rabbitmq_url}/api/nodes',
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        )
        nodes = list(map(lambda x: x['running'], r.json()))
        node_count = nodes.count(True)
        if node_count == int(wait_for):
            return True

        raise Exception(f"Number of nodes must be {wait_for}, but {node_count}")

    @utils.timeout()
    def create_test_user_and_vhost(
            self,
            test_user,
            test_password,
            test_vhost
    ):

        logger.debug('connecting with' + self._user)

        res = requests.put(
            f'{self._rabbitmq_url}/api/vhosts/' + test_vhost,
            auth=(self._user, self._password),
            timeout=30,
            verify=self.verify
        )
        res.raise_for_status()

        time.sleep(20)

        res = requests.put(
            f'{self._rabbitmq_url}/api/users/' + test_user,
            json={'password': test_password, 'tags': 'administrator'},
            auth=(self._user, self._password),
            timeout=20,
            verify=self.verify
        )
        res.raise_for_status()

        res = requests.put(
            f'{self._rabbitmq_url}/api/permissions/{test_vhost}/{test_user}',
            json={'configure': '.*', 'write': '.*', 'read': '.*'},
            auth=(self._user, self._password),
            timeout=60,
            verify=self.verify
        )
        res.raise_for_status()
        return res

    @utils.timeout()
    def delete_test_user(
            self,
            test_user
    ):

        res = requests.delete(
            f'{self._rabbitmq_url}/api/users/{test_user}',
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        )
        res.raise_for_status()
        return res

    @utils.timeout()
    def delete_vhost(
            self,
            test_vhost
    ):

        res = requests.delete(
            f'{self._rabbitmq_url}/api/vhosts/{test_vhost}',
            auth=(self._user, self._password),
            verify=self.verify
        )
        res.raise_for_status()
        return res

    @utils.timeout()
    def create_queue(self, vhost, queue, node_number, queue_type: Optional[str] = None):

        url = f'{self._rabbitmq_url}/api/queues/{vhost}/{queue}'
        if EXTERNAL_ENABLED:
            data = {
                'auto_delete': False,
                'durable': True
            }
        else:
            matched_nodes = list(filter(lambda x: f'rmqlocal-{node_number}' in x, self._get_list_nodes()))

            if len(matched_nodes) != 1:
                raise Exception(f"Number of nodes must be 1, but {matched_nodes}")
            data = {
                'auto_delete': False,
                'durable': True,
                'node': matched_nodes[0]
            }

        if queue_type == 'quorum':
            data['arguments'] = {'x-queue-type': 'quorum'}

        headers = {'Accept': 'application/json'}

        res = requests.put(
            url=url,
            headers=headers,
            auth=(self._user, self._password),
            json=data,
            verify=self.verify,
            timeout=30
        )
        res.raise_for_status()

    @utils.timeout()
    def delete_queue(self, vhost, queue):

        res = requests.delete(
            url=f'{self._rabbitmq_url}/api/queues/{vhost}/{queue}',
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        )
        res.raise_for_status()

    def _get_list_nodes(self):
        r = requests.get(
            url=f'{self._rabbitmq_url}/api/nodes',
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        )
        return list(map(lambda x: x['name'], r.json()))

    def _get_nodes(self):
        r = requests.get(
            url=f'{self._rabbitmq_url}/api/nodes',
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        )
        return r.json()

    def get_users(self):
        r = requests.get(
            url=f'{self._rabbitmq_url}/api/definitions',
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        )

        return list(map(lambda x: x.get('name'), r.json().get('users')))

    @utils.timeout()
    def get_vhosts(self):

        r = requests.get(
            url=f'{self._rabbitmq_url}/api/definitions',
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        )
        return list(map(lambda x: x['name'], r.json().get('vhosts')))

    @utils.timeout()
    def publish_msg(self, vhost, queue):

        payload = {"properties": {}, "routing_key": queue, "payload": "my body", "payload_encoding": "string"}

        r = requests.post(
            f'{self._rabbitmq_url}/api/exchanges/{vhost}/amq.default/publish',
            data=json.dumps(payload),
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        ).status_code
        logger.console('message have published')

        return r

    @utils.timeout()
    def get_msg(self, vhost, queue):

        payload = {"count": 1, "requeue": True, "encoding": "auto", "ackmode": "ack_requeue_true"}

        r = requests.post(
            f'{self._rabbitmq_url}/api/queues/{vhost}/{queue}/get',
            data=json.dumps(payload),
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        )

        if r.status_code == requests.codes.ok:
            return json.loads(r.content)
        else:
            raise Exception(f'Expected the {requests.codes.ok} http status code')

    @staticmethod
    def get_queue_from_msg(msg):

        return msg[0].get('routing_key')

    @utils.timeout()
    def queue_exist(self, vhost, queue):

        r = requests.get(
            f'{self._rabbitmq_url}/api/queues/{vhost}/{queue}',
            auth=(self._user, self._password),
            verify=self.verify,
            timeout=30
        )
        return r.status_code == requests.codes.ok

    # noinspection PyUnreachableCode
    @utils.timeout()
    def check_backup_health(self):
        res = requests.get(self._backuper_url + "/health", verify=self.verify)
        backuper_status = res.status_code
        if backuper_status == 200:
            return True
        else:
            return False

    @utils.timeout()
    def make_rabbitmq_full_backup(self):
        res = requests.post(self._backuper_url + "/backup", verify=self.verify)
        return res.text

    @utils.timeout()
    def make_rabbitmq_granular_backup(self, vhost):
        headers = {'Content-Type': 'application/json'}
        data = '{"dbs":["' + vhost + '"]}'
        res = requests.post(self._backuper_url + "/backup", headers=headers, data=data, verify=self.verify)
        return res.text

    @utils.timeout()
    def make_rabbitmq_not_evictable_backup(self):
        headers = {'Content-Type': 'application/json'}
        data = '{"allow_eviction":"False"}'
        res = requests.post(self._backuper_url + "/backup", headers=headers, data=data, verify=self.verify)
        return res.text

    @utils.timeout()
    def wait_job_success(self, job_name):
        for i in range(40):
            res = requests.get(self._backuper_url + '/jobstatus/' + job_name, verify=self.verify)
            backup_status = json.loads(res.text)['status']
            if backup_status == "Failed":
                raise Exception("backup status is failed")
            if backup_status == "Successful":
                break
            time.sleep(10)

    @utils.timeout()
    def make_rabbitmq_full_restore(self, vault_name):
        headers = {'Content-Type': 'application/json'}
        data = '{"vault":"' + vault_name + '"}'
        res = requests.post(self._backuper_url + "/restore", headers=headers, data=data, verify=self.verify)
        return res.text

    @utils.timeout()
    def make_rabbitmq_granular_restore(self, vault_name, vhost):
        headers = {'Content-Type': 'application/json'}
        data = '{"vault":"' + vault_name + '", "dbs":["' + vhost + '"]}'
        res = requests.post(self._backuper_url + "/restore", headers=headers, data=data, verify=self.verify)
        return res.text

    @utils.timeout()
    def evict_vault(self, vault_name):
        res = requests.post(self._backuper_url + "/evict/" + vault_name, verify=self.verify)
        return res.text

    @utils.timeout()
    def check_list_of_backups(self):
        res = requests.get(self._backuper_url + "/listbackups", verify=self.verify)
        return res.text

    @utils.timeout()
    def check_backup_information(self, vault_name):
        res = requests.get(self._backuper_url + "/listbackups/" + vault_name, verify=self.verify)
        return res.text

    def bulk_make_rabbitmq_queues(self, count, vhost, queue, node_number):
        if EXTERNAL_ENABLED:
            data = {
                'auto_delete': False,
                'durable': True
            }
        else:
            matched_nodes = list(filter(lambda x: f'rmqlocal-{node_number}' in x, self._get_list_nodes()))

            if len(matched_nodes) != 1:
                raise Exception(f"Number of nodes must be 1, but {matched_nodes}")

            data = {'auto_delete': False, 'durable': True, 'node': matched_nodes[0]}
        headers = {'Accept': 'application/json'}
        for i in range(int(count)):
            url = f'{self._rabbitmq_url}/api/queues/{vhost}/{queue}{i}'
            res = requests.put(
                url=url,
                headers=headers,
                auth=(self._user, self._password),
                json=data,
                verify=self.verify
            )
            res.raise_for_status()

    def bulk_delete_queue(self, count, vhost, queue):
        for i in range(int(count)):
            res = requests.delete(
                url=f'{self._rabbitmq_url}/api/queues/{vhost}/{queue}{i}',
                auth=(self._user, self._password),
                verify=self.verify
            )
            res.raise_for_status()

    @utils.timeout()
    def bulk_queue_exist(self, count, vhost, queue):
        for i in range(int(count)):
            r = requests.get(
                f'{self._rabbitmq_url}/api/queues/{vhost}/{queue}{i}',
                auth=(self._user, self._password),
                verify=self.verify
            )
            if r.status_code != 200:
                return False
        return True
