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
#
import os
import time

from PlatformLibrary import PlatformLibrary


def _strtobool(val):
    """Replace distutils.util.strtobool (removed in Python 3.12+). Returns 1 for true, 0 for false."""
    v = (val or "").lower()
    if v in ("y", "yes", "t", "true", "1", "on"):
        return 1
    if v in ("n", "no", "f", "false", "0", "off"):
        return 0
    raise ValueError(f"invalid truth value {val!r}")


environ = os.environ
auto_reboot = environ.get('RABBITMQ_AUTO_REBOOT', 'false').lower() in ("yes", "true", "t", "1")
external_enabled = _strtobool(environ.get('EXTERNAL_ENABLED', 'false'))
namespace = environ.get('NAMESPACE')
initial_sleep = 120 if auto_reboot else 40
service = 'rmqlocal'
timeout = 300

if __name__ == '__main__':
    time.sleep(initial_sleep)
    if external_enabled:
        print('No need to check readiness of external RabbitMQ pods')
        exit(0)
    print('Checking RabbitMQ pods are ready')
    try:
        k8s_lib = PlatformLibrary(managed_by_operator='true')
    except Exception as e:
        print(e)
        exit(1)
    start = time.time()
    while time.time() < start + timeout:
        replicas = 0
        ready_replicas = 0
        updated_replicas = 0
        try:
            stateful_set_names = k8s_lib.get_stateful_set_names_by_label(namespace=namespace,
                                                                         label_value=service,
                                                                         label_name='app')
            for stateful_set_name in stateful_set_names:
                stateful_set = k8s_lib.get_stateful_set(stateful_set_name, namespace)
                replicas += stateful_set.status.replicas if stateful_set.status.replicas else 0
                ready_replicas += stateful_set.status.ready_replicas if stateful_set.status.ready_replicas else 0
                updated_replicas += stateful_set.status.updated_replicas if stateful_set.status.updated_replicas else 0
            print(f'[Check status] RabbitMQ pods: {replicas}, ready pods: {ready_replicas}, '
                  f'updated pods: {updated_replicas}')
        except Exception as e:
            print(e)
            exit(1)
        if replicas != 0 and replicas == ready_replicas and (not auto_reboot or replicas == updated_replicas):
            print('RabbitMQ pods are ready')
            time.sleep(30)
            exit(0)
        time.sleep(10)
    print('RabbitMQ pods are not ready')
    exit(1)
