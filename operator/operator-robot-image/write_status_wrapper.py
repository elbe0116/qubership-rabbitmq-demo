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
# Wrapper for write_status.py: runs base image write_status.py (used when statusWritingEnabled is true).

import os
import runpy
import sys

robot_home = os.environ.get("ROBOT_HOME", "/opt/robot")
write_status_path = os.path.join(robot_home, "write_status.py")
sys.argv[0] = write_status_path
runpy.run_path(write_status_path, run_name="__main__")
