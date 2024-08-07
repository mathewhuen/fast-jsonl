# Copyright 2024 Mathew Huerta-Enochian
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

r"""
Library-wide constants.

Environment variable constants:

* FAST_JSONL_DIR_METHOD: If set to "user", fast-jsonl cache files will be saved to `<user-home>/.local/share/fj_cache/`\. If set to "local", fast-jsonl cache files will be saved to `<jsonl-parent-directory>/.fj_cache/`\. Defaults to "user" if not specified.

"""

import os


DIR_METHOD_ENV = "FAST_JSONL_DIR_METHOD"
DEFAULT_DIR_METHOD = "user"
DIR_METHOD = os.environ.get(DIR_METHOD_ENV, DEFAULT_DIR_METHOD)
