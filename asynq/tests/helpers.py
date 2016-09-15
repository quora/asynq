# Copyright 2016 Quora, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time


class Profiler(object):
    def __init__(self, label):
        self.label = label
        self.diff = 0

    def __enter__(self):
        print('Entering %s:' % self.label)
        self.start = time.time()

    def __exit__(self, type, value, traceback):
        self.diff = time.time() - self.start
        print('{0} took {1:.3f}s'.format(self.label, self.diff))
