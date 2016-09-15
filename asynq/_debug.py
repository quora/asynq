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

import sys
from qcore.debug import get_bool_by_mask, set_by_mask


class DebugOptions(object):
    """
    All async debug options are stored in this structure.

    The class itself is necessary mainly by performance reasons:
    we want to avoid dictionary lookups on checks of debug options.

    """
    def __init__(self):
        self.DUMP_PRE_ERROR_STATE  = False
        self.DUMP_EXCEPTIONS       = False
        self.DUMP_AWAIT_RECURSION  = False
        self.DUMP_SCHEDULE_TASK    = False
        self.DUMP_CONTINUE_TASK    = False
        self.DUMP_SCHEDULE_BATCH   = False
        self.DUMP_FLUSH_BATCH      = False
        self.DUMP_DEPENDENCIES     = False
        self.DUMP_COMPUTED         = False
        self.DUMP_NEW_TASKS        = False
        self.DUMP_YIELD_RESULTS    = False
        self.DUMP_QUEUED_RESULTS   = False
        self.DUMP_CONTEXTS         = False
        self.DUMP_SCHEDULER_CHANGE = False
        self.DUMP_SYNC             = False
        self.DUMP_PRIMER           = False
        self.DUMP_STACK            = False  # When it's meaningful, e.g. on batch flush
        self.DUMP_SCHEDULER_STATE  = False

        self.SCHEDULER_STATE_DUMP_INTERVAL = 1     # In seconds
        self.DEBUG_STR_REPR_MAX_LENGTH     = 240   # In characters, 0 means infinity
        self.STACK_DUMP_LIMIT              = 0     # In frames, None means infinity

        self.ENABLE_COMPLEX_ASSERTIONS = True

    def DUMP_ALL(self, value=None):
        if value is None:
            return get_bool_by_mask(self, "DUMP_")
        set_by_mask(self, "DUMP_", value)


options = DebugOptions()
globals()['options'] = options
