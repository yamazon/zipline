# Copyright 2016 Quantopian, Inc.
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
from abc import ABCMeta, abstractmethod, abstractproperty
from six import with_metaclass


class SessionBarReader(with_metaclass(ABCMeta)):
    """
    Reader for OHCLV pricing data at a daily frequency.
    """
    @abstractmethod
    def load_raw_arrays(self, columns, start_date, end_date, assets):
        pass

    @abstractmethod
    def spot_price(self, sid, day, colname):
        pass

    @abstractproperty
    def sessions(self):
        pass

    @abstractproperty
    def last_available_dt(self):
        pass

    @abstractproperty
    def trading_calendar(self):
        """
        Returns the zipline.utils.calendar.trading_calendar used to read
         the data.  Can be None (if the writer didn't specify it).
        """
        pass
