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
from numpy import nan
from pandas import DataFrame, date_range
from six import iteritems

from zipline.data.minute_bars import MinuteBarReader
from zipline.data.resample import MinuteResampleSessionBarReader
from zipline.utils.memoize import lazyval

from zipline.testing.fixtures import (
    WithTradingCalendars,
    ZiplineTestCase
)


class DataFrameMinuteBarReader(MinuteBarReader):
    def __init__(self, calendar, frames):
        self._calendar = calendar
        self._frames = frames

    @lazyval
    def last_available_dt(self):
        last_dts = [frame.index[-1] for _, frame in iteritems(self._frames)]
        return max(last_dts)

    @lazyval
    def first_trading_day(self):
        pass

    def get_value(self, sid, dt, field):
        return self._frames[sid].ix[dt, field]

    def get_last_traded_dt(self, asset, dt):
        frame = self._frames[asset]
        loc = frame.index.searchsorted(dt)
        volume = frame['volume'].iloc[loc]
        while volume != 0 and loc != 0:
            loc -= 1
            volume = frame['volume'].iloc[loc]
        return frame.index[loc]

    def load_raw_arrays(self, fields, start_dt, end_dt, sids):
        results = []

        for field in fields:
            if field != 'volume':
                out = np.full(shape, np.nan)
            else:
                out = np.zeros(shape, dtype=np.uint32)

            for i, sid in enumerate(sids):
                carray = self._open_minute_file(field, sid)
                values = carray[start_idx:end_idx + 1]
                if indices_to_exclude is not None:
                    for excl_start, excl_stop in indices_to_exclude[::-1]:
                        excl_slice = np.s_[
                            excl_start - start_idx:excl_stop - start_idx + 1]
                        values = np.delete(values, excl_slice)
                where = values != 0
                # first slice down to len(where) because we might not have
                # written data for all the minutes requested
                out[:len(where), i][where] = values[where]
            if field != 'volume':
                out *= self._ohlc_inverse
            results.append(out)

        return results


class TestResampleSessionBars(WithTradingCalendars,
                              ZiplineTestCase):

    TRADING_CALENDAR_STRS = ('CME',)

    def test_resample(self):
        calendar = self.trading_calendars['CME']
        minutes = date_range('2016-03-15 3:31',
                             '2016-03-15 3:36',
                             freq='min',
                             tz='US/Eastern').tz_convert('UTC')
        session = calendar.minute_to_session_label(minutes[0])
        frames = {}
        frames[1] = DataFrame({
            'open': [nan, 103.50, 102.50, 104.50, 101.50, nan],
            'high': [nan, 103.90, 102.90, 104.90, 101.90, nan],
            'low': [nan, 103.10, 102.10, 104.10, 101.10, nan],
            'close': [nan, 103.30, 102.30, 104.30, 101.30, nan],
            'volume': [0, 1003, 1002, 1004, 1001, 0]
        },
            index=minutes,
        )
        minute_bar_reader = DataFrameMinuteBarReader(calendar, frames)

        session_bar_reader = MinuteResampleSessionBarReader(minute_bar_reader)

        result = session_bar_reader.load_raw_arrays(
            ['open'], session, session, [1])

        import nose; nose.tools.set_trace()
        assert True
