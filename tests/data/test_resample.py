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
from pandas import (
    date_range,
    DataFrame,
    Timestamp
)

from zipline.data.resample import MinuteResampleSessionBarReader

from zipline.testing.fixtures import (
    WithTradingCalendars,
    WithBcolzFutureMinuteBarReader,
    ZiplineTestCase
)


class TestResampleSessionBars(WithBcolzFutureMinuteBarReader,
                              WithTradingCalendars,
                              ZiplineTestCase):

    TRADING_CALENDAR_STRS = ('CME',)

    _minutes_a = date_range('2016-03-15 3:31',
                            '2016-03-15 3:36',
                            freq='min',
                            tz='US/Eastern').tz_convert('UTC')

    _minutes_b = date_range('2016-03-16 3:31',
                            '2016-03-16 3:36',
                            freq='min',
                            tz='US/Eastern').tz_convert('UTC')

    @classmethod
    def init_class_fixtures(cls):
        cls.minutes = cls._minutes_a.union(cls._minutes_b)
        super(TestResampleSessionBars, cls).init_class_fixtures()

    START_DATE = Timestamp('2016-03-15')
    END_DATE = Timestamp('2016-03-16')

    @classmethod
    def make_future_minute_bar_data(cls):
        yield 1, DataFrame({
            'open': [nan, 103.50, 102.50, 104.50, 101.50, nan,
                     nan, 113.50, 112.50, 114.50, 111.50, nan],
            'high': [nan, 103.90, 102.90, 104.90, 101.90, nan,
                     nan, 113.90, 112.90, 114.90, 111.90, nan],
            'low': [nan, 103.10, 102.10, 104.10, 101.10, nan,
                    nan, 113.10, 112.10, 114.10, 111.10, nan],
            'close': [nan, 103.30, 102.30, 104.30, 101.30, nan,
                      nan, 103.30, 102.30, 104.30, 101.30, nan],
            'volume': [0, 1003, 1002, 1004, 1001, 0,
                       0, 10013, 1012, 1014, 1011, 0]
        },
            index=cls.minutes,
        )

    def test_resample(self):
        calendar = self.trading_calendars['CME']

        session = calendar.minute_to_session_label(self.minutes[0])
        m_open, _ = calendar.open_and_close_for_session(session)
        session = calendar.minute_to_session_label(self.minutes[-1])
        _, m_close = calendar.open_and_close_for_session(session)
        session_bar_reader = MinuteResampleSessionBarReader(
            calendar,
            self.bcolz_future_minute_bar_reader
        )
        result = session_bar_reader.load_raw_arrays(
            ['open', 'high', 'low', 'close', 'volume'],
            m_open, m_close, [1])

        import nose; nose.tools.set_trace()
        assert True
