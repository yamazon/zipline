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
from pandas import DataFrame

from zipline.data.session_bars import SessionBarReader
from zipline.utils.memoize import lazyval

_MINUTE_TO_SESSION_OHCLV_HOW = {
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last',
    'volume': 'sum'
}


def minute_to_session(minute_frame, calendar):
    """
    Resample a DataFrame with minute data into the frame expected by a
    BcolzDailyBarWriter.

    Parameters
    ----------
    minute_frame : pd.DataFrame
        A DataFrame with the columns `open`, `high`, `low`, `close`, `volume`,
        and `dt` (minute dts)
    calendar : zipline.utils.calendars.trading_calendar.TradingCalendar
        A TradingCalendar on which session labels to resample from minute
        to session.

    Return
    ------
    session_frame : pd.DataFrame
        A DataFrame with the columns `open`, `high`, `low`, `close`, `volume`,
        and `day` (datetime-like).
    """
    return minute_frame.dropna().groupby(calendar.minute_to_session_label).agg(
        _MINUTE_TO_SESSION_OHCLV_HOW)


class MinuteResampleSessionBarReader(SessionBarReader):

    def __init__(self, calendar, minute_bar_reader):
        self.calendar = calendar
        self._minute_bar_reader = minute_bar_reader

    def load_raw_arrays(self, columns, start_date, end_date, assets):
        # TODO: Do we need to get open and close?
        minute_data = self._minute_bar_reader.load_raw_arrays(
            columns, start_date, end_date, assets)
        dts = self.calendar.minutes_in_range(start_date, end_date)
        frame = DataFrame(
            [d.T[0] for d in minute_data], index=columns, columns=dts).T
        # Now need to resample here and then return result.
        return minute_to_session(frame, self.calendar)

    def spot_price(self, sid, day, colname):
        pass

    @lazyval
    def sessions(self):
        cal = self.trading_calendar
        first = self._minute_bar_reader.first_trading_day
        last = cal.minute_to_session_label(
            self._minute_bar_reader.last_available_dt)
        return cal.sessions_in_range(first, last)

    @lazyval
    def last_available_dt(self):
        self.trading_calendar.minute_to_session_label(
            self._minute_bar_reader.last_available_dt
        )

    def trading_calendar(self):
        """
        Returns the zipline.utils.calendar.trading_calendar used to read
         the data.  Can be None (if the writer didn't specify it).
        """
        return self._minute_bar_reader.trading_calendar
