import unittest
from datetime import datetime
from unittest.mock import patch

from database.market_calendar_db import IST, is_market_open


class TestMarketOpen(unittest.TestCase):

    def set_mock_time(self, year: int, month: int, day: int, hour: int, minute: int):
        """Helper to mock datetime.now() with timezone-aware datetime"""
        target_time = IST.localize(datetime(year, month, day, hour, minute))

        mock_dt = patch("database.market_calendar_db.datetime")
        mock_obj = mock_dt.start()
        mock_obj.now.return_value = target_time
        mock_obj.combine = datetime.combine
        mock_obj.strptime = datetime.strptime
        mock_obj.min = datetime.min
        self.addCleanup(mock_dt.stop)

    def test_regular_trading_hours(self):
        # Regular Monday at 10:00 AM EDT for US Equities
        self.set_mock_time(2026, 9, 21, 10, 0)
        self.assertTrue(is_market_open("US"))

    def test_full_holiday_closure(self):
        # Christmas Day 2026-12-25
        self.set_mock_time(2026, 12, 25, 11, 0)
        self.assertFalse(is_market_open("US"))
        self.assertFalse(is_market_open("NSE"))

    def test_early_close_day(self):
        # Day After Thanksgiving: Nov 27, 2026 (US closes at 13:00 EDT)
        # 11:00 AM -> Open
        self.set_mock_time(2026, 11, 27, 11, 0)
        self.assertTrue(is_market_open("US"))

        # 14:00 PM -> Closed (after 13:00 close)
        self.set_mock_time(2026, 11, 27, 14, 0)
        self.assertFalse(is_market_open("US"))

    def test_partial_holiday_mcx_evening(self):
        # Ganesh Chaturthi: Sept 14, 2026
        # MCX evening session is 17:00-23:55 IST (07:30-14:25 EDT)
        # At 10:00 AM EDT (19:30 IST): NSE is closed, MCX is open
        self.set_mock_time(2026, 9, 14, 10, 0)
        self.assertFalse(is_market_open("NSE"))
        self.assertTrue(is_market_open("MCX"))

    def test_crypto_always_open(self):
        # Sunday at midnight
        self.set_mock_time(2026, 9, 20, 0, 0)
        self.assertTrue(is_market_open("CRYPTO"))


if __name__ == "__main__":
    unittest.main()