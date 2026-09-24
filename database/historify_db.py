# database/historify_db.py
"""
Historify DuckDB Database Module

High-performance columnar storage for historical market data.
Optimized for backtesting and analytical queries.
"""

import os
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

from utils.logging import get_logger

# Initialize logger
logger = get_logger(__name__)

# Load environment variables
load_dotenv()

# Read target timezone string directly from environment (defaults to Asia/Kolkata)
TIMEZONE_STR = os.getenv("TIMEZONE", "Asia/Kolkata")

# Database path - in /db folder like other OpenAlgo databases
HISTORIFY_DB_PATH = os.getenv("HISTORIFY_DATABASE_PATH", "db/historify.duckdb")


def get_db_path() -> str:
    """Get absolute path to the DuckDB database file."""
    if os.path.isabs(HISTORIFY_DB_PATH):
        return HISTORIFY_DB_PATH
    # Relative to the openalgo directory
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, HISTORIFY_DB_PATH)


def ensure_db_directory():
    """Ensure the database directory exists."""
    db_path = get_db_path()
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        logger.info(f"Created database directory: {db_dir}")


@contextmanager
def get_connection(max_retries: int = 3, retry_delay: float = 0.5):
    """
    Get a DuckDB connection with proper resource management and retry logic.

    DuckDB uses exclusive file locking on Windows. This function includes retry
    logic to handle temporary file access conflicts in concurrent scenarios.

    Args:
        max_retries: Maximum number of connection attempts (default: 3)
        retry_delay: Delay in seconds between retries (default: 0.5)

    Usage:
        with get_connection() as conn:
            result = conn.execute("SELECT * FROM market_data").fetchdf()
    """
    import time

    ensure_db_directory()
    db_path = get_db_path()
    conn = None
    last_error = None

    for attempt in range(max_retries):
        try:
            import duckdb

            conn = duckdb.connect(db_path)
            break
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                logger.debug(f"DuckDB connection attempt {attempt + 1} failed, retrying: {e}")
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
            else:
                logger.exception(f"Failed to connect to DuckDB after {max_retries} attempts: {e}")

    if conn is None:
        raise last_error or Exception("Failed to connect to DuckDB")

    try:
        yield conn
    finally:
        conn.close()


def init_database():
    """
    Initialize the Historify database schema.
    Creates all required tables if they don't exist.
    """
    ensure_db_directory()

    with get_connection() as conn:
        # Main OHLCV data table - unified table approach
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                symbol VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                interval VARCHAR NOT NULL,
                timestamp BIGINT NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume BIGINT NOT NULL,
                oi BIGINT DEFAULT 0,
                created_at TIMESTAMP DEFAULT current_timestamp,
                PRIMARY KEY (symbol, exchange, interval, timestamp)
            )
        """)

        # Watchlist table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY,
                symbol VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                display_name VARCHAR,
                added_at TIMESTAMP DEFAULT current_timestamp,
                UNIQUE (symbol, exchange)
            )
        """)

        # Data catalog for tracking downloaded data ranges
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_catalog (
                id INTEGER PRIMARY KEY,
                symbol VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                interval VARCHAR NOT NULL,
                first_timestamp BIGINT,
                last_timestamp BIGINT,
                record_count BIGINT DEFAULT 0,
                last_download_at TIMESTAMP,
                UNIQUE (symbol, exchange, interval)
            )
        """)

        # Download Jobs Table - for tracking bulk operations
        conn.execute("""
            CREATE TABLE IF NOT EXISTS download_jobs (
                id VARCHAR PRIMARY KEY,
                job_type VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                total_symbols INTEGER DEFAULT 0,
                completed_symbols INTEGER DEFAULT 0,
                failed_symbols INTEGER DEFAULT 0,
                interval VARCHAR,
                start_date VARCHAR,
                end_date VARCHAR,
                config VARCHAR,
                created_at TIMESTAMP DEFAULT current_timestamp,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message VARCHAR
            )
        """)

        # Job Items Table - individual symbol status within a job
        conn.execute("""
            CREATE TABLE IF NOT EXISTS job_items (
                id INTEGER PRIMARY KEY,
                job_id VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                records_downloaded INTEGER DEFAULT 0,
                error_message VARCHAR,
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)

        # Symbol Metadata Table - enriched symbol info for display
        conn.execute("""
            CREATE TABLE IF NOT EXISTS symbol_metadata (
                symbol VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                name VARCHAR,
                expiry VARCHAR,
                strike DOUBLE,
                lotsize INTEGER,
                instrumenttype VARCHAR,
                tick_size DOUBLE,
                last_updated TIMESTAMP DEFAULT current_timestamp,
                PRIMARY KEY (symbol, exchange)
            )
        """)

        # Scheduler Tables
        # Schedule configurations
        conn.execute("""
            CREATE TABLE IF NOT EXISTS historify_schedules (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                description VARCHAR,
                schedule_type VARCHAR NOT NULL,
                interval_value INTEGER,
                interval_unit VARCHAR,
                time_of_day VARCHAR,
                download_source VARCHAR DEFAULT 'watchlist',
                data_interval VARCHAR NOT NULL,
                lookback_days INTEGER DEFAULT 1,
                is_enabled BOOLEAN DEFAULT TRUE,
                is_paused BOOLEAN DEFAULT FALSE,
                status VARCHAR DEFAULT 'idle',
                apscheduler_job_id VARCHAR,
                created_at TIMESTAMP DEFAULT current_timestamp,
                last_run_at TIMESTAMP,
                next_run_at TIMESTAMP,
                last_run_status VARCHAR,
                total_runs INTEGER DEFAULT 0,
                successful_runs INTEGER DEFAULT 0,
                failed_runs INTEGER DEFAULT 0
            )
        """)

        # Execution history
        conn.execute("""
            CREATE TABLE IF NOT EXISTS historify_schedule_executions (
                id INTEGER PRIMARY KEY,
                schedule_id VARCHAR NOT NULL,
                download_job_id VARCHAR,
                status VARCHAR NOT NULL,
                started_at TIMESTAMP DEFAULT current_timestamp,
                completed_at TIMESTAMP,
                symbols_processed INTEGER DEFAULT 0,
                symbols_success INTEGER DEFAULT 0,
                symbols_failed INTEGER DEFAULT 0,
                records_downloaded INTEGER DEFAULT 0,
                error_message VARCHAR
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_job_items_job_id
            ON job_items (job_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_download_jobs_status
            ON download_jobs (status)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_historify_schedules_enabled
            ON historify_schedules (is_enabled, is_paused)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_historify_schedule_executions_schedule_id
            ON historify_schedule_executions (schedule_id)
        """)

        logger.debug("Historify database initialized successfully")


# =============================================================================
# Watchlist Operations
# =============================================================================


def get_watchlist() -> list[dict[str, Any]]:
    """Get all symbols in the watchlist."""
    with get_connection() as conn:
        result = conn.execute("""
            SELECT id, symbol, exchange, display_name, added_at
            FROM watchlist
            ORDER BY added_at DESC
        """).fetchdf()

        if result.empty:
            return []

        return result.to_dict("records")


def add_to_watchlist(symbol: str, exchange: str, display_name: str = None) -> tuple[bool, str]:
    """
    Add a symbol to the watchlist.

    Returns:
        Tuple of (success, message)
    """
    try:
        with get_connection() as conn:
            existing = conn.execute(
                """
                SELECT id FROM watchlist WHERE symbol = ? AND exchange = ?
            """,
                [symbol.upper(), exchange.upper()],
            ).fetchone()

            if existing:
                return True, f"{symbol} already in watchlist"

            result = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM watchlist").fetchone()
            next_id = result[0] if result else 1

            conn.execute(
                """
                INSERT INTO watchlist (id, symbol, exchange, display_name)
                VALUES (?, ?, ?, ?)
            """,
                [next_id, symbol.upper(), exchange.upper(), display_name],
            )

        logger.info(f"Added {symbol}:{exchange} to watchlist")
        return True, f"Added {symbol} to watchlist"
    except Exception as e:
        logger.exception(f"Error adding to watchlist: {e}")
        return False, str(e)


def bulk_add_to_watchlist(symbols: list[dict[str, str]]) -> tuple[int, int, list[dict[str, str]]]:
    """
    Add multiple symbols to the watchlist in a single transaction.

    Args:
        symbols: List of dicts with 'symbol', 'exchange', and optional 'display_name' keys

    Returns:
        Tuple of (added_count, skipped_count, failed_list)
    """
    added = 0
    skipped = 0
    failed = []

    try:
        with get_connection() as conn:
            existing_result = conn.execute("""
                SELECT symbol, exchange FROM watchlist
            """).fetchall()
            existing_set = {(row[0], row[1]) for row in existing_result}

            max_id_result = conn.execute("SELECT COALESCE(MAX(id), 0) FROM watchlist").fetchone()
            next_id = max_id_result[0] + 1

            records_to_insert = []
            for item in symbols:
                symbol = item.get("symbol", "").upper()
                exchange = item.get("exchange", "").upper()
                display_name = item.get("display_name")

                if not symbol or not exchange:
                    failed.append(
                        {
                            "symbol": symbol,
                            "exchange": exchange,
                            "error": "Missing symbol or exchange",
                        }
                    )
                    continue

                if (symbol, exchange) in existing_set:
                    skipped += 1
                    continue

                records_to_insert.append((next_id, symbol, exchange, display_name))
                existing_set.add((symbol, exchange))
                next_id += 1

            if records_to_insert:
                conn.executemany(
                    """
                    INSERT INTO watchlist (id, symbol, exchange, display_name)
                    VALUES (?, ?, ?, ?)
                """,
                    records_to_insert,
                )
                added = len(records_to_insert)

        logger.info(f"Bulk added {added} symbols to watchlist (skipped {skipped} existing)")
        return added, skipped, failed

    except Exception as e:
        logger.exception(f"Error bulk adding to watchlist: {e}")
        return 0, 0, [{"symbol": "batch", "exchange": "", "error": str(e)}]


def remove_from_watchlist(symbol: str, exchange: str) -> tuple[bool, str]:
    """
    Remove a symbol from the watchlist.

    Returns:
        Tuple of (success, message)
    """
    try:
        with get_connection() as conn:
            conn.execute(
                """
                DELETE FROM watchlist
                WHERE symbol = ? AND exchange = ?
            """,
                [symbol.upper(), exchange.upper()],
            )

        logger.info(f"Removed {symbol}:{exchange} from watchlist")
        return True, f"Removed {symbol} from watchlist"
    except Exception as e:
        logger.exception(f"Error removing from watchlist: {e}")
        return False, str(e)


def bulk_remove_from_watchlist(
    symbols: list[dict[str, str]],
) -> tuple[int, int, list[dict[str, str]]]:
    """
    Remove multiple symbols from the watchlist in a single transaction.

    Args:
        symbols: List of dicts with 'symbol' and 'exchange' keys

    Returns:
        Tuple of (removed_count, skipped_count, failed_list)
    """
    removed = 0
    skipped = 0
    failed = []

    try:
        with get_connection() as conn:
            existing_result = conn.execute("""
                SELECT symbol, exchange FROM watchlist
            """).fetchall()
            existing_set = {(row[0], row[1]) for row in existing_result}

            for item in symbols:
                symbol = item.get("symbol", "").upper()
                exchange = item.get("exchange", "").upper()

                if not symbol or not exchange:
                    failed.append({
                        "symbol": symbol or "MISSING",
                        "exchange": exchange or "MISSING",
                        "error": "Missing symbol or exchange",
                    })
                    continue

                if (symbol, exchange) not in existing_set:
                    skipped += 1
                    continue

                try:
                    conn.execute(
                        """
                        DELETE FROM watchlist
                        WHERE symbol = ? AND exchange = ?
                        """,
                        [symbol, exchange],
                    )
                    removed += 1
                    existing_set.discard((symbol, exchange))
                except Exception as e:
                    failed.append({
                        "symbol": symbol,
                        "exchange": exchange,
                        "error": str(e),
                    })

        logger.info(f"Bulk watchlist remove: {removed} removed, {skipped} skipped, {len(failed)} failed")
        return removed, skipped, failed

    except Exception as e:
        logger.exception(f"Error in bulk remove from watchlist: {e}")
        return 0, 0, [{"symbol": "ALL", "exchange": "ALL", "error": str(e)}]


def clear_watchlist() -> tuple[bool, str]:
    """Clear all symbols from watchlist."""
    try:
        with get_connection() as conn:
            conn.execute("DELETE FROM watchlist")
        logger.info("Cleared watchlist")
        return True, "Watchlist cleared"
    except Exception as e:
        logger.exception(f"Error clearing watchlist: {e}")
        return False, str(e)


# =============================================================================
# Market Data Operations
# =============================================================================


def upsert_market_data(df: pd.DataFrame, symbol: str, exchange: str, interval: str) -> int:
    """
    Insert or update OHLCV data from a pandas DataFrame.

    Args:
        df: DataFrame with columns: timestamp, open, high, low, close, volume, oi (optional)
        symbol: Trading symbol
        exchange: Exchange code
        interval: Time interval (1m, 5m, 15m, 30m, 1h, D)

    Returns:
        Number of records inserted/updated
    """
    if df.empty:
        return 0

    try:
        df = df.copy()
        df["symbol"] = symbol.upper()
        df["exchange"] = exchange.upper()
        df["interval"] = interval

        if "oi" not in df.columns:
            df["oi"] = 0

        if df["timestamp"].dtype != "int64":
            df["timestamp"] = pd.to_datetime(df["timestamp"]).astype("int64") // 10**9

        df = df[
            [
                "symbol",
                "exchange",
                "interval",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "oi",
            ]
        ]

        with get_connection() as conn:
            conn.execute("""
                INSERT INTO market_data
                (symbol, exchange, interval, timestamp, open, high, low, close, volume, oi)
                SELECT symbol, exchange, interval, timestamp, open, high, low, close, volume, oi
                FROM df
                ON CONFLICT (symbol, exchange, interval, timestamp) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    oi = EXCLUDED.oi
            """)

            existing = conn.execute(
                """
                SELECT id FROM data_catalog
                WHERE symbol = ? AND exchange = ? AND interval = ?
            """,
                [symbol.upper(), exchange.upper(), interval],
            ).fetchone()

            if existing:
                conn.execute(
                    """
                    UPDATE data_catalog SET
                        first_timestamp = (SELECT MIN(timestamp) FROM market_data
                                          WHERE symbol = ? AND exchange = ? AND interval = ?),
                        last_timestamp = (SELECT MAX(timestamp) FROM market_data
                                         WHERE symbol = ? AND exchange = ? AND interval = ?),
                        record_count = (SELECT COUNT(*) FROM market_data
                                       WHERE symbol = ? AND exchange = ? AND interval = ?),
                        last_download_at = current_timestamp
                    WHERE symbol = ? AND exchange = ? AND interval = ?
                """,
                    [
                        symbol.upper(),
                        exchange.upper(),
                        interval,
                        symbol.upper(),
                        exchange.upper(),
                        interval,
                        symbol.upper(),
                        exchange.upper(),
                        interval,
                        symbol.upper(),
                        exchange.upper(),
                        interval,
                    ],
                )
            else:
                next_id_result = conn.execute(
                    "SELECT COALESCE(MAX(id), 0) + 1 FROM data_catalog"
                ).fetchone()
                next_id = next_id_result[0] if next_id_result else 1

                conn.execute(
                    """
                    INSERT INTO data_catalog
                    (id, symbol, exchange, interval, first_timestamp, last_timestamp,
                     record_count, last_download_at)
                    SELECT
                        ?, ?, ?, ?,
                        MIN(timestamp), MAX(timestamp), COUNT(*),
                        current_timestamp
                    FROM market_data
                    WHERE symbol = ? AND exchange = ? AND interval = ?
                """,
                    [
                        next_id,
                        symbol.upper(),
                        exchange.upper(),
                        interval,
                        symbol.upper(),
                        exchange.upper(),
                        interval,
                    ],
                )

        logger.info(f"Upserted {len(df)} records for {symbol}:{exchange}:{interval}")
        return len(df)

    except Exception as e:
        logger.exception(f"Error upserting market data: {e}")
        raise


STORAGE_INTERVALS = {"1m", "D"}
COMPUTED_INTERVALS = {"5m", "15m", "30m", "1h"}
INTERVAL_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
}


def parse_interval(interval: str) -> dict[str, Any] | None:
    """Parse an interval string into its components."""
    import re

    if not interval:
        return None

    interval = interval.strip()

    if interval == "D":
        return {"type": "daily", "days": 1, "value": 1, "unit": "D"}
    if interval == "W":
        return {"type": "weekly", "days": 7, "value": 1, "unit": "W"}
    if interval == "M":
        return {"type": "monthly", "months": 1, "value": 1, "unit": "M"}
    if interval == "Q":
        return {"type": "quarterly", "months": 3, "value": 1, "unit": "Q"}
    if interval == "Y":
        return {"type": "yearly", "months": 12, "value": 1, "unit": "Y"}

    match = re.match(r"^(\d+)([mhDWMQY])$", interval)
    if not match:
        return None

    value = int(match.group(1))
    unit = match.group(2)

    if value <= 0:
        return None

    if unit == "m":
        return {"type": "intraday", "minutes": value, "value": value, "unit": "m"}
    elif unit == "h":
        return {"type": "intraday", "minutes": value * 60, "value": value, "unit": "h"}
    elif unit == "D":
        return {"type": "daily", "days": value, "value": value, "unit": "D"}
    elif unit == "W":
        return {"type": "weekly", "days": value * 7, "value": value, "unit": "W"}
    elif unit == "M":
        return {"type": "monthly", "months": value, "value": value, "unit": "M"}
    elif unit == "Q":
        return {"type": "quarterly", "months": value * 3, "value": value, "unit": "Q"}
    elif unit == "Y":
        return {"type": "yearly", "months": value * 12, "value": value, "unit": "Y"}

    return None


def is_custom_interval(interval: str) -> bool:
    """Check if an interval is a custom intraday interval."""
    if interval in STORAGE_INTERVALS:
        return False

    parsed = parse_interval(interval)
    if not parsed:
        return False

    return parsed["type"] == "intraday"


def is_daily_aggregated_interval(interval: str) -> bool:
    """Check if an interval needs aggregation from Daily (D) data."""
    parsed = parse_interval(interval)
    if not parsed:
        return False

    return parsed["type"] in ("weekly", "monthly", "quarterly", "yearly")


def get_ohlcv(
    symbol: str,
    exchange: str,
    interval: str,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
) -> pd.DataFrame:
    """Retrieve OHLCV data for a symbol."""
    try:
        if is_daily_aggregated_interval(interval):
            return _get_daily_aggregated_ohlcv(
                symbol=symbol,
                exchange=exchange,
                target_interval=interval,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )

        if interval in COMPUTED_INTERVALS or is_custom_interval(interval):
            return _get_aggregated_ohlcv(
                symbol=symbol,
                exchange=exchange,
                target_interval=interval,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )

        query = """
            SELECT timestamp, open, high, low, close, volume, oi
            FROM market_data
            WHERE symbol = ? AND exchange = ? AND interval = ?
        """
        params = [symbol.upper(), exchange.upper(), interval]

        if start_timestamp:
            query += " AND timestamp >= ?"
            params.append(start_timestamp)

        if end_timestamp:
            query += " AND timestamp <= ?"
            params.append(end_timestamp)

        query += " ORDER BY timestamp ASC"

        with get_connection() as conn:
            result = conn.execute(query, params).fetchdf()

        return result

    except Exception as e:
        logger.exception(f"Error fetching OHLCV data: {e}")
        return pd.DataFrame()


EXCHANGE_MARKET_OPEN_SECONDS = {
    "NSE": 33300,  # 09:15
    "BSE": 33300,  # 09:15
    "NFO": 33300,  # 09:15
    "BFO": 33300,  # 09:15
    "CDS": 32400,  # 09:00
    "BCD": 32400,  # 09:00
    "MCX": 32400,  # 09:00
    "NSE_INDEX": 33300,  # 09:15
    "BSE_INDEX": 33300,  # 09:15
}


def _get_market_open_seconds(exchange: str) -> int:
    """Get market open time in seconds from midnight for an exchange."""
    try:
        from database.market_calendar_db import get_market_timing

        timing = get_market_timing(exchange.upper())
        if timing and timing.get("start_offset"):
            return timing["start_offset"] // 1000
    except Exception:
        pass

    return EXCHANGE_MARKET_OPEN_SECONDS.get(exchange.upper(), 33300)


def _get_aggregated_ohlcv(
    symbol: str,
    exchange: str,
    target_interval: str,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
) -> pd.DataFrame:
    """
    Aggregate 1m data to higher timeframes using DuckDB SQL timezone functions.
    Dynamically adjusts for DST using TIMEZONE_STR.
    """
    try:
        minutes = INTERVAL_MINUTES.get(target_interval)
        if minutes is None:
            parsed = parse_interval(target_interval)
            if parsed and parsed["type"] == "intraday":
                minutes = parsed["minutes"]
            else:
                logger.error(f"Cannot aggregate to interval: {target_interval}")
                return pd.DataFrame()

        interval_seconds = minutes * 60
        market_open_seconds = _get_market_open_seconds(exchange)

        query = f"""
            WITH local_data AS (
                SELECT
                    *,
                    timezone(?, to_timestamp(timestamp)) as local_ts
                FROM market_data
                WHERE symbol = ? AND exchange = ? AND interval = '1m'
        """
        params = [TIMEZONE_STR, symbol.upper(), exchange.upper()]

        if start_timestamp:
            query += " AND timestamp >= ?"
            params.append(start_timestamp)

        if end_timestamp:
            query += " AND timestamp <= ?"
            params.append(end_timestamp)

        query += f"""
            ),
            bucketed_data AS (
                SELECT
                    *,
                    date_trunc('day', local_ts) +
                    INTERVAL (
                        {market_open_seconds} +
                        FLOOR((date_diff('second', date_trunc('day', local_ts), local_ts) - {market_open_seconds}) / {interval_seconds}) * {interval_seconds}
                    ) SECOND as local_bucket_start
                FROM local_data
                WHERE date_diff('second', date_trunc('day', local_ts), local_ts) >= {market_open_seconds}
            )
            SELECT
                epoch(timezone(?, local_bucket_start)) as timestamp,
                FIRST(open ORDER BY local_ts) as open,
                MAX(high) as high,
                MIN(low) as low,
                LAST(close ORDER BY local_ts) as close,
                SUM(volume) as volume,
                LAST(oi ORDER BY local_ts) as oi
            FROM bucketed_data
            GROUP BY local_bucket_start
            ORDER BY timestamp ASC
        """
        params.append(TIMEZONE_STR)

        with get_connection() as conn:
            result = conn.execute(query, params).fetchdf()

        return result

    except Exception as e:
        logger.exception(f"Error aggregating OHLCV data to {target_interval}: {e}")
        return pd.DataFrame()


def _get_daily_aggregated_ohlcv(
    symbol: str,
    exchange: str,
    target_interval: str,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
) -> pd.DataFrame:
    """
    Aggregate Daily (D) data to higher timeframes (W, M, Q, Y) using DuckDB SQL.
    Converts timestamps using TIMEZONE_STR and CTEs to avoid Binder exceptions.
    """
    try:
        parsed = parse_interval(target_interval)
        if not parsed:
            logger.error(f"Cannot parse interval: {target_interval}")
            return pd.DataFrame()

        interval_type = parsed["type"]
        interval_value = parsed.get("value", 1)

        # Simplify group expressions using the CTE 'local_ts' alias
        if interval_type == "weekly":
            if interval_value == 1:
                group_expr = "DATE_TRUNC('week', local_ts)"
            else:
                group_expr = f"DATE_TRUNC('week', local_ts) - INTERVAL ((EXTRACT(WEEK FROM local_ts) - 1) % {interval_value}) WEEK"
        elif interval_type == "monthly":
            if interval_value == 1:
                group_expr = "DATE_TRUNC('month', local_ts)"
            else:
                group_expr = f"DATE_TRUNC('month', local_ts) - INTERVAL ((EXTRACT(MONTH FROM local_ts) - 1) % {interval_value}) MONTH"
        elif interval_type == "quarterly":
            months = parsed.get("months", 3)
            if months == 3:
                group_expr = "DATE_TRUNC('quarter', local_ts)"
            else:
                group_expr = f"DATE_TRUNC('quarter', local_ts) - INTERVAL ((EXTRACT(QUARTER FROM local_ts) - 1) % {interval_value}) QUARTER"
        elif interval_type == "yearly":
            if interval_value == 1:
                group_expr = "DATE_TRUNC('year', local_ts)"
            else:
                group_expr = f"DATE_TRUNC('year', local_ts) - INTERVAL ((EXTRACT(YEAR FROM local_ts) % {interval_value})) YEAR"
        else:
            logger.error(f"Unsupported interval type for daily aggregation: {interval_type}")
            return pd.DataFrame()

        query = f"""
            WITH local_data AS (
                SELECT
                    *,
                    timezone(?, to_timestamp(timestamp)) as local_ts
                FROM market_data
                WHERE symbol = ? AND exchange = ? AND interval = 'D'
        """
        
        # Base parameters for the initial CTE
        params = [TIMEZONE_STR, symbol.upper(), exchange.upper()]

        if start_timestamp:
            query += " AND timestamp >= ?"
            params.append(start_timestamp)

        if end_timestamp:
            query += " AND timestamp <= ?"
            params.append(end_timestamp)

        query += f"""
            ),
            bucketed_data AS (
                SELECT
                    *,
                    {group_expr} as local_bucket_start
                FROM local_data
            )
            SELECT
                epoch(timezone(?, local_bucket_start)) as timestamp,
                FIRST(open ORDER BY timestamp) as open,
                MAX(high) as high,
                MIN(low) as low,
                LAST(close ORDER BY timestamp) as close,
                SUM(volume) as volume,
                LAST(oi ORDER BY timestamp) as oi
            FROM bucketed_data
            GROUP BY local_bucket_start
            ORDER BY timestamp ASC
        """
        
        # One final parameter for the SELECT epoch extraction
        params.append(TIMEZONE_STR)

        with get_connection() as conn:
            result = conn.execute(query, params).fetchdf()

        return result

    except Exception as e:
        logger.exception(f"Error aggregating daily OHLCV data to {target_interval}: {e}")
        return pd.DataFrame()

def get_data_catalog() -> list[dict[str, Any]]:
    """Get summary of all available data in the database."""
    try:
        with get_connection() as conn:
            result = conn.execute("""
                SELECT
                    symbol, exchange, interval,
                    first_timestamp, last_timestamp,
                    record_count, last_download_at
                FROM data_catalog
                ORDER BY exchange, symbol, interval
            """).fetchdf()

        if result.empty:
            return []

        return result.to_dict("records")

    except Exception as e:
        logger.exception(f"Error fetching data catalog: {e}")
        return []


def get_available_symbols() -> list[dict[str, str]]:
    """Get list of unique symbol-exchange combinations with data."""
    try:
        with get_connection() as conn:
            result = conn.execute("""
                SELECT DISTINCT symbol, exchange
                FROM data_catalog
                ORDER BY exchange, symbol
            """).fetchdf()

        if result.empty:
            return []

        return result.to_dict("records")

    except Exception as e:
        logger.exception(f"Error fetching available symbols: {e}")
        return []


def get_data_range(symbol: str, exchange: str, interval: str) -> dict[str, Any] | None:
    """Get the date range of available data for a symbol."""
    try:
        with get_connection() as conn:
            result = conn.execute(
                """
                SELECT first_timestamp, last_timestamp, record_count
                FROM data_catalog
                WHERE symbol = ? AND exchange = ? AND interval = ?
            """,
                [symbol.upper(), exchange.upper(), interval],
            ).fetchone()

        if result:
            return {
                "first_timestamp": result[0],
                "last_timestamp": result[1],
                "record_count": result[2],
            }
        return None

    except Exception as e:
        logger.exception(f"Error fetching data range: {e}")
        return None


def delete_market_data(symbol: str, exchange: str, interval: str | None = None) -> tuple[bool, str]:
    """Delete market data for a symbol."""
    try:
        with get_connection() as conn:
            if interval:
                conn.execute(
                    """
                    DELETE FROM market_data
                    WHERE symbol = ? AND exchange = ? AND interval = ?
                """,
                    [symbol.upper(), exchange.upper(), interval],
                )
                conn.execute(
                    """
                    DELETE FROM data_catalog
                    WHERE symbol = ? AND exchange = ? AND interval = ?
                """,
                    [symbol.upper(), exchange.upper(), interval],
                )
                msg = f"Deleted {symbol}:{exchange}:{interval} data"
            else:
                conn.execute(
                    """
                    DELETE FROM market_data
                    WHERE symbol = ? AND exchange = ?
                """,
                    [symbol.upper(), exchange.upper()],
                )
                conn.execute(
                    """
                    DELETE FROM data_catalog
                    WHERE symbol = ? AND exchange = ?
                """,
                    [symbol.upper(), exchange.upper()],
                )
                msg = f"Deleted all {symbol}:{exchange} data"

        logger.info(msg)
        return True, msg

    except Exception as e:
        logger.exception(f"Error deleting market data: {e}")
        return False, str(e)


def bulk_delete_market_data(
    symbols: list[dict[str, str]],
) -> tuple[int, int, list[dict[str, str]]]:
    """Delete market data for multiple symbols in a single transaction."""
    deleted = 0
    skipped = 0
    failed = []

    try:
        with get_connection() as conn:
            for item in symbols:
                symbol = item.get("symbol", "").upper()
                exchange = item.get("exchange", "").upper()

                if not symbol or not exchange:
                    failed.append({
                        "symbol": symbol or "MISSING",
                        "exchange": exchange or "MISSING",
                        "error": "Missing symbol or exchange",
                    })
                    continue

                try:
                    result = conn.execute(
                        """
                        DELETE FROM market_data
                        WHERE symbol = ? AND exchange = ?
                        """,
                        [symbol, exchange],
                    )
                    rows_deleted = result.rowcount if hasattr(result, 'rowcount') else 0

                    conn.execute(
                        """
                        DELETE FROM data_catalog
                        WHERE symbol = ? AND exchange = ?
                        """,
                        [symbol, exchange],
                    )

                    if rows_deleted > 0:
                        deleted += 1
                        logger.info(f"Bulk delete: Deleted {symbol}:{exchange}")
                    else:
                        skipped += 1
                        logger.debug(f"Bulk delete: No data found for {symbol}:{exchange}")

                except Exception as e:
                    failed.append({
                        "symbol": symbol,
                        "exchange": exchange,
                        "error": str(e),
                    })
                    logger.error(f"Bulk delete: Failed to delete {symbol}:{exchange}: {e}")

        logger.info(f"Bulk delete completed: {deleted} deleted, {skipped} skipped, {len(failed)} failed")
        return deleted, skipped, failed

    except Exception as e:
        logger.exception(f"Error in bulk delete market data: {e}")
        return 0, 0, [{"symbol": "ALL", "exchange": "ALL", "error": str(e)}]


# =============================================================================
# Export Operations
# =============================================================================


def export_to_csv(
    output_path: str,
    symbol: str | None = None,
    exchange: str | None = None,
    interval: str | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
) -> tuple[bool, str]:
    """Export market data to CSV file."""
    try:
        conditions = []
        params = []

        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol.upper())
        if exchange:
            conditions.append("exchange = ?")
            params.append(exchange.upper())
        if interval:
            conditions.append("interval = ?")
            params.append(interval)
        if start_timestamp:
            conditions.append("timestamp >= ?")
            params.append(start_timestamp)
        if end_timestamp:
            conditions.append("timestamp <= ?")
            params.append(end_timestamp)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT
                symbol, exchange, interval,
                strftime(timezone(?, to_timestamp(timestamp)), '%Y-%m-%d') as date,
                strftime(timezone(?, to_timestamp(timestamp)), '%H:%M:%S') as time,
                open, high, low, close, volume, oi
            FROM market_data
            WHERE {where_clause}
            ORDER BY symbol, exchange, interval, timestamp
        """
        full_params = [TIMEZONE_STR, TIMEZONE_STR] + params

        import tempfile

        temp_dir = tempfile.gettempdir()
        abs_output = os.path.abspath(output_path)
        if not abs_output.startswith(os.path.abspath(temp_dir)):
            return False, "Invalid output path: must be within temp directory"

        with get_connection() as conn:
            df = conn.execute(query, full_params).fetchdf()
            df.to_csv(output_path, index=False)

        logger.info(f"Exported data to {output_path}")
        return True, f"Data exported to {output_path}"

    except Exception as e:
        logger.exception(f"Error exporting to CSV: {e}")
        return False, str(e)


def export_to_dataframe(
    symbol: str,
    exchange: str,
    interval: str,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
) -> pd.DataFrame:
    """Export market data to pandas DataFrame."""
    df = get_ohlcv(symbol, exchange, interval, start_timestamp, end_timestamp)

    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
    df.set_index("datetime", inplace=True)
    df.drop("timestamp", axis=1, inplace=True)

    return df


def get_database_stats() -> dict[str, Any]:
    """Get database statistics."""
    try:
        db_path = get_db_path()
        db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0

        with get_connection() as conn:
            total_records = conn.execute("SELECT COUNT(*) FROM market_data").fetchone()[0]
            total_symbols = conn.execute(
                "SELECT COUNT(DISTINCT symbol || exchange) FROM market_data"
            ).fetchone()[0]
            watchlist_count = conn.execute("SELECT COUNT(*) FROM watchlist").fetchone()[0]

        return {
            "database_path": db_path,
            "database_size_mb": round(db_size / (1024 * 1024), 2),
            "total_records": total_records,
            "total_symbols": total_symbols,
            "watchlist_count": watchlist_count,
        }

    except Exception as e:
        logger.exception(f"Error fetching database stats: {e}")
        return {
            "database_path": get_db_path(),
            "database_size_mb": 0,
            "total_records": 0,
            "total_symbols": 0,
            "watchlist_count": 0,
        }


def vacuum_database():
    """Vacuum database space."""
    try:
        with get_connection() as conn:
            conn.execute("VACUUM")
        logger.info("Database vacuumed successfully")
    except Exception as e:
        logger.exception(f"Error vacuuming database: {e}")


LOCALE = os.getenv("LOCALE", "en-US")
if LOCALE == "en-IN":
    SUPPORTED_EXCHANGES = [
        "NSE", "BSE", "NFO", "BFO", "MCX", "CDS", "BCD", "NCO",
        "NSE_INDEX", "BSE_INDEX", "MCX_INDEX", "GLOBAL_INDEX",
        "CRYPTO",
    ]
elif LOCALE == "en-US":
    SUPPORTED_EXCHANGES = [
        "US", "OPRA", "CRYPTO",
    ]
else:
    SUPPORTED_EXCHANGES = [
        "CRYPTO",
    ]


def get_supported_intervals(api_key: str) -> list[str]:
    """Get supported intervals dynamically from the broker."""
    try:
        from services.intervals_service import get_intervals

        success, response, _ = get_intervals(api_key=api_key)

        if success and response.get("status") == "success":
            intervals_data = response.get("data", {})
            all_intervals = []
            for category in ["seconds", "minutes", "hours", "days", "weeks", "months"]:
                all_intervals.extend(intervals_data.get(category, []))
            return all_intervals
        return []
    except Exception as e:
        logger.exception(f"Error fetching supported intervals: {e}")
        return []


def import_from_csv(
    file_path: str, symbol: str, exchange: str, interval: str
) -> tuple[bool, str, int]:
    """Import OHLCV data from a CSV file into the database."""
    try:
        df = pd.read_csv(file_path)

        if df.empty:
            return False, "CSV file is empty", 0

        df.columns = df.columns.str.lower().str.strip()

        if "timestamp" in df.columns:
            if pd.api.types.is_numeric_dtype(df["timestamp"]):
                first_val = df["timestamp"].iloc[0]
                if first_val > 1e12:
                    df["timestamp"] = df["timestamp"] // 1000
            else:
                df["timestamp"] = pd.to_datetime(df["timestamp"]).astype("int64") // 10**9
        elif "datetime" in df.columns:
            df["timestamp"] = pd.to_datetime(df["datetime"]).astype("int64") // 10**9
        elif "date" in df.columns:
            if "time" in df.columns:
                df["datetime"] = df["date"].astype(str) + " " + df["time"].astype(str)
            else:
                df["datetime"] = df["date"].astype(str)
            df["timestamp"] = pd.to_datetime(df["datetime"]).astype("int64") // 10**9
        else:
            return False, "CSV must have 'timestamp', 'datetime', or 'date' column", 0

        required_cols = ["open", "high", "low", "close", "volume"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            return False, f"Missing required columns: {', '.join(missing_cols)}", 0

        if "oi" not in df.columns:
            df["oi"] = 0

        df = df[["timestamp", "open", "high", "low", "close", "volume", "oi"]]

        df["timestamp"] = df["timestamp"].astype("int64")
        df["open"] = pd.to_numeric(df["open"], errors="coerce")
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
        df["low"] = pd.to_numeric(df["low"], errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
        df["oi"] = pd.to_numeric(df["oi"], errors="coerce").fillna(0).astype("int64")

        initial_count = len(df)
        df = df.dropna(subset=["open", "high", "low", "close"])
        dropped_count = initial_count - len(df)

        if df.empty:
            return False, "No valid data rows after parsing", 0

        records = upsert_market_data(df, symbol, exchange, interval)

        msg = f"Imported {records} records"
        if dropped_count > 0:
            msg += f" ({dropped_count} rows skipped due to invalid data)"

        logger.info(f"CSV import: {msg} for {symbol}:{exchange}:{interval}")
        return True, msg, records

    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error: {e}")
        return False, f"CSV parsing error: {str(e)}", 0
    except Exception as e:
        logger.exception(f"Error importing CSV: {e}")
        return False, str(e), 0


def import_from_parquet(
    file_path: str, symbol: str, exchange: str, interval: str
) -> tuple[bool, str, int]:
    """Import OHLCV data from a Parquet file into the database."""
    try:
        df = pd.read_parquet(file_path)

        if df.empty:
            return False, "Parquet file is empty", 0

        df.columns = df.columns.str.lower().str.strip()

        if "timestamp" in df.columns:
            if pd.api.types.is_numeric_dtype(df["timestamp"]):
                first_val = df["timestamp"].iloc[0]
                if first_val > 1e12:
                    df["timestamp"] = df["timestamp"] // 1000
            else:
                df["timestamp"] = pd.to_datetime(df["timestamp"]).astype("int64") // 10**9
        elif "datetime" in df.columns:
            df["timestamp"] = pd.to_datetime(df["datetime"]).astype("int64") // 10**9
        elif "date" in df.columns:
            if "time" in df.columns:
                df["datetime"] = df["date"].astype(str) + " " + df["time"].astype(str)
            else:
                df["datetime"] = df["date"].astype(str)
            df["timestamp"] = pd.to_datetime(df["datetime"]).astype("int64") // 10**9
        else:
            return False, "Parquet must have 'timestamp', 'datetime', or 'date' column", 0

        required_cols = ["open", "high", "low", "close", "volume"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            return False, f"Missing required columns: {', '.join(missing_cols)}", 0

        if "oi" not in df.columns:
            df["oi"] = 0

        df = df[["timestamp", "open", "high", "low", "close", "volume", "oi"]]

        df["timestamp"] = df["timestamp"].astype("int64")
        df["open"] = pd.to_numeric(df["open"], errors="coerce")
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
        df["low"] = pd.to_numeric(df["low"], errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
        df["oi"] = pd.to_numeric(df["oi"], errors="coerce").fillna(0).astype("int64")

        initial_count = len(df)
        df = df.dropna(subset=["open", "high", "low", "close"])
        dropped_count = initial_count - len(df)

        if df.empty:
            return False, "No valid data rows after parsing", 0

        records = upsert_market_data(df, symbol, exchange, interval)

        msg = f"Imported {records} records"
        if dropped_count > 0:
            msg += f" ({dropped_count} rows skipped due to invalid data)"

        logger.info(f"Parquet import: {msg} for {symbol}:{exchange}:{interval}")
        return True, msg, records

    except Exception as e:
        logger.exception(f"Error importing Parquet: {e}")
        return False, str(e), 0


def create_download_job(
    job_id: str,
    job_type: str,
    symbols: list[dict[str, str]],
    interval: str,
    start_date: str,
    end_date: str,
    config: dict[str, Any] = None,
) -> tuple[bool, str]:
    """Create a new download job with symbol items."""
    import json

    try:
        with get_connection() as conn:
            conn.execute("BEGIN TRANSACTION")

            try:
                conn.execute(
                    """
                    INSERT INTO download_jobs
                    (id, job_type, status, total_symbols, interval, start_date, end_date, config)
                    VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)
                """,
                    [
                        job_id,
                        job_type,
                        len(symbols),
                        interval,
                        start_date,
                        end_date,
                        json.dumps(config) if config else None,
                    ],
                )

                if symbols:
                    symbols_df = pd.DataFrame(
                        [
                            {
                                "job_id": job_id,
                                "symbol": sym["symbol"].upper(),
                                "exchange": sym["exchange"].upper(),
                                "status": "pending",
                            }
                            for sym in symbols
                        ]
                    )

                    conn.execute("""
                        INSERT INTO job_items (id, job_id, symbol, exchange, status)
                        SELECT
                            (SELECT COALESCE(MAX(id), 0) FROM job_items) + ROW_NUMBER() OVER () as id,
                            job_id, symbol, exchange, status
                        FROM symbols_df
                    """)

                conn.execute("COMMIT")

            except Exception as inner_e:
                conn.execute("ROLLBACK")
                raise inner_e

        logger.info(f"Created download job {job_id} with {len(symbols)} symbols")
        return True, f"Job created with {len(symbols)} symbols"

    except Exception as e:
        logger.exception(f"Error creating download job: {e}")
        return False, str(e)


def _safe_timestamp(val) -> str | None:
    """Convert timestamp to ISO string."""
    if val is None or pd.isna(val):
        return None
    try:
        if hasattr(val, "isoformat"):
            return val.isoformat()
        return str(val)
    except Exception as e:
        logger.warning(f"Failed to convert timestamp {val!r} to ISO format: {e}")
        return None


def get_download_job(job_id: str) -> dict[str, Any] | None:
    """Get a download job by ID."""
    try:
        with get_connection() as conn:
            result = conn.execute(
                """
                SELECT id, job_type, status, total_symbols, completed_symbols,
                       failed_symbols, interval, start_date, end_date, config,
                       created_at, started_at, completed_at, error_message
                FROM download_jobs
                WHERE id = ?
            """,
                [job_id],
            ).fetchone()

            if result:
                return {
                    "id": result[0],
                    "job_type": result[1],
                    "status": result[2],
                    "total_symbols": result[3],
                    "completed_symbols": result[4],
                    "failed_symbols": result[5],
                    "interval": result[6],
                    "start_date": result[7],
                    "end_date": result[8],
                    "config": result[9],
                    "created_at": _safe_timestamp(result[10]),
                    "started_at": _safe_timestamp(result[11]),
                    "completed_at": _safe_timestamp(result[12]),
                    "error_message": result[13],
                }
            return None

    except Exception as e:
        logger.exception(f"Error fetching download job: {e}")
        return None


def get_all_download_jobs(status: str = None, limit: int = 50) -> list[dict[str, Any]]:
    """Get all download jobs."""
    try:
        with get_connection() as conn:
            if status:
                result = conn.execute(
                    """
                    SELECT id, job_type, status, total_symbols, completed_symbols,
                           failed_symbols, interval, start_date, end_date,
                           created_at, started_at, completed_at
                    FROM download_jobs
                    WHERE status = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                """,
                    [status, limit],
                ).fetchdf()
            else:
                result = conn.execute(
                    """
                    SELECT id, job_type, status, total_symbols, completed_symbols,
                           failed_symbols, interval, start_date, end_date,
                           created_at, started_at, completed_at
                    FROM download_jobs
                    ORDER BY created_at DESC
                    LIMIT ?
                """,
                    [limit],
                ).fetchdf()

            if result.empty:
                return []

            for col in ["created_at", "started_at", "completed_at"]:
                if col in result.columns:
                    result[col] = result[col].apply(
                        lambda x: x.isoformat() if pd.notna(x) else None
                    )

            return result.to_dict("records")

    except Exception as e:
        logger.exception(f"Error fetching download jobs: {e}")
        return []


def get_job_items(job_id: str, status: str = None) -> list[dict[str, Any]]:
    """Get all items for a job."""
    try:
        with get_connection() as conn:
            if status:
                result = conn.execute(
                    """
                    SELECT id, job_id, symbol, exchange, status,
                           records_downloaded, error_message, started_at, completed_at
                    FROM job_items
                    WHERE job_id = ? AND status = ?
                    ORDER BY id
                """,
                    [job_id, status],
                ).fetchdf()
            else:
                result = conn.execute(
                    """
                    SELECT id, job_id, symbol, exchange, status,
                           records_downloaded, error_message, started_at, completed_at
                    FROM job_items
                    WHERE job_id = ?
                    ORDER BY id
                """,
                    [job_id],
                ).fetchdf()

            if result.empty:
                return []

            for col in ["started_at", "completed_at"]:
                if col in result.columns:
                    result[col] = result[col].apply(
                        lambda x: x.isoformat() if pd.notna(x) else None
                    )

            return result.to_dict("records")

    except Exception as e:
        logger.exception(f"Error fetching job items: {e}")
        return []


def update_job_status(job_id: str, status: str, error_message: str = None) -> bool:
    """Update job status."""
    try:
        with get_connection() as conn:
            if status == "running":
                conn.execute(
                    """
                    UPDATE download_jobs
                    SET status = ?, started_at = current_timestamp
                    WHERE id = ?
                """,
                    [status, job_id],
                )
            elif status in ("completed", "failed", "cancelled"):
                conn.execute(
                    """
                    UPDATE download_jobs
                    SET status = ?, completed_at = current_timestamp, error_message = ?
                    WHERE id = ?
                """,
                    [status, error_message, job_id],
                )
            else:
                conn.execute(
                    """
                    UPDATE download_jobs
                    SET status = ?
                    WHERE id = ?
                """,
                    [status, job_id],
                )

        logger.info(f"Updated job {job_id} status to {status}")
        return True

    except Exception as e:
        logger.exception(f"Error updating job status: {e}")
        return False


def update_job_item_status(
    item_id: int, status: str, records_downloaded: int = 0, error_message: str = None
) -> bool:
    """Update job item status."""
    try:
        with get_connection() as conn:
            if status == "downloading":
                conn.execute(
                    """
                    UPDATE job_items
                    SET status = ?, started_at = current_timestamp
                    WHERE id = ?
                """,
                    [status, item_id],
                )
            elif status in ("success", "error", "skipped"):
                conn.execute(
                    """
                    UPDATE job_items
                    SET status = ?, records_downloaded = ?, error_message = ?,
                        completed_at = current_timestamp
                    WHERE id = ?
                """,
                    [status, records_downloaded, error_message, item_id],
                )
            else:
                conn.execute(
                    """
                    UPDATE job_items
                    SET status = ?
                    WHERE id = ?
                """,
                    [status, item_id],
                )

        return True

    except Exception as e:
        logger.exception(f"Error updating job item status: {e}")
        return False


def update_job_progress(job_id: str, completed: int, failed: int) -> bool:
    """Update job progress counters."""
    try:
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE download_jobs
                SET completed_symbols = ?, failed_symbols = ?
                WHERE id = ?
            """,
                [completed, failed, job_id],
            )
        return True

    except Exception as e:
        logger.exception(f"Error updating job progress: {e}")
        return False


def delete_download_job(job_id: str) -> tuple[bool, str]:
    """Delete a download job."""
    try:
        with get_connection() as conn:
            conn.execute("DELETE FROM job_items WHERE job_id = ?", [job_id])
            conn.execute("DELETE FROM download_jobs WHERE id = ?", [job_id])

        logger.info(f"Deleted job {job_id}")
        return True, f"Job {job_id} deleted"

    except Exception as e:
        logger.exception(f"Error deleting job: {e}")
        return False, str(e)


def upsert_symbol_metadata(symbols: list[dict[str, Any]]) -> int:
    """Insert or update symbol metadata."""
    if not symbols:
        return 0

    try:
        with get_connection() as conn:
            for sym in symbols:
                existing = conn.execute(
                    """
                    SELECT symbol FROM symbol_metadata
                    WHERE symbol = ? AND exchange = ?
                """,
                    [sym.get("symbol", "").upper(), sym.get("exchange", "").upper()],
                ).fetchone()

                if existing:
                    conn.execute(
                        """
                        UPDATE symbol_metadata SET
                            name = ?, expiry = ?, strike = ?, lotsize = ?,
                            instrumenttype = ?, tick_size = ?,
                            last_updated = current_timestamp
                        WHERE symbol = ? AND exchange = ?
                    """,
                        [
                            sym.get("name"),
                            sym.get("expiry"),
                            sym.get("strike"),
                            sym.get("lotsize"),
                            sym.get("instrumenttype"),
                            sym.get("tick_size"),
                            sym.get("symbol", "").upper(),
                            sym.get("exchange", "").upper(),
                        ],
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO symbol_metadata
                        (symbol, exchange, name, expiry, strike, lotsize, instrumenttype, tick_size)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        [
                            sym.get("symbol", "").upper(),
                            sym.get("exchange", "").upper(),
                            sym.get("name"),
                            sym.get("expiry"),
                            sym.get("strike"),
                            sym.get("lotsize"),
                            sym.get("instrumenttype"),
                            sym.get("tick_size"),
                        ],
                    )

        logger.info(f"Upserted metadata for {len(symbols)} symbols")
        return len(symbols)

    except Exception as e:
        logger.exception(f"Error upserting symbol metadata: {e}")
        return 0


def get_symbol_metadata(symbol: str, exchange: str) -> dict[str, Any] | None:
    """Get metadata for a specific symbol."""
    try:
        with get_connection() as conn:
            result = conn.execute(
                """
                SELECT symbol, exchange, name, expiry, strike, lotsize,
                       instrumenttype, tick_size, last_updated
                FROM symbol_metadata
                WHERE symbol = ? AND exchange = ?
            """,
                [symbol.upper(), exchange.upper()],
            ).fetchone()

            if result:
                return {
                    "symbol": result[0],
                    "exchange": result[1],
                    "name": result[2],
                    "expiry": result[3],
                    "strike": result[4],
                    "lotsize": result[5],
                    "instrumenttype": result[6],
                    "tick_size": result[7],
                    "last_updated": result[8],
                }
            return None

    except Exception as e:
        logger.exception(f"Error fetching symbol metadata: {e}")
        return None


def get_catalog_with_metadata() -> list[dict[str, Any]]:
    """Get catalog enriched with metadata."""
    try:
        with get_connection() as conn:
            result = conn.execute("""
                SELECT
                    c.symbol, c.exchange, c.interval,
                    c.first_timestamp, c.last_timestamp,
                    c.record_count, c.last_download_at,
                    m.name, m.expiry, m.strike, m.lotsize,
                    m.instrumenttype, m.tick_size
                FROM data_catalog c
                LEFT JOIN symbol_metadata m
                    ON c.symbol = m.symbol AND c.exchange = m.exchange
                ORDER BY c.exchange, m.name, c.symbol, c.interval
            """).fetchdf()

            if result.empty:
                return []
            return result.to_dict("records")

    except Exception as e:
        logger.exception(f"Error fetching catalog with metadata: {e}")
        return []


def get_catalog_grouped(group_by: str = "underlying") -> dict[str, list[dict[str, Any]]]:
    """Get catalog grouped by underlying or exchange."""
    try:
        catalog = get_catalog_with_metadata()
        grouped = {}

        if group_by == "underlying":
            for item in catalog:
                key = item.get("name") or item.get("symbol", "Unknown")
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(item)
        else:
            for item in catalog:
                key = item.get("exchange", "Unknown")
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(item)

        return grouped

    except Exception as e:
        logger.exception(f"Error grouping catalog: {e}")
        return {}


def export_to_parquet(
    output_path: str,
    symbols: list[dict[str, str]] | None = None,
    interval: str | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    compression: str = "zstd",
) -> tuple[bool, str, int]:
    """Export market data to Parquet format using TIMEZONE_STR for time binning."""
    import tempfile

    try:
        temp_dir = tempfile.gettempdir()
        abs_output = os.path.abspath(output_path)
        if not abs_output.startswith(os.path.abspath(temp_dir)):
            return False, "Invalid output path: must be within temp directory", 0

        skipped_intervals: list[str] = []
        frames: list[pd.DataFrame] = []

        with get_connection() as conn:
            if symbols and len(symbols) > 0:
                symbols_list = [(s["symbol"].upper(), s["exchange"].upper()) for s in symbols]
            else:
                symbols_df = conn.execute("""
                    SELECT DISTINCT symbol, exchange FROM data_catalog
                    ORDER BY symbol, exchange
                """).fetchdf()
                symbols_list = [(row["symbol"], row["exchange"]) for _, row in symbols_df.iterrows()]

            if not symbols_list:
                return False, "No symbols found to export", 0

            target_interval = interval if interval else "D"
            is_daily_agg = is_daily_aggregated_interval(target_interval)
            is_intraday_computed = (
                target_interval in COMPUTED_INTERVALS or is_custom_interval(target_interval)
            )

            for sym, exch in symbols_list:
                df: pd.DataFrame | None = None

                if is_daily_agg:
                    check_query = """
                        SELECT COUNT(*) FROM market_data
                        WHERE symbol = ? AND exchange = ? AND interval = 'D'
                    """
                    check_params = [sym, exch]
                    if start_timestamp:
                        check_query += " AND timestamp >= ?"
                        check_params.append(start_timestamp)
                    if end_timestamp:
                        check_query += " AND timestamp <= ?"
                        check_params.append(end_timestamp)
                    if conn.execute(check_query, check_params).fetchone()[0] == 0:
                        skipped_intervals.append(f"{sym}:{exch}:{target_interval}")
                        continue

                    df = _get_daily_aggregated_ohlcv(
                        symbol=sym,
                        exchange=exch,
                        target_interval=target_interval,
                        start_timestamp=start_timestamp,
                        end_timestamp=end_timestamp,
                    )

                elif is_intraday_computed:
                    check_query = """
                        SELECT COUNT(*) FROM market_data
                        WHERE symbol = ? AND exchange = ? AND interval = '1m'
                    """
                    check_params = [sym, exch]
                    if start_timestamp:
                        check_query += " AND timestamp >= ?"
                        check_params.append(start_timestamp)
                    if end_timestamp:
                        check_query += " AND timestamp <= ?"
                        check_params.append(end_timestamp)
                    if conn.execute(check_query, check_params).fetchone()[0] == 0:
                        skipped_intervals.append(f"{sym}:{exch}:{target_interval}")
                        continue

                    minutes = INTERVAL_MINUTES.get(target_interval)
                    if minutes is None:
                        parsed = parse_interval(target_interval)
                        if parsed and parsed["type"] == "intraday":
                            minutes = parsed["minutes"]
                        else:
                            skipped_intervals.append(f"{sym}:{exch}:{target_interval}")
                            continue
                    interval_seconds = minutes * 60
                    market_open_seconds = _get_market_open_seconds(exch)

                    query = f"""
                        WITH local_data AS (
                            SELECT *, timezone(?, to_timestamp(timestamp)) as local_ts
                            FROM market_data
                            WHERE symbol = ? AND exchange = ? AND interval = '1m'
                    """
                    params = [TIMEZONE_STR, sym, exch]
                    if start_timestamp:
                        query += " AND timestamp >= ?"
                        params.append(start_timestamp)
                    if end_timestamp:
                        query += " AND timestamp <= ?"
                        params.append(end_timestamp)
                    query += f"""
                        ),
                        bucketed_data AS (
                            SELECT *,
                                date_trunc('day', local_ts) +
                                INTERVAL (
                                    {market_open_seconds} +
                                    FLOOR((date_diff('second', date_trunc('day', local_ts), local_ts) - {market_open_seconds}) / {interval_seconds}) * {interval_seconds}
                                ) SECOND as local_bucket_start
                            FROM local_data
                            WHERE date_diff('second', date_trunc('day', local_ts), local_ts) >= {market_open_seconds}
                        )
                        SELECT
                            epoch(timezone(?, local_bucket_start)) as timestamp,
                            FIRST(open ORDER BY local_ts) as open,
                            MAX(high) as high,
                            MIN(low) as low,
                            LAST(close ORDER BY local_ts) as close,
                            SUM(volume) as volume,
                            LAST(oi ORDER BY local_ts) as oi
                        FROM bucketed_data
                        GROUP BY local_bucket_start
                        ORDER BY timestamp ASC
                    """
                    params.append(TIMEZONE_STR)

                    df = conn.execute(query, params).fetchdf()

                else:
                    query = """
                        SELECT timestamp, open, high, low, close, volume, oi
                        FROM market_data
                        WHERE symbol = ? AND exchange = ? AND interval = ?
                    """
                    params = [sym, exch, target_interval]
                    if start_timestamp:
                        query += " AND timestamp >= ?"
                        params.append(start_timestamp)
                    if end_timestamp:
                        query += " AND timestamp <= ?"
                        params.append(end_timestamp)
                    query += " ORDER BY timestamp"
                    df = conn.execute(query, params).fetchdf()

                if df is None or df.empty:
                    continue

                df = df.assign(symbol=sym, exchange=exch, interval=target_interval)
                df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
                df = df[
                    [
                        "symbol", "exchange", "interval", "timestamp",
                        "open", "high", "low", "close", "volume", "oi",
                        "datetime",
                    ]
                ]
                frames.append(df)

        if not frames:
            return False, "No data matching the criteria", 0

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.sort_values(["symbol", "exchange", "interval", "timestamp"])

        pq_compression = None if compression == "none" else compression
        combined.to_parquet(abs_output, compression=pq_compression, index=False)

        record_count = len(combined)
        file_size = os.path.getsize(abs_output) / (1024 * 1024)
        message = f"Exported {record_count} records ({file_size:.2f} MB)"
        return True, message, record_count

    except Exception as e:
        logger.exception(f"Error exporting to Parquet: {e}")
        if "abs_output" in locals() and os.path.exists(abs_output):
            try:
                os.remove(abs_output)
            except Exception:
                pass
        return False, str(e), 0


def export_to_txt(
    output_path: str,
    symbols: list[dict[str, str]] | None = None,
    interval: str | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    delimiter: str = "\t",
) -> tuple[bool, str, int]:
    """Export market data to TXT format."""
    import tempfile

    try:
        conditions = []
        params = []

        if symbols and len(symbols) > 0:
            symbol_conditions = []
            for sym in symbols:
                symbol_conditions.append("(symbol = ? AND exchange = ?)")
                params.extend([sym["symbol"].upper(), sym["exchange"].upper()])
            conditions.append(f"({' OR '.join(symbol_conditions)})")

        if interval:
            conditions.append("interval = ?")
            params.append(interval)
        if start_timestamp:
            conditions.append("timestamp >= ?")
            params.append(start_timestamp)
        if end_timestamp:
            conditions.append("timestamp <= ?")
            params.append(end_timestamp)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        temp_dir = tempfile.gettempdir()
        abs_output = os.path.abspath(output_path)
        if not abs_output.startswith(os.path.abspath(temp_dir)):
            return False, "Invalid output path: must be within temp directory", 0

        query = f"""
            SELECT
                symbol, exchange, interval,
                strftime(timezone(?, to_timestamp(timestamp)), '%Y-%m-%d') as date,
                strftime(timezone(?, to_timestamp(timestamp)), '%H:%M:%S') as time,
                open, high, low, close, volume, oi
            FROM market_data
            WHERE {where_clause}
            ORDER BY symbol, exchange, interval, timestamp
        """
        full_params = [TIMEZONE_STR, TIMEZONE_STR] + params

        with get_connection() as conn:
            df = conn.execute(query, full_params).fetchdf()

            if df.empty:
                return False, "No data matching the criteria", 0

            df.to_csv(output_path, index=False, sep=delimiter)
            record_count = len(df)

        logger.info(f"Exported {record_count} records to TXT")
        return True, f"Exported {record_count} records", record_count

    except Exception as e:
        logger.exception(f"Error exporting to TXT: {e}")
        return False, str(e), 0


def _sanitize_filename(name: str) -> str:
    """Remove path traversal and special characters from filename."""
    import re
    name = name.replace("/", "_").replace("\\", "_").replace("\x00", "")
    return re.sub(r"[^A-Za-z0-9_\-.]", "_", name)


def export_to_zip(
    output_path: str,
    symbols: list[dict[str, str]] | None = None,
    intervals: list[str] | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    split_by: str = "symbol",
) -> tuple[bool, str, int]:
    """Export market data to ZIP archive containing CSVs."""
    import tempfile
    import zipfile

    try:
        temp_dir = tempfile.gettempdir()
        abs_output = os.path.abspath(output_path)
        if not abs_output.startswith(os.path.abspath(temp_dir)):
            return False, "Invalid output path: must be within temp directory", 0

        total_records = 0
        skipped_intervals = []

        with zipfile.ZipFile(abs_output, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            with get_connection() as conn:
                if symbols and len(symbols) > 0:
                    symbols_list = [(s["symbol"].upper(), s["exchange"].upper()) for s in symbols]
                else:
                    symbols_df = conn.execute("""
                        SELECT DISTINCT symbol, exchange FROM data_catalog
                        ORDER BY symbol, exchange
                    """).fetchdf()
                    symbols_list = [
                        (row["symbol"], row["exchange"]) for _, row in symbols_df.iterrows()
                    ]

                if not symbols_list:
                    return False, "No symbols found to export", 0

                intervals_to_export = intervals if intervals else ["D"]

                for sym, exch in symbols_list:
                    market_open_seconds = _get_market_open_seconds(exch)

                    for interval in intervals_to_export:
                        is_daily_agg = is_daily_aggregated_interval(interval)
                        is_intraday_computed = interval in COMPUTED_INTERVALS or is_custom_interval(
                            interval
                        )

                        if is_daily_agg:
                            check_query = """
                                SELECT COUNT(*) FROM market_data
                                WHERE symbol = ? AND exchange = ? AND interval = 'D'
                            """
                            check_params = [sym, exch]
                            if start_timestamp:
                                check_query += " AND timestamp >= ?"
                                check_params.append(start_timestamp)
                            if end_timestamp:
                                check_query += " AND timestamp <= ?"
                                check_params.append(end_timestamp)

                            count = conn.execute(check_query, check_params).fetchone()[0]
                            if count == 0:
                                skipped_intervals.append(f"{sym}:{exch}:{interval}")
                                continue

                            df = _get_daily_aggregated_ohlcv(
                                symbol=sym,
                                exchange=exch,
                                target_interval=interval,
                                start_timestamp=start_timestamp,
                                end_timestamp=end_timestamp,
                            )

                            if not df.empty:
                                df["date"] = pd.to_datetime(df["timestamp"], unit="s").dt.strftime("%Y-%m-%d")
                                df["time"] = pd.to_datetime(df["timestamp"], unit="s").dt.strftime("%H:%M:%S")
                                df = df[
                                    ["date", "time", "open", "high", "low", "close", "volume", "oi"]
                                ]

                                csv_buffer = df.to_csv(index=False)
                                safe_sym = _sanitize_filename(sym)
                                safe_exch = _sanitize_filename(exch)
                                safe_int = _sanitize_filename(interval)
                                filename = f"{safe_sym}_{safe_exch}_{safe_int}.csv"

                                zf.writestr(filename, csv_buffer)
                                total_records += len(df)

                        elif is_intraday_computed:
                            check_query = """
                                SELECT COUNT(*) FROM market_data
                                WHERE symbol = ? AND exchange = ? AND interval = '1m'
                            """
                            check_params = [sym, exch]
                            if start_timestamp:
                                check_query += " AND timestamp >= ?"
                                check_params.append(start_timestamp)
                            if end_timestamp:
                                check_query += " AND timestamp <= ?"
                                check_params.append(end_timestamp)

                            count = conn.execute(check_query, check_params).fetchone()[0]
                            if count == 0:
                                skipped_intervals.append(f"{sym}:{exch}:{interval}")
                                continue

                            minutes = INTERVAL_MINUTES.get(interval)
                            if minutes is None:
                                parsed = parse_interval(interval)
                                if parsed and parsed["type"] == "intraday":
                                    minutes = parsed["minutes"]
                                else:
                                    skipped_intervals.append(f"{sym}:{exch}:{interval}")
                                    continue
                            interval_seconds = minutes * 60

                            query = f"""
                                WITH local_data AS (
                                    SELECT *, timezone(?, to_timestamp(timestamp)) as local_ts
                                    FROM market_data
                                    WHERE symbol = ? AND exchange = ? AND interval = '1m'
                            """
                            params = [TIMEZONE_STR, sym, exch]

                            if start_timestamp:
                                query += " AND timestamp >= ?"
                                params.append(start_timestamp)

                            if end_timestamp:
                                query += " AND timestamp <= ?"
                                params.append(end_timestamp)

                            query += f"""
                                ),
                                bucketed_data AS (
                                    SELECT *,
                                        date_trunc('day', local_ts) +
                                        INTERVAL (
                                            {market_open_seconds} +
                                            FLOOR((date_diff('second', date_trunc('day', local_ts), local_ts) - {market_open_seconds}) / {interval_seconds}) * {interval_seconds}
                                        ) SECOND as local_bucket_start
                                    FROM local_data
                                    WHERE date_diff('second', date_trunc('day', local_ts), local_ts) >= {market_open_seconds}
                                )
                                SELECT
                                    epoch(timezone(?, local_bucket_start)) as ts,
                                    FIRST(open ORDER BY local_ts) as open,
                                    MAX(high) as high,
                                    MIN(low) as low,
                                    LAST(close ORDER BY local_ts) as close,
                                    SUM(volume) as volume,
                                    LAST(oi ORDER BY local_ts) as oi
                                FROM bucketed_data
                                GROUP BY local_bucket_start
                                ORDER BY ts ASC
                            """
                            params.append(TIMEZONE_STR)

                            df = conn.execute(query, params).fetchdf()

                            if not df.empty:
                                df["date"] = pd.to_datetime(df["ts"], unit="s").dt.strftime("%Y-%m-%d")
                                df["time"] = pd.to_datetime(df["ts"], unit="s").dt.strftime("%H:%M:%S")
                                df = df[
                                    ["date", "time", "open", "high", "low", "close", "volume", "oi"]
                                ]

                        else:
                            query = f"""
                                SELECT
                                    strftime(timezone(?, to_timestamp(timestamp)), '%Y-%m-%d') as date,
                                    strftime(timezone(?, to_timestamp(timestamp)), '%H:%M:%S') as time,
                                    open, high, low, close, volume, oi
                                FROM market_data
                                WHERE symbol = ? AND exchange = ? AND interval = ?
                            """
                            params = [TIMEZONE_STR, TIMEZONE_STR, sym, exch, interval]

                            if start_timestamp:
                                query += " AND timestamp >= ?"
                                params.append(start_timestamp)

                            if end_timestamp:
                                query += " AND timestamp <= ?"
                                params.append(end_timestamp)

                            query += " ORDER BY timestamp"

                            df = conn.execute(query, params).fetchdf()

                        if not df.empty:
                            csv_content = df.to_csv(index=False)
                            filename = f"{_sanitize_filename(sym)}_{_sanitize_filename(exch)}_{_sanitize_filename(interval)}.csv"
                            zf.writestr(filename, csv_content)
                            total_records += len(df)

        if total_records == 0:
            if os.path.exists(abs_output):
                os.remove(abs_output)
            return False, "No data matching the criteria", 0

        file_size = os.path.getsize(abs_output) / (1024 * 1024)
        message = f"Exported {total_records} records ({file_size:.2f} MB)"
        logger.info(message)
        return True, message, total_records

    except Exception as e:
        logger.exception(f"Error exporting to ZIP: {e}")
        if os.path.exists(abs_output):
            try:
                os.remove(abs_output)
            except Exception:
                pass
        return False, str(e), 0


def export_bulk_csv(
    output_path: str,
    symbols: list[dict[str, str]],
    interval: str | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
) -> tuple[bool, str, int]:
    """Export multiple symbols to a single CSV file."""
    import tempfile

    try:
        conditions = []
        params = []

        if symbols and len(symbols) > 0:
            symbol_conditions = []
            for sym in symbols:
                symbol_conditions.append("(symbol = ? AND exchange = ?)")
                params.extend([sym["symbol"].upper(), sym["exchange"].upper()])
            conditions.append(f"({' OR '.join(symbol_conditions)})")

        if interval:
            conditions.append("interval = ?")
            params.append(interval)
        if start_timestamp:
            conditions.append("timestamp >= ?")
            params.append(start_timestamp)
        if end_timestamp:
            conditions.append("timestamp <= ?")
            params.append(end_timestamp)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        temp_dir = tempfile.gettempdir()
        abs_output = os.path.abspath(output_path)
        if not abs_output.startswith(os.path.abspath(temp_dir)):
            return False, "Invalid output path: must be within temp directory", 0

        query = f"""
            SELECT
                symbol, exchange, interval,
                strftime(timezone(?, to_timestamp(timestamp)), '%Y-%m-%d') as date,
                strftime(timezone(?, to_timestamp(timestamp)), '%H:%M:%S') as time,
                open, high, low, close, volume, oi
            FROM market_data
            WHERE {where_clause}
            ORDER BY symbol, exchange, interval, timestamp
        """
        full_params = [TIMEZONE_STR, TIMEZONE_STR] + params

        with get_connection() as conn:
            df = conn.execute(query, full_params).fetchdf()

            if df.empty:
                return False, "No data matching the criteria", 0

            df.to_csv(output_path, index=False)
            record_count = len(df)

        logger.info(f"Exported {record_count} records to CSV")
        return True, f"Exported {record_count} records", record_count

    except Exception as e:
        logger.exception(f"Error exporting bulk CSV: {e}")
        return False, str(e), 0


def get_export_preview(
    symbols: list[dict[str, str]] | None = None,
    interval: str | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
) -> dict[str, Any]:
    """Get export preview statistics."""
    try:
        conditions = []
        params = []

        if symbols and len(symbols) > 0:
            symbol_conditions = []
            for sym in symbols:
                symbol_conditions.append("(symbol = ? AND exchange = ?)")
                params.extend([sym["symbol"].upper(), sym["exchange"].upper()])
            conditions.append(f"({' OR '.join(symbol_conditions)})")

        if interval:
            conditions.append("interval = ?")
            params.append(interval)
        if start_timestamp:
            conditions.append("timestamp >= ?")
            params.append(start_timestamp)
        if end_timestamp:
            conditions.append("timestamp <= ?")
            params.append(end_timestamp)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as symbol_count,
                COUNT(DISTINCT exchange) as exchange_count,
                COUNT(DISTINCT interval) as interval_count,
                MIN(timestamp) as first_timestamp,
                MAX(timestamp) as last_timestamp
            FROM market_data
            WHERE {where_clause}
        """

        with get_connection() as conn:
            result = conn.execute(query, params).fetchone()

            if result[0] == 0:
                return {
                    "total_records": 0,
                    "symbol_count": 0,
                    "exchange_count": 0,
                    "interval_count": 0,
                    "first_date": None,
                    "last_date": None,
                    "estimated_size_csv_mb": 0,
                    "estimated_size_parquet_mb": 0,
                }

            csv_size = (result[0] * 100) / (1024 * 1024)
            parquet_size = (result[0] * 20) / (1024 * 1024)

            return {
                "total_records": result[0],
                "symbol_count": result[1],
                "exchange_count": result[2],
                "interval_count": result[3],
                "first_date": datetime.fromtimestamp(result[4]).strftime("%Y-%m-%d")
                if result[4]
                else None,
                "last_date": datetime.fromtimestamp(result[5]).strftime("%Y-%m-%d")
                if result[5]
                else None,
                "estimated_size_csv_mb": round(csv_size, 2),
                "estimated_size_parquet_mb": round(parquet_size, 2),
            }

    except Exception as e:
        logger.exception(f"Error getting export preview: {e}")
        return {
            "total_records": 0,
            "symbol_count": 0,
            "exchange_count": 0,
            "interval_count": 0,
            "first_date": None,
            "last_date": None,
            "estimated_size_csv_mb": 0,
            "estimated_size_parquet_mb": 0,
            "error": str(e),
        }


# =============================================================================
# Scheduler Operations
# =============================================================================


def create_schedule(
    schedule_id: str,
    name: str,
    schedule_type: str,
    data_interval: str,
    interval_value: int | None = None,
    interval_unit: str | None = None,
    time_of_day: str | None = None,
    download_source: str = "watchlist",
    lookback_days: int = 1,
    description: str | None = None,
) -> tuple[bool, str]:
    """Create a new schedule configuration."""
    try:
        with get_connection() as conn:
            existing = conn.execute(
                "SELECT id FROM historify_schedules WHERE id = ?", [schedule_id]
            ).fetchone()

            if existing:
                return False, f"Schedule ID '{schedule_id}' already exists"

            conn.execute(
                """
                INSERT INTO historify_schedules
                (id, name, description, schedule_type, interval_value, interval_unit,
                 time_of_day, download_source, data_interval, lookback_days,
                 is_enabled, is_paused, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, FALSE, 'idle', current_timestamp)
            """,
                [
                    schedule_id,
                    name,
                    description,
                    schedule_type,
                    interval_value,
                    interval_unit,
                    time_of_day,
                    download_source,
                    data_interval,
                    lookback_days,
                ],
            )

        logger.info(f"Created schedule: {name} ({schedule_id})")
        return True, f"Schedule '{name}' created successfully"

    except Exception as e:
        logger.exception(f"Error creating schedule: {e}")
        return False, str(e)


def _clean_schedule_record(record: dict[str, Any]) -> dict[str, Any]:
    """Clean a schedule record for JSON serialization."""
    import pandas as pd

    cleaned = {}
    for key, value in record.items():
        if pd.isna(value):
            cleaned[key] = None
        elif isinstance(value, pd.Timestamp):
            cleaned[key] = value.isoformat() if not pd.isna(value) else None
        elif hasattr(value, "isoformat"):
            cleaned[key] = value.isoformat()
        else:
            cleaned[key] = value
    return cleaned


def get_schedule(schedule_id: str) -> dict[str, Any] | None:
    """Get a schedule by ID."""
    try:
        with get_connection() as conn:
            result = conn.execute(
                """
                SELECT id, name, description, schedule_type, interval_value,
                       interval_unit, time_of_day, download_source, data_interval,
                       lookback_days, is_enabled, is_paused, status, apscheduler_job_id,
                       created_at, last_run_at, next_run_at, last_run_status,
                       total_runs, successful_runs, failed_runs
                FROM historify_schedules
                WHERE id = ?
            """,
                [schedule_id],
            ).fetchdf()

            if result.empty:
                return None

            record = result.to_dict("records")[0]
            return _clean_schedule_record(record)

    except Exception as e:
        logger.exception(f"Error getting schedule: {e}")
        return None


def get_all_schedules() -> list[dict[str, Any]]:
    """Get all schedules."""
    try:
        with get_connection() as conn:
            result = conn.execute("""
                SELECT id, name, description, schedule_type, interval_value,
                       interval_unit, time_of_day, download_source, data_interval,
                       lookback_days, is_enabled, is_paused, status, apscheduler_job_id,
                       created_at, last_run_at, next_run_at, last_run_status,
                       total_runs, successful_runs, failed_runs
                FROM historify_schedules
                ORDER BY created_at DESC
            """).fetchdf()

            if result.empty:
                return []

            records = result.to_dict("records")
            return [_clean_schedule_record(r) for r in records]

    except Exception as e:
        logger.exception(f"Error getting schedules: {e}")
        return []


def update_schedule(
    schedule_id: str,
    name: str | None = None,
    description: str | None = None,
    schedule_type: str | None = None,
    interval_value: int | None = None,
    interval_unit: str | None = None,
    time_of_day: str | None = None,
    download_source: str | None = None,
    data_interval: str | None = None,
    lookback_days: int | None = None,
    is_enabled: bool | None = None,
    is_paused: bool | None = None,
    status: str | None = None,
    apscheduler_job_id: str | None = None,
    next_run_at: datetime | None = None,
    last_run_at: datetime | None = None,
    last_run_status: str | None = None,
) -> tuple[bool, str]:
    """Update a schedule configuration."""
    try:
        updates = []
        params = []

        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if description is not None:
            updates.append("description = ?")
            params.append(description)
        if schedule_type is not None:
            updates.append("schedule_type = ?")
            params.append(schedule_type)
        if interval_value is not None:
            updates.append("interval_value = ?")
            params.append(interval_value)
        if interval_unit is not None:
            updates.append("interval_unit = ?")
            params.append(interval_unit)
        if time_of_day is not None:
            updates.append("time_of_day = ?")
            params.append(time_of_day)
        if download_source is not None:
            updates.append("download_source = ?")
            params.append(download_source)
        if data_interval is not None:
            updates.append("data_interval = ?")
            params.append(data_interval)
        if lookback_days is not None:
            updates.append("lookback_days = ?")
            params.append(lookback_days)
        if is_enabled is not None:
            updates.append("is_enabled = ?")
            params.append(is_enabled)
        if is_paused is not None:
            updates.append("is_paused = ?")
            params.append(is_paused)
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if apscheduler_job_id is not None:
            updates.append("apscheduler_job_id = ?")
            params.append(apscheduler_job_id)
        if next_run_at is not None:
            updates.append("next_run_at = ?")
            params.append(next_run_at)
        if last_run_at is not None:
            updates.append("last_run_at = ?")
            params.append(last_run_at)
        if last_run_status is not None:
            updates.append("last_run_status = ?")
            params.append(last_run_status)

        if not updates:
            return False, "No fields to update"

        params.append(schedule_id)
        query = f"UPDATE historify_schedules SET {', '.join(updates)} WHERE id = ?"

        with get_connection() as conn:
            conn.execute(query, params)

        logger.info(f"Updated schedule: {schedule_id}")
        return True, "Schedule updated successfully"

    except Exception as e:
        logger.exception(f"Error updating schedule: {e}")
        return False, str(e)


def delete_schedule(schedule_id: str) -> tuple[bool, str]:
    """Delete a schedule and its execution history."""
    try:
        with get_connection() as conn:
            conn.execute(
                "DELETE FROM historify_schedule_executions WHERE schedule_id = ?", [schedule_id]
            )
            conn.execute("DELETE FROM historify_schedules WHERE id = ?", [schedule_id])

        logger.info(f"Deleted schedule: {schedule_id}")
        return True, "Schedule deleted successfully"

    except Exception as e:
        logger.exception(f"Error deleting schedule: {e}")
        return False, str(e)


def increment_schedule_run_counts(schedule_id: str, is_success: bool) -> tuple[bool, str]:
    """Increment run counts for a schedule."""
    try:
        with get_connection() as conn:
            if is_success:
                conn.execute(
                    """
                    UPDATE historify_schedules
                    SET total_runs = total_runs + 1,
                        successful_runs = successful_runs + 1,
                        last_run_at = current_timestamp
                    WHERE id = ?
                """,
                    [schedule_id],
                )
            else:
                conn.execute(
                    """
                    UPDATE historify_schedules
                    SET total_runs = total_runs + 1,
                        failed_runs = failed_runs + 1,
                        last_run_at = current_timestamp
                    WHERE id = ?
                """,
                    [schedule_id],
                )

        return True, "Run counts updated"

    except Exception as e:
        logger.exception(f"Error incrementing run counts: {e}")
        return False, str(e)


def create_schedule_execution(schedule_id: str, download_job_id: str | None = None) -> int | None:
    """Create a new execution record for a schedule."""
    import time

    try:
        execution_id = int(time.time() * 1000000) % 1000000000

        with get_connection() as conn:
            for attempt in range(3):
                try:
                    conn.execute(
                        """
                        INSERT INTO historify_schedule_executions
                        (id, schedule_id, download_job_id, status, started_at)
                        VALUES (?, ?, ?, 'running', current_timestamp)
                    """,
                        [execution_id + attempt, schedule_id, download_job_id],
                    )
                    execution_id = execution_id + attempt
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    continue

        logger.info(f"Created execution {execution_id} for schedule {schedule_id}")
        return execution_id

    except Exception as e:
        logger.exception(f"Error creating execution record: {e}")
        return None


def update_schedule_execution(
    execution_id: int,
    status: str | None = None,
    completed_at: datetime | None = None,
    symbols_processed: int | None = None,
    symbols_success: int | None = None,
    symbols_failed: int | None = None,
    records_downloaded: int | None = None,
    error_message: str | None = None,
    download_job_id: str | None = None,
) -> tuple[bool, str]:
    """Update an execution record."""
    try:
        updates = []
        params = []

        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if completed_at is not None:
            updates.append("completed_at = ?")
            params.append(completed_at)
        if symbols_processed is not None:
            updates.append("symbols_processed = ?")
            params.append(symbols_processed)
        if symbols_success is not None:
            updates.append("symbols_success = ?")
            params.append(symbols_success)
        if symbols_failed is not None:
            updates.append("symbols_failed = ?")
            params.append(symbols_failed)
        if download_job_id is not None:
            updates.append("download_job_id = ?")
            params.append(download_job_id)
        if records_downloaded is not None:
            updates.append("records_downloaded = ?")
            params.append(records_downloaded)
        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)

        if not updates:
            return False, "No fields to update"

        params.append(execution_id)
        query = f"UPDATE historify_schedule_executions SET {', '.join(updates)} WHERE id = ?"

        with get_connection() as conn:
            conn.execute(query, params)

        return True, "Execution updated"

    except Exception as e:
        logger.exception(f"Error updating execution: {e}")
        return False, str(e)


def get_schedule_executions(schedule_id: str, limit: int = 20) -> list[dict[str, Any]]:
    """Get execution history for a schedule."""
    try:
        with get_connection() as conn:
            result = conn.execute(
                """
                SELECT id, schedule_id, download_job_id, status,
                       started_at, completed_at, symbols_processed,
                       symbols_success, symbols_failed, records_downloaded,
                       error_message
                FROM historify_schedule_executions
                WHERE schedule_id = ?
                ORDER BY started_at DESC
                LIMIT ?
            """,
                [schedule_id, limit],
            ).fetchdf()

            if result.empty:
                return []

            records = result.to_dict("records")
            return [_clean_schedule_record(r) for r in records]

    except Exception as e:
        logger.exception(f"Error getting executions: {e}")
        return []


def get_active_schedules() -> list[dict[str, Any]]:
    """Get all enabled and non-paused schedules."""
    try:
        with get_connection() as conn:
            result = conn.execute("""
                SELECT id, name, description, schedule_type, interval_value,
                       interval_unit, time_of_day, download_source, data_interval,
                       lookback_days, is_enabled, is_paused, status, apscheduler_job_id,
                       created_at, last_run_at, next_run_at, last_run_status,
                       total_runs, successful_runs, failed_runs
                FROM historify_schedules
                WHERE is_enabled = TRUE AND is_paused = FALSE
                ORDER BY created_at DESC
            """).fetchdf()

            if result.empty:
                return []

            records = result.to_dict("records")
            return [_clean_schedule_record(r) for r in records]

    except Exception as e:
        logger.exception(f"Error getting active schedules: {e}")
        return []