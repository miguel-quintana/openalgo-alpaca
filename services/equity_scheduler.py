# services/equity_scheduler.py
"""
Equity Scheduler Service
Handles daily portfolio equity snapshots using APScheduler
"""

import os
import threading
from datetime import datetime
from typing import Optional

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from database.apscheduler_jobstore_db import (
    ensure_jobstore_table,
    get_database_url,
)
from database.engine_factory import create_db_engine
from database.equity_history_db import save_equity_snapshot, init_equity_db
from utils.logging import get_logger

logger = get_logger(__name__)

# Dedicated table for this specific scheduler's job metadata
EQUITY_JOBSTORE_TABLE = "apscheduler_equity_jobs"


class EquityScheduler:
    """Singleton scheduler for Equity History snapshots"""

    _instance: Optional["EquityScheduler"] = None
    _scheduler: BackgroundScheduler | None = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def init(self, db_url: str = None):
        """Initialize the scheduler with database URL for job persistence"""
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            if db_url is None:
                db_url = get_database_url()

            try:
                # Ensure the main equity data table exists
                init_equity_db()

                # Ensure the APScheduler metadata table exists before APScheduler tries
                ensure_jobstore_table(EQUITY_JOBSTORE_TABLE, database_url=db_url)

                jobstores = {
                    "default": SQLAlchemyJobStore(
                        engine=create_db_engine(db_url), tablename=EQUITY_JOBSTORE_TABLE
                    )
                }
                
                self._scheduler = BackgroundScheduler(
                    jobstores=jobstores,
                    job_defaults={
                        "coalesce": True,
                        "max_instances": 1,
                        "misfire_grace_time": 300,  # 5 minutes grace for missed jobs
                    },
                )
                self._scheduler.start()
                self._initialized = True
                logger.debug("Equity Snapshot Scheduler initialized and started")

                # Reconcile schedule to ensure it always exists
                self._schedule_snapshot_job()

            except Exception as e:
                logger.exception(f"Failed to initialize Equity Snapshot Scheduler: {e}")
                raise

    @property
    def scheduler(self) -> BackgroundScheduler:
        """Get the scheduler instance"""
        if self._scheduler is None:
            raise RuntimeError("Scheduler not initialized. Call init() first.")
        return self._scheduler

    def _schedule_snapshot_job(self):
        """Register or update the cron job with APScheduler"""
        job_id = "daily_equity_snapshot"
        
        try:
            tz_str = os.getenv("TIMEZONE", "America/New_York")
            
            # Run at 16:05 (4:05 PM ET) Monday through Friday
            trigger = CronTrigger(day_of_week='mon-fri', hour=20, minute=5, timezone=tz_str)
            
            self.scheduler.add_job(
                execute_equity_snapshot,
                trigger=trigger,
                id=job_id,
                name="Daily Portfolio Equity Snapshot",
                replace_existing=True,
            )
            logger.info(f"Registered Equity Snapshot scheduler 20:05 Mon-Fri {tz_str}).")

        except Exception as e:
            logger.exception(f"Error adding equity schedule job: {e}")

    def shutdown(self):
        """Shutdown the scheduler"""
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._initialized = False
            logger.info("Equity Snapshot Scheduler shutdown")


def execute_equity_snapshot():
    """Background task to fetch live funds and save to the database."""
    import requests
    
    try:
        api_key = os.getenv("OPENALGO_API_KEY", "")
        # Query your own local OpenAlgo API to fetch the mapped broker balances
        host_server = os.getenv("HOST_SERVER", "http://127.0.0.1:5000")
        url = host_server + "/api/v1/funds"
        payload = {"apikey": api_key} if api_key else {}
        
        response = requests.post(url, json=payload)
        data = response.json()
        
        if data.get("status") == "success":
            funds_data = data.get("data", {})
            
            # Extract cash and portfolio value
            cash = float(funds_data.get("availablecash", 0.0))
            # Fallback chain to catch net_liquidation or portfolio_value maps from Alpaca
            portfolio_value = float(funds_data.get("portfolio_value", funds_data.get("net_liquidation", cash)))
            
            save_equity_snapshot(cash=cash, portfolio_value=portfolio_value)
            logger.info(f"Saved daily equity snapshot: Cash=${cash}, Portfolio=${portfolio_value}")
        else:
            logger.error(f"Failed to fetch funds for equity snapshot: {data}")
            
    except Exception as e:
        logger.exception(f"Error in equity snapshot background job: {e}")
        
    finally:
        # Prevent SQLAlchemy connection leaks on background threads
        from utils.db_sessions import remove_all_scoped_sessions
        remove_all_scoped_sessions()


# Global scheduler instance
equity_scheduler = EquityScheduler()


def get_equity_scheduler() -> EquityScheduler:
    """Get the global equity scheduler instance"""
    return equity_scheduler


def setup_equity_scheduler(db_url: str = None):
    """Initialize the equity scheduler"""
    equity_scheduler.init(db_url=db_url)
    return equity_scheduler