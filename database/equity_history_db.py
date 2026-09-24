import os
from datetime import datetime
import pytz
from sqlalchemy import Column, Integer, Float, DateTime
from database.market_calendar_db import Base, db_session, engine # Assumes standard OpenAlgo DB engine

class AccountEquityHistory(Base):
    """
    Stores daily snapshots of the portfolio's total value and cash.
    """
    __tablename__ = "account_equity_history"
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    cash = Column(Float, nullable=False, default=0.0)
    portfolio_value = Column(Float, nullable=False, default=0.0)

def init_equity_db():
    """Creates the table if it doesn't exist."""
    Base.metadata.create_all(bind=engine)

def save_equity_snapshot(cash: float, portfolio_value: float):
    """Saves a new snapshot using the configured timezone."""
    tz = pytz.timezone(os.getenv("TIMEZONE", "America/New_York"))
    
    # Store aware datetime converted to UTC for database standardization
    now = datetime.now(tz)
    
    snapshot = AccountEquityHistory(
        timestamp=now,
        cash=cash,
        portfolio_value=portfolio_value
    )
    db_session.add(snapshot)
    db_session.commit()

def get_equity_history(start_date=None, end_date=None):
    """Retrieves the portfolio curve within a given date range."""
    query = AccountEquityHistory.query
    if start_date:
        query = query.filter(AccountEquityHistory.timestamp >= start_date)
    if end_date:
        query = query.filter(AccountEquityHistory.timestamp <= end_date)
        
    return query.order_by(AccountEquityHistory.timestamp.asc()).all()