# broker/alpaca/database/master_contract_db.py

import os
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import Column, Float, Index, Integer, Sequence, String, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from database.engine_factory import create_db_engine
from extensions import socketio
from utils.httpx_client import get_httpx_client
from utils.logging import get_logger

from broker.alpaca.api.baseurl import BASE_URL

logger = get_logger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_db_engine(DATABASE_URL)

if engine.name == "sqlite":
    # Enable WAL mode for better concurrent access
    try:
        with engine.connect() as conn:
            conn.execute(text("PRAGMA journal_mode=WAL"))
            conn.execute(text("PRAGMA synchronous=NORMAL"))
            conn.execute(text("PRAGMA temp_store=memory"))
            conn.execute(text("PRAGMA mmap_size=268435456"))
            conn.commit()
    except Exception as e:
        logger.warning(f"Could not set SQLite pragmas for master_contract_db: {e}")
elif engine.name == "postgresql":
    # Optional: Apply PostgreSQL specific optimization parameters here if needed
    pass
else:
    logger.warning(f"Database engine {engine.name} is not specifically optimized in this code.")

db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()


class SymToken(Base):
    __tablename__ = "symtoken"
    id = Column(Integer, Sequence("symtoken_id_seq"), primary_key=True)
    symbol = Column(String, nullable=False, index=True)
    brsymbol = Column(String, nullable=False, index=True)
    name = Column(String)
    exchange = Column(String, index=True)
    brexchange = Column(String, index=True)
    token = Column(String, index=True)
    expiry = Column(String)
    strike = Column(Float)
    lotsize = Column(Integer)
    instrumenttype = Column(String)
    tick_size = Column(Float)
    contract_value = Column(Float, default=1.0)

    __table_args__ = (Index("idx_symbol_exchange", "symbol", "exchange"),)


def init_db():
    logger.info("Initializing Master Contract DB")
    Base.metadata.create_all(bind=engine)
    try:
        from sqlalchemy import inspect as sa_inspect
        insp = sa_inspect(engine)
        existing_cols = {c["name"] for c in insp.get_columns("symtoken")}
        if "contract_value" not in existing_cols:
            with engine.connect() as conn:
                conn.execute(text("ALTER TABLE symtoken ADD COLUMN contract_value REAL DEFAULT 1.0"))
                conn.commit()
    except Exception as e:
        logger.error(f"Migration FAILED: {e}")


def delete_symtoken_table():
    logger.info("Deleting Symtoken Table")
    SymToken.query.delete()
    db_session.commit()


def copy_from_dataframe(df):
    logger.info("Performing Bulk Insert")
    data_dict = df.to_dict(orient="records")

    try:
        from sqlalchemy import inspect as sa_inspect
        _db_cols = {c["name"] for c in sa_inspect(engine).get_columns("symtoken")}
    except Exception:
        _db_cols = None

    if _db_cols is not None:
        extra_cols = {k for k in (data_dict[0] if data_dict else {}) if k not in _db_cols}
        if extra_cols:
            data_dict = [{k: v for k, v in row.items() if k not in extra_cols} for row in data_dict]

    existing_tokens = {result.token for result in db_session.query(SymToken.token).all()}
    filtered_data_dict = [row for row in data_dict if row["token"] not in existing_tokens]

    chunk_size = 500
    total_inserted = 0

    try:
        if filtered_data_dict:
            logger.info(f"Starting bulk insert of {len(filtered_data_dict)} records")
            for i in range(0, len(filtered_data_dict), chunk_size):
                chunk = filtered_data_dict[i : i + chunk_size]
                try:
                    db_session.bulk_insert_mappings(SymToken, chunk)
                    db_session.commit()
                    total_inserted += len(chunk)
                except Exception as chunk_error:
                    db_session.rollback()
                    try:
                        time.sleep(0.1)
                        db_session.bulk_insert_mappings(SymToken, chunk)
                        db_session.commit()
                        total_inserted += len(chunk)
                    except Exception as retry_error:
                        db_session.rollback()
                        continue
                time.sleep(0.005)
            logger.info(f"Bulk insert completed successfully with {total_inserted} new records.")
    except Exception as e:
        logger.exception(f"Error during bulk insert: {e}")
        db_session.rollback()


def fetch_alpaca_products():
    """Fetch all active Equities and Crypto assets from Alpaca /v2/assets."""
    api_key = os.getenv("BROKER_API_KEY", "").strip()
    api_secret = os.getenv("BROKER_API_SECRET", "").strip()

    if not api_key or not api_secret:
        logger.error("[Alpaca] Missing BROKER_API_KEY or BROKER_API_SECRET in environment.")
        return [], False

    url = f"{BASE_URL}/v2/assets"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
        "Accept": "application/json",
    }
    params = {"status": "active"}

    try:
        response = get_httpx_client().get(url, headers=headers, params=params, timeout=30.0)
        
        if response.status_code != 200:
            logger.error(f"[Alpaca] Failed to fetch assets: HTTP {response.status_code} - {response.text}")
            return [], False
            
        data = response.json()
        if not isinstance(data, list):
            logger.error("[Alpaca] Unexpected response format from /v2/assets.")
            return [], False
            
        return data, True
    except Exception as e:
        logger.error(f"[Alpaca] Exception fetching products: {e}")
        return [], False


def fetch_alpaca_options():
    """Fetch all active option contracts from Alpaca /v2/options/contracts with pagination."""
    api_key = os.getenv("BROKER_API_KEY", "").strip()
    api_secret = os.getenv("BROKER_API_SECRET", "").strip()

    if not api_key or not api_secret:
        logger.error("[Alpaca] Missing BROKER_API_KEY or BROKER_API_SECRET for options fetch.")
        return []

    url = f"{BASE_URL}/v2/options/contracts"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
        "Accept": "application/json",
    }

    all_contracts = []
    page_token = None
    client = get_httpx_client()

    while True:
        params = {"status": "active", "limit": 10000}
        if page_token:
            params["page_token"] = page_token

        try:
            response = client.get(url, headers=headers, params=params, timeout=60.0)
            if response.status_code != 200:
                logger.error(f"[Alpaca] Failed to fetch option contracts: HTTP {response.status_code} - {response.text}")
                break

            data = response.json()
            contracts = data.get("option_contracts", [])
            if not contracts:
                break

            all_contracts.extend(contracts)
            page_token = data.get("next_page_token")
            if not page_token:
                break
        except Exception as e:
            logger.error(f"[Alpaca] Exception fetching option contracts: {e}")
            break

    logger.info(f"Fetched {len(all_contracts)} option contracts from Alpaca.")
    return all_contracts


def process_alpaca_products(products):
    """Map Alpaca Asset objects to OpenAlgo's SymToken schema."""
    if not products:
        return pd.DataFrame()

    rows = []
    for p in products:
        if p.get("status") != "active" or not p.get("tradable"):
            continue

        brsymbol = p.get("symbol", "")
        asset_class = p.get("class", "")
        
        exchange = "CRYPTO" if asset_class == "crypto" else "US"
        canonical_symbol = brsymbol.replace("/", "").replace("-", "")

        try:
            lotsize = float(p.get("min_order_size") or 1)
        except (ValueError, TypeError):
            lotsize = 1.0

        try:
            tick_size = float(p.get("min_trade_increment") or 0.01)
        except (ValueError, TypeError):
            tick_size = 0.01

        rows.append({
            "token": str(p.get("id")),
            "symbol": canonical_symbol,
            "brsymbol": brsymbol,
            "name": p.get("name", ""),
            "exchange": exchange,
            "brexchange": p.get("exchange", "ALPACA"),
            "expiry": "",
            "strike": 0.0,
            "lotsize": lotsize,
            "instrumenttype": "SPOT",
            "tick_size": tick_size,
            "contract_value": 1.0,
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["token"], keep="first")
    logger.info(f"Processed {len(df)} live Alpaca assets.")
    return df


def process_alpaca_options(contracts):
    """Map Alpaca Option Contract objects to OpenAlgo's SymToken schema."""
    if not contracts:
        return pd.DataFrame()

    rows = []
    for c in contracts:
        if c.get("status") != "active" or not c.get("tradable"):
            continue

        brsymbol = c.get("symbol", "")
        underlying = c.get("underlying_symbol", "")
        expiration_date = c.get("expiration_date", "")
        contract_type = c.get("type", "").lower()
        strike_price = float(c.get("strike_price") or 0.0)

        instrumenttype = "CE" if contract_type == "call" else "PE"

        expiry_str = ""
        if expiration_date:
            try:
                dt = datetime.strptime(expiration_date, "%Y-%m-%d")
                expiry_str = dt.strftime("%d-%b-%y").upper()
            except Exception:
                expiry_str = expiration_date

        rows.append({
            "token": str(c.get("id")),
            "symbol": brsymbol,
            "brsymbol": brsymbol,
            "name": underlying,
            "exchange": "OPRA",
            "brexchange": "ALPACA",
            "expiry": expiry_str,
            "strike": strike_price,
            "lotsize": int(c.get("size") or 100),
            "instrumenttype": instrumenttype,
            "tick_size": 0.01,
            "contract_value": 1.0,
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["token"], keep="first")
    logger.info(f"Processed {len(df)} live Alpaca option contracts.")
    return df


def _safe_emit(event, data):
    """Safely emit socket events if the socketio server is initialized."""
    try:
        if socketio and getattr(socketio, "server", None) is not None:
            socketio.emit(event, data)
        else:
            logger.info(f"SocketIO not active — event '{event}': {data}")
    except Exception as e:
        logger.debug(f"SocketIO emit suppressed: {e}")


def master_contract_download():
    """Download and refresh the Master Contract database from Alpaca, including Equities, Crypto, and Options."""
    logger.info("Downloading Master Contract (Equities, Crypto, Options) from Alpaca")

    try:
        products, fetch_success = fetch_alpaca_products()
        token_df_equity = process_alpaca_products(products) if fetch_success else pd.DataFrame()

        options = fetch_alpaca_options()
        token_df_options = process_alpaca_options(options)

        dfs = [df for df in [token_df_equity, token_df_options] if not df.empty]
        if not dfs:
            _safe_emit(
                "master_contract_download",
                {"status": "error", "message": "No tradable instruments or options found from Alpaca."},
            )
            return

        token_df = pd.concat(dfs, ignore_index=True)

        delete_symtoken_table()
        copy_from_dataframe(token_df)
        
        success_msg = f"Successfully Downloaded {len(token_df)} Alpaca Instruments (Equities, Crypto, & Options)."
        logger.info(success_msg)
        _safe_emit(
            "master_contract_download",
            {
                "status": "success",
                "message": success_msg,
            },
        )

    except Exception as e:
        logger.exception(f"Error during Alpaca master contract download: {e}")
        _safe_emit("master_contract_download", {"status": "error", "message": str(e)})

SYMBOL_ALIASES = {
    "BTCUSDT": "BTCUSD",
    "ETHUSDT": "ETHUSD"
}


def search_symbols(symbol, exchange):
    canonical = SYMBOL_ALIASES.get(symbol.upper())
    if canonical:
        symbol = canonical
    return SymToken.query.filter(
        SymToken.symbol.ilike(f"%{symbol}%"), SymToken.exchange == exchange
    ).all()