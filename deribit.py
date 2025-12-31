"""
Crypto Data Extraction System
Extracts data from exchanges and stores in MySQL database with resume capability.
"""

import ccxt
import mysql.connector
from mysql.connector import Error
import json
import time
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional, Tuple
import re
from concurrent.futures import ThreadPoolExecutor, as_completed


# Selected exchanges - DERIBIT ONLY
SELECTED_EXCHANGES = [
    'deribit'
]

# Database names (uppercase exchange names)
EXCHANGE_DATABASES = [ex.upper() for ex in SELECTED_EXCHANGES]

# Get project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Progress checkpoint file - DERIBIT SPECIFIC (separate from main checkpoint)
CHECKPOINT_FILE = os.path.join(PROJECT_ROOT, 'crypto_data', 'extraction_checkpoint_deribit.json')

# Database credentials file
CREDENTIALS_FILE = os.path.join(PROJECT_ROOT, 'credentials.txt')


def read_credentials(file_path=CREDENTIALS_FILE):
    """Read database credentials from file."""
    credentials = {}
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        patterns = {
            'host': r'DATABASE_IP_ADDRESS\s*=\s*"([^"]+)"',
            'user': r'USER_NAME\s*=\s*"([^"]+)"',
            'password': r'USER_PASSWORD\s*=\s*"([^"]+)"',
            'database': r'DATABASE_NAME\s*=\s*"([^"]+)"',
            'port': r'PORT\s*=\s*(\d+)'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, content)
            if match:
                if key == 'port':
                    credentials[key] = int(match.group(1))
                else:
                    credentials[key] = match.group(1)
        
        return credentials
    except Exception as e:
        print(f"Error reading credentials: {e}")
        raise


def get_database_connection(creds, database_name=None):
    """Get MySQL database connection. Creates database if it doesn't exist."""
    try:
        if not database_name:
            # If no database name provided, connect without database
            connection = mysql.connector.connect(
                host=creds['host'],
                port=creds['port'],
                user=creds['user'],
                password=creds['password']
            )
            return connection
        
        # First try to connect to the database
        try:
            connection = mysql.connector.connect(
                host=creds['host'],
                port=creds['port'],
                user=creds['user'],
                password=creds['password'],
                database=database_name
            )
            return connection
        except Error as e:
            # If database doesn't exist, create it
            if e.errno == 1049:  # Unknown database
                temp_conn = mysql.connector.connect(
                    host=creds['host'],
                    port=creds['port'],
                    user=creds['user'],
                    password=creds['password']
                )
                cursor = temp_conn.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                temp_conn.commit()
                cursor.close()
                temp_conn.close()
                
                # Now connect to the created database
                connection = mysql.connector.connect(
                    host=creds['host'],
                    port=creds['port'],
                    user=creds['user'],
                    password=creds['password'],
                    database=database_name
                )
                return connection
            else:
                raise
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        raise


def create_database_if_not_exists(creds, db_name):
    """Create database if it doesn't exist."""
    try:
        # Connect without database to create it
        connection = get_database_connection(creds, database_name=None)
        cursor = connection.cursor()
        
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        connection.commit()
        
        cursor.close()
        connection.close()
        
        print(f"✓ Database '{db_name}' ready")
        return True
    except Error as e:
        print(f"Error creating database: {e}")
        return False


def sanitize_table_name(name):
    """Sanitize table name for MySQL."""
    # Remove or replace invalid characters
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Ensure it starts with letter or underscore
    if name and name[0].isdigit():
        name = '_' + name
    # Limit length
    if len(name) > 64:
        name = name[:64]
    return name.lower()


def create_instrument_table(connection, exchange_name, instrument_type, symbol, market_info, timeframe=None):
    """Create table for a specific instrument and timeframe with all public data columns."""
    base_name = f"{exchange_name}_{instrument_type}_{sanitize_table_name(symbol)}"
    
    # Add timeframe suffix if provided
    if timeframe:
        tf_suffix = sanitize_table_name(f"_tf_{timeframe}")
        table_name = base_name + tf_suffix
    else:
        table_name = base_name
    
    # Ensure table name is valid
    table_name = sanitize_table_name(table_name)
    
    try:
        cursor = connection.cursor()
        
        # Check if table exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = '{table_name}'
        """)
        
        if cursor.fetchone()[0] > 0:
            cursor.close()
            return table_name  # Table already exists
        
        # Create comprehensive table with all public data fields
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME NOT NULL,
            -- OHLCV data
            open DECIMAL(20, 8) NOT NULL,
            high DECIMAL(20, 8) NOT NULL,
            low DECIMAL(20, 8) NOT NULL,
            close DECIMAL(20, 8) NOT NULL,
            volume DECIMAL(30, 8) NOT NULL,
            quote_volume DECIMAL(30, 8),
            trades_count INT,
            -- Ticker data
            bid DECIMAL(20, 8),
            ask DECIMAL(20, 8),
            bid_volume DECIMAL(30, 8),
            ask_volume DECIMAL(30, 8),
            last_price DECIMAL(20, 8),
            change_24h DECIMAL(20, 8),
            change_percent_24h DECIMAL(10, 4),
            -- Futures/Perpetuals specific
            funding_rate DECIMAL(20, 10),
            open_interest DECIMAL(30, 8),
            funding_rate_next DECIMAL(20, 10),
            funding_rate_predicted DECIMAL(20, 10),
            -- Options specific
            strike_price DECIMAL(20, 8),
            option_type VARCHAR(10),
            expiry_date DATETIME,
            -- Order book data (snapshot)
            best_bid DECIMAL(20, 8),
            best_ask DECIMAL(20, 8),
            best_bid_size DECIMAL(30, 8),
            best_ask_size DECIMAL(30, 8),
            -- Additional metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_timestamp (timestamp),
            INDEX idx_timestamp (timestamp),
            INDEX idx_close (close)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        cursor.execute(create_query)
        connection.commit()
        cursor.close()
        
        print(f"  ✓ Created table: {table_name}")
        return table_name
        
    except Error as e:
        print(f"  ✗ Error creating table {table_name}: {e}")
        return None


def load_checkpoint():
    """Load progress checkpoint."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
            return {}
    return {}


def save_checkpoint(checkpoint_data):
    """Save progress checkpoint."""
    try:
        os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f, indent=2, default=str)
    except Exception as e:
        print(f"Error saving checkpoint: {e}")


def get_last_timestamp(connection, table_name):
    """Get the last timestamp from a table."""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT MAX(timestamp) FROM `{table_name}`")
        result = cursor.fetchone()
        cursor.close()
        
        if result and result[0]:
            return result[0]
        return None
    except Error:
        return None


def insert_comprehensive_data(connection, table_name, ohlcv_data, ticker_data, orderbook_data, 
                             funding_data, open_interest_data, instrument_type, market_info):
    """Insert comprehensive public data into table with fallback for missing fields."""
    if not ohlcv_data:
        return 0
    
    try:
        cursor = connection.cursor()
        
        inserted = 0
        for candle in ohlcv_data:
            try:
                timestamp = datetime.fromtimestamp(candle[0] / 1000)
                open_price = float(candle[1]) if len(candle) > 1 and candle[1] is not None else None
                high = float(candle[2]) if len(candle) > 2 and candle[2] is not None else None
                low = float(candle[3]) if len(candle) > 3 and candle[3] is not None else None
                close = float(candle[4]) if len(candle) > 4 and candle[4] is not None else None
                volume = float(candle[5]) if len(candle) > 5 and candle[5] is not None else 0
                quote_volume = float(candle[6]) if len(candle) > 6 and candle[6] is not None else None
                
                # Skip if essential data is missing
                if open_price is None or high is None or low is None or close is None:
                    continue
                
                # Get corresponding ticker data if available (with fallback)
                ticker = None
                if ticker_data:
                    # Try to get ticker for this timestamp, or use latest available
                    ticker = ticker_data.get(int(candle[0]))
                    if not ticker and ticker_data:
                        # Use the most recent ticker
                        ticker = list(ticker_data.values())[-1] if ticker_data else None
                
                # Get funding rate and open interest for futures (with fallback)
                funding_rate = None
                open_interest = None
                if instrument_type in ['future', 'futures', 'swap', 'perpetual']:
                    if funding_data:
                        funding_rate = funding_data.get(int(candle[0]))
                        if funding_rate is None and funding_data:
                            funding_rate = list(funding_data.values())[-1] if funding_data else None
                    if open_interest_data:
                        open_interest = open_interest_data.get(int(candle[0]))
                        if open_interest is None and open_interest_data:
                            open_interest = list(open_interest_data.values())[-1] if open_interest_data else None
                
                # Extract ticker fields (with fallback)
                bid = None
                ask = None
                bid_volume = None
                ask_volume = None
                last_price = close
                change_24h = None
                change_percent_24h = None
                
                if ticker:
                    try:
                        bid = float(ticker.get('bid')) if ticker.get('bid') is not None else None
                        ask = float(ticker.get('ask')) if ticker.get('ask') is not None else None
                        bid_volume = float(ticker.get('bidVolume')) if ticker.get('bidVolume') is not None else None
                        ask_volume = float(ticker.get('askVolume')) if ticker.get('askVolume') is not None else None
                        last_price = float(ticker.get('last')) if ticker.get('last') is not None else close
                        change_24h = float(ticker.get('change')) if ticker.get('change') is not None else None
                        change_percent_24h = float(ticker.get('percentage')) if ticker.get('percentage') is not None else None
                    except (ValueError, TypeError):
                        pass  # Use defaults if conversion fails
                
                # Extract order book data (with fallback)
                best_bid = None
                best_ask = None
                best_bid_size = None
                best_ask_size = None
                if orderbook_data:
                    ob = orderbook_data.get(int(candle[0]))
                    if not ob and orderbook_data:
                        ob = list(orderbook_data.values())[-1] if orderbook_data else None
                    
                    if ob and isinstance(ob, dict):
                        try:
                            if 'bids' in ob and ob['bids'] and len(ob['bids']) > 0:
                                best_bid = float(ob['bids'][0][0]) if ob['bids'][0][0] is not None else None
                                best_bid_size = float(ob['bids'][0][1]) if len(ob['bids'][0]) > 1 and ob['bids'][0][1] is not None else None
                            if 'asks' in ob and ob['asks'] and len(ob['asks']) > 0:
                                best_ask = float(ob['asks'][0][0]) if ob['asks'][0][0] is not None else None
                                best_ask_size = float(ob['asks'][0][1]) if len(ob['asks'][0]) > 1 and ob['asks'][0][1] is not None else None
                        except (ValueError, TypeError, IndexError):
                            pass  # Skip if extraction fails
                
                # Options specific (with fallback)
                strike_price = None
                option_type = None
                expiry_date = None
                if instrument_type == 'option':
                    try:
                        strike_price = float(market_info.get('strike')) if market_info.get('strike') is not None else None
                        option_type = str(market_info.get('option')) if market_info.get('option') else None
                        if market_info.get('expiry'):
                            expiry_date = datetime.fromtimestamp(market_info['expiry'] / 1000)
                    except (ValueError, TypeError):
                        pass  # Skip if extraction fails
                
                # Comprehensive insert query
                insert_query = f"""
                INSERT INTO `{table_name}` 
                (timestamp, open, high, low, close, volume, quote_volume, trades_count,
                 bid, ask, bid_volume, ask_volume, last_price, change_24h, change_percent_24h,
                 funding_rate, open_interest, funding_rate_next, funding_rate_predicted,
                 strike_price, option_type, expiry_date, best_bid, best_ask, best_bid_size, best_ask_size)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    high = VALUES(high),
                    low = VALUES(low),
                    close = VALUES(close),
                    volume = VALUES(volume),
                    quote_volume = COALESCE(VALUES(quote_volume), quote_volume),
                    trades_count = VALUES(trades_count),
                    bid = COALESCE(VALUES(bid), bid),
                    ask = COALESCE(VALUES(ask), ask),
                    bid_volume = COALESCE(VALUES(bid_volume), bid_volume),
                    ask_volume = COALESCE(VALUES(ask_volume), ask_volume),
                    last_price = COALESCE(VALUES(last_price), last_price),
                    change_24h = COALESCE(VALUES(change_24h), change_24h),
                    change_percent_24h = COALESCE(VALUES(change_percent_24h), change_percent_24h),
                    funding_rate = COALESCE(VALUES(funding_rate), funding_rate),
                    open_interest = COALESCE(VALUES(open_interest), open_interest),
                    best_bid = COALESCE(VALUES(best_bid), best_bid),
                    best_ask = COALESCE(VALUES(best_ask), best_ask),
                    best_bid_size = COALESCE(VALUES(best_bid_size), best_bid_size),
                    best_ask_size = COALESCE(VALUES(best_ask_size), best_ask_size)
                """
                
                cursor.execute(insert_query, (
                    timestamp, open_price, high, low, close, volume, quote_volume, 0,
                    bid, ask, bid_volume, ask_volume, last_price, change_24h, change_percent_24h,
                    funding_rate, open_interest, None, None,  # funding_rate_next, funding_rate_predicted
                    strike_price, option_type, expiry_date,
                    best_bid, best_ask, best_bid_size, best_ask_size
                ))
                
                inserted += 1
            except Exception as e:
                # Skip individual candle if it fails, continue with next
                continue
        
        connection.commit()
        cursor.close()
        return inserted
        
    except Error as e:
        print(f"    ✗ Error inserting data: {e}")
        connection.rollback()
        return 0
    except Exception as e:
        print(f"    ✗ Unexpected error: {e}")
        connection.rollback()
        return 0


def get_listing_timestamp(market_info):
    """Get listing timestamp from market info."""
    # Try to get listing date from market info
    if market_info.get('created'):
        try:
            return int(market_info['created'])
        except:
            pass
    
    # Default to 5 years ago if not available
    return int((datetime.now() - timedelta(days=5*365)).timestamp() * 1000)


def table_has_data(connection, table_name):
    """Check if table exists and has data."""
    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = '{table_name}'
        """)
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            cursor.close()
            return False
        
        # Check if table has data
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    except Error:
        return False


def check_and_clear_if_incomplete(connection, table_name):
    """
    Check if table's last entry is until December 28th.
    If not, clear the table and return True (needs restart).
    If yes or no data, return False (continue normally).
    """
    try:
        cursor = connection.cursor()
        
        # Check if table exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = '{table_name}'
        """)
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            cursor.close()
            return False
        
        # Get last timestamp from table
        cursor.execute(f"SELECT MAX(timestamp) FROM `{table_name}`")
        result = cursor.fetchone()
        
        if not result or not result[0]:
            # Table exists but has no data
            cursor.close()
            return False
        
        last_timestamp = result[0]
        
        # Check if last timestamp is until December 28th (not current day)
        cutoff_date = datetime(2025, 12, 28).date()  # Check only until Dec 28th
        last_date = last_timestamp.date() if isinstance(last_timestamp, datetime) else last_timestamp
        
        if last_date < cutoff_date:
            # Last entry is NOT until Dec 28th - continue fetching from next timestamp
            print(f"      ⚠ Last entry is from {last_date}, will continue fetching until Dec 28, 2025...")
            cursor.close()
            return False  # Don't clear, just continue fetching
        
        # Last entry is until Dec 28th - data is up to date
        cursor.close()
        return False
        
    except Error as e:
        print(f"      ⚠ Error checking table: {e}")
        return False


def fetch_all_public_data(exchange, symbol, instrument_type, market_info, connection, table_name, 
                          timeframe='1h', limit=5000, checkpoint=None, exchange_id=None, instrument_key=None):
    """Fetch all available public data and store in database with batch-by-batch insertion."""
    try:
        # Check if table has data and if last entry is until December 28th
        # If not until Dec 28th, continue fetching from next timestamp
        was_cleared = check_and_clear_if_incomplete(connection, table_name)
        
        # Check if table has data until Dec 28, 2025 - if yes, skip it
        if table_has_data(connection, table_name):
            last_timestamp = get_last_timestamp(connection, table_name)
            if last_timestamp:
                cutoff_date = datetime(2025, 12, 28).date()
                last_date = last_timestamp.date() if isinstance(last_timestamp, datetime) else last_timestamp
                if last_date >= cutoff_date:
                    return -1, None  # Return -1 to indicate skipped (data is up to date until Dec 28, 2025)
        
        # Get last timestamp from checkpoint first, then from database
        last_timestamp = None
        checkpoint_timestamp = None
        
        if checkpoint and exchange_id and instrument_key:
            # Ensure 'exchanges' key exists
            if 'exchanges' not in checkpoint:
                checkpoint['exchanges'] = {}
            exchange_checkpoint = checkpoint['exchanges'].get(exchange_id, {})
            if instrument_key in exchange_checkpoint:
                ts_str = exchange_checkpoint[instrument_key].get('last_timestamp')
                if ts_str:
                    try:
                        checkpoint_timestamp = datetime.fromisoformat(ts_str)
                    except:
                        pass
        
        # Check database for last timestamp
        db_timestamp = get_last_timestamp(connection, table_name)
        
        # Use the most recent timestamp
        if checkpoint_timestamp and db_timestamp:
            last_timestamp = max(checkpoint_timestamp, db_timestamp)
        elif checkpoint_timestamp:
            last_timestamp = checkpoint_timestamp
        elif db_timestamp:
            last_timestamp = db_timestamp
        
        # Determine since parameter - start from listing date if no data exists
        since = None
        if last_timestamp:
            # Start from last timestamp + timeframe duration
            timeframe_ms = get_timeframe_duration_ms(timeframe)
            since = int((last_timestamp.timestamp() * 1000) + timeframe_ms)
        else:
            # Start from listing date
            since = get_listing_timestamp(market_info)
        
        # Get exchange rate limit for calculating sleep times
        rate_limit_ms = exchange.rateLimit if exchange.rateLimit else 50
        rate_limit_seconds = rate_limit_ms / 1000
        
        # Fetch additional public data ONCE at the start (not per batch for speed)
        ticker_data = {}
        orderbook_data = {}
        funding_data = {}
        open_interest_data = {}
        
        # Fetch ticker data once (skip if fails)
        if hasattr(exchange, 'fetchTicker') and exchange.has.get('fetchTicker', False):
            try:
                ticker = exchange.fetch_ticker(symbol)
                if ticker:
                    ticker_data[int(datetime.now().timestamp() * 1000)] = ticker
            except Exception:
                pass  # Skip if not available
        
        # Fetch order book once (skip if fails)
        if hasattr(exchange, 'fetchOrderBook') and exchange.has.get('fetchOrderBook', False):
            try:
                orderbook = exchange.fetch_order_book(symbol, limit=20)
                if orderbook:
                    orderbook_data[int(datetime.now().timestamp() * 1000)] = orderbook
            except Exception:
                pass  # Skip if not available
        
        # Fetch funding rate for futures once (skip if fails)
        if instrument_type in ['future', 'futures', 'swap', 'perpetual']:
            if hasattr(exchange, 'fetchFundingRate') and exchange.has.get('fetchFundingRate', False):
                try:
                    funding = exchange.fetch_funding_rate(symbol)
                    if funding and isinstance(funding, dict):
                        funding_data[int(datetime.now().timestamp() * 1000)] = funding.get('fundingRate')
                except Exception:
                    pass  # Skip if not available
            
            # Fetch open interest once (skip if fails)
            if hasattr(exchange, 'fetchOpenInterest') and exchange.has.get('fetchOpenInterest', False):
                try:
                    oi = exchange.fetch_open_interest(symbol)
                    if oi and isinstance(oi, dict):
                        open_interest_data[int(datetime.now().timestamp() * 1000)] = oi.get('openInterestAmount')
                except Exception:
                    pass  # Skip if not available
        
        current_since = since
        total_inserted = 0
        batch_count = 0
        
        print(f"      Fetching OHLCV from {datetime.fromtimestamp(since/1000)}...", end=' ')
        
        # Optimize batch size - use larger batches but stay within safe limits
        # Most exchanges support 1000-5000, but we'll use 2000 for balance of speed and safety
        optimized_limit = min(limit, 2000)  # Safe batch size that's still 2x faster
        
        # Fetch and insert in batches
        while True:
            try:
                # Fetch one batch with optimized limit
                ohlcv_batch = exchange.fetch_ohlcv(symbol, timeframe, since=current_since, limit=optimized_limit)
                
                if not ohlcv_batch or len(ohlcv_batch) == 0:
                    break
                
                batch_count += 1
                
                # Insert batch immediately (using pre-fetched additional data)
                inserted = insert_comprehensive_data(
                    connection, table_name, ohlcv_batch, ticker_data, orderbook_data,
                    funding_data, open_interest_data, instrument_type, market_info
                )
                
                total_inserted += inserted
                
                # Update checkpoint immediately after each batch
                if inserted > 0 and checkpoint and exchange_id and instrument_key:
                    last_batch_timestamp = ohlcv_batch[-1][0] if ohlcv_batch else None
                    if last_batch_timestamp:
                        # Ensure 'exchanges' key exists in checkpoint
                        if 'exchanges' not in checkpoint:
                            checkpoint['exchanges'] = {}
                        if exchange_id not in checkpoint['exchanges']:
                            checkpoint['exchanges'][exchange_id] = {}
                        
                        # Update instrument-level timestamp
                        if instrument_key not in checkpoint['exchanges'][exchange_id]:
                            checkpoint['exchanges'][exchange_id][instrument_key] = {}
                        checkpoint['exchanges'][exchange_id][instrument_key]['last_timestamp'] = datetime.fromtimestamp(last_batch_timestamp / 1000).isoformat()
                        
                        # Update exchange-level last_updated timestamp
                        checkpoint['exchanges'][exchange_id]['last_updated'] = datetime.now().isoformat()
                        
                        # Update global last_updated timestamp
                        checkpoint['last_updated'] = datetime.now().isoformat()
                        
                        save_checkpoint(checkpoint)
                
                # Log progress after every batch
                batch_timestamp = datetime.fromtimestamp(ohlcv_batch[-1][0] / 1000).strftime('%Y-%m-%d %H:%M:%S') if ohlcv_batch else 'N/A'
                print(f"      [Batch {batch_count}] Inserted {inserted} candles | Total: {total_inserted} | Last timestamp: {batch_timestamp}")
                
                # Update since for next batch
                current_since = ohlcv_batch[-1][0] + 1
                
                # Check if we've reached December 28, 2025 (cutoff date)
                cutoff_timestamp = int(datetime(2025, 12, 28, 23, 59, 59).timestamp() * 1000)
                if ohlcv_batch[-1][0] >= cutoff_timestamp:
                    print(f"      ✓ Reached cutoff date (Dec 28, 2025), stopping fetch...")
                    break
                
                # Optimized rate limiting - use exchange rate limit as delay
                sleep_time = rate_limit_seconds
                time.sleep(sleep_time)
                
            except ccxt.RateLimitExceeded:
                # If rate limited, implement exponential backoff based on rate limit
                print("      ⚠ Rate limit hit, backing off...", end=' ')
                backoff_multiplier = min(5 * (batch_count % 5 + 1), 30)
                backoff_time = max(rate_limit_seconds * backoff_multiplier, rate_limit_seconds * 10)
                time.sleep(backoff_time)
                print(f"resuming after {backoff_time:.2f}s...", end=' ')
                # Increase delay temporarily after rate limit
                time.sleep(rate_limit_seconds * 2)
                continue
            except ccxt.DDoSProtection:
                # DDoS protection triggered - wait longer based on rate limit
                print("      ⚠ DDoS protection triggered, waiting...", end=' ')
                ddos_wait = max(rate_limit_seconds * 20, rate_limit_seconds * 10)
                time.sleep(ddos_wait)
                print("resuming...", end=' ')
                continue
            except Exception as e:
                error_msg = str(e)
                # Check if it's a rate limit related error
                if 'rate limit' in error_msg.lower() or '429' in error_msg or 'too many requests' in error_msg.lower():
                    print("      ⚠ Rate limit detected, backing off...", end=' ')
                    time.sleep(rate_limit_seconds * 10)
                    print("resuming...", end=' ')
                    continue
                # Skip non-critical errors and continue
                elif 'not found' in error_msg.lower() or 'invalid' in error_msg.lower():
                    print(f"⚠ {error_msg[:50]}")
                    break
                else:
                    print(f"⚠ {error_msg[:50]}, retrying...", end=' ')
                    time.sleep(rate_limit_seconds * 2)
                    continue
        
        print(f"✓ {total_inserted} candles in {batch_count} batch(es)")
        
        # Final progress log if we processed many batches
        if batch_count >= 1000:
            print(f"      [Final] Total: {batch_count} batches, {total_inserted} candles")
        
        # Return last timestamp for checkpoint
        if total_inserted > 0:
            # Get the last timestamp from the table
            last_db_timestamp = get_last_timestamp(connection, table_name)
            if last_db_timestamp:
                return total_inserted, int(last_db_timestamp.timestamp() * 1000)
        
        return total_inserted, None
        
    except Exception as e:
        print(f"    ✗ Error in fetch_all_public_data: {e}")
        # Don't print full traceback for expected errors
        if 'not found' not in str(e).lower() and 'invalid' not in str(e).lower():
            import traceback
            traceback.print_exc()
        return 0, None


def get_timeframe_duration_ms(timeframe):
    """Convert timeframe string to milliseconds."""
    timeframe_map = {
        '1s': 1000,
        '1m': 60 * 1000,
        '3m': 3 * 60 * 1000,
        '5m': 5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '30m': 30 * 60 * 1000,
        '1h': 60 * 60 * 1000,
        '2h': 2 * 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '6h': 6 * 60 * 60 * 1000,
        '8h': 8 * 60 * 60 * 1000,
        '12h': 12 * 60 * 60 * 1000,
        '1d': 24 * 60 * 60 * 1000,
        '3d': 3 * 24 * 60 * 60 * 1000,
        '1w': 7 * 24 * 60 * 60 * 1000,
        '1M': 30 * 24 * 60 * 60 * 1000,
    }
    return timeframe_map.get(timeframe, 60 * 60 * 1000)  # Default to 1h


def process_exchange(exchange_id, creds, checkpoint):
    """Process a single exchange."""
    print(f"\n{'='*80}")
    print(f"Processing Exchange: {exchange_id.upper()}")
    print('='*80)
    
    # Check if exchange is already completed
    if exchange_id in checkpoint.get('completed_exchanges', []):
        print(f"✓ Exchange {exchange_id} already completed, skipping...")
        return True
    
    try:
        # Initialize exchange
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        
        print(f"Exchange: {exchange.name}")
        
        # Get exchange rate limit for calculating sleep times
        rate_limit_ms = exchange.rateLimit if exchange.rateLimit else 50
        rate_limit_seconds = rate_limit_ms / 1000
        
        # Load markets with retry for network issues
        print("Loading markets...", end=' ')
        max_retries = 3
        retry_count = 0
        markets = None
        
        while retry_count < max_retries:
            try:
                markets = exchange.load_markets()
                print(f"✓ ({len(markets)} markets)")
                break
            except (ccxt.NetworkError, ccxt.RequestTimeout, ccxt.ExchangeError) as e:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"⚠ Network error (retry {retry_count}/{max_retries})...", end=' ')
                    # Get rate limit for backoff calculation
                    rate_limit_ms = exchange.rateLimit if exchange.rateLimit else 50
                    rate_limit_seconds = rate_limit_ms / 1000
                    time.sleep(rate_limit_seconds * 5 * retry_count)  # Exponential backoff based on rate limit
                else:
                    print(f"✗ Failed after {max_retries} retries: {e}")
                    raise
            except Exception as e:
                print(f"✗ Error: {e}")
                raise
        
        if not markets:
            print("✗ Failed to load markets")
            return False
        
        # Create database for this exchange
        db_name = exchange_id.upper()
        create_database_if_not_exists(creds, db_name)
        
        # Get connection to exchange database
        connection = get_database_connection(creds, db_name)
        
        # Categorize markets
        spot_markets = [(s, m) for s, m in markets.items() 
                       if m.get('type') == 'spot' and m.get('active', True)]
        futures_markets = [(s, m) for s, m in markets.items() 
                          if m.get('type') in ['future', 'futures', 'swap', 'perpetual'] 
                          and m.get('active', True)]
        options_markets = [(s, m) for s, m in markets.items() 
                          if m.get('type') == 'option' and m.get('active', True)]
        
        print(f"\nMarkets to process:")
        print(f"  - Spot: {len(spot_markets)}")
        print(f"  - Futures/Perpetuals: {len(futures_markets)}")
        print(f"  - Options: {len(options_markets)}")
        
        # Get available timeframes - only specific ones we want
        all_timeframes = list(exchange.timeframes.keys()) if hasattr(exchange, 'timeframes') and exchange.timeframes else []
        desired_timeframes = ['1s', '1m', '3m', '1h', '4h', '8h', '1d']
        timeframes = [tf for tf in desired_timeframes if tf in all_timeframes]
        
        print(f"\nAvailable timeframes: {len(all_timeframes)} total")
        print(f"  All: {', '.join(sorted(all_timeframes))}")
        print(f"Selected timeframes: {len(timeframes)}")
        print(f"  {', '.join(sorted(timeframes))}")
        
        if not timeframes:
            print("  ⚠ No desired timeframes available, skipping exchange")
            return False
        
        # Process each market type
        all_markets = [
            ('spot', spot_markets),
            ('futures', futures_markets),
            ('options', options_markets)
        ]
        
        exchange_checkpoint = checkpoint.get('exchanges', {}).get(exchange_id, {})
        
        for market_type, market_list in all_markets:
            if not market_list:
                continue
            
            print(f"\nProcessing {market_type.upper()} markets...")
            
            completed_instruments = exchange_checkpoint.get('completed_instruments', [])
            
            for symbol, market_info in market_list:
                # Process each timeframe for this instrument
                for timeframe in sorted(timeframes):
                    # Check if already completed for this timeframe
                    instrument_key = f"{market_type}_{symbol}_{timeframe}"
                    if instrument_key in completed_instruments:
                        continue
                    
                    print(f"  Processing {symbol} ({market_type}) - {timeframe}...")
                    
                    try:
                        # Create table for this timeframe
                        table_name = create_instrument_table(
                            connection, exchange_id, market_type, symbol, market_info, timeframe=timeframe
                        )
                        
                        if not table_name:
                            print(f"    ✗ Failed to create table")
                            continue
                        
                        # Check if table has data and if it's up to date (until Dec 28th)
                        # Will continue fetching from next timestamp if data is incomplete
                        was_cleared = check_and_clear_if_incomplete(connection, table_name)
                        
                        # If table has data until Dec 28th, skip it
                        if table_has_data(connection, table_name):
                            last_timestamp = get_last_timestamp(connection, table_name)
                            if last_timestamp:
                                cutoff_date = datetime(2025, 12, 28).date()
                                last_date = last_timestamp.date() if isinstance(last_timestamp, datetime) else last_timestamp
                                if last_date >= cutoff_date:
                                    print(f"    ⏭ Skipping (table already has data until Dec 28th)")
                                    # Mark as completed in checkpoint
                                    if 'exchanges' not in checkpoint:
                                        checkpoint['exchanges'] = {}
                                    if exchange_id not in checkpoint['exchanges']:
                                        checkpoint['exchanges'][exchange_id] = {}
                                    if 'completed_instruments' not in checkpoint['exchanges'][exchange_id]:
                                        checkpoint['exchanges'][exchange_id]['completed_instruments'] = []
                                    if instrument_key not in checkpoint['exchanges'][exchange_id]['completed_instruments']:
                                        checkpoint['exchanges'][exchange_id]['completed_instruments'].append(instrument_key)
                                    
                                    # Update timestamps
                                    checkpoint['exchanges'][exchange_id]['last_updated'] = datetime.now().isoformat()
                                    checkpoint['last_updated'] = datetime.now().isoformat()
                                    
                                    save_checkpoint(checkpoint)
                                    continue
                        
                        # If table needs continuation (data incomplete), ensure it's not marked as completed
                        if was_cleared:
                            if 'exchanges' not in checkpoint:
                                checkpoint['exchanges'] = {}
                            if exchange_id not in checkpoint['exchanges']:
                                checkpoint['exchanges'][exchange_id] = {}
                            if 'completed_instruments' in checkpoint['exchanges'][exchange_id]:
                                if instrument_key in checkpoint['exchanges'][exchange_id]['completed_instruments']:
                                    checkpoint['exchanges'][exchange_id]['completed_instruments'].remove(instrument_key)
                                # Also remove the timestamp entry
                                if instrument_key in checkpoint['exchanges'][exchange_id]:
                                    del checkpoint['exchanges'][exchange_id][instrument_key]
                            
                            # Update timestamps
                            checkpoint['exchanges'][exchange_id]['last_updated'] = datetime.now().isoformat()
                            checkpoint['last_updated'] = datetime.now().isoformat()
                            
                            save_checkpoint(checkpoint)
                        
                        # Fetch and store all public data with checkpoint (optimized batch size)
                        inserted, last_timestamp = fetch_all_public_data(
                            exchange, symbol, market_type, market_info, 
                            connection, table_name, timeframe=timeframe, limit=5000,
                            checkpoint=checkpoint, exchange_id=exchange_id, instrument_key=instrument_key
                        )
                        
                        # Check if skipped (returned -1)
                        if inserted == -1:
                            print(f"    ⏭ Skipped (already has data)")
                            continue
                        
                        if inserted > 0:
                            print(f"    ✓ Inserted {inserted} candles")
                            
                            # Update checkpoint
                            if 'exchanges' not in checkpoint:
                                checkpoint['exchanges'] = {}
                            if exchange_id not in checkpoint['exchanges']:
                                checkpoint['exchanges'][exchange_id] = {}
                            if 'completed_instruments' not in checkpoint['exchanges'][exchange_id]:
                                checkpoint['exchanges'][exchange_id]['completed_instruments'] = []
                            
                            checkpoint['exchanges'][exchange_id]['completed_instruments'].append(instrument_key)
                            checkpoint['exchanges'][exchange_id]['last_updated'] = datetime.now().isoformat()
                            
                            if last_timestamp:
                                if instrument_key not in checkpoint['exchanges'][exchange_id]:
                                    checkpoint['exchanges'][exchange_id][instrument_key] = {}
                                checkpoint['exchanges'][exchange_id][instrument_key]['last_timestamp'] = datetime.fromtimestamp(last_timestamp / 1000).isoformat()
                            
                            # Update global last_updated timestamp
                            checkpoint['last_updated'] = datetime.now().isoformat()
                            
                            save_checkpoint(checkpoint)
                        else:
                            print(f"    ⚠ No new data")
                        
                        # Minimal rate limiting between timeframes (based on exchange rate limit)
                        time.sleep(rate_limit_seconds)
                        
                    except Exception as e:
                        print(f"    ✗ Error: {e}")
                        continue
                
                # Minimal rate limiting between instruments (based on exchange rate limit)
                time.sleep(rate_limit_seconds)
        
        # Mark exchange as completed
        if exchange_id not in checkpoint.get('completed_exchanges', []):
            checkpoint['completed_exchanges'] = checkpoint.get('completed_exchanges', [])
            checkpoint['completed_exchanges'].append(exchange_id)
        
        # Update exchange and global timestamps
        if 'exchanges' not in checkpoint:
            checkpoint['exchanges'] = {}
        if exchange_id not in checkpoint['exchanges']:
            checkpoint['exchanges'][exchange_id] = {}
        checkpoint['exchanges'][exchange_id]['last_updated'] = datetime.now().isoformat()
        checkpoint['last_updated'] = datetime.now().isoformat()
        save_checkpoint(checkpoint)
        
        connection.close()
        print(f"\n✓ Exchange {exchange_id} processing complete!")
        return True
        
    except Exception as e:
        print(f"\n✗ Error processing exchange {exchange_id}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main extraction function."""
    print("="*80)
    print("CRYPTO DATA EXTRACTION SYSTEM")
    print("="*80)
    print(f"\nExchanges to process: {', '.join(SELECTED_EXCHANGES)}")
    print(f"Checkpoint file: {CHECKPOINT_FILE}")
    print("\nStarting extraction...")
    print("="*80)
    
    # Load credentials
    try:
        creds = read_credentials()
        print(f"\n✓ Credentials loaded")
    except Exception as e:
        print(f"\n✗ Error loading credentials: {e}")
        return
    
    # Load checkpoint
    checkpoint = load_checkpoint()
    print(f"✓ Checkpoint loaded")
    
    # Initialize global timestamps if not present
    if 'last_updated' not in checkpoint:
        checkpoint['last_updated'] = datetime.now().isoformat()
    checkpoint['start_time'] = datetime.now().isoformat()
    save_checkpoint(checkpoint)
    
    if checkpoint.get('completed_exchanges'):
        print(f"  Resuming: {len(checkpoint['completed_exchanges'])} exchange(s) already completed")
    
    # Process each exchange sequentially (one at a time for maximum speed per exchange)
    for exchange_id in SELECTED_EXCHANGES:
        try:
            success = process_exchange(exchange_id, creds, checkpoint)
            if success:
                print(f"\n✓ {exchange_id.upper()} completed successfully")
            else:
                print(f"\n✗ {exchange_id.upper()} had errors")
        except KeyboardInterrupt:
            print(f"\n\n⚠ Extraction interrupted by user")
            print(f"Progress saved to checkpoint. Resume by running again.")
            break
        except Exception as e:
            print(f"\n✗ Error with {exchange_id}: {e}")
            continue
    
    # Update final timestamps
    checkpoint['last_updated'] = datetime.now().isoformat()
    checkpoint['end_time'] = datetime.now().isoformat()
    save_checkpoint(checkpoint)
    
    print("\n" + "="*80)
    print("EXTRACTION COMPLETE")
    print("="*80)
    print(f"\nCompleted exchanges: {len(checkpoint.get('completed_exchanges', []))}/{len(SELECTED_EXCHANGES)}")
    print(f"Checkpoint saved to: {CHECKPOINT_FILE}")
    if checkpoint.get('start_time'):
        start_dt = datetime.fromisoformat(checkpoint['start_time'])
        end_dt = datetime.fromisoformat(checkpoint['end_time'])
        duration = end_dt - start_dt
        print(f"Total duration: {duration}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nExtraction interrupted. Progress saved to checkpoint.")
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()

