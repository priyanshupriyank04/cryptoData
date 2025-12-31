"""
Crypto Data Extraction System - Parallel Instance
Extracts data from exchanges and stores in MySQL database with resume capability.
This version is designed to run in parallel with other instances by using separate checkpoint files.
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
import sys


# Selected exchanges - MODIFY THIS LIST to process different exchanges
# Each parallel instance should have a different set of exchanges
SELECTED_EXCHANGES = [
    'bybit',  # Put different exchange first for this instance
    'okx',
    'htx',
    'gate',
    'deribit',
    'phemex',
    'binance'  # Move binance to end if you want to process it later
]

# Database names (uppercase exchange names)
EXCHANGE_DATABASES = [ex.upper() for ex in SELECTED_EXCHANGES]

# Get project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Progress checkpoint file - UNIQUE PER INSTANCE based on exchanges
# This ensures multiple instances don't conflict
CHECKPOINT_FILE = os.path.join(
    PROJECT_ROOT, 
    'crypto_data', 
    f'extraction_checkpoint_{"_".join(SELECTED_EXCHANGES[:2])}.json'  # Uses first 2 exchange names
)

# Database credentials file
CREDENTIALS_FILE = os.path.join(PROJECT_ROOT, 'credentials.txt')


def read_credentials(file_path=CREDENTIALS_FILE):
    """Read database credentials from file."""
    credentials = {}
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        patterns = {
            'ip': r'DATABASE_IP_ADDRESS\s*=\s*["\']([^"\']+)["\']',
            'user': r'USER_NAME\s*=\s*["\']([^"\']+)["\']',
            'password': r'USER_PASSWORD\s*=\s*["\']([^"\']+)["\']',
            'database': r'DATABASE_NAME\s*=\s*["\']([^"\']+)["\']',
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
        raise Exception(f"Error reading credentials: {e}")


def get_database_connection(creds, database_name=None):
    """Get MySQL database connection, creating database if it doesn't exist."""
    try:
        # First, try to connect without specifying database (to create it if needed)
        connection = mysql.connector.connect(
            host=creds['ip'],
            user=creds['user'],
            password=creds['password'],
            port=creds.get('port', 3306)
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Create database if it doesn't exist
            if database_name:
                try:
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
                    connection.commit()
                    print(f"  ✓ Database '{database_name}' ready")
                except Error as e:
                    print(f"  ⚠ Database creation check: {e}")
            
            # Now connect to the specific database
            cursor.close()
            connection.close()
            
            connection = mysql.connector.connect(
                host=creds['ip'],
                user=creds['user'],
                password=creds['password'],
                port=creds.get('port', 3306),
                database=database_name
            )
            
            return connection
    except Error as e:
        if e.errno == 1049:  # Database doesn't exist
            # Try to create it
            try:
                temp_conn = mysql.connector.connect(
                    host=creds['ip'],
                    user=creds['user'],
                    password=creds['password'],
                    port=creds.get('port', 3306)
                )
                cursor = temp_conn.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
                temp_conn.commit()
                cursor.close()
                temp_conn.close()
                
                # Now connect to the new database
                connection = mysql.connector.connect(
                    host=creds['ip'],
                    user=creds['user'],
                    password=creds['password'],
                    port=creds.get('port', 3306),
                    database=database_name
                )
                return connection
            except Error as e2:
                raise Exception(f"Error creating database: {e2}")
        else:
            raise Exception(f"Error connecting to database: {e}")
    return None


def create_database_if_not_exists(creds, database_name):
    """Create database if it doesn't exist."""
    try:
        connection = mysql.connector.connect(
            host=creds['ip'],
            user=creds['user'],
            password=creds['password'],
            port=creds.get('port', 3306)
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
            connection.commit()
            cursor.close()
            connection.close()
            return True
    except Error as e:
        print(f"  ⚠ Error creating database: {e}")
        return False
    return False


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
    return timeframe_map.get(timeframe, 60 * 1000)


def get_listing_timestamp(market_info):
    """Get listing timestamp from market info, fallback to 5 years ago."""
    if market_info and 'created' in market_info and market_info['created']:
        try:
            return int(market_info['created'])
        except:
            pass
    
    # Fallback: 5 years ago
    five_years_ago = datetime.now() - timedelta(days=5*365)
    return int(five_years_ago.timestamp() * 1000)


def create_instrument_table(connection, exchange_id, market_type, symbol, market_info, timeframe='1m'):
    """Create table for an instrument with comprehensive schema."""
    try:
        # Clean symbol for table name
        clean_symbol = symbol.replace('/', '_').replace('-', '_').upper()
        table_name = f"{exchange_id}_{market_type}_{clean_symbol}_tf_{timeframe}"
        
        cursor = connection.cursor()
        
        # Comprehensive table schema
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            timestamp BIGINT PRIMARY KEY,
            open DECIMAL(30, 10),
            high DECIMAL(30, 10),
            low DECIMAL(30, 10),
            close DECIMAL(30, 10),
            volume DECIMAL(30, 10),
            trade_count INT,
            ticker_bid DECIMAL(30, 10),
            ticker_ask DECIMAL(30, 10),
            ticker_last DECIMAL(30, 10),
            ticker_volume_24h DECIMAL(30, 10),
            ticker_high_24h DECIMAL(30, 10),
            ticker_low_24h DECIMAL(30, 10),
            ticker_change_24h DECIMAL(30, 10),
            orderbook_bids_price DECIMAL(30, 10),
            orderbook_bids_amount DECIMAL(30, 10),
            orderbook_asks_price DECIMAL(30, 10),
            orderbook_asks_amount DECIMAL(30, 10),
            funding_rate DECIMAL(30, 10),
            open_interest DECIMAL(30, 10),
            options_strike DECIMAL(30, 10),
            options_expiry BIGINT,
            options_type VARCHAR(10),
            options_underlying VARCHAR(50),
            options_iv DECIMAL(30, 10),
            options_delta DECIMAL(30, 10),
            options_gamma DECIMAL(30, 10),
            options_theta DECIMAL(30, 10),
            options_vega DECIMAL(30, 10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_timestamp (timestamp),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        
        return table_name
    except Error as e:
        print(f"    ✗ Error creating table: {e}")
        return None


def table_has_data(connection, table_name):
    """Check if table has any data."""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    except:
        return False


def get_last_timestamp(connection, table_name):
    """Get the last timestamp from a table."""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT MAX(timestamp) FROM `{table_name}`")
        result = cursor.fetchone()
        cursor.close()
        if result and result[0]:
            return datetime.fromtimestamp(result[0] / 1000)
        return None
    except:
        return None


def check_and_clear_if_incomplete(connection, table_name):
    """Check if table data is up to date (last entry from today). If not, clear it."""
    try:
        if not table_has_data(connection, table_name):
            return False
        
        last_timestamp = get_last_timestamp(connection, table_name)
        if not last_timestamp:
            return False
        
        # Check if last entry is from today
        today = datetime.now().date()
        last_date = last_timestamp.date()
        
        if last_date < today:
            # Data is incomplete/old, clear the table
            print(f"    ⚠ Table data incomplete (last entry: {last_date}), clearing...")
            cursor = connection.cursor()
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
            connection.commit()
            cursor.close()
            return True
        
        return False
    except Exception as e:
        print(f"    ⚠ Error checking table completeness: {e}")
        return False


def insert_comprehensive_data(connection, table_name, ohlcv_batch, ticker_data, orderbook_data,
                             funding_data, open_interest_data, instrument_type, market_info):
    """Insert comprehensive data into table."""
    if not ohlcv_batch:
        return 0
    
    try:
        cursor = connection.cursor()
        inserted = 0
        
        for candle in ohlcv_batch:
            timestamp = candle[0]
            
            # Get additional data for this timestamp (or closest)
            ticker = ticker_data.get(timestamp) or (ticker_data.get(max([t for t in ticker_data.keys() if t <= timestamp], default=None)) if ticker_data else None)
            orderbook = orderbook_data.get(timestamp) or (orderbook_data.get(max([t for t in orderbook_data.keys() if t <= timestamp], default=None)) if orderbook_data else None)
            funding = funding_data.get(timestamp) or (funding_data.get(max([t for t in funding_data.keys() if t <= timestamp], default=None)) if funding_data else None)
            oi = open_interest_data.get(timestamp) or (open_interest_data.get(max([t for t in open_interest_data.keys() if t <= timestamp], default=None)) if open_interest_data else None)
            
            # Extract OHLCV data
            open_price = candle[1] if len(candle) > 1 else None
            high = candle[2] if len(candle) > 2 else None
            low = candle[3] if len(candle) > 3 else None
            close = candle[4] if len(candle) > 4 else None
            volume = candle[5] if len(candle) > 5 else None
            trade_count = candle[6] if len(candle) > 6 else None
            
            # Extract ticker data
            ticker_bid = ticker.get('bid') if ticker else None
            ticker_ask = ticker.get('ask') if ticker else None
            ticker_last = ticker.get('last') if ticker else None
            ticker_volume_24h = ticker.get('quoteVolume') if ticker else None
            ticker_high_24h = ticker.get('high') if ticker else None
            ticker_low_24h = ticker.get('low') if ticker else None
            ticker_change_24h = ticker.get('percentage') if ticker else None
            
            # Extract orderbook data
            orderbook_bids_price = orderbook['bids'][0][0] if orderbook and orderbook.get('bids') and len(orderbook['bids']) > 0 else None
            orderbook_bids_amount = orderbook['bids'][0][1] if orderbook and orderbook.get('bids') and len(orderbook['bids']) > 0 else None
            orderbook_asks_price = orderbook['asks'][0][0] if orderbook and orderbook.get('asks') and len(orderbook['asks']) > 0 else None
            orderbook_asks_amount = orderbook['asks'][0][1] if orderbook and orderbook.get('asks') and len(orderbook['asks']) > 0 else None
            
            # Extract funding rate
            funding_rate = funding if funding else None
            
            # Extract open interest
            open_interest = oi if oi else None
            
            # Extract options data from market_info
            options_strike = market_info.get('strike') if market_info else None
            options_expiry = market_info.get('expiry') if market_info else None
            options_type = market_info.get('option') if market_info else None
            options_underlying = market_info.get('underlying') if market_info else None
            options_iv = market_info.get('info', {}).get('iv') if market_info and 'info' in market_info else None
            options_delta = market_info.get('info', {}).get('delta') if market_info and 'info' in market_info else None
            options_gamma = market_info.get('info', {}).get('gamma') if market_info and 'info' in market_info else None
            options_theta = market_info.get('info', {}).get('theta') if market_info and 'info' in market_info else None
            options_vega = market_info.get('info', {}).get('vega') if market_info and 'info' in market_info else None
            
            insert_query = f"""
            INSERT INTO `{table_name}` (
                timestamp, open, high, low, close, volume, trade_count,
                ticker_bid, ticker_ask, ticker_last, ticker_volume_24h, ticker_high_24h, ticker_low_24h, ticker_change_24h,
                orderbook_bids_price, orderbook_bids_amount, orderbook_asks_price, orderbook_asks_amount,
                funding_rate, open_interest,
                options_strike, options_expiry, options_type, options_underlying,
                options_iv, options_delta, options_gamma, options_theta, options_vega
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume),
                trade_count = VALUES(trade_count),
                ticker_bid = VALUES(ticker_bid),
                ticker_ask = VALUES(ticker_ask),
                ticker_last = VALUES(ticker_last),
                ticker_volume_24h = VALUES(ticker_volume_24h),
                ticker_high_24h = VALUES(ticker_high_24h),
                ticker_low_24h = VALUES(ticker_low_24h),
                ticker_change_24h = VALUES(ticker_change_24h),
                orderbook_bids_price = VALUES(orderbook_bids_price),
                orderbook_bids_amount = VALUES(orderbook_bids_amount),
                orderbook_asks_price = VALUES(orderbook_asks_price),
                orderbook_asks_amount = VALUES(orderbook_asks_amount),
                funding_rate = VALUES(funding_rate),
                open_interest = VALUES(open_interest),
                options_strike = VALUES(options_strike),
                options_expiry = VALUES(options_expiry),
                options_type = VALUES(options_type),
                options_underlying = VALUES(options_underlying),
                options_iv = VALUES(options_iv),
                options_delta = VALUES(options_delta),
                options_gamma = VALUES(options_gamma),
                options_theta = VALUES(options_theta),
                options_vega = VALUES(options_vega),
                updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(insert_query, (
                timestamp, open_price, high, low, close, volume, trade_count,
                ticker_bid, ticker_ask, ticker_last, ticker_volume_24h, ticker_high_24h, ticker_low_24h, ticker_change_24h,
                orderbook_bids_price, orderbook_bids_amount, orderbook_asks_price, orderbook_asks_amount,
                funding_rate, open_interest,
                options_strike, options_expiry, options_type, options_underlying,
                options_iv, options_delta, options_gamma, options_theta, options_vega
            ))
            inserted += 1
        
        connection.commit()
        cursor.close()
        return inserted
    except Error as e:
        print(f"    ✗ Error inserting data: {e}")
        connection.rollback()
        return 0


def fetch_all_public_data(exchange, symbol, market_type, market_info, connection, table_name,
                          timeframe='1m', limit=5000, checkpoint=None, exchange_id=None, instrument_key=None):
    """Fetch all public data for an instrument and store in database."""
    try:
        # Check if already completed
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
        last_timestamp = None
        
        if db_timestamp:
            last_timestamp = db_timestamp
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
                orderbook = exchange.fetch_order_book(symbol)
                if orderbook:
                    orderbook_data[int(datetime.now().timestamp() * 1000)] = orderbook
            except Exception:
                pass  # Skip if not available
        
        # Fetch funding rate for futures/perps (skip if fails)
        if market_type in ['future', 'swap'] and hasattr(exchange, 'fetchFundingRate'):
            try:
                funding = exchange.fetch_funding_rate(symbol)
                if funding:
                    funding_data[int(datetime.now().timestamp() * 1000)] = funding.get('fundingRate')
            except Exception:
                pass  # Skip if not available
        
        # Fetch open interest for futures/perps (skip if fails)
        if market_type in ['future', 'swap'] and hasattr(exchange, 'fetchOpenInterest'):
            try:
                oi = exchange.fetch_open_interest(symbol)
                if oi:
                    open_interest_data[int(datetime.now().timestamp() * 1000)] = oi.get('openInterestAmount')
            except Exception:
                pass  # Skip if not available
        
        # Determine instrument type
        instrument_type = market_type
        
        # Fetch and insert in batches
        current_since = since
        batch_count = 0
        total_inserted = 0
        
        # Optimize batch size based on timeframe
        optimized_limit = min(limit, 2000)
        
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
                
                # Check if we've reached current time
                if ohlcv_batch[-1][0] >= int(datetime.now().timestamp() * 1000):
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
        
        # Get database connection
        connection = get_database_connection(creds, db_name)
        if not connection:
            print(f"✗ Failed to connect to database {db_name}")
            return False
        
        print(f"✓ Connected to database: {db_name}")
        
        # Filter markets by type
        spot_markets = [(symbol, info) for symbol, info in markets.items() if info.get('type') == 'spot']
        futures_markets = [(symbol, info) for symbol, info in markets.items() if info.get('type') in ['future', 'swap']]
        options_markets = [(symbol, info) for symbol, info in markets.items() if info.get('type') == 'option']
        
        print(f"  Spot markets: {len(spot_markets)}")
        print(f"  Futures/Perp markets: {len(futures_markets)}")
        print(f"  Options markets: {len(options_markets)}")
        
        # Get available timeframes
        timeframes = exchange.timeframes if hasattr(exchange, 'timeframes') and exchange.timeframes else ['1m', '5m', '15m', '1h', '4h', '1d']
        print(f"  Available timeframes: {', '.join(sorted(timeframes))}")
        
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
                        
                        # Check if table has data and if it's up to date (from today)
                        # This will clear table if data is incomplete/old
                        was_cleared = check_and_clear_if_incomplete(connection, table_name)
                        
                        # If table has data from today, skip it
                        if not was_cleared and table_has_data(connection, table_name):
                            print(f"    ⏭ Skipping (table already has up-to-date data)")
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
                        
                        # If table was cleared, remove from completed_instruments to restart
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
    print("CRYPTO DATA EXTRACTION SYSTEM - PARALLEL INSTANCE")
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
    if checkpoint.get('start_time') and checkpoint.get('end_time'):
        start_dt = datetime.fromisoformat(checkpoint['start_time'])
        end_dt = datetime.fromisoformat(checkpoint['end_time'])
        duration = end_dt - start_dt
        print(f"Total duration: {duration}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Script interrupted by user")
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()

