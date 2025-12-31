"""
MySQL Insert Debug Script
Tests MySQL connection and insert operations to debug connection loss issues.
"""

import mysql.connector
from mysql.connector import Error
import re
import os
from datetime import datetime
import time

# Get project root directory (parent of crypto_data folder)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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
        print(f"[ERROR] Error reading credentials: {e}")
        raise


def get_database_connection(creds, database_name='cryptoData2', reconnect=True):
    """Get MySQL database connection with proper settings."""
    try:
        connection = mysql.connector.connect(
            host=creds['host'],
            port=creds['port'],
            user=creds['user'],
            password=creds['password'],
            database=database_name,
            autocommit=False,
            connect_timeout=10,
            connection_timeout=10,
            # Connection pool settings
            pool_name='test_pool',
            pool_size=1,
            pool_reset_session=True,
            # Keep connection alive
            sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION',
            # Timeout settings
            init_command="SET SESSION wait_timeout=28800, interactive_timeout=28800"
        )
        return connection
    except Error as e:
        print(f"[ERROR] Connection error: {e}")
        raise


def check_connection(connection):
    """Check if connection is still alive."""
    try:
        if connection is None:
            return False
        if not connection.is_connected():
            return False
        # Try a simple query
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        return True
    except Exception as e:
        print(f"[WARN] Connection check failed: {e}")
        return False


def reconnect(creds, database_name='cryptoData2'):
    """Reconnect to database."""
    try:
        print("[INFO] Attempting to reconnect...")
        connection = get_database_connection(creds, database_name)
        if connection.is_connected():
            print("[OK] Reconnected successfully")
            return connection
        else:
            print("[ERROR] Reconnection failed - not connected")
            return None
    except Exception as e:
        print(f"[ERROR] Reconnection error: {e}")
        return None


def test_simple_insert(connection, table_name='test_table'):
    """Test simple insert operation."""
    try:
        print(f"\n[*] Test 1: Creating test table '{table_name}'...")
        cursor = connection.cursor()
        
        # Create test table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME NOT NULL,
                value DECIMAL(20, 8),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_timestamp (timestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        connection.commit()
        print(f"   [OK] Table created/verified")
        
        # Test insert
        print(f"\n[*] Test 2: Inserting test data...")
        test_timestamp = datetime.now()
        insert_query = f"""
            INSERT INTO `{table_name}` (timestamp, value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value)
        """
        
        cursor.execute(insert_query, (test_timestamp, 123.456))
        connection.commit()
        print(f"   [OK] Insert successful")
        
        # Verify insert
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        print(f"   [OK] Table now has {count} row(s)")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"   [ERROR] Insert failed: {e}")
        print(f"   [ERROR] Error code: {e.errno}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
        return False
    except Exception as e:
        print(f"   [ERROR] Unexpected error: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
        return False


def test_batch_insert(connection, table_name='test_table', batch_size=100):
    """Test batch insert operation."""
    try:
        print(f"\n[*] Test 3: Batch insert ({batch_size} rows)...")
        cursor = connection.cursor()
        
        # Check connection before batch
        if not check_connection(connection):
            print(f"   [ERROR] Connection lost before batch insert")
            return False
        
        insert_query = f"""
            INSERT INTO `{table_name}` (timestamp, value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value)
        """
        
        # Prepare batch data
        batch_data = []
        base_time = datetime.now()
        for i in range(batch_size):
            timestamp = base_time.replace(microsecond=i * 1000)
            batch_data.append((timestamp, 100.0 + i))
        
        # Insert batch
        print(f"   [INFO] Inserting {len(batch_data)} rows...")
        cursor.executemany(insert_query, batch_data)
        connection.commit()
        print(f"   [OK] Batch insert successful")
        
        # Verify
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        print(f"   [OK] Table now has {count} row(s)")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"   [ERROR] Batch insert failed: {e}")
        print(f"   [ERROR] Error code: {e.errno}")
        if 'Lost connection' in str(e):
            print(f"   [WARN] Connection lost during batch insert!")
        if connection:
            try:
                connection.rollback()
            except:
                pass
        return False
    except Exception as e:
        print(f"   [ERROR] Unexpected error: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
        return False


def test_connection_stability(connection, creds, table_name='test_table', duration=30):
    """Test connection stability over time."""
    try:
        print(f"\n[*] Test 4: Connection stability test ({duration} seconds)...")
        cursor = connection.cursor()
        
        insert_query = f"""
            INSERT INTO `{table_name}` (timestamp, value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value)
        """
        
        start_time = time.time()
        insert_count = 0
        error_count = 0
        
        while time.time() - start_time < duration:
            try:
                # Check connection
                if not check_connection(connection):
                    print(f"   [WARN] Connection lost at {insert_count} inserts, reconnecting...")
                    connection = reconnect(creds, 'cryptoData2')
                    if not connection:
                        print(f"   [ERROR] Failed to reconnect, stopping test")
                        break
                    cursor = connection.cursor()
                    error_count += 1
                    continue
                
                # Insert
                test_timestamp = datetime.now()
                cursor.execute(insert_query, (test_timestamp, time.time()))
                connection.commit()
                insert_count += 1
                
                if insert_count % 10 == 0:
                    print(f"   [INFO] {insert_count} inserts completed...")
                
                time.sleep(1)  # 1 second between inserts
                
            except Error as e:
                error_count += 1
                print(f"   [ERROR] Insert {insert_count} failed: {e}")
                if 'Lost connection' in str(e):
                    print(f"   [WARN] Connection lost, attempting reconnect...")
                    try:
                        connection.rollback()
                    except:
                        pass
                    connection = reconnect(creds, 'cryptoData2')
                    if connection:
                        cursor = connection.cursor()
                    else:
                        break
                else:
                    try:
                        connection.rollback()
                    except:
                        pass
                    time.sleep(1)
        
        cursor.close()
        print(f"\n   [RESULT] Completed {insert_count} inserts with {error_count} errors")
        return insert_count > 0 and error_count == 0
        
    except Exception as e:
        print(f"   [ERROR] Stability test failed: {e}")
        return False


def main():
    """Main test function."""
    print("=" * 80)
    print("MySQL Insert Debug Test")
    print("=" * 80)
    
    # Read credentials
    try:
        print("\n[*] Reading credentials...")
        creds = read_credentials()
        print(f"   [OK] Credentials loaded")
        print(f"   - Host: {creds.get('host')}")
        print(f"   - Port: {creds.get('port')}")
        print(f"   - User: {creds.get('user')}")
        print(f"   - Database: cryptoData2")
    except Exception as e:
        print(f"[ERROR] Failed to load credentials: {e}")
        return
    
    # Connect
    connection = None
    try:
        print("\n[*] Connecting to MySQL...")
        connection = get_database_connection(creds, 'cryptoData2')
        if connection.is_connected():
            db_info = connection.server_info
            print(f"   [OK] Connected to MySQL Server")
            print(f"   [INFO] MySQL Version: {db_info}")
        else:
            print(f"   [ERROR] Connection failed")
            return
    except Error as e:
        print(f"   [ERROR] Connection error: {e}")
        return
    
    # Run tests
    try:
        # Test 1: Simple insert
        if not test_simple_insert(connection):
            print("\n[WARN] Simple insert test failed")
        
        # Test 2: Batch insert
        if not test_batch_insert(connection, batch_size=50):
            print("\n[WARN] Batch insert test failed")
        
        # Test 3: Connection stability (shorter test)
        print("\n[*] Running short stability test (10 seconds)...")
        test_connection_stability(connection, creds, duration=10)
        
    finally:
        # Cleanup
        if connection and connection.is_connected():
            try:
                cursor = connection.cursor()
                cursor.execute("DROP TABLE IF EXISTS test_table")
                connection.commit()
                cursor.close()
                print("\n[INFO] Test table cleaned up")
            except:
                pass
            
            connection.close()
            print("[INFO] Connection closed")
    
    print("\n" + "=" * 80)
    print("Test Complete")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[INFO] Test interrupted by user")
    except Exception as e:
        print(f"\n[ERROR] Fatal error: {e}")
        import traceback
        traceback.print_exc()

