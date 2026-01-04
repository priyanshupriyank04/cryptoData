"""
Get Top Volume Crypto Coins from Binance
Fetches 6 months of daily OHLCV data and aggregates volume to find:
- Top 10 unique coins (by base currency, aggregated across all pairs)
- Next top 10 coins by volume (may include duplicates)
"""

import ccxt
import time
from datetime import datetime, timedelta
from operator import itemgetter

def get_top_volume_coins():
    """Fetch top 20 coins (10 unique + 10 by volume) by aggregated daily volume over past 6 months from Binance."""
    print("="*80)
    print("FETCHING TOP VOLUME COINS FROM BINANCE")
    print("(Aggregated Daily Volume - Past 6 Months)")
    print("(Top 10 Unique Coins + Next Top 10 by Volume)")
    print("="*80)
    
    try:
        # Initialize Binance exchange
        print("\n[1/5] Initializing Binance exchange...")
        exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        print("✓ Exchange initialized")
        
        # Load markets
        print("\n[2/5] Loading markets...")
        markets = exchange.load_markets()
        print(f"✓ Loaded {len(markets)} markets")
        
        # Filter spot markets only
        spot_markets = {s: m for s, m in markets.items() 
                       if m.get('type') == 'spot' and m.get('active', True)}
        print(f"✓ Found {len(spot_markets)} active spot markets")
        
        # Calculate 6 months ago timestamp
        six_months_ago = datetime.now() - timedelta(days=180)
        since_timestamp = int(six_months_ago.timestamp() * 1000)
        print(f"\n[3/5] Fetching daily OHLCV data from {six_months_ago.strftime('%Y-%m-%d')}...")
        print(f"      (This may take several minutes due to rate limiting)")
        
        # Fetch historical volume data for each market
        volume_data = []
        processed = 0
        failed = 0
        
        for symbol, market_info in spot_markets.items():
            processed += 1
            try:
                # Fetch daily OHLCV data for past 6 months
                # Binance allows up to 1000 candles per request, so we need multiple requests
                ohlcv_data = []
                current_since = since_timestamp
                max_limit = 1000  # Binance max limit per request
                
                while True:
                    try:
                        # Fetch one batch
                        batch = exchange.fetch_ohlcv(symbol, '1d', since=current_since, limit=max_limit)
                        
                        if not batch or len(batch) == 0:
                            break
                        
                        ohlcv_data.extend(batch)
                        
                        # Update since for next batch
                        current_since = batch[-1][0] + 1
                        
                        # Check if we've reached today
                        if batch[-1][0] >= int(datetime.now().timestamp() * 1000):
                            break
                        
                        # Rate limiting
                        time.sleep(exchange.rateLimit / 1000 * 1.1)
                        
                    except ccxt.RateLimitExceeded:
                        print(f"      ⚠ Rate limit hit, waiting...")
                        time.sleep(5)
                        continue
                    except Exception as e:
                        if 'not found' in str(e).lower() or 'invalid' in str(e).lower():
                            break
                        time.sleep(2)
                        continue
                
                # Aggregate volumes (quote volume from OHLCV)
                total_volume = 0
                days_count = 0
                
                for candle in ohlcv_data:
                    if len(candle) >= 6:  # [timestamp, open, high, low, close, volume, quote_volume]
                        # Use quote_volume if available (index 6), otherwise calculate from volume * close
                        if len(candle) > 6 and candle[6] is not None:
                            total_volume += float(candle[6])  # quote_volume
                        else:
                            # Calculate quote volume: volume * close price
                            volume = float(candle[5]) if candle[5] else 0
                            close = float(candle[4]) if candle[4] else 0
                            total_volume += volume * close
                        days_count += 1
                
                if total_volume > 0:
                    volume_data.append({
                        'symbol': symbol,
                        'total_volume_6m': total_volume,
                        'avg_daily_volume': total_volume / days_count if days_count > 0 else 0,
                        'days_count': days_count,
                        'base': market_info.get('base'),
                        'quote': market_info.get('quote')
                    })
                
                # Progress update every 50 markets
                if processed % 50 == 0:
                    print(f"      Processed {processed}/{len(spot_markets)} markets... (Failed: {failed})")
                
            except ccxt.RateLimitExceeded:
                print(f"      ⚠ Rate limit exceeded, waiting longer...")
                time.sleep(10)
                failed += 1
                continue
            except Exception as e:
                # Skip markets that fail (might be delisted, invalid, etc.)
                failed += 1
                if processed % 100 == 0:  # Only print errors occasionally
                    print(f"      ⚠ Skipped {symbol}: {str(e)[:50]}")
                continue
        
        print(f"\n[4/5] Processing volume data...")
        print(f"✓ Processed {processed} markets ({failed} failed)")
        
        # Sort by total aggregated volume (descending)
        volume_data.sort(key=itemgetter('total_volume_6m'), reverse=True)
        
        # Group by base currency and aggregate volumes
        base_volume_map = {}
        base_best_pair = {}
        
        for coin in volume_data:
            base = coin['base']
            volume = coin['total_volume_6m']
            
            if base not in base_volume_map:
                base_volume_map[base] = 0
                base_best_pair[base] = coin
            
            # Sum volumes for same base coin
            base_volume_map[base] += volume
            
            # Keep the pair with highest volume for this base
            if coin['total_volume_6m'] > base_best_pair[base]['total_volume_6m']:
                base_best_pair[base] = coin
        
        # Create list of unique coins with aggregated volumes
        unique_coins = []
        for base, total_volume in base_volume_map.items():
            best_pair = base_best_pair[base]
            unique_coins.append({
                'symbol': best_pair['symbol'],
                'base': base,
                'total_volume_6m': total_volume,  # Aggregated across all pairs
                'avg_daily_volume': total_volume / best_pair['days_count'] if best_pair['days_count'] > 0 else 0,
                'days_count': best_pair['days_count'],
                'quote': best_pair['quote']
            })
        
        # Sort unique coins by aggregated volume
        unique_coins.sort(key=itemgetter('total_volume_6m'), reverse=True)
        top_10_unique = unique_coins[:10]
        
        # Get next top 10 from original volume_data (excluding already selected symbols)
        selected_symbols = {coin['symbol'] for coin in top_10_unique}
        next_top_10 = [coin for coin in volume_data if coin['symbol'] not in selected_symbols][:10]
        
        print(f"✓ Found top 10 unique coins and next top 10 by volume")
        
        # Display results
        print("\n" + "="*80)
        print("TOP 10 UNIQUE COINS BY AGGREGATED DAILY VOLUME (PAST 6 MONTHS):")
        print("="*80)
        for i, coin in enumerate(top_10_unique, 1):
            total_vol = coin['total_volume_6m']
            avg_daily = coin['avg_daily_volume']
            days = coin['days_count']
            
            total_str = f"${total_vol/1e9:.2f}B" if total_vol >= 1e9 else f"${total_vol/1e6:.2f}M"
            avg_str = f"${avg_daily/1e6:.2f}M" if avg_daily >= 1e6 else f"${avg_daily/1e3:.2f}K"
            
            print(f"{i:2d}. {coin['symbol']:20s} | Total 6M: {total_str:>12s} | Avg Daily: {avg_str:>12s} | Days: {days:3d} | {coin['base']}/{coin['quote']}")
        
        print("\n" + "="*80)
        print("NEXT TOP 10 COINS BY VOLUME (MAY INCLUDE DUPLICATES):")
        print("="*80)
        for i, coin in enumerate(next_top_10, 1):
            total_vol = coin['total_volume_6m']
            avg_daily = coin['avg_daily_volume']
            days = coin['days_count']
            
            total_str = f"${total_vol/1e9:.2f}B" if total_vol >= 1e9 else f"${total_vol/1e6:.2f}M"
            avg_str = f"${avg_daily/1e6:.2f}M" if avg_daily >= 1e6 else f"${avg_daily/1e3:.2f}K"
            
            print(f"{i:2d}. {coin['symbol']:20s} | Total 6M: {total_str:>12s} | Avg Daily: {avg_str:>12s} | Days: {days:3d} | {coin['base']}/{coin['quote']}")
        
        # Save to volume.txt: first 10 unique, then next 10
        print("\n[5/5] Writing to volume.txt...")
        with open('volume.txt', 'w') as f:
            # Write top 10 unique coins
            for coin in top_10_unique:
                f.write(f"{coin['symbol']}\n")
            # Write next top 10 coins
            for coin in next_top_10:
                f.write(f"{coin['symbol']}\n")
        
        print("✓ Saved to volume.txt (20 coins total: 10 unique + 10 by volume)")
        print("\n" + "="*80)
        print("COMPLETE")
        print("="*80)
        print(f"\nTop 20 coins saved to: volume.txt")
        print(f"  - First 10: Unique coins (aggregated volume by base currency)")
        print(f"  - Next 10: Top volume coins (may include duplicates)")
        
        return top_10_unique + next_top_10
        
    except ccxt.RateLimitExceeded:
        print("✗ Rate limit exceeded. Please wait and try again.")
        return None
    except ccxt.NetworkError as e:
        print(f"✗ Network error: {e}")
        return None
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    try:
        get_top_volume_coins()
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()

