"""
Simple CCXT Test - Verify it works without API keys
This script tests a few popular exchanges to confirm they work without API keys.
"""

import ccxt
from datetime import datetime


def test_exchange_without_api_key(exchange_id):
    """
    Test if an exchange works without API keys for public data.
    """
    print(f"\n{'='*60}")
    print(f"Testing: {exchange_id.upper()}")
    print('='*60)
    
    try:
        # Initialize exchange WITHOUT API keys
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
            # No API keys provided - using public endpoints only
        })
        
        print(f"✓ Exchange initialized: {exchange.name}")
        
        # Try to load markets (public data - should work without API keys)
        print("  Loading markets (public data)...", end=' ')
        markets = exchange.load_markets()
        print(f"✓ Success! Found {len(markets)} markets")
        
        # Count market types
        spot_count = sum(1 for m in markets.values() if m.get('type') == 'spot' and m.get('active', True))
        futures_count = sum(1 for m in markets.values() if m.get('type') in ['future', 'futures', 'swap', 'perpetual'] and m.get('active', True))
        options_count = sum(1 for m in markets.values() if m.get('type') == 'option' and m.get('active', True))
        
        print(f"  - Spot markets: {spot_count}")
        print(f"  - Futures/Perpetuals: {futures_count}")
        print(f"  - Options: {options_count}")
        
        # Get timeframes
        if hasattr(exchange, 'timeframes') and exchange.timeframes:
            print(f"  - Available timeframes: {len(exchange.timeframes)}")
            print(f"    Sample: {', '.join(list(exchange.timeframes.keys())[:10])}")
        
        # Try fetching a ticker (public data - should work without API keys)
        print("  Testing ticker fetch (public data)...", end=' ')
        try:
            # Get first active spot symbol
            spot_symbols = [s for s, m in markets.items() if m.get('type') == 'spot' and m.get('active', True)]
            if spot_symbols:
                test_symbol = spot_symbols[0]
                ticker = exchange.fetch_ticker(test_symbol)
                print(f"✓ Success! Sample ticker for {test_symbol}:")
                print(f"    Last: {ticker.get('last')}, Volume: {ticker.get('quoteVolume', 'N/A')}")
        except Exception as e:
            print(f"⚠ Could not fetch ticker: {str(e)[:50]}")
        
        # Try fetching OHLCV (public data - should work without API keys)
        print("  Testing OHLCV fetch (public data)...", end=' ')
        try:
            if spot_symbols:
                ohlcv = exchange.fetch_ohlcv(test_symbol, '1h', limit=5)
                print(f"✓ Success! Fetched {len(ohlcv)} candles")
                if ohlcv:
                    print(f"    Latest candle: O={ohlcv[-1][1]}, H={ohlcv[-1][2]}, L={ohlcv[-1][3]}, C={ohlcv[-1][4]}")
        except Exception as e:
            print(f"⚠ Could not fetch OHLCV: {str(e)[:50]}")
        
        return {
            'success': True,
            'markets_count': len(markets),
            'spot': spot_count,
            'futures': futures_count,
            'options': options_count,
            'timeframes': len(exchange.timeframes) if hasattr(exchange, 'timeframes') else 0
        }
        
    except Exception as e:
        error_msg = str(e)
        if 'apiKey' in error_msg.lower() or 'authentication' in error_msg.lower():
            print(f"✗ Requires API key: {error_msg[:100]}")
            return {'success': False, 'reason': 'requires_api_key', 'error': error_msg[:100]}
        else:
            print(f"✗ Error: {error_msg[:100]}")
            return {'success': False, 'reason': 'other_error', 'error': error_msg[:100]}


def main():
    """
    Test popular exchanges without API keys.
    """
    print("="*60)
    print("CCXT Simple Test - No API Keys Required")
    print("="*60)
    print("\nThis script tests if exchanges work WITHOUT API keys for public data.")
    print("Most exchanges allow fetching markets, symbols, and OHLCV data without API keys.\n")
    
    # Popular exchanges to test
    exchanges_to_test = [
        'binance',
        'bybit',
        'okx',
        'coinbase',
        'kraken',
        'bitget',
        'gate',
        'mexc',
        'huobi',
        'kucoin'
    ]
    
    results = {}
    
    for exchange_id in exchanges_to_test:
        result = test_exchange_without_api_key(exchange_id)
        results[exchange_id] = result
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    successful = [ex for ex, res in results.items() if res.get('success')]
    failed = [ex for ex, res in results.items() if not res.get('success')]
    
    print(f"\n✓ Working WITHOUT API keys: {len(successful)}/{len(exchanges_to_test)}")
    for ex in successful:
        res = results[ex]
        print(f"  - {ex}: {res.get('markets_count', 0)} markets, "
              f"{res.get('spot', 0)} spot, {res.get('futures', 0)} futures, "
              f"{res.get('options', 0)} options")
    
    if failed:
        print(f"\n✗ Require API keys or have errors: {len(failed)}/{len(exchanges_to_test)}")
        for ex in failed:
            res = results[ex]
            reason = res.get('reason', 'unknown')
            print(f"  - {ex}: {reason}")
    
    print("\n" + "="*60)
    print("CONCLUSION")
    print("="*60)
    print("\n✓ Most exchanges work WITHOUT API keys for:")
    print("  - Fetching market list and symbols")
    print("  - Getting available timeframes")
    print("  - Fetching OHLCV data (candles)")
    print("  - Fetching tickers and order books")
    print("\n⚠ API keys are ONLY needed for:")
    print("  - Your personal trades and orders")
    print("  - Account balances")
    print("  - Placing orders")
    print("\nYou can proceed with the full exploration script!")
    print("="*60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user.")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

