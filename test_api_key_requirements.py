"""
API Key Requirements Tester - Public Data Only
This script tests what PUBLIC data can be fetched WITHOUT API keys
for the selected exchanges: BINANCE, BYBIT, OKX, HTX, GATE, DERIBIT, PHEMEX

Tests only public endpoints:
- Market data (OHLCV, tickers, order books, trades)
- Market information (markets list, currencies)
- Futures data (funding rates, open interest, leverage tiers)

Also provides:
- Available market types (spot, futures, options)
- Available timeframes for OHLCV data
- Sample symbols for testing
"""

import ccxt
from datetime import datetime, timedelta
import time


# Selected exchanges to test
SELECTED_EXCHANGES = [
    'binance',
    'bybit',
    'okx',
    'htx',  # HTX (formerly Huobi)
    'gate',
    'deribit',
    'phemex'
]

# Public data types to test (NO API keys required)
PUBLIC_DATA_TYPES = [
    ('fetchMarkets', 'Load markets list'),
    ('fetchTicker', 'Get ticker data'),
    ('fetchOHLCV', 'Get OHLCV/candles'),
    ('fetchOrderBook', 'Get order book'),
    ('fetchTrades', 'Get recent trades'),
    ('fetchCurrencies', 'Get currencies list'),
    ('fetchFundingRate', 'Get funding rate (futures)'),
    ('fetchOpenInterest', 'Get open interest (futures)'),
    ('fetchLeverageTiers', 'Get leverage tiers'),
]


def test_exchange_data_access(exchange_id):
    """
    Test what public data can be accessed without API keys.
    
    Args:
        exchange_id: Exchange ID (e.g., 'binance')
        
    Returns:
        dict: Test results
    """
    print(f"\n{'='*80}")
    print(f"Testing: {exchange_id.upper()}")
    print('='*80)
    
    results = {
        'exchange_id': exchange_id,
        'exchange_name': None,
        'public_data': {},
        'market_types_available': [],
        'timeframes_available': [],
        'sample_symbols': {},
        'errors': []
    }
    
    try:
        # Initialize exchange WITHOUT API keys
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        
        results['exchange_name'] = exchange.name if hasattr(exchange, 'name') else exchange_id
        print(f"Exchange: {results['exchange_name']}")
        
        # Load markets
        print("\n1. Loading markets...", end=' ')
        try:
            markets = exchange.load_markets()
            print(f"✓ Success! ({len(markets)} markets)")
            
            # Categorize markets
            spot_symbols = [s for s, m in markets.items() 
                           if m.get('type') == 'spot' and m.get('active', True)]
            futures_symbols = [s for s, m in markets.items() 
                              if m.get('type') in ['future', 'futures', 'swap', 'perpetual'] 
                              and m.get('active', True)]
            options_symbols = [s for s, m in markets.items() 
                             if m.get('type') == 'option' and m.get('active', True)]
            
            results['market_types_available'] = []
            results['sample_symbols'] = {}
            
            if spot_symbols:
                results['market_types_available'].append('spot')
                results['sample_symbols']['spot'] = spot_symbols[0] if spot_symbols else None
                print(f"   - Spot: {len(spot_symbols)} markets")
            
            if futures_symbols:
                results['market_types_available'].append('futures')
                results['sample_symbols']['futures'] = futures_symbols[0] if futures_symbols else None
                print(f"   - Futures/Perpetuals: {len(futures_symbols)} markets")
            
            if options_symbols:
                results['market_types_available'].append('options')
                results['sample_symbols']['options'] = options_symbols[0] if options_symbols else None
                print(f"   - Options: {len(options_symbols)} markets")
            
            # Get timeframes
            if hasattr(exchange, 'timeframes') and exchange.timeframes:
                results['timeframes_available'] = list(exchange.timeframes.keys())
                print(f"   - Timeframes: {len(results['timeframes_available'])} available")
            
        except Exception as e:
            error_msg = str(e)
            results['errors'].append(f"load_markets: {error_msg}")
            print(f"✗ Failed: {error_msg[:100]}")
            return results
        
        # Test public data endpoints
        print("\n2. Testing PUBLIC data endpoints (NO API keys required):")
        print("-" * 80)
        
        for method_name, description in PUBLIC_DATA_TYPES:
            if not hasattr(exchange, method_name):
                results['public_data'][method_name] = {
                    'available': False,
                    'reason': 'Method not available in exchange'
                }
                print(f"   ✗ {description}: Method not available")
                continue
            
            try:
                # Get a test symbol based on method requirements
                test_symbol = None
                if 'Funding' in description or 'OpenInterest' in description or 'Leverage' in description:
                    # These need futures
                    test_symbol = results['sample_symbols'].get('futures')
                else:
                    # Default to spot
                    test_symbol = results['sample_symbols'].get('spot')
                
                if not test_symbol:
                    results['public_data'][method_name] = {
                        'available': False,
                        'reason': 'No suitable test symbol available'
                    }
                    print(f"   ⚠ {description}: No test symbol available")
                    continue
                
                method = getattr(exchange, method_name)
                
                # Call method with appropriate parameters
                if method_name == 'fetchOHLCV':
                    result = method(test_symbol, '1h', limit=1)
                elif method_name == 'fetchOrderBook':
                    result = method(test_symbol, limit=5)
                elif method_name == 'fetchTrades':
                    result = method(test_symbol, limit=1)
                elif method_name == 'fetchTicker':
                    result = method(test_symbol)
                elif method_name == 'fetchFundingRate':
                    result = method(test_symbol)
                elif method_name == 'fetchOpenInterest':
                    result = method(test_symbol)
                elif method_name == 'fetchLeverageTiers':
                    result = method()
                elif method_name == 'fetchMarkets':
                    result = method()
                elif method_name == 'fetchCurrencies':
                    result = method()
                else:
                    result = method()
                
                results['public_data'][method_name] = {
                    'available': True,
                    'requires_api_key': False,
                    'test_symbol': test_symbol,
                    'result_type': type(result).__name__
                }
                print(f"   ✓ {description}: Works WITHOUT API key")
                
                # Rate limiting
                time.sleep(exchange.rateLimit / 1000 if exchange.rateLimit else 0.1)
                
            except ccxt.AuthenticationError:
                results['public_data'][method_name] = {
                    'available': True,
                    'requires_api_key': True,
                    'reason': 'Authentication required'
                }
                print(f"   ⚠ {description}: Requires API key")
            except Exception as e:
                error_msg = str(e)
                if 'apiKey' in error_msg.lower() or 'authentication' in error_msg.lower():
                    results['public_data'][method_name] = {
                        'available': True,
                        'requires_api_key': True,
                        'reason': error_msg[:100]
                    }
                    print(f"   ⚠ {description}: Requires API key")
                else:
                    results['public_data'][method_name] = {
                        'available': False,
                        'requires_api_key': False,
                        'reason': error_msg[:100]
                    }
                    print(f"   ✗ {description}: {error_msg[:80]}")
        
        
    except Exception as e:
        error_msg = str(e)
        results['errors'].append(f"General error: {error_msg}")
        print(f"\n✗ Error testing exchange: {error_msg[:100]}")
    
    return results


def print_summary(all_results):
    """
    Print summary of all test results.
    """
    print("\n" + "="*80)
    print("SUMMARY - API KEY REQUIREMENTS")
    print("="*80)
    
    for result in all_results:
        if result.get('errors') and len(result['errors']) > 0:
            continue
        
        print(f"\n{result['exchange_name'].upper()} ({result['exchange_id']})")
        print("-" * 80)
        
        print(f"Market Types: {', '.join(result['market_types_available']) if result['market_types_available'] else 'None'}")
        print(f"Timeframes: {len(result['timeframes_available'])} available")
        
        # Public data summary
        public_works = [k for k, v in result['public_data'].items() 
                       if v.get('available') and not v.get('requires_api_key')]
        public_needs_key = [k for k, v in result['public_data'].items() 
                           if v.get('available') and v.get('requires_api_key')]
        
        print(f"\nPublic Data (NO API key needed): {len(public_works)}/{len(PUBLIC_DATA_TYPES)}")
        if public_works:
            print(f"  ✓ {', '.join(public_works)}")
        
        if public_needs_key:
            print(f"\nPublic Data (REQUIRES API key): {len(public_needs_key)}/{len(PUBLIC_DATA_TYPES)}")
            print(f"  ⚠ {', '.join(public_needs_key)}")


def print_detailed_report(all_results):
    """
    Print detailed report for each exchange.
    """
    print("\n" + "="*80)
    print("DETAILED REPORT")
    print("="*80)
    
    for result in all_results:
        if result.get('errors') and len(result['errors']) > 0:
            print(f"\n{result['exchange_name'].upper()}: Errors occurred - check output above")
            continue
        
        print(f"\n{'='*80}")
        print(f"{result['exchange_name'].upper()} ({result['exchange_id']})")
        print('='*80)
        
        print(f"\nMarket Types Available: {', '.join(result['market_types_available'])}")
        print(f"Timeframes: {', '.join(result['timeframes_available'][:15])}")
        if len(result['timeframes_available']) > 15:
            print(f"  ... and {len(result['timeframes_available']) - 15} more")
        
        print(f"\nPUBLIC DATA ENDPOINTS:")
        for method_name, description in PUBLIC_DATA_TYPES:
            if method_name in result['public_data']:
                data = result['public_data'][method_name]
                if data.get('available'):
                    if data.get('requires_api_key'):
                        print(f"  ⚠ {description}: REQUIRES API KEY")
                    else:
                        print(f"  ✓ {description}: Works without API key")
                else:
                    print(f"  ✗ {description}: {data.get('reason', 'Not available')}")


def main():
    """
    Main function to test all selected exchanges.
    """
    print("="*80)
    print("API KEY REQUIREMENTS TESTER")
    print("="*80)
    print(f"\nTesting {len(SELECTED_EXCHANGES)} exchanges:")
    for ex in SELECTED_EXCHANGES:
        print(f"  - {ex}")
    
    print("\nThis script will test:")
    print("  1. What public data can be fetched WITHOUT API keys")
    print("  2. Available market types (spot, futures, options)")
    print("  3. Available timeframes for OHLCV data")
    print("  4. Sample symbols for each market type")
    print("\nNote: Only public data endpoints are tested (no API keys needed).")
    print("="*80)
    
    all_results = []
    
    for exchange_id in SELECTED_EXCHANGES:
        try:
            result = test_exchange_data_access(exchange_id)
            all_results.append(result)
        except Exception as e:
            print(f"\n✗ Failed to test {exchange_id}: {e}")
            all_results.append({
                'exchange_id': exchange_id,
                'errors': [str(e)]
            })
    
    # Print summaries
    print_summary(all_results)
    print_detailed_report(all_results)
    
    print("\n" + "="*80)
    print("TESTING COMPLETE")
    print("="*80)
    print("\nKey Findings:")
    print("  • Most public data (OHLCV, tickers, order books) works WITHOUT API keys")
    print("  • Some exchanges may require API keys even for public data (noted above)")
    print("\nNext Steps:")
    print("  1. Review which public data endpoints work for each exchange")
    print("  2. Use the working endpoints to extract data")
    print("  3. API keys are NOT needed for public data extraction")
    print("="*80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user.")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

