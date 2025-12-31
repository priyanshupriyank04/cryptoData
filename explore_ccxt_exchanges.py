"""
CCXT Exchange Explorer
This script explores all available exchanges in ccxt and their capabilities:
- Available exchanges
- Market types (spot, futures, options)
- Symbols/coins available
- Timeframes for OHLCV data
- Expired contracts/options
- Other available data types

IMPORTANT: API Keys are NOT required for this script!
- Most exchanges allow fetching public market data (markets list, symbols, timeframes) without API keys
- The script will work without any API key setup
- If an exchange requires API keys for market data, it will be noted in the results
- API keys are only needed for private data (your trades, orders, balances)
- For pulling OHLCV data and public market data, API keys are typically NOT needed
"""

import ccxt
import json
from datetime import datetime
from collections import defaultdict

# Required features that exchanges must support
REQUIRED_FEATURES = [
    'fetchOHLCV',
    'fetchTicker',
    'fetchTrades',
    'fetchOrderBook',
    'fetchBalance',
    'createOrder',
    'cancelOrder',
    'fetchMarkets',
    'fetchCurrencies',
    'fetchFundingRate',
    'fetchFundingHistory',
    'fetchOpenInterest',
    'fetchLeverageTiers',
    'fetchPositions',
    'fetchMyTrades',
    'fetchOrders'
]

# Minimum markets requirement for spot + futures
MIN_SPOT_FUTURES_MARKETS = 750


def get_exchange_info(exchange_id):
    """
    Get detailed information about a specific exchange.
    
    Args:
        exchange_id: The exchange ID string (e.g., 'binance', 'bybit')
        
    Returns:
        dict: Exchange information and capabilities
    """
    try:
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'  # Start with spot
            }
        })
        
        info = {
            'id': exchange_id,
            'name': exchange.name if hasattr(exchange, 'name') else exchange_id,
            'countries': exchange.countries if hasattr(exchange, 'countries') else [],
            'urls': exchange.urls if hasattr(exchange, 'urls') else {},
            'has': exchange.has if hasattr(exchange, 'has') else {},
            'markets': {},
            'timeframes': {},
            'rateLimit': exchange.rateLimit if hasattr(exchange, 'rateLimit') else None,
            'api': exchange.api if hasattr(exchange, 'api') else {},
        }
        
        # Try to load markets (this may require API keys for some exchanges)
        try:
            markets = exchange.load_markets()
            info['markets_loaded'] = True
            info['market_count'] = len(markets)
            
            # Categorize markets by type
            market_types = defaultdict(list)
            spot_symbols = []
            futures_symbols = []
            options_symbols = []
            expired_futures = []
            expired_options = []
            
            for symbol, market in markets.items():
                market_type = market.get('type', 'unknown')
                market_types[market_type].append(symbol)
                
                # Check if expired
                is_expired = market.get('expired', False)
                active = market.get('active', True)
                
                if market_type == 'spot':
                    if active and not is_expired:
                        spot_symbols.append(symbol)
                elif market_type in ['future', 'futures', 'swap', 'perpetual']:
                    if active and not is_expired:
                        futures_symbols.append(symbol)
                    elif is_expired or not active:
                        expired_futures.append(symbol)
                elif market_type == 'option':
                    if active and not is_expired:
                        options_symbols.append(symbol)
                    elif is_expired or not active:
                        expired_options.append(symbol)
            
            info['markets'] = {
                'spot': {
                    'count': len(spot_symbols),
                    'symbols': spot_symbols[:50] if len(spot_symbols) > 50 else spot_symbols,  # Limit to 50 for display
                    'total': len(spot_symbols)
                },
                'futures': {
                    'count': len(futures_symbols),
                    'symbols': futures_symbols[:50] if len(futures_symbols) > 50 else futures_symbols,
                    'total': len(futures_symbols)
                },
                'options': {
                    'count': len(options_symbols),
                    'symbols': options_symbols[:50] if len(options_symbols) > 50 else options_symbols,
                    'total': len(options_symbols)
                },
                'expired_futures': {
                    'count': len(expired_futures),
                    'symbols': expired_futures[:50] if len(expired_futures) > 50 else expired_futures,
                    'total': len(expired_futures)
                },
                'expired_options': {
                    'count': len(expired_options),
                    'symbols': expired_options[:50] if len(expired_options) > 50 else expired_options,
                    'total': len(expired_options)
                },
                'all_types': dict(market_types)
            }
            
            # Get timeframes
            if hasattr(exchange, 'timeframes') and exchange.timeframes:
                info['timeframes'] = exchange.timeframes
            else:
                info['timeframes'] = {}
            
            # Get unique base currencies (coins)
            base_currencies = set()
            for symbol, market in markets.items():
                if market.get('active', True) and not market.get('expired', False):
                    base = market.get('base', '')
                    if base:
                        base_currencies.add(base)
            
            info['base_currencies'] = {
                'count': len(base_currencies),
                'currencies': sorted(list(base_currencies))
            }
            
        except Exception as e:
            info['markets_loaded'] = False
            info['market_error'] = str(e)
            info['markets'] = {}
            info['timeframes'] = exchange.timeframes if hasattr(exchange, 'timeframes') else {}
        
        return info
        
    except Exception as e:
        return {
            'id': exchange_id,
            'error': str(e),
            'markets_loaded': False
        }


def explore_all_exchanges():
    """
    Explore all available exchanges in ccxt.
    """
    print("=" * 80)
    print("CCXT Exchange Explorer")
    print("=" * 80)
    print(f"\nExploring exchanges at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Get all exchange IDs
    exchange_ids = ccxt.exchanges
    print(f"Total exchanges available: {len(exchange_ids)}\n")
    
    results = {
        'total_exchanges': len(exchange_ids),
        'explored': {},
        'summary': {
            'with_spot': [],
            'with_futures': [],
            'with_options': [],
            'with_expired_futures': [],
            'with_expired_options': [],
            'exchanges_by_feature': defaultdict(list)
        }
    }
    
    # Explore each exchange
    print("Exploring exchanges (this may take a while)...\n")
    for i, exchange_id in enumerate(exchange_ids, 1):
        print(f"[{i}/{len(exchange_ids)}] Exploring {exchange_id}...", end=' ')
        
        info = get_exchange_info(exchange_id)
        results['explored'][exchange_id] = info
        
        if info.get('markets_loaded'):
            markets = info.get('markets', {})
            
            if markets.get('spot', {}).get('count', 0) > 0:
                results['summary']['with_spot'].append(exchange_id)
            
            if markets.get('futures', {}).get('count', 0) > 0:
                results['summary']['with_futures'].append(exchange_id)
            
            if markets.get('options', {}).get('count', 0) > 0:
                results['summary']['with_options'].append(exchange_id)
            
            if markets.get('expired_futures', {}).get('count', 0) > 0:
                results['summary']['with_expired_futures'].append(exchange_id)
            
            if markets.get('expired_options', {}).get('count', 0) > 0:
                results['summary']['with_expired_options'].append(exchange_id)
            
            # Check capabilities
            has = info.get('has', {})
            for feature, supported in has.items():
                if supported:
                    results['summary']['exchanges_by_feature'][feature].append(exchange_id)
            
            print(f"✓ ({markets.get('spot', {}).get('count', 0)} spot, "
                  f"{markets.get('futures', {}).get('count', 0)} futures, "
                  f"{markets.get('options', {}).get('count', 0)} options)")
        else:
            error = info.get('error', info.get('market_error', 'Unknown error'))
            print(f"✗ ({error[:50]})")
    
    return results


def print_detailed_exchange_info(exchange_id, info):
    """
    Print detailed information about a specific exchange.
    """
    print("\n" + "=" * 80)
    print(f"Exchange: {info.get('name', exchange_id).upper()}")
    print("=" * 80)
    
    if info.get('error'):
        print(f"Error: {info['error']}")
        return
    
    print(f"\nBasic Info:")
    print(f"  - ID: {info.get('id')}")
    print(f"  - Name: {info.get('name')}")
    print(f"  - Countries: {', '.join(info.get('countries', []))}")
    print(f"  - Rate Limit: {info.get('rateLimit')} ms")
    
    if info.get('urls'):
        print(f"\nURLs:")
        for url_type, url in info.get('urls', {}).items():
            if isinstance(url, dict):
                for k, v in url.items():
                    print(f"  - {url_type}.{k}: {v}")
            else:
                print(f"  - {url_type}: {url}")
    
    # Capabilities
    has = info.get('has', {})
    if has:
        print(f"\nCapabilities:")
        important_features = [
            'fetchOHLCV', 'fetchTicker', 'fetchTrades', 'fetchOrderBook',
            'fetchBalance', 'createOrder', 'cancelOrder',
            'fetchMarkets', 'fetchCurrencies',
            'fetchFundingRate', 'fetchFundingHistory',
            'fetchOpenInterest', 'fetchLeverageTiers',
            'fetchPositions', 'fetchMyTrades', 'fetchOrders'
        ]
        for feature in important_features:
            if feature in has:
                status = "✓" if has[feature] else "✗"
                print(f"  {status} {feature}")
    
    # Markets
    if info.get('markets_loaded'):
        markets = info.get('markets', {})
        
        print(f"\nMarkets Summary:")
        print(f"  - Total Markets: {info.get('market_count', 0)}")
        
        if markets.get('spot'):
            spot_info = markets['spot']
            print(f"\n  Spot Markets: {spot_info['count']} active")
            if spot_info['symbols']:
                print(f"    Sample symbols: {', '.join(spot_info['symbols'][:10])}")
                if spot_info['total'] > 10:
                    print(f"    ... and {spot_info['total'] - 10} more")
        
        if markets.get('futures'):
            futures_info = markets['futures']
            print(f"\n  Futures/Perpetuals: {futures_info['count']} active")
            if futures_info['symbols']:
                print(f"    Sample symbols: {', '.join(futures_info['symbols'][:10])}")
                if futures_info['total'] > 10:
                    print(f"    ... and {futures_info['total'] - 10} more")
        
        if markets.get('options'):
            options_info = markets['options']
            print(f"\n  Options: {options_info['count']} active")
            if options_info['symbols']:
                print(f"    Sample symbols: {', '.join(options_info['symbols'][:10])}")
                if options_info['total'] > 10:
                    print(f"    ... and {options_info['total'] - 10} more")
        
        if markets.get('expired_futures', {}).get('count', 0) > 0:
            exp_futures = markets['expired_futures']
            print(f"\n  Expired Futures: {exp_futures['count']}")
            if exp_futures['symbols']:
                print(f"    Sample: {', '.join(exp_futures['symbols'][:10])}")
        
        if markets.get('expired_options', {}).get('count', 0) > 0:
            exp_options = markets['expired_options']
            print(f"\n  Expired Options: {exp_options['count']}")
            if exp_options['symbols']:
                print(f"    Sample: {', '.join(exp_options['symbols'][:10])}")
        
        # Base currencies
        if info.get('base_currencies'):
            currencies = info['base_currencies']
            print(f"\n  Base Currencies (Coins): {currencies['count']}")
            if currencies['currencies']:
                print(f"    Sample: {', '.join(currencies['currencies'][:20])}")
                if currencies['count'] > 20:
                    print(f"    ... and {currencies['count'] - 20} more")
        
        # Timeframes
        timeframes = info.get('timeframes', {})
        if timeframes:
            print(f"\n  Available Timeframes: {len(timeframes)}")
            print(f"    {', '.join(sorted(timeframes.keys()))}")
    else:
        print(f"\nMarkets: Could not load (may require API keys)")
        if info.get('market_error'):
            print(f"  Error: {info['market_error']}")
    
    print()


def filter_qualified_exchanges(results):
    """
    Filter exchanges based on criteria:
    1. Spot + Futures (>=1000 markets) with ALL required features
    2. OR Options support
    
    Returns:
        dict: {
            'spot_futures_qualified': list of exchange info dicts,
            'options_qualified': list of exchange info dicts
        }
    """
    spot_futures_qualified = []
    options_qualified = []
    
    for exchange_id, info in results['explored'].items():
        if not info.get('markets_loaded'):
            continue
        
        has = info.get('has', {})
        markets = info.get('markets', {})
        
        spot_count = markets.get('spot', {}).get('count', 0)
        futures_count = markets.get('futures', {}).get('count', 0)
        options_count = markets.get('options', {}).get('count', 0)
        total_spot_futures = spot_count + futures_count
        
        # Check if all required features are supported
        all_features_supported = all(
            has.get(feature, False) for feature in REQUIRED_FEATURES
        )
        
        # Criteria 1: Spot + Futures >= 1000 AND all features supported
        if total_spot_futures >= MIN_SPOT_FUTURES_MARKETS and all_features_supported:
            exchange_summary = {
                'id': exchange_id,
                'name': info.get('name', exchange_id),
                'spot_markets': spot_count,
                'futures_markets': futures_count,
                'total_spot_futures': total_spot_futures,
                'options_markets': options_count,
                'missing_features': [f for f in REQUIRED_FEATURES if not has.get(f, False)],
                'timeframes': list(info.get('timeframes', {}).keys()),
                'rate_limit': info.get('rateLimit'),
                'urls': info.get('urls', {})
            }
            spot_futures_qualified.append(exchange_summary)
        
        # Criteria 2: Options support (any amount)
        if options_count > 0:
            exchange_summary = {
                'id': exchange_id,
                'name': info.get('name', exchange_id),
                'spot_markets': spot_count,
                'futures_markets': futures_count,
                'options_markets': options_count,
                'total_markets': info.get('market_count', 0),
                'timeframes': list(info.get('timeframes', {}).keys()),
                'rate_limit': info.get('rateLimit'),
                'urls': info.get('urls', {}),
                'has_all_features': all_features_supported,
                'missing_features': [f for f in REQUIRED_FEATURES if not has.get(f, False)]
            }
            options_qualified.append(exchange_summary)
    
    # Sort by total markets (descending)
    spot_futures_qualified.sort(key=lambda x: x['total_spot_futures'], reverse=True)
    options_qualified.sort(key=lambda x: x['options_markets'], reverse=True)
    
    return {
        'spot_futures_qualified': spot_futures_qualified,
        'options_qualified': options_qualified
    }


def print_summary(results):
    """
    Print summary of all exchanges.
    """
    print("\n" + "=" * 80)
    print("EXPLORATION SUMMARY")
    print("=" * 80)
    
    summary = results['summary']
    
    print(f"\nTotal Exchanges Explored: {results['total_exchanges']}")
    
    print(f"\nExchanges with Spot Markets: {len(summary['with_spot'])}")
    if summary['with_spot']:
        print(f"  {', '.join(summary['with_spot'][:20])}")
        if len(summary['with_spot']) > 20:
            print(f"  ... and {len(summary['with_spot']) - 20} more")
    
    print(f"\nExchanges with Futures/Perpetuals: {len(summary['with_futures'])}")
    if summary['with_futures']:
        print(f"  {', '.join(summary['with_futures'][:20])}")
        if len(summary['with_futures']) > 20:
            print(f"  ... and {len(summary['with_futures']) - 20} more")
    
    print(f"\nExchanges with Options: {len(summary['with_options'])}")
    if summary['with_options']:
        print(f"  {', '.join(summary['with_options'])}")
    
    print("\n" + "=" * 80)


def print_qualified_exchanges(qualified):
    """
    Print the filtered qualified exchanges in two separate lists.
    """
    print("\n" + "=" * 80)
    print("QUALIFIED EXCHANGES - FINAL COMPILATION")
    print("=" * 80)
    
    # List 1: Spot + Futures (>=1000) with ALL required features
    print("\n" + "=" * 80)
    print(f"LIST 1: EXCHANGES WITH SPOT + FUTURES (≥{MIN_SPOT_FUTURES_MARKETS} markets)")
    print(f"        AND ALL REQUIRED FEATURES SUPPORTED")
    print("=" * 80)
    
    spot_futures = qualified['spot_futures_qualified']
    
    if spot_futures:
        print(f"\n✓ Found {len(spot_futures)} qualified exchange(s):\n")
        
        for i, ex in enumerate(spot_futures, 1):
            print(f"{i}. {ex['name'].upper()} ({ex['id']})")
            print(f"   - Spot Markets: {ex['spot_markets']}")
            print(f"   - Futures/Perpetuals: {ex['futures_markets']}")
            print(f"   - Total Spot+Futures: {ex['total_spot_futures']}")
            print(f"   - Options: {ex['options_markets']}")
            print(f"   - Rate Limit: {ex['rate_limit']} ms")
            print(f"   - Timeframes: {len(ex['timeframes'])} available")
            if ex['timeframes']:
                print(f"     Sample: {', '.join(ex['timeframes'][:10])}")
            if ex.get('urls', {}).get('www'):
                print(f"   - Website: {ex['urls']['www']}")
            print()
    else:
        print(f"\n✗ No exchanges found matching criteria:")
        print(f"  - Spot + Futures markets >= {MIN_SPOT_FUTURES_MARKETS}")
        print(f"  - All {len(REQUIRED_FEATURES)} required features supported")
    
    # List 2: Options support
    print("\n" + "=" * 80)
    print("LIST 2: EXCHANGES WITH OPTIONS SUPPORT")
    print("=" * 80)
    
    options = qualified['options_qualified']
    
    if options:
        print(f"\n✓ Found {len(options)} exchange(s) with options:\n")
        
        # Print summary table for quick comparison
        print("QUICK COMPARISON TABLE:")
        print("-" * 80)
        print(f"{'Exchange':<20} {'Options':<12} {'Spot':<12} {'Futures':<12} {'Total':<12} {'All Features':<15}")
        print("-" * 80)
        for ex in options:
            name = ex['name'].upper()[:18]
            opts = str(ex['options_markets'])
            spot = str(ex['spot_markets'])
            fut = str(ex['futures_markets'])
            total = str(ex['total_markets'])
            features = '✓ Yes' if ex['has_all_features'] else '✗ No'
            print(f"{name:<20} {opts:<12} {spot:<12} {fut:<12} {total:<12} {features:<15}")
        print("-" * 80)
        print()
        
        # Detailed information for each exchange
        print("DETAILED INFORMATION:\n")
        for i, ex in enumerate(options, 1):
            print(f"{i}. {ex['name'].upper()} ({ex['id']})")
            print(f"   ┌─ Market Summary:")
            print(f"   │  • Options Markets: {ex['options_markets']:,}")
            print(f"   │  • Spot Markets: {ex['spot_markets']:,}")
            print(f"   │  • Futures/Perpetuals Markets: {ex['futures_markets']:,}")
            print(f"   │  • Total Markets: {ex['total_markets']:,}")
            print(f"   │  • Spot + Futures Total: {ex['spot_markets'] + ex['futures_markets']:,}")
            print(f"   ├─ Features:")
            print(f"   │  • All Required Features: {'✓ Yes' if ex['has_all_features'] else '✗ No'}")
            if not ex['has_all_features'] and ex['missing_features']:
                print(f"   │  • Missing Features ({len(ex['missing_features'])}): {', '.join(ex['missing_features'][:5])}")
                if len(ex['missing_features']) > 5:
                    print(f"   │    ... and {len(ex['missing_features']) - 5} more")
            print(f"   ├─ Technical:")
            print(f"   │  • Rate Limit: {ex['rate_limit']} ms")
            print(f"   │  • Timeframes: {len(ex['timeframes'])} available")
            if ex['timeframes']:
                print(f"   │    Sample: {', '.join(ex['timeframes'][:10])}")
            if ex.get('urls', {}).get('www'):
                print(f"   └─ Website: {ex['urls']['www']}")
            else:
                print(f"   └─")
            print()
    else:
        print("\n✗ No exchanges found with options support")
    
    # Required features list
    print("\n" + "=" * 80)
    print("REQUIRED FEATURES (for List 1):")
    print("=" * 80)
    print("\nAll of these features must be supported:")
    for i, feature in enumerate(REQUIRED_FEATURES, 1):
        print(f"  {i:2d}. {feature}")
    
    print("\n" + "=" * 80)


def save_results_to_file(results, filename='ccxt_exploration_results.json'):
    """
    Save exploration results to a JSON file.
    """
    # Convert sets to lists for JSON serialization
    def convert_for_json(obj):
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, defaultdict):
            return dict(obj)
        elif isinstance(obj, dict):
            return {k: convert_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_for_json(item) for item in obj]
        else:
            return obj
    
    json_results = convert_for_json(results)
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(json_results, f, indent=2, ensure_ascii=False)
    
    print(f"\nResults saved to: {filename}")


def main():
    """
    Main function to explore all exchanges.
    """
    print("Starting CCXT Exchange Exploration...\n")
    
    # Explore all exchanges
    results = explore_all_exchanges()
    
    # Print general summary
    print_summary(results)
    
    # Filter qualified exchanges
    qualified = filter_qualified_exchanges(results)
    
    # Print qualified exchanges (the main output)
    print_qualified_exchanges(qualified)
    
    # Add qualified exchanges to results for saving
    results['qualified'] = qualified
    
    # Save results
    save_results_to_file(results)
    
    print("\n" + "=" * 80)
    print("Exploration Complete!")
    print("=" * 80)
    print("\nSummary:")
    print(f"  - List 1 (Spot+Futures ≥{MIN_SPOT_FUTURES_MARKETS} + All Features): {len(qualified['spot_futures_qualified'])} exchange(s)")
    print(f"  - List 2 (Options Support): {len(qualified['options_qualified'])} exchange(s)")
    print("\nNext steps:")
    print("1. Review the qualified exchanges lists above")
    print("2. Check ccxt_exploration_results.json for full data")
    print("3. Choose exchanges from the qualified lists")
    print("4. Set up API keys if needed for private data")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nExploration interrupted by user.")
    except Exception as e:
        print(f"\nError during exploration: {e}")
        import traceback
        traceback.print_exc()

