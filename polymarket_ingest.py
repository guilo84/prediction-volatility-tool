import asyncio
import websockets
import json
import os
import asyncpg
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Polymarket's raw CLOB WebSocket Endpoint
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

def get_active_tokens():
    """Fetches the most active current tokens from Polymarket's Gamma API."""
    print("Fetching active Polymarket markets...")
    try:
        # Get the top 50 most active open markets
        res = requests.get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=50")
        markets = res.json()
        tokens = []
        
        for m in markets:
            # Polymarket now uses 'clobTokenIds' 
            clob_ids = m.get('clobTokenIds')
            
            if clob_ids:
                # Catch the double-encoded JSON string and parse it into a list
                if isinstance(clob_ids, str):
                    try:
                        clob_ids = json.loads(clob_ids)
                    except json.JSONDecodeError:
                        continue
                        
                if isinstance(clob_ids, list):
                    tokens.extend(clob_ids)
                    
        print(f"Found {len(tokens)} active tokens to monitor.")
        return tokens
    except Exception as e:
        print(f"Error fetching markets: {e}")
        return []

async def insert_poly_trade(pool, trade_data):
    """Inserts a single Polymarket trade into TimescaleDB."""
    query = """
        INSERT INTO polymarket_trades (time, condition_id, asset_id, price, size, side)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT DO NOTHING;
    """
    
    # Safely parse the timestamp
    try:
        ts = datetime.fromtimestamp(int(trade_data.get('timestamp', datetime.now().timestamp() * 1000)) / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError):
        ts = datetime.now(timezone.utc)
        
    async with pool.acquire() as connection:
        await connection.execute(
            query,
            ts,
            trade_data.get('condition_id', 'unknown'),
            trade_data.get('asset_id', trade_data.get('token', 'unknown')),
            float(trade_data.get('price', 0)),
            float(trade_data.get('size', 0)),
            trade_data.get('side', 'unknown')
        )

async def connect_poly_ws(pool):
    """Connects to Polymarket WS and streams trades for active tokens."""
    tokens = get_active_tokens()
    
    if not tokens:
        print("No tokens found. Retrying in 10 seconds...")
        await asyncio.sleep(10)
        return

    async with websockets.connect(POLY_WS_URL, ping_interval=20, ping_timeout=20) as ws:
        print("Connected to Polymarket WebSocket.")
        
        # Subscribe to the active tokens
        subscribe_msg = {
            "type": "market",
            "assets_ids": tokens
        }
        await ws.send(json.dumps(subscribe_msg))
        
        while True:
            try:
                response = await ws.recv()
                
                # Polymarket sometimes sends empty lists [] as a heartbeat/ACK
                if response == "[]":
                    continue
                    
                data = json.loads(response)
                
                # We are looking for price changes and trades
                if isinstance(data, list):
                    for item in data:
                        # Polymarket streams trade events as 'price_change' or 'last_trade_price'
                        if item.get('event_type') in ['last_trade_price', 'trade']:
                            await insert_poly_trade(pool, item)
                            asset = item.get('asset_id', item.get('token', 'Unknown'))[:8]
                            price = item.get('price', '0')
                            print(f"Inserted Poly Trade: Asset {asset}... @ ${price}")
                            
            except websockets.ConnectionClosed:
                print("WebSocket connection closed. Reconnecting...")
                break
            except Exception as e:
                print(f"Error processing message: {e}")

async def main():
    db_dsn = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    pool = await asyncpg.create_pool(db_dsn)
    print("Database connection pool established for Polymarket.")
    
    while True:
        await connect_poly_ws(pool)
        await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down Polymarket ingestion engine.")
