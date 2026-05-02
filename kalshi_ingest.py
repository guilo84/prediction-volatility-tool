import asyncio
import websockets
import json
import os
import asyncpg
from dotenv import load_dotenv
from datetime import datetime, timezone

# Load environment variables from .env
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

async def insert_trade(pool, trade_data):
    """Inserts a single trade into TimescaleDB."""
    query = """
        INSERT INTO kalshi_trades (time, market_ticker, trade_id, price_cents, count, taker_side)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (trade_id, time) DO NOTHING;
    """
    
    # Convert Unix timestamp to Postgres TIMESTAMPTZ
    ts = datetime.fromtimestamp(trade_data['ts'], tz=timezone.utc)
    
    async with pool.acquire() as connection:
        await connection.execute(
            query,
            ts,
            trade_data['market_ticker'],
            trade_data['trade_id'],
            trade_data['price'],
            trade_data['count'],
            trade_data['taker_side']
        )

async def connect_kalshi_ws(pool):
    """Connects to Kalshi WS and listens for public trades."""
    async with websockets.connect(KALSHI_WS_URL) as ws:
        print("Connected to Kalshi WebSocket.")
        
        # Subscribe to all public trades
        subscribe_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["trade"]
            }
        }
        await ws.send(json.dumps(subscribe_msg))
        
        while True:
            try:
                response = await ws.recv()
                data = json.loads(response)
                
                # Check if it's a trade message
                if data.get('type') == 'trade':
                    trade_info = data.get('msg')
                    await insert_trade(pool, trade_info)
                    print(f"Inserted trade: {trade_info['market_ticker']} @ {trade_info['price']}c")
                    
            except websockets.ConnectionClosed:
                print("WebSocket connection closed. Reconnecting...")
                break
            except Exception as e:
                print(f"Error processing message: {e}")

async def main():
    # Create the async database connection pool
    db_dsn = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    pool = await asyncpg.create_pool(db_dsn)
    
    print("Database connection pool established.")
    
    # Keep the websocket connection alive
    while True:
        await connect_kalshi_ws(pool)
        await asyncio.sleep(5) # Brief pause before reconnecting

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down ingestion engine.")
