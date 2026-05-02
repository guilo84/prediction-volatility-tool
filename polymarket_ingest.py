import asyncio
import os
import asyncpg
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from datetime import datetime, timezone

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# We use the Polygon Mainnet host for real data
HOST = "https://clob.polymarket.com"

async def insert_poly_trade(pool, trade):
    """Inserts a single Polymarket trade into TimescaleDB."""
    query = """
        INSERT INTO polymarket_trades (time, condition_id, asset_id, price, size, side)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT DO NOTHING;
    """
    
    # Polymarket timestamp is usually a string integer in milliseconds
    try:
        ts = datetime.fromtimestamp(int(trade.get('timestamp')) / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError):
        ts = datetime.now(timezone.utc)
        
    async with pool.acquire() as connection:
        await connection.execute(
            query,
            ts,
            trade.get('condition_id', 'unknown'),
            trade.get('asset_id', 'unknown'),
            float(trade.get('price', 0)),
            float(trade.get('size', 0)),
            trade.get('side', 'unknown')
        )

def process_message(message, pool, loop):
    """Callback function triggered by the CLOB websocket."""
    # Polymarket groups trades in a list under the 'trades' key
    if isinstance(message, list):
        for item in message:
            if item.get('event_type') == 'trade':
                # Schedule the async database insert from the sync callback
                asyncio.run_coroutine_threadsafe(insert_poly_trade(pool, item), loop)
                print(f"Inserted Poly Trade: Asset {item.get('asset_id')[:8]}... @ ${item.get('price')} (Vol: {item.get('size')})")

async def main():
    db_dsn = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    pool = await asyncpg.create_pool(db_dsn)
    print("Database connection pool established for Polymarket.")
    
    loop = asyncio.get_running_loop()

    # Initialize the client without credentials (read-only mode)
    client = ClobClient(HOST, chain_id=137) # 137 is Polygon Mainnet
    
    def on_message(msg):
        process_message(msg, pool, loop)

    client.set_data_callback(on_message)
    
    print("Connecting to Polymarket CLOB...")
    # Subscribe to all trades across all markets
    client.subscribe(["*"]) 
    
    # Keep the main loop running
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down Polymarket ingestion engine.")
