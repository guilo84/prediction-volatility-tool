import asyncio
import websockets
import json
import os
import asyncpg
import time
import base64
from datetime import datetime, timezone
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization

# Load environment variables from .env
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

KALSHI_KEY_ID = os.getenv("KALSHI_KEY_ID")
KALSHI_KEY_PATH = os.getenv("KALSHI_KEY_PATH")

KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

def generate_kalshi_auth_headers():
    """Generates the RSA signature headers required by Kalshi."""
    # Read the private key file
    with open(KALSHI_KEY_PATH, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )
    
    # Kalshi requires timestamp in milliseconds
    timestamp = str(int(time.time() * 1000))
    
    # The message to sign is the timestamp + method + path
    msg_string = timestamp + "GET" + "/trade-api/ws/v2"
    
    # Generate RSA signature
    signature = private_key.sign(
        msg_string.encode('utf-8'),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    
    encoded_signature = base64.b64encode(signature).decode('utf-8')
    
    return {
        "KALSHI-ACCESS-KEY": KALSHI_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": encoded_signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp
    }

async def insert_trade(pool, trade_data):
    """Inserts a single trade into TimescaleDB."""
    query = """
        INSERT INTO kalshi_trades (time, market_ticker, trade_id, price_cents, count, taker_side)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (trade_id, time) DO NOTHING;
    """
    ts = datetime.fromtimestamp(trade_data['ts'], tz=timezone.utc)
    async with pool.acquire() as connection:
        await connection.execute(
            query, ts, trade_data['market_ticker'], trade_data['trade_id'],
            trade_data['price'], trade_data['count'], trade_data['taker_side']
        )

async def connect_kalshi_ws(pool):
    """Connects to Kalshi WS with auth and listens for public trades."""
    auth_headers = generate_kalshi_auth_headers()
    
    # Pass the headers into the websocket connection
    async with websockets.connect(KALSHI_WS_URL, additional_headers=auth_headers) as ws:
        print("Connected and Authenticated to Kalshi WebSocket.")
        
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
    db_dsn = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    pool = await asyncpg.create_pool(db_dsn)
    print("Database connection pool established.")
    
    while True:
        await connect_kalshi_ws(pool)
        await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down ingestion engine.")
