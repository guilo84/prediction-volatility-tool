-- Create the standard table for raw Kalshi trades
CREATE TABLE IF NOT EXISTS kalshi_trades (
    time TIMESTAMPTZ NOT NULL,
    market_ticker VARCHAR(255) NOT NULL,
    trade_id VARCHAR(255) NOT NULL,
    price_cents INTEGER NOT NULL,
    count INTEGER NOT NULL,
    taker_side VARCHAR(10) NOT NULL,
    UNIQUE (trade_id, time)
);

-- Convert the standard Postgres table into a TimescaleDB Hypertable
-- This partitions the data by the 'time' column automatically
SELECT create_hypertable('kalshi_trades', 'time', if_not_exists => TRUE);

-- Create an index on the market_ticker to speed up future querying
CREATE INDEX ix_kalshi_ticker_time ON kalshi_trades (market_ticker, time DESC);
