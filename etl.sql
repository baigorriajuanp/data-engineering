-- Tabla creada en Redshift mediante consola SQL de Dbeaver

CREATE TABLE crypto_data (
    id VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    name VARCHAR(100),
    current_price DECIMAL(18, 8),
    market_cap BIGINT,
    total_volume BIGINT,
    timestamp TIMESTAMP NOT NULL
);