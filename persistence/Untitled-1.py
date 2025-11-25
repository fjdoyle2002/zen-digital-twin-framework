# -*- coding: utf-8 -*-
"""
postgres_persistence_etv.py
Entity-Timestamp-Value (ETV) persistence for ZEN Digital Twin
Optimized for Seeq, TimescaleDB, and long-term scalability
Author: Francis Doyle (updated 2025)
"""

import psycopg2
from psycopg2 import sql
import pandas as pd
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class PostgresPersistenceETV:
    def __init__(self, config, sensors_df: pd.DataFrame):
        self.sensors_df = sensors_df.copy()
        self.config = config

        # Database connection params
        self.dbname = config.get('DATABASE', 'DatabaseName')
        self.dbhost = config.get('DATABASE', 'DatabaseHost')
        self.dbport = config.get('DATABASE', 'DatabasePort')
        self.dbuser = config.get('DATABASE', 'DatabaseUser')
        self.dbpass = config.get('DATABASE', 'DatabasePass')
        self.dt_name = config.get('DEFAULT', 'DigitalTwinIdentifier')

        self.conn = None
        self._connect()
        self._ensure_schema()

    def _connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.dbhost,
                port=self.dbport,
                database=self.dbname,
                user=self.dbuser,
                password=self.dbpass
            )
            self.conn.autocommit = False
            logger.info("Connected to PostgreSQL for ETV persistence")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _ensure_schema(self):
        """Create signals metadata table and measurements table if not exist"""
        cur = self.conn.cursor()

        # 1. signals metadata table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id SERIAL PRIMARY KEY,
                signal_key TEXT UNIQUE NOT NULL,
                name TEXT,
                description TEXT,
                units TEXT,
                data_type TEXT,
                persistence_name TEXT,
                sensor_name TEXT,
                sensor_instance TEXT,
                source_type TEXT DEFAULT 'digital_twin',
                digital_twin_id TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # 2. measurements table (hypertable ready for TimescaleDB)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS measurements (
                time TIMESTAMPTZ NOT NULL,
                signal_id INTEGER NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
                value DOUBLE PRECISION,
                PRIMARY KEY (time, signal_id)
            );
        """)

        # Optional: Convert to TimescaleDB hypertable (uncomment if enabled)
        # cur.execute("SELECT create_hypertable('measurements', 'time', if_not_exists => TRUE);")

        # Index for fast queries by signal_key
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_measurements_signal_id ON measurements(signal_id);
            CREATE INDEX IF NOT EXISTS idx_measurements_time ON measurements(time DESC);
        """)

        self.conn.commit()
        cur.close()
        logger.info("ETV schema ensured (signals + measurements)")

    def _upsert_signals(self):
        """
        Insert or update signal metadata from sensors_df
        Uses signal_key = PersistenceName as natural key
        """
        cur = self.conn.cursor()

        upsert_sql = sql.SQL("""
            INSERT INTO signals (
                signal_key, name, description, units, data_type,
                persistence_name, sensor_name, sensor_instance,
                digital_twin_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (signal_key) 
            DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                units = EXCLUDED.units,
                data_type = EXCLUDED.data_type,
                sensor_name = EXCLUDED.sensor_name,
                sensor_instance = EXCLUDED.sensor_instance,
                updated_at = NOW();
        """)

        for idx in self.sensors_df.index:
            row = self.sensors_df.iloc[idx]
            persistence_name = row['PersistenceName']

            cur.execute(upsert_sql, (
                persistence_name,                    # signal_key
                row.get('SensorName', persistence_name),  # name
                row.get('Description', ''),          # description
                row.get('Units', ''),                # units
                row.get('DataType', 'REAL'),         # data_type (for EP)
                persistence_name,                    # persistence_name (legacy)
                row.get('SensorName', ''),           # sensor_name
                row.get('SensorInstance', ''),       # sensor_instance
                self.dt_name                         # digital_twin_id
            ))

        self.conn.commit()
        cur.close()
        logger.info(f"Upserted {len(self.sensors_df)} signals into metadata table")

    def _get_signal_ids(self) -> Dict[str, int]:
        """Cache signal_id lookup by PersistenceName"""
        cur = self.conn.cursor()
        cur.execute("SELECT signal_key, id FROM signals WHERE digital_twin_id = %s", (self.dt_name,))
        result = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        return result

    def persist(self, timestamp: pd.Timestamp):
        """
        Persist all current sensor values in ETV format
        """
        if not self.sensors_df['current_val'].notna().any():
            logger.warning("No valid sensor values to persist at %s", timestamp)
            return

        self._upsert_signals()  # Ensure metadata exists
        signal_id_map = self._get_signal_ids()

        cur = self.conn.cursor()

        # Prepare batch insert
        records = []
        for idx in self.sensors_df.index:
            persistence_name = self.sensors_df.loc[idx, 'PersistenceName']
            value = self.sensors_df.loc[idx, 'current_val']

            if pd.isna(value):
                continue  # Skip NaN

            signal_id = signal_id_map.get(persistence_name)
            if signal_id is None:
                logger.warning(f"Signal {persistence_name} not found in DB, skipping")
                continue

            records.append((timestamp, signal_id, float(value)))

        if not records:
            logger.debug("No records to insert at %s", timestamp)
            cur.close()
            return

        # Bulk insert
        insert_sql = """
            INSERT INTO measurements (time, signal_id, value)
            VALUES (%s, %s, %s)
            ON CONFLICT (time, signal_id) DO UPDATE SET value = EXCLUDED.value;
        """

        try:
            cur.executemany(insert_sql, records)
            self.conn.commit()
            logger.info(f"Persisted {len(records)} measurements at {timestamp}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to persist measurements: {e}")
            raise
        finally:
            cur.close()

    def close(self):
        if self.conn:
            self.conn.close()