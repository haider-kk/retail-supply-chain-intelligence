# ============================================================
# RETAIL SUPPLY CHAIN INTELLIGENCE
# Script: data_cleaning.py
# Purpose: Deep Data Cleaning & Feature Engineering
# ============================================================

import pandas as pd
import numpy as np
import sqlite3
import yaml
import logging
import os
import sys
from datetime import datetime

# ============================================================
# LOAD CONFIG & LOGGING
# ============================================================
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def setup_logging(log_path):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

# ============================================================
# LOAD FROM WAREHOUSE
# ============================================================
def load_from_warehouse(config, logger):
    """Load all tables from SQL warehouse"""
    db_path = os.path.join(
        config['database']['path'],
        config['database']['name']
    )
    conn = sqlite3.connect(db_path)
    
    tables = {
        'orders': 'dim_orders',
        'customers': 'dim_customers',
        'products': 'dim_products',
        'order_items': 'fact_order_items',
        'payments': 'fact_payments',
        'reviews': 'fact_reviews',
        'sellers': 'dim_sellers'
    }
    
    dataframes = {}
    for name, table in tables.items():
        dataframes[name] = pd.read_sql(f"SELECT * FROM {table}", conn)
        logger.info(f"✅ Loaded {table} | Rows: {dataframes[name].shape[0]:,}")
    
    conn.close()
    return dataframes

# ============================================================
# FEATURE ENGINEERING
# ============================================================
def engineer_features(dataframes, logger):
    """Create new meaningful features from existing data"""
    logger.info("\n" + "=" * 60)
    logger.info("FEATURE ENGINEERING")
    logger.info("=" * 60)
    
    # --- Orders Feature Engineering ---
    df_orders = dataframes['orders'].copy()
    
    # Convert dates
    date_cols = ['order_purchase_timestamp', 'order_approved_at',
                 'order_delivered_carrier_date', 'order_delivered_customer_date',
                 'order_estimated_delivery_date']
    for col in date_cols:
        df_orders[col] = pd.to_datetime(df_orders[col], errors='coerce')
    
    # Delivery time in days
    df_orders['delivery_time_days'] = (
        df_orders['order_delivered_customer_date'] -
        df_orders['order_purchase_timestamp']
    ).dt.days
    
    # Was delivery late?
    df_orders['is_late_delivery'] = (
        df_orders['order_delivered_customer_date'] >
        df_orders['order_estimated_delivery_date']
    ).astype(int)
    
    # Order purchase time features
    df_orders['purchase_year'] = df_orders['order_purchase_timestamp'].dt.year
    df_orders['purchase_month'] = df_orders['order_purchase_timestamp'].dt.month
    df_orders['purchase_day'] = df_orders['order_purchase_timestamp'].dt.day
    df_orders['purchase_hour'] = df_orders['order_purchase_timestamp'].dt.hour
    df_orders['purchase_dayofweek'] = df_orders['order_purchase_timestamp'].dt.dayofweek
    df_orders['purchase_quarter'] = df_orders['order_purchase_timestamp'].dt.quarter
    
    # Approval time in hours
    df_orders['approval_time_hours'] = (
        df_orders['order_approved_at'] -
        df_orders['order_purchase_timestamp']
    ).dt.total_seconds() / 3600
    
    dataframes['orders'] = df_orders
    logger.info(f"✅ Orders features engineered | New columns: delivery_time_days, is_late_delivery, purchase_year/month/day/hour")
    
    # --- Payments Feature Engineering ---
    df_payments = dataframes['payments'].copy()
    
    # Total payment per order
    payment_agg = df_payments.groupby('order_id').agg(
        total_payment=('payment_value', 'sum'),
        num_installments=('payment_installments', 'max'),
        payment_types=('payment_type', 'nunique')
    ).reset_index()
    
    dataframes['payment_summary'] = payment_agg
    logger.info(f"✅ Payment summary created | Rows: {payment_agg.shape[0]:,}")
    
    # --- Master Dataset ---
    logger.info("\n📊 Creating Master Dataset...")
    
    master = df_orders.merge(dataframes['customers'], on='customer_id', how='left')
    master = master.merge(payment_agg, on='order_id', how='left')
    master = master.merge(dataframes['reviews'][['order_id', 'review_score']], 
                         on='order_id', how='left')
    
    logger.info(f"✅ Master dataset created | Rows: {master.shape[0]:,} | Columns: {master.shape[1]}")
    
    dataframes['master'] = master
    return dataframes

# ============================================================
# OUTLIER DETECTION
# ============================================================
def detect_outliers(dataframes, logger):
    """Detect and handle outliers using IQR method"""
    logger.info("\n" + "=" * 60)
    logger.info("OUTLIER DETECTION")
    logger.info("=" * 60)
    
    df_payments = dataframes['payments'].copy()
    
    # IQR method for payment values
    Q1 = df_payments['payment_value'].quantile(0.25)
    Q3 = df_payments['payment_value'].quantile(0.75)
    IQR = Q3 - Q1
    
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    
    outliers = df_payments[
        (df_payments['payment_value'] < lower) |
        (df_payments['payment_value'] > upper)
    ]
    
    logger.info(f"📊 Payment Value Stats:")
    logger.info(f" Q1: R${Q1:.2f} | Q3: R${Q3:.2f} | IQR: R${IQR:.2f}")
    logger.info(f" Lower Bound: R${lower:.2f} | Upper Bound: R${upper:.2f}")
    logger.info(f" Outliers Found: {len(outliers):,} rows ({len(outliers)/len(df_payments)*100:.2f}%)")
    
    return dataframes

# ============================================================
# SAVE FINAL CLEAN DATA
# ============================================================
def save_final_data(dataframes, config, logger):
    """Save master dataset to warehouse"""
    logger.info("\n" + "=" * 60)
    logger.info("SAVING FINAL CLEANED DATA")
    logger.info("=" * 60)
    
    db_path = os.path.join(
        config['database']['path'],
        config['database']['name']
    )
    conn = sqlite3.connect(db_path)
    
    # Save master dataset
    master = dataframes['master']
    
    # Convert datetime columns to string for SQLite
    date_cols = master.select_dtypes(include=['datetime64']).columns
    for col in date_cols:
        master[col] = master[col].astype(str)
    
    master.to_sql('master_dataset', conn, if_exists='replace', index=False)
    logger.info(f"✅ Master dataset saved to warehouse | Rows: {master.shape[0]:,}")
    
    # Also save as CSV to warehouse folder
    warehouse_path = config['paths']['warehouse_data']
    master.to_csv(os.path.join(warehouse_path, 'master_dataset.csv'), index=False)
    logger.info(f"✅ Master dataset saved as CSV")
    
    conn.close()

# ============================================================
# MAIN
# ============================================================
def main():
    config = load_config()
    logger = setup_logging(config['paths']['logs'])
    
    logger.info("\n" + "=" * 60)
    logger.info("STARTING DEEP CLEANING PIPELINE")
    logger.info("=" * 60)
    
    # Load from warehouse
    dataframes = load_from_warehouse(config, logger)
    
    # Engineer features
    dataframes = engineer_features(dataframes, logger)
    
    # Detect outliers
    dataframes = detect_outliers(dataframes, logger)
    
    # Save final data
    save_final_data(dataframes, config, logger)
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ DEEP CLEANING COMPLETE!")
    logger.info("=" * 60)
    
    return dataframes

if __name__ == "__main__":
    main()