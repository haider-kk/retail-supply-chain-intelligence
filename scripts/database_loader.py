# ============================================================
# RETAIL SUPPLY CHAIN INTELLIGENCE
# Script: database_loader.py
# Purpose: Load cleaned data into SQL Data Warehouse
# ============================================================

import pandas as pd
import numpy as np
import sqlite3
import yaml
import logging
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from tqdm import tqdm

# ============================================================
# LOAD CONFIG
# ============================================================
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

# ============================================================
# SETUP LOGGING
# ============================================================
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
# CLEAN DATA - STAGING LAYER
# ============================================================
def clean_data(dataframes, logger):
    """Clean and prepare data for warehouse loading"""
    logger.info("\n" + "=" * 60)
    logger.info("STARTING DATA CLEANING - STAGING LAYER")
    logger.info("=" * 60)
    
    cleaned = {}
    
    # --- Clean Orders ---
    df_orders = dataframes['orders'].copy()
    date_cols = ['order_purchase_timestamp', 'order_approved_at',
                 'order_delivered_carrier_date', 'order_delivered_customer_date',
                 'order_estimated_delivery_date']
    for col in date_cols:
        df_orders[col] = pd.to_datetime(df_orders[col], errors='coerce')
    df_orders.drop_duplicates(subset='order_id', inplace=True)
    cleaned['orders'] = df_orders
    logger.info(f" Orders cleaned | Rows: {df_orders.shape[0]:,}")
    
    # --- Clean Customers ---
    df_customers = dataframes['customers'].copy()
    df_customers.drop_duplicates(subset='customer_id', inplace=True)
    df_customers['customer_city'] = df_customers['customer_city'].str.title()
    df_customers['customer_state'] = df_customers['customer_state'].str.upper()
    cleaned['customers'] = df_customers
    logger.info(f" Customers cleaned | Rows: {df_customers.shape[0]:,}")
    
    # --- Clean Products ---
    df_products = dataframes['products'].copy()
    df_products.drop_duplicates(subset='product_id', inplace=True)
    df_products.fillna({
        'product_category_name': 'unknown',
        'product_weight_g': df_products['product_weight_g'].median(),
        'product_length_cm': df_products['product_length_cm'].median(),
        'product_height_cm': df_products['product_height_cm'].median(),
        'product_width_cm': df_products['product_width_cm'].median()
    }, inplace=True)
    cleaned['products'] = df_products
    logger.info(f" Products cleaned | Rows: {df_products.shape[0]:,}")
    
    # --- Clean Order Items ---
    df_items = dataframes['order_items'].copy()
    df_items.drop_duplicates(inplace=True)
    df_items['shipping_limit_date'] = pd.to_datetime(
        df_items['shipping_limit_date'], errors='coerce')
    cleaned['order_items'] = df_items
    logger.info(f" Order Items cleaned | Rows: {df_items.shape[0]:,}")
    
    # --- Clean Payments ---
    df_payments = dataframes['payments'].copy()
    df_payments = df_payments[df_payments['payment_value'] > 0]
    cleaned['payments'] = df_payments
    logger.info(f"[OK] Payments cleaned | Rows: {df_payments.shape[0]:,}")
    df_payments.drop_duplicates(inplace=True)
    df_payments = df_payments[df_payments['payment_value'] > 0]
    cleaned['payments'] = df_payments
    logger.info(f" Payments cleaned | Rows: {df_payments.shape[0]:,}")
    
    # --- Clean Reviews ---
    df_reviews = dataframes['reviews'].copy()
    df_reviews.drop_duplicates(subset='review_id', inplace=True)
    df_reviews.fillna({
        'review_comment_title': 'No Title',
        'review_comment_message': 'No Comment'
    }, inplace=True)
    cleaned['reviews'] = df_reviews
    logger.info(f" Reviews cleaned | Rows: {df_reviews.shape[0]:,}")
    
    # --- Clean Sellers ---
    df_sellers = dataframes['sellers'].copy()
    df_sellers.drop_duplicates(subset='seller_id', inplace=True)
    df_sellers['seller_city'] = df_sellers['seller_city'].str.title()
    df_sellers['seller_state'] = df_sellers['seller_state'].str.upper()
    cleaned['sellers'] = df_sellers
    logger.info(f" Sellers cleaned | Rows: {df_sellers.shape[0]:,}")
    
    return cleaned

# ============================================================
# SAVE TO STAGING LAYER
# ============================================================
def save_to_staging(cleaned_data, staging_path, logger):
    """Save cleaned data to staging folder as CSV"""
    logger.info("\n" + "=" * 60)
    logger.info("SAVING TO STAGING LAYER")
    logger.info("=" * 60)
    
    for name, df in tqdm(cleaned_data.items(), desc="Saving staged files"):
        filepath = os.path.join(staging_path, f"staged_{name}.csv")
        df.to_csv(filepath, index=False)
        logger.info(f" Saved staged_{name}.csv")

# ============================================================
# LOAD INTO SQL WAREHOUSE
# ============================================================
def load_to_warehouse(cleaned_data, config, logger):
    """Load all cleaned data into SQLite data warehouse"""
    logger.info("\n" + "=" * 60)
    logger.info("LOADING INTO SQL DATA WAREHOUSE")
    logger.info("=" * 60)
    
    db_path = os.path.join(
        config['database']['path'],
        config['database']['name']
    )
    
    engine = create_engine(f'sqlite:///{db_path}')
    
    table_map = {
        'orders': 'dim_orders',
        'customers': 'dim_customers',
        'products': 'dim_products',
        'order_items': 'fact_order_items',
        'payments': 'fact_payments',
        'reviews': 'fact_reviews',
        'sellers': 'dim_sellers'
    }
    
    for name, df in tqdm(cleaned_data.items(), desc="Loading to warehouse"):
        table_name = table_map.get(name, name)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f" Loaded {table_name} | Rows: {df.shape[0]:,}")
    
    # Verify tables
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ))
        tables = [row[0] for row in result]
        logger.info(f"\n Tables in Warehouse: {tables}")
    
    logger.info("\n DATA WAREHOUSE READY!")
    return engine

# ============================================================
# MAIN
# ============================================================
def main(dataframes):
    config = load_config()
    log_path = config['paths']['logs']
    logger = setup_logging(log_path)
    
    # Clean data
    cleaned_data = clean_data(dataframes, logger)
    
    # Save to staging
    save_to_staging(cleaned_data, config['paths']['staging_data'], logger)
    
    # Load to warehouse
    engine = load_to_warehouse(cleaned_data, config, logger)
    
    logger.info("\n" + "=" * 60)
    logger.info(" FULL PIPELINE COMPLETE!")
    logger.info("=" * 60)
    
    return engine, cleaned_data

if __name__ == "__main__":
    # Import ingestion pipeline
    sys.path.append(os.path.dirname(__file__))
    from ingestion_pipeline import main as ingest
    dataframes = ingest()
    main(dataframes)