# ============================================================
# RETAIL SUPPLY CHAIN INTELLIGENCE
# Script: ingestion_pipeline.py
# Purpose: Data Ingestion - Raw Layer to Staging
# Author: Your Name
# ============================================================

import pandas as pd
import numpy as np
import yaml
import logging
import os
import sys
from datetime import datetime
from tqdm import tqdm

# ============================================================
# STEP 1: LOAD CONFIGURATION
# ============================================================
def load_config():
    """Load project configuration from config.yaml"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

# ============================================================
# STEP 2: SETUP LOGGING
# ============================================================
def setup_logging(log_path):
    """Setup logging to both file and console"""
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
# STEP 3: INGEST RAW DATA
# ============================================================
def ingest_raw_data(config, logger):
    """Read all CSV files from raw layer"""
    raw_path = config['paths']['raw_data']
    files = config['files']
    
    dataframes = {}
    
    logger.info("=" * 60)
    logger.info("STARTING DATA INGESTION PIPELINE")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info("=" * 60)
    
    for name, filename in tqdm(files.items(), desc="Ingesting files"):
        filepath = os.path.join(raw_path, filename)
        try:
            df = pd.read_csv(filepath)
            dataframes[name] = df
            logger.info(f" Loaded {filename} | Rows: {df.shape[0]:,} | Columns: {df.shape[1]}")
        except FileNotFoundError:
            logger.error(f" File not found: {filepath}")
        except Exception as e:
            logger.error(f" Error loading {filename}: {str(e)}")
    
    return dataframes

# ============================================================
# STEP 4: BASIC VALIDATION
# ============================================================
def validate_data(dataframes, logger):
    """Validate loaded dataframes"""
    logger.info("\n" + "=" * 60)
    logger.info("DATA VALIDATION REPORT")
    logger.info("=" * 60)
    
    validation_results = {}
    
    for name, df in dataframes.items():
        null_counts = df.isnull().sum().sum()
        duplicate_counts = df.duplicated().sum()
        
        validation_results[name] = {
            'rows': df.shape[0],
            'columns': df.shape[1],
            'null_values': null_counts,
            'duplicates': duplicate_counts
        }
        
        logger.info(f"\n📊 {name.upper()}")
        logger.info(f" Rows: {df.shape[0]:,}")
        logger.info(f" Columns: {df.shape[1]}")
        logger.info(f" Null Values: {null_counts:,}")
        logger.info(f" Duplicates: {duplicate_counts:,}")
    
    return validation_results

# ============================================================
# MAIN PIPELINE
# ============================================================
def main():
    # Load config
    config = load_config()
    
    # Setup logging
    log_path = config['paths']['logs']
    logger = setup_logging(log_path)
    
    # Ingest data
    dataframes = ingest_raw_data(config, logger)
    
    # Validate
    validation_results = validate_data(dataframes, logger)
    
    logger.info("\n" + "=" * 60)
    logger.info(f" INGESTION COMPLETE | {len(dataframes)} files loaded")
    logger.info("=" * 60)
    
    return dataframes

if __name__ == "__main__":
    main()