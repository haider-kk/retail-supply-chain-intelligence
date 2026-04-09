# Retail Supply Chain Intelligence Platform

## End-to-End Data Analytics & Engineering Pipeline
### Built on Real-World Brazilian E-Commerce Data (100K+ orders)

---

## Project Overview
This project simulates a real-world data platform built for
a multi-region e-commerce company. It covers the full data
lifecycle from raw ingestion to machine learning predictions.

---

## Architecture
Raw Layer (Bronze)
|
Staging Layer (Silver)
|
Data Warehouse (Gold)
|
Analytics & ML Layer
---

## Tech Stack
- **Languages**: Python, SQL
- **Data Engineering**: Pandas, NumPy, SQLAlchemy, SQLite
- **Visualization**: Matplotlib, Seaborn, Plotly
- **Machine Learning**: Scikit-learn
- **Environment**: Jupyter Notebook
- **Version Control**: Git, GitHub

---

## Dataset
- **Source**: Brazilian E-Commerce by Olist (Kaggle)
- **Size**: 100,000+ orders, 8 datasets
- **Period**: 2016-2018

---

## Project Structure
retail-supply-chain-intelligence/
├── data/
│ ├── raw/ # Original source data
│ ├── staging/ # Cleaned intermediate data
│ └── warehouse/ # SQL data warehouse
├── notebooks/ # Jupyter analysis notebooks
├── scripts/ # ETL pipeline scripts
├── reports/ # Charts and visualizations
├── config/ # Configuration files
└── logs/ # Pipeline execution logs
---

## Day 1 - Data Engineering
- Built ETL ingestion pipeline (8 CSV files)
- Designed Star Schema SQL warehouse
- Cleaned 100K+ records across all datasets
- Feature engineering - delivery time, late flags
- Outlier detection using IQR method

---

## Day 2 - Business Analytics
- 10 SQL business questions answered
- Monthly revenue trend analysis
- Geographic revenue analysis (top states)
- Product category performance
- Delivery performance & late delivery rate
- Customer review score analysis
- Peak shopping hours & days
- RFM customer segmentation
- Payment method analysis
- 8 professional charts generated

---

## Day 3 - Machine Learning
- Churn Prediction (3 models compared)
  - Logistic Regression
  - Random Forest
  - Gradient Boosting
- Demand Forecasting Model
- Delivery Delay Prediction
- Customer Lifetime Value Analysis

---

## Business Questions Answered
1. What is overall business performance?
2. How has revenue grown month over month?
3. Which states generate most revenue?
4. Which product categories are most profitable?
5. What is our late delivery rate?
6. What do customer reviews tell us?
7. When do customers shop most?
8. Who are our top sellers?
9. What payment methods are preferred?
10. Who are our most valuable customers?

---

## Key Insights
- Revenue peaks in Q4 holiday season
- Sao Paulo generates highest revenue
- Late deliveries strongly correlate with low reviews
- Credit card is most preferred payment method
- Recency is strongest churn predictor
- Champions segment generates 3x more revenue

---

## Reports Generated
- monthly_revenue_trend.png
- top_states_analysis.png
- category_performance.png
- delivery_performance.png
- review_analysis.png
- peak_time_analysis.png
- seller_performance.png
- payment_analysis.png
- churn_model_evaluation.png
- feature_importance.png
- demand_forecast.png
- customer_lifetime_value.png
- delay_prediction.png