import pandas as pd
import sqlite3
from pathlib import Path
import argparse

CSV_PATH = r"C:\Users\kamid\OneDrive\Desktop\datacard_transcations\data_credit_card_transactions.csv"
DB_PATH= r"credit_card_transactions.db"

# Step 1: Extract
df = pd.read_csv(CSV_PATH)

print("Initial Data Snapshot:")
print(df.head())
print(df.info())

# Step 2: Transform
df = df.drop_duplicates()

# 🔹 Optional cleanup (add here)
if 'Unnamed: 0' in df.columns:
    df = df.drop(columns=['Unnamed: 0'])

if 'is_fraud' in df.columns:
    df['is_fraud'] = df['is_fraud'].astype('int64')

if 'amt' in df.columns:
    df['amt'] = df['amt'].astype('float64')

# Fill missing numeric values with 0
numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
df[numeric_cols] = df[numeric_cols].fillna(0)

# Fill missing categorical values with 'Unknown'
categorical_cols = df.select_dtypes(include=['object']).columns
df[categorical_cols] = df[categorical_cols].fillna('Unknown')

# Convert transaction date to datetime and create month/year
if 'trans_date_trans_time' in df.columns:
    df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'])
    df['transaction_month'] = df['trans_date_trans_time'].dt.month
    df['transaction_year'] = df['trans_date_trans_time'].dt.year

print("Transformed Data Snapshot:")
print(df.head())

# Step 3: Load
conn = sqlite3.connect(DB_PATH)
df.to_sql("transactions", conn, if_exists="replace", index=False)
conn.close()

print(f"ETL process completed. Data loaded into {DB_PATH} (table: transactions).")
