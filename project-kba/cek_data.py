import pandas as pd

file_path = 'data/raw/pizza_sales.csv'

print(f"--- Mengecek data: {file_path} ---")
df = pd.read_csv(file_path)

print("\n1. Lima Baris Pertama:")
print(df.head())

print("\n2. Info Tipe Data:")
print(df.info())

print("\n3. Data Kosong (Null):")
print(df.isnull().sum())

print("\n4. Jumlah Duplikat:")
print(df.duplicated().sum())