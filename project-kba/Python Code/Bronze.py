import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ================================
# 🔌 KONFIGURASI DATABASE
# ================================

password = quote_plus("@Ulunnuha21_mysql")

# koneksi tanpa database dulu
engine = create_engine(f"mysql+pymysql://root:{password}@localhost:3306/")

# ================================
# 🏗️ BUAT DATABASE (SCHEMA)
# ================================

with engine.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS kba_pizza_sales"))
    print("✅ Database kba_pizza_sales siap")

# koneksi ke database baru
engine = create_engine(f"mysql+pymysql://root:{password}@localhost:3306/kba_pizza_sales")

# ================================
# 📥 LOAD DATA CSV
# ================================

file_path = r"C:\Users\Zaqy Ulunnuha\OneDrive\Documents\GitHub\KBDA KEL 4\project-kba\data\raw\pizza_sales.csv"

df = pd.read_csv(file_path)

print(f"📊 Data CSV: {df.shape[0]} baris, {df.shape[1]} kolom")

# ================================
# 📤 SIMPAN KE BRONZE TABLE
# ================================

df.to_sql(
    name="bronze_pizza_sales_raw",
    con=engine,
    if_exists="replace",  # ganti jadi "append" kalau mau nambah data
    index=False
)

print("✅ Data berhasil masuk ke Bronze Layer!")