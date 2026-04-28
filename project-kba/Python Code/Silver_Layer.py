import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# 🔌 Koneksi ke MySQL
password = quote_plus("@Ulunnuha21_mysql")
engine = create_engine(f"mysql+pymysql://root:{password}@localhost:3306/bronze")

# 📥 Load data dari Bronze Layer
query = "SELECT * FROM pizza_sales"
df = pd.read_sql(query, engine)

print(f"📊 Data awal (Bronze): {df.shape[0]} baris")

# ================================
# 🧹 SILVER PROCESS + VALIDATION
# ================================

# 1. Hapus NULL pada kolom penting
before = df.shape[0]
df = df.dropna(subset=["order_id", "pizza_id", "quantity", "total_price"])
after = df.shape[0]
print(f"🧹 Hapus NULL: {before} → {after} (hapus {before - after} baris)")

# 2. Remove duplicate
before = df.shape[0]
df = df.drop_duplicates()
after = df.shape[0]
print(f"🧹 Hapus duplikat: {before} → {after} (hapus {before - after} baris)")

# 3. Konversi tipe data
df["order_id"] = df["order_id"].astype(int)
df["pizza_id"] = df["pizza_id"].astype(int)
df["quantity"] = df["quantity"].astype(int)
df["total_price"] = df["total_price"].astype(float)
df["unit_price"] = df["unit_price"].astype(float)

print(f"🔄 Konversi tipe data: {df.shape[0]} baris (tidak berubah)")

# # 4. Format tanggal & waktu
# before = df.shape[0]
# df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
# df["order_time"] = pd.to_datetime(df["order_time"], format="%H:%M", errors="coerce").dt.time
# df = df.dropna(subset=["order_date", "order_time"])
# after = df.shape[0]
# print(f"📅 Validasi tanggal/waktu: {before} → {after} (hapus {before - after} baris)")

# 5. Validasi nilai (hapus data tidak logis)
before = df.shape[0]
df = df[(df["quantity"] > 0) & (df["total_price"] >= 0)]
after = df.shape[0]
print(f"⚠️ Validasi nilai: {before} → {after} (hapus {before - after} baris)")

# 6. Final check NULL
before = df.shape[0]
df = df.dropna()
after = df.shape[0]
print(f"✅ Final NULL check: {before} → {after} (hapus {before - after} baris)")

print(f"\n🎯 Total data setelah Silver: {df.shape[0]} baris")

# ================================
# 📤 SIMPAN KE SILVER LAYER
# ================================

df.to_sql(
    name="silver_pizza_sales_clean",
    con=engine,
    if_exists="replace",
    index=False
)

print("✅ Silver Layer berhasil dibuat!")