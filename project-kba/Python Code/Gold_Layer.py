import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# 🔌 Koneksi ke MySQL
password = quote_plus("@Ulunnuha21_mysql")
engine = create_engine(f"mysql+pymysql://root:{password}@localhost:3306/bronze")

# 📥 Load dari Silver Layer
query = "SELECT * FROM silver_pizza_sales_clean"
df = pd.read_sql(query, engine)

print(f"📊 Data Silver: {df.shape[0]} baris")

# ================================
# 🧹 FIX FORMAT DATE & TIME
# ================================

df["order_date"] = pd.to_datetime(df["order_date"], dayfirst=True, errors="coerce")
df["order_time"] = pd.to_datetime(df["order_time"], format="%H:%M:%S", errors="coerce")

# ⚠️ JANGAN DROP DATA → hanya isi default
df["order_date"] = df["order_date"].fillna(pd.to_datetime("2000-01-01"))
df["order_time"] = df["order_time"].fillna(pd.to_datetime("00:00:00"))

# ================================
# 📚 DIMENSION: dim_pizza
# ================================

dim_pizza = df[[
    "pizza_id",
    "pizza_name",
    "pizza_category",
    "pizza_size"
]].drop_duplicates(subset=["pizza_id"]).reset_index(drop=True)

print(f"📚 dim_pizza: {dim_pizza.shape[0]} baris")

# ================================
# 📅 DIMENSION: dim_date
# ================================

dim_date = df[["order_date"]].drop_duplicates().copy()

# urutkan & buat ID stabil
dim_date = dim_date.sort_values("order_date").reset_index(drop=True)
dim_date["date_id"] = dim_date.index + 1

dim_date["day"] = dim_date["order_date"].dt.day
dim_date["month"] = dim_date["order_date"].dt.month
dim_date["year"] = dim_date["order_date"].dt.year
dim_date["month_name"] = dim_date["order_date"].dt.strftime("%B")

print(f"📅 dim_date: {dim_date.shape[0]} baris")

# ================================
# ⏰ DIMENSION: dim_time
# ================================

dim_time = df[["order_time"]].drop_duplicates().copy()

# urutkan & buat ID stabil
dim_time = dim_time.sort_values("order_time").reset_index(drop=True)
dim_time["time_id"] = dim_time.index + 1

dim_time["hour"] = dim_time["order_time"].dt.hour
dim_time["minute"] = dim_time["order_time"].dt.minute

# kategori waktu
def get_time_bucket(hour):
    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 17:
        return "Afternoon"
    elif 17 <= hour < 21:
        return "Evening"
    else:
        return "Night"

dim_time["time_bucket"] = dim_time["hour"].apply(get_time_bucket)

print(f"⏰ dim_time: {dim_time.shape[0]} baris")

# ================================
# 📊 FACT TABLE: fact_sales
# ================================

print(f"🔎 Sebelum merge: {df.shape[0]} baris")

fact_sales = df.merge(dim_date[["order_date", "date_id"]], on="order_date", how="left")
print(f"🔎 Setelah merge date: {fact_sales.shape[0]} baris")

fact_sales = fact_sales.merge(dim_time[["order_time", "time_id"]], on="order_time", how="left")
print(f"🔎 Setelah merge time: {fact_sales.shape[0]} baris")

fact_sales = fact_sales[[
    "order_id",
    "pizza_id",
    "date_id",
    "time_id",
    "quantity",
    "total_price"
]]

print(f"📊 fact_sales: {fact_sales.shape[0]} baris")

# ================================
# 📤 SIMPAN KE DATABASE (GOLD)
# ================================

dim_pizza.to_sql("gold_dim_pizza", engine, if_exists="replace", index=False)
dim_date.to_sql("gold_dim_date", engine, if_exists="replace", index=False)
dim_time.to_sql("gold_dim_time", engine, if_exists="replace", index=False)
fact_sales.to_sql("gold_fact_sales", engine, if_exists="replace", index=False)

print("✅ Gold Layer (Star Schema) berhasil dibuat!")