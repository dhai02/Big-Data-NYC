import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os


# Tạo thư mục image nếu chưa có
os.makedirs('image', exist_ok=True)

df_hour = pd.read_parquet('src/data/by_hour.parquet')
df_heatmap = pd.read_parquet('src/data/by_dow_hour.parquet')
df_payment = pd.read_parquet('src/data/by_payment.parquet')
df_location = pd.read_parquet('src/data/by_pickup_location_top20.parquet')

print(df_hour.head())
print(df_heatmap.head())
print(df_payment.head())
print(df_location.head())


# --- BIỂU ĐỒ 1: Lượng khách theo giờ (Line Chart) ---
plt.figure(figsize=(12, 6))
sns.lineplot(data=df_hour, x='pickup_hour', y='total_trips', marker='o', color='b')
plt.title('Lượng khách Taxi theo khung giờ trong ngày (01/2026)')
plt.xlabel('Giờ trong ngày')
plt.ylabel('Số lượng chuyến đi')
plt.grid(True)
plt.savefig('image/hourly_trend.png')
plt.show()

# --- BIỂU ĐỒ 2: Heatmap (Thứ trong tuần vs Giờ) ---
# Chuyển đổi dữ liệu sang dạng ma trận cho Heatmap
pivot_table = df_heatmap.pivot(index='pickup_dayofweek', columns='pickup_hour', values='total_trips')

plt.figure(figsize=(15, 8))
sns.heatmap(pivot_table, annot=False, cmap='viridis', linewidths=.5)
plt.title('Giờ cao điểm theo các ngày trong tuần')
plt.xlabel('Giờ trong ngày')
plt.ylabel('Thứ (1=CN, 7=T7)')
plt.savefig('image/taxi_heatmap.png')
plt.show()

# --- BIỂU ĐỒ 3: Phương thức thanh toán (Bar Chart) ---
plt.figure(figsize=(10, 8))
sns.barplot(data=df_payment, x='payment_type', y='total_trips', palette='Set2')
plt.title('Số lượng chuyến đi theo phương thức thanh toán')
plt.xlabel('Phương thức thanh toán')
plt.ylabel('Số lượng chuyến đi')
plt.xticks(rotation=45)
plt.savefig('image/payment_methods.png')
plt.show()


# --- BIỂU ĐỒ 4: Phương thức thanh toán (Pie Chart) ---
# 1. Định nghĩa bảng mã chuẩn của TLC
payment_map = {
    1: 'Credit Card',
    2: 'Cash',
    3: 'No Charge',
    4: 'Dispute'
}

# 2. Chuẩn bị dữ liệu 
df_payment['payment_name'] = df_payment['payment_type'].map(payment_map)
df_payment = df_payment.sort_values(by='total_trips', ascending=False)

# 3. Vẽ biểu đồ
plt.figure(figsize=(12, 7))
colors = sns.color_palette('pastel')[0:4] # Lấy 4 màu nhẹ nhàng

# Tách nhẹ tất cả các miếng để nhìn rõ ranh giới
explode = [0.05] * len(df_payment) 

patches, texts, autotexts = plt.pie(
    df_payment['total_trips'], 
    autopct='%1.1f%%', 
    startangle=140, 
    colors=colors,
    explode=explode,
    pctdistance=0.85, # Đẩy số % ra gần rìa
 
)

# Làm cho chữ phần trăm dễ đọc hơn (màu trắng hoặc đậm)
plt.setp(autotexts, size=10, weight="bold")

# 4.Dùng Legend thay vì dán nhãn trực tiếp
plt.legend(
    patches, 
    df_payment['payment_name'],
    title="Loại thanh toán",
    loc="center left",
    bbox_to_anchor=(1, 0, 0.5, 1) # Đẩy bảng chú thích ra hẳn bên ngoài vòng tròn
)

plt.title('Tỷ lệ phương thức thanh toán Taxi NYC (Tháng 01/2026)', fontsize=14, pad=20)
plt.axis('equal')
plt.savefig('image/payment_pie.png')
plt.show() 





# --- BIỂU ĐỒ 5: Top 20 điểm nóng (Bar Chart) ---
df_location['PULocationID'] = df_location['PULocationID'].astype(str)
plt.figure(figsize=(12, 8))
# Sắp xếp lại để cột dài nhất nằm trên cùng
df_location = df_location.sort_values('total_trips', ascending=False)
sns.barplot(data=df_location, x='total_trips', y='PULocationID', palette='viridis')
plt.title('Top 20 điểm nóng đón khách (PULocationID) - Số chuyến đi')
plt.xlabel('Số lượng chuyến đi')
plt.ylabel('PULocationID')
plt.savefig('image/top_locations.png')
plt.show()