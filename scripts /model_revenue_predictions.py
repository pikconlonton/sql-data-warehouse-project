import pyodbc
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


#Phải Thay đổi Kết nối database phù hợp

def make_revenue_by_month_csv():
    # Thông tin kết nối
    server = 'localhost'  # hoặc 'Tên_Máy\\Tên_Instance'
    database = 'hms_hqt3'
    username = 'tenuser'  # Nếu dùng SQL Authentication
    password = 'matkhau'  # Nếu dùng SQL Authentication

    # Kết nối (nếu dùng Windows Authentication, bỏ UID và PWD)
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=hms_hqt3;'
        'Trusted_Connection=yes;'
    )

    cursor = conn.cursor()
    import csv

    cursor.execute("SELECT * FROM revenue_by_month")
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    with open("revenue_by_month.csv", "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)


def make_revenue_prediction_in_5_months_csv():
    df = pd.read_csv('revenue_by_month.csv')  # ← Thay bằng tên file CSV thật

    # Tạo cột datetime
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')
    df = df.sort_values('date')

    # Tạo chỉ số tháng
    df['month_index'] = np.arange(len(df))

    # Huấn luyện model
    X = df[['month_index']]
    y = df['total_revenue']
    model = LinearRegression()
    model.fit(X, y)

    # Dự đoán 3 tháng tiếp theo
    future_months = np.arange(len(df), len(df) + 5).reshape(-1, 1)
    future_preds = model.predict(future_months)
    future_dates = pd.date_range(start=df['date'].max() + pd.DateOffset(months=1), periods=5, freq='MS')

    df_future = pd.DataFrame({
        'date': future_dates,
        'total_revenue': future_preds,
        'type': 'Dự đoán'
    })

    # Gộp lại dữ liệu
    df['type'] = 'Thực tế'
    df_all = pd.concat([df[['date', 'total_revenue', 'type']], df_future], ignore_index=True)

    df_all.to_csv('revenue_prediction_in_5_months.csv', index=False)

    # Hiển thị bảng kết quả
    print(df_all)

def make_tbl_revenue_prediction_in_5_months():
    import pyodbc

    # Kết nối đến SQL Server
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=hms_hqt3;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

    # Câu lệnh SQL tạo bảng
    create_table_query = '''
    IF OBJECT_ID('revenue_prediction_in_5_months', 'U') IS NOT NULL
        DROP TABLE revenue_prediction_in_5_months;

    CREATE TABLE revenue_prediction_in_5_months (
        [date] VARCHAR(20),              -- Ví dụ: '2025-08'
        total_revenue FLOAT,
        [type] NVARCHAR(20)
    );
    '''

    # Thực thi tạo bảng
    cursor.execute(create_table_query)
    conn.commit()

    print("✅ Đã tạo bảng revenue_prediction_in_5_months thành công.")

    cursor.close()
    conn.close()



def load_tbl_revenue_prediction_in_5_months():
    import pyodbc
    import pandas as pd

    # Kết nối SQL Server
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=hms_hqt3;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

    # Giả sử df_5months đã được tạo như ở bước trước
    # Nếu chưa có thì load lại từ CSV:
    df_5months = pd.read_csv('revenue_prediction_in_5_months.csv')

    # Chèn dữ liệu từng dòng
    for index, row in df_5months.iterrows():
        cursor.execute("""
            INSERT INTO revenue_prediction_in_5_months ([date], total_revenue, [type])
            VALUES (?, ?, ?)
        """, row['date'], row['total_revenue'], row['type'])

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Dữ liệu đã được chèn vào bảng revenue_prediction_in_5_months")


if __name__ == '__main__':
    make_revenue_by_month_csv()
    make_revenue_prediction_in_5_months_csv()
    make_tbl_revenue_prediction_in_5_months()
    load_tbl_revenue_prediction_in_5_months()
