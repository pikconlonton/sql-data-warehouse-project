import shutil

# Đường dẫn đến file gốc và file đích
def cop_csv(source_file, destination_file):

    # Mở file A.csv và đọc nội dung
    chunk_size = 1024 * 1024  # Đọc theo khối 1MB

    with open(source_file, 'r', encoding='utf-8') as src, open(destination_file, 'w', encoding='utf-8') as dest:
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            dest.write(chunk)

    print(f"Đã sao chép nội dung từ {source_file} sang {destination_file}")


list_path_file =[
    'data1k/appointments.csv',
    'data1k/doctors.csv',
    'data1k/patients.csv',
    'data1k\check.csv',
    'data1k\diseases.csv',
    'data1k\lab_results.csv',
    'data1k\prescriptions.csv',
    'data1k/treatments.csv',
    'data1k/vital_signs.csv',
    'data1k\hospital_fee.csv',
]
des_path_file = [
    'data1k\process_csv/appointments.csv',
    'data1k\process_csv/doctors.csv',
    'data1k\process_csv/patients.csv',
    'data1k\process_csv\check.csv',
    'data1k\process_csv\diseases.csv',
    'data1k\process_csv\lab_results.csv',
    'data1k\process_csv\prescriptions.csv',
    'data1k\process_csv/treatments.csv',
    'data1k\process_csv/vital_signs.csv',
    'data1k\process_csv\hospital_fee.csv',
]
for i in range(len(list_path_file)):
    cop_csv(list_path_file[i], des_path_file[i])
