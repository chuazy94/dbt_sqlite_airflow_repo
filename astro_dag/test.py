import os

INPUT_FOLDER = "./include/data/input/"
OUTPUT_FOLDER = "./include/output/"
DB_PATH = "../sqlite.db"

csv_files = []
for dirpath, dirnames, filenames in os.walk(INPUT_FOLDER):
    for file in filenames:
        if file.endswith('.csv'):
            csv_files.append(os.path.join(dirpath, file))

print(csv_files)