import os
import csv

# Set the directory you want to read
directory_path = r"C:\Users\dell\Downloads\final"

# Get list of all files in the directory (excluding subdirectories)
file_names = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]

# Output CSV path
output_csv = 'file_list.csv'

# Write file names to CSV
with open(output_csv, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Filename'])  # Header
    for file in file_names:
        writer.writerow([file])

print(f"File names written to {output_csv}")