import os
import shutil

# 🔧 Set your source and destination directories
source_directory = r"C:\Users\dell\Downloads\ext"
destination_directory = r"C:\Users\dell\Downloads\final"

# ✅ Make sure the destination folder exists
os.makedirs(destination_directory, exist_ok=True)

# 🔍 Search and copy files
for root, dirs, files in os.walk(source_directory):
    for file in files:
        if "- Ledger History" in file:
            source_path = os.path.join(root, file)
            destination_path = os.path.join(destination_directory, file)

            # Avoid overwriting files with the same name
            base, extension = os.path.splitext(file)
            counter = 1
            while os.path.exists(destination_path):
                destination_path = os.path.join(destination_directory, f"{base}_{counter}{extension}")
                counter += 1

            # Copy the file
            shutil.copy2(source_path, destination_path)
            print(f"Copied: {source_path} -> {destination_path}")