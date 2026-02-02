import os
import shutil

# 🔧 Set your source and destination directories here
source_directory = r"C:\Users\dell\Downloads\Greens storage"
destination_directory = r"C:\Users\dell\Downloads\ext"

# ✅ Make sure destination folder exists
os.makedirs(destination_directory, exist_ok=True)

# 🧭 Walk through all subdirectories
for root, dirs, files in os.walk(source_directory):
    for file in files:
        if file.lower().endswith('.pdf'):
            source_file = os.path.join(root, file)
            destination_file = os.path.join(destination_directory, file)

            # 📛 Avoid overwriting files with the same name
            base, extension = os.path.splitext(file)
            counter = 1
            while os.path.exists(destination_file):
                destination_file = os.path.join(destination_directory, f"{base}_{counter}{extension}")
                counter += 1

            # 📥 Copy the file
            shutil.copy2(source_file, destination_file)
            print(f"Copied: {source_file} -> {destination_file}")

print("✅ Done! All PDFs have been collected.")