import pandas as pd
import os

# Step 1: Define the original file path
file_path = r"C:\Users\dell\OneDrive - Tenant Inc\Desktop\Migration\Storeedge\MIG-17204_Storelocal Storage - Austin\StorEdgeFiles_20251024-151045\StorEdgeTenantDocuments_24-10-2025_15-06-51-931.csvcd"  # Change this to your actual path

# Step 2: Read the CSV
df = pd.read_csv(file_path)

# Step 3: Drop rows where 'File Name' or 'Moved-In Date' is null
df_cleaned = df.dropna(subset=['File Name', 'Moved-In Date'])

# Step 4: Get directory and filename
directory = os.path.dirname(file_path)
original_filename = os.path.basename(file_path)
name_without_ext = os.path.splitext(original_filename)[0]

# Step 5: Create new filename
new_filename = f"{name_without_ext}_cleaned.csv"
output_path = os.path.join(directory, new_filename)

# Step 6: Save cleaned data
df_cleaned.to_csv(output_path, index=False)

print("✅ Cleaned CSV saved to:", output_path)