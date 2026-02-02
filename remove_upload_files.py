import csv
import os

def identify_files(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)

            with open(file_path, 'r') as file:
                csv_reader = csv.reader(file)

                # Skip header row if it exists
                next(csv_reader, None) 

                mig_docs = []
                for row in csv_reader:
                    mig_filename = row[13]
                    mig_docs.append(mig_filename)

    return mig_docs

def remove_unnecessary_files(folder_path, doc_files):
    for filename in os.listdir(folder_path):
        if not (filename.endswith(".csv") or filename.endswith(".py")):
            file_path = os.path.join(folder_path, filename)

            if not filename in doc_files:
                os.remove(file_path)
                print(f"Deleted: {filename}")

if __name__ == "__main__":
    folder_path = "C:\\Users\\dell\\Downloads\\StorEdgeFiles_20250523-071208_American_Classic-documents\\Documents -Full"

    doc_list = identify_files(folder_path)

    remove_unnecessary_files(folder_path, doc_list)