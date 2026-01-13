import os
import csv
file_path = r"data/source/meta-glasses-reviews.csv"  # raw string
duplicates_storage = r"checkpoints/duplicates.csv"   # raw string
# ------------------------------
# Validate CSV file exists and not empty
# ------------------------------
def validate_csv(file_path):
    validated = True
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
        validated = False
        return validated
    
    if os.path.getsize(file_path) == 0:
        raise ValueError(f"The file {file_path} is empty.")
        validated = False
        return validated
    else:
        print("Validation passed!")
        return validated
    

# ------------------------------
# Read CSV header
# ------------------------------
def read_csv_header(file_path):
    with open(file=file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        header = next(reader, None)

    if not header:
        raise ValueError(f"The CSV file {file_path} has no header")

    return [col.strip() for col in header]
columns = read_csv_header(file_path=file_path)
print(f"Columns: {columns}")

# Extract rows as list of dictionaries
def extract_rows(file_path):
    records = []
    invalid_rows = 0

    with open(file=file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row_number, row in enumerate(reader, start=2):
            if not row:
                invalid_rows += 1
                continue

            records.append(row)
    
    return records, invalid_rows

# ------------------------------
# Find duplicates and save immediately
# ------------------------------
def find_duplicates(records, id_column="reviewID", duplicates_file=None):
    if not duplicates_file:
        raise ValueError("You must provide a path for the duplicates_file.")

    # Check if the folder exists
    folder = os.path.dirname(duplicates_file)
    if not os.path.exists(folder):
        raise FileNotFoundError(f"The folder for duplicates_file does not exist: {folder}")

    exists = {}
    clean_records = []
    duplicate_records = []
    missing_id_records = []

    # Open duplicates CSV for writing
    duplicates_written = False
    with open(duplicates_file, "w", newline="", encoding="utf-8") as f:
        writer = None

        for row in records:
            review_id = row.get(id_column)  # ✅ row must be dict

            # Missing IDs
            if not review_id or review_id.strip() == "":
                missing_id_records.append(row)
                continue

            # Duplicate IDs
            if review_id in exists:
                duplicate_records.append(row)

                # Initialize CSV writer dynamically with headers
                if not writer:
                    writer = csv.DictWriter(f, fieldnames=row.keys())
                    writer.writeheader()
                writer.writerow(row)
                duplicates_written = True
                continue

            # First occurrence → keep
            exists[review_id] = True
            clean_records.append(row)

    if duplicates_written:
        print(f"Found {len(duplicate_records)} duplicates → saved to {duplicates_file}")
    else:
        print("No duplicates found.")

    return clean_records, missing_id_records

# ------------------------------
# Main workflow
# ------------------------------
def extract_data(file_path, duplicates_file, id_column = "reviewID"):
    validate_csv(file_path)
    columns = read_csv_header(file_path)
    records, invalid_rows = extract_rows(file_path)
    records, missing_id_records = find_duplicates(records=records, id_column="reviewID", duplicates_file=duplicates_storage)
    
    return {
        "columns": columns,
        "clean_records": records,
        "missing_id_records": missing_id_records,
        "invalid_rows": invalid_rows
    }


# # Ensure checkpoints folder exists
# os.makedirs(os.path.dirname(duplicates_storage), exist_ok=True)

# # Step 1: Validate CSV
# validate_csv(file_path)
# print("Validation passed")

# # Step 2: Read CSV header
# columns = read_csv_header(file_path=file_path)
# print("Columns:", columns)

# # Step 3: Extract rows
# records, invalid_rows = extract_rows(file_path)
# print("Total extracted rows:", len(records))
# print("Invalid rows skipped:", invalid_rows)

# # Step 4: Find duplicates and save them
# clean_records, missing_ids = find_duplicates(records=records, duplicates_file=duplicates_storage)
# print("Clean records:", len(clean_records))
# print("Missing ID records:", len(missing_ids))
