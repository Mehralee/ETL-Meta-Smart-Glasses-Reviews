from extract import extract_data, read_csv_header, extract_rows
import re

file_path = r"data/source/meta-glasses-reviews.csv"  # raw string
duplicates_storage = r"checkpoints/duplicates.csv"   # raw string

columns = read_csv_header(file_path=file_path)

def rename_columns(records, columns):
    renamed_columns = []

    def to_snake_case(name):
        # Add underscore before capital letters, then lowercase everything
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower().replace(" ", "_")
    
    mapping = {col: to_snake_case(col.strip()) for col in columns}

    for row in records:
        new_row = {}
        for old_col, new_col in mapping.items():
            new_row[new_col] = row.get(old_col)
        renamed_columns.append(new_row)
    
    return renamed_columns


def convert_columns(records):
    for row in records:
        # Rating column
        try:
            row['rating'] = float(row['rating'])
        except:
            row['rating'] = None
        # Helpful columns

        try:
            row['helpful'] = int(row['helpful'])
        except:
            row['helpful'] = None

        try:
            row['helpfulness_score'] = round(float(row['helpfulness_score']),2)
        except:
            row['helpfulness_score'] = None
        
        return records
    
def clean_review(records):
    for row in records:
        row['review'] = row['review'].replace("<br>", "")
    return records

def eager_to_recommend(records):
    
    for row in records:
        is_positive = row.get('is_positive_review', None)
        try:
            is_positive = int(is_positive) if is_positive is not None else None
        except (TypeError, ValueError):
            is_positive = None



        if is_positive == 1:
            row['is_eager'] = True
        elif is_positive == 0:
            row['is_eager'] = False
        else:
            row['is_eager'] = None
    
    return records
            


records, invalid_rows = extract_rows(file_path=file_path)

renamed_columns = rename_columns(records=records, columns=columns)
sample_records = convert_columns(records=records)
sample_records = clean_review(records=records)
sample_records = eager_to_recommend(records=records)
print("Sample records: ", renamed_columns[0])
print("Second sample: ", sample_records[0])