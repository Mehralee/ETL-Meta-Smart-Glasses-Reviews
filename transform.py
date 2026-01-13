from extract import extract_data

file_path = r"data/source/meta-glasses-reviews.csv"  # raw string
duplicates_storage = r"checkpoints/duplicates.csv"   # raw string

extracted_data = extract_data(file_path=file_path, duplicates_file=duplicates_storage)