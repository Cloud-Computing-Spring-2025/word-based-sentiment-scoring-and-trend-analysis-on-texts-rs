import pandas as pd

# Load metadata and cleaned text
metadata = pd.read_csv('gutenberg_metadata.csv')  # Contains book_id, title, author, year, download_link
cleaned = pd.read_csv('cleaned_books.tsv', sep="\t", names=["book_id", "cleaned_text"])

# Clean book_id to ensure consistent types
metadata["book_id"] = metadata["book_id"].astype(str)
cleaned["book_id"] = cleaned["book_id"].astype(str)

# Merge on book_id
merged = pd.merge(metadata, cleaned, on="book_id", how="inner")

# Keep only required columns
final_df = merged[["book_id", "title", "year", "cleaned_text"]]

# Save final cleaned CSV
final_df.to_csv("final_cleaned_books.csv", index=False)

print("✅ Final cleaned dataset saved as 'final_cleaned_books.csv'")
print(f"✅ Total books in final dataset: {len(final_df)}")


