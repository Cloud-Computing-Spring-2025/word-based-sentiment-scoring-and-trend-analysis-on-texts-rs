import pandas as pd

# Load Metadata
metadata = pd.read_csv("gutenberg_metadata.csv")
metadata["book_id"] = metadata["book_id"].astype(str)

# Load Cleaned Text
cleaned_text = pd.read_csv("cleaned_books.tsv", sep="\t", names=["book_id", "cleaned_text"])
cleaned_text["book_id"] = cleaned_text["book_id"].astype(str)

# Debug: Check how many book IDs match
print("📌 Total books in metadata:", len(metadata["book_id"].unique()))
print("📌 Total books in cleaned text:", len(cleaned_text["book_id"].unique()))
print("📌 Matching books:", len(set(metadata["book_id"]).intersection(set(cleaned_text["book_id"]))))

# Merge using 'outer' to retain all books
merged = metadata.merge(cleaned_text, on="book_id", how="outer")

# Drop books that don’t have processed text (optional)
merged = merged.dropna(subset=["cleaned_text"])

# Keep only required columns
final_output = merged[["book_id", "title", "year"]]

# Save final cleaned dataset
final_output.to_csv("final_cleaned_books.csv", index=False)

print("✅ Final cleaned dataset saved! Books in final dataset:", len(final_output))

