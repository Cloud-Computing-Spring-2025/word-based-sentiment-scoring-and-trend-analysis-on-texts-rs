import os
import re

# Directory containing downloaded books
books_dir = "gutenberg_books"

# Stop words list (basic example)
stop_words = set(["the", "and", "of", "to", "in", "a", "is", "that", "was", "he", "for"])

# Process books
cleaned_data = []

for filename in os.listdir(books_dir):
    if filename.endswith(".txt"):
        with open(os.path.join(books_dir, filename), "r", encoding="utf-8") as f:
            text = f.read()
        
        # Extract book ID from filename
        book_id = filename.split("_")[1].replace(".txt", "")

        # Clean text: lowercase, remove punctuation, remove stopwords
        text = text.lower()
        text = re.sub(r"[^a-z0-9\s]", "", text)  # Remove punctuation
        words = text.split()
        words = [word for word in words if word not in stop_words]

        cleaned_data.append((book_id, " ".join(words)))

# Save processed data
with open("cleaned_books.tsv", "w", encoding="utf-8") as f:
    for book_id, text in cleaned_data:
        f.write(f"{book_id}\t{text}\n")

print("Processed books saved to cleaned_books.tsv")
