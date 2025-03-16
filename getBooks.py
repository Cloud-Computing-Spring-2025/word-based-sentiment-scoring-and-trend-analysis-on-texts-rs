import requests
import pandas as pd
import os

# Define output directory
books_dir = "gutenberg_books"
os.makedirs(books_dir, exist_ok=True)

# Query books from Gutendex API
url = "https://gutendex.com/books/?languages=en&author_year_start=1700&author_year_end=1899"
response = requests.get(url)
data = response.json()

# Extract metadata
books = []
for book in data["results"]:
    book_id = book["id"]
    title = book["title"]
    author = book["authors"][0]["name"] if book["authors"] else "Unknown"
    year = book["authors"][0]["birth_year"] if book["authors"] else "Unknown"
    
    # Try to find a valid text file format
    formats = book["formats"]
    download_link = (
        formats.get("text/plain") or
        formats.get("text/plain; charset=us-ascii") or
        formats.get("text/plain; charset=utf-8") or
        formats.get("text/html") or
        "N/A"
    )

    books.append({
        "book_id": book_id,
        "title": title,
        "author": author,
        "year": year,
        "download_link": download_link
    })

    # Download the book
    if download_link != "N/A" and download_link.startswith("http"):
        book_filename = os.path.join(books_dir, f"book_{book_id}.txt")
        try:
            book_response = requests.get(download_link)
            if book_response.status_code == 200:
                with open(book_filename, "w", encoding="utf-8") as f:
                    f.write(book_response.text)
                print(f"✅ Downloaded: {title} ({book_id})")
            else:
                print(f"❌ Failed to download {title} ({book_id}) - HTTP {book_response.status_code}")
        except Exception as e:
            print(f"⚠️ Error downloading {title} ({book_id}): {e}")

# Save metadata
df = pd.DataFrame(books)
df.to_csv("gutenberg_metadata.csv", index=False)
print("📂 Metadata saved to gutenberg_metadata.csv!")
