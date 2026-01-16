# src/ingest.py

from pathlib import Path
import json
import uuid

RAW_DIR = Path("data/raw")
OUT_FILE = Path("data/processed/chunks.jsonl")


def load_text_file(path: Path) -> str:
    """Load a plain text file."""
    return path.read_text(encoding="utf-8")


def simple_chunk(text: str, chunk_size: int = 500, overlap: int = 100):
    """Split text into overlapping chunks."""
    words = text.split()
    chunks = []

    start = 0
    while start < len(words):
        end = start + chunk_size
        chunk_words = words[start:end]
        chunks.append(" ".join(chunk_words))
        start = end - overlap

    return chunks


def ingest_text_file(
    path: Path,
    source: str,
    title: str,
    organization: str,
    year: int,
    language: str = "en",
):
    text = load_text_file(path)
    chunks = simple_chunk(text)

    documents = []
    for chunk in chunks:
        documents.append(
            {
                "id": str(uuid.uuid4()),
                "content": chunk,
                "source": source,
                "modality": "text",
                "metadata": {
                    "title": title,
                    "organization": organization,
                    "year": year,
                    "language": language,
                },
            }
        )

    return documents


def main():
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    all_docs = []

    for txt_file in RAW_DIR.rglob("*.txt"):
        docs = ingest_text_file(
            path=txt_file,
            source=str(txt_file),
            title=txt_file.stem,
            organization="WHO",
            year=2024,
        )
        all_docs.extend(docs)

    with OUT_FILE.open("w", encoding="utf-8") as f:
        for doc in all_docs:
            f.write(json.dumps(doc) + "\n")

    print(f"Saved {len(all_docs)} document chunks to {OUT_FILE}")


if __name__ == "__main__":
    main()
