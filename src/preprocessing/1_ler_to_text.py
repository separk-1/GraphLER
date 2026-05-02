import os
import pdfplumber
import fitz  # PyMuPDF
from tqdm import tqdm

RAW_LER_DIR = "../../data/10y/raw/ler"
OUTPUT_TEXT_DIR = "../../data/10y/processed/ler_text"

os.makedirs(OUTPUT_TEXT_DIR, exist_ok=True)

def extract_with_pdfplumber(pdf_path):
    # Extract text using pdfplumber (primary method)
    with pdfplumber.open(pdf_path) as pdf:
        texts = [(p.extract_text() or "") for p in pdf.pages]
    return "\n".join(texts)

def extract_with_pymupdf(pdf_path):
    # Extract text using PyMuPDF (fallback method)
    doc = fitz.open(pdf_path)
    texts = [p.get_text() for p in doc]
    return "\n".join(texts)

def process_all_pdfs(raw_dir, output_dir):
    pdf_files = [f for f in os.listdir(raw_dir) if f.lower().endswith(".pdf")]

    for pdf_file in tqdm(pdf_files, desc="Processing LER PDFs", unit="file"):
        pdf_path = os.path.join(raw_dir, pdf_file)
        txt_path = os.path.join(output_dir, pdf_file.replace(".pdf", ".txt"))

        extracted_text = ""

        try:
            extracted_text = extract_with_pymupdf(pdf_path)
        except Exception as e:
            print(f"[PyMuPDF FAIL] {pdf_file}: {e}")

        if not extracted_text.strip() or "(cid:" in extracted_text:
            try:
                extracted_text = extract_with_pdfplumber(pdf_path)
                print(f"[pdfplumber fallback] {pdf_file}")
            except Exception as e:
                print(f"[pdfplumber FAIL] {pdf_file}: {e}")
                extracted_text = "Error extracting text."

        # Save extracted text to file
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(extracted_text)

# Execute processing
process_all_pdfs(RAW_LER_DIR, OUTPUT_TEXT_DIR)