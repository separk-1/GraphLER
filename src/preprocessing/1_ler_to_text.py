import os
import re
import pdfplumber
from tqdm import tqdm

RAW_LER_DIR = "/../../../../../data/raw/ler"
OUTPUT_TEXT_DIR = "/../../../../../data/processed/ler_text"

os.makedirs(OUTPUT_TEXT_DIR, exist_ok=True)

def process_all_pdfs(raw_dir, output_dir):
    pdf_files = [f for f in os.listdir(raw_dir) if f.lower().endswith(".pdf")]

    for pdf_file in tqdm(pdf_files, desc="Processing LER PDFs", unit="file"):
        pdf_path = os.path.join(raw_dir, pdf_file)
        try:
            with pdfplumber.open(pdf_path) as pdf:
                start_page = None
                # Check each page's text for the presence of "LICENSEE EVENT REPORT (LER)"
                for idx, page in enumerate(pdf.pages):
                    text = page.extract_text() or ""
                    # Check if the text contains "LICENSEE EVENT REPORT (LER)"
                    if "LICENSEE EVENT REPORT (LER)" in text.upper():
                        start_page = idx
                        break

                if start_page is None:
                    # If no LER page is found in the PDF, handle it with empty text or a "Not Found" message
                    extracted_text = "LICENSEE EVENT REPORT (LER) not found."
                else:
                    # Extract text from start_page to the last page and concatenate it
                    extracted_texts = []
                    for p_idx in range(start_page, len(pdf.pages)):
                        p_text = pdf.pages[p_idx].extract_text() or ""
                        extracted_texts.append(p_text)
                    extracted_text = "\n".join(extracted_texts)

                # Save the extracted text as a .txt file
                txt_filename = os.path.splitext(pdf_file)[0] + ".txt"
                txt_path = os.path.join(output_dir, txt_filename)
                with open(txt_path, "w", encoding="utf-8") as f:
                    f.write(extracted_text)

        except Exception as e:
            # If an error occurs during PDF processing, log the error and create an empty file
            print(f"Error processing {pdf_file}: {e}")
            txt_filename = os.path.splitext(pdf_file)[0] + ".txt"
            txt_path = os.path.join(output_dir, txt_filename)
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write("Error extracting text.")

# Execute
process_all_pdfs(RAW_LER_DIR, OUTPUT_TEXT_DIR)