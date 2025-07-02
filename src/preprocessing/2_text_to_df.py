import os
import re
import pandas as pd
from tqdm import tqdm

LER_TEXT_DIR = "/../../../../../data/processed/ler_text"
OUTPUT_CSV_PATH = "/../../../../../data/processed/ler_structured.csv"

def find_line(keyword, lines):
    # Find the index of the line containing the specified keyword
    for i, l in enumerate(lines):
        if keyword.lower() in l.lower():
            return i
    return None

def extract_multi_line_section(lines, start_keyword, stop_keywords):
    stop_keywords = sorted(stop_keywords, key=len, reverse=True)
    # Extract section between start_keyword and any of the stop_keywords
    start_idx = find_line(start_keyword, lines)
    if start_idx is None:
        return "Not Found"
    start_idx += 1

    extracted = []
    for line in lines[start_idx:]:
        if any(stop.lower() in line.lower() for stop in stop_keywords):
            break
        extracted.append(line)

    return " ".join(extracted).strip() if extracted else "Not Found"


def extract_abstract(lines):
    # Extract the abstract section of the text
    abs_idx = find_line("16. Abstract", lines)
    if abs_idx is None:
        return "Not Found"
    extracted = []
    for l in lines[abs_idx+1:]:
        if "NRC FORM" in l:
            break
        extracted.append(l)
    return " ".join(extracted).strip() if extracted else "Not Found"

def extract_cfr(lines):
    # Extract the CFR information from the text
    cfr_start = find_line("11. This Report is Submitted Pursuant", lines)
    if cfr_start is None:
        return "Not Found"
    cfr_pattern = re.compile(r"/\s*([0-9]+\.[0-9]+\(a\)\(\d+\)\(iv\)\([A-Za-z]+\))")
    for i in range(cfr_start, len(lines)):
        cm = cfr_pattern.search(lines[i])
        if cm:
            return cm.group(1)
    return "Not Found"

def extract_narrative(lines):
    # Extract the narrative section of the text
    nar_idx = find_line("NARRATIVE", lines)
    if nar_idx is None:
        return "Not Found"
    extracted = []
    for l in lines[nar_idx+1:]:
        if "NRC FORM 366A" in l:
            break
        extracted.append(l)
    return " ".join(extracted).strip() if extracted else "Not Found"

def process_txt_file(txt_path):
    # Process a single text file and extract relevant information
    with open(txt_path, "r", encoding="utf-8") as f:
        lines = [line.replace("(cid:9)", " ").strip() for line in f]

    facility_name = "Not Found"
    title = "Not Found"
    event_date = "Not Found"
    abstract = "Not Found"
    narrative = "Not Found"

    file_name = os.path.splitext(os.path.basename(txt_path))[0]

    # === Facility Name & Unit Extraction ===
    idx_fname = find_line("1. Facility Name", lines)
    unit = "Unknown Unit"

    if idx_fname is not None:
        # Get all lines from "1. Facility Name" to "4. Title"
        idx_title = find_line("4. Title", lines)
        end_idx = idx_title if idx_title is not None else idx_fname + 5
        search_scope = lines[idx_fname:end_idx]

        # Normalize "Unit No. X" to "Unit X"
        search_scope = [re.sub(r"Unit\s*No\.\s*(\d+)", r"Unit \1", line) for line in search_scope]

        # Find the first valid line containing alphabet characters
        selected_line = next((l for l in search_scope if re.search(r'[a-zA-Z]', l) and not re.match(r'^\d', l)), "")

        # Extract Unit if present
        unit_match = re.search(r"(Unit\s*\d+)", selected_line)
        unit = unit_match.group(1) if unit_match else "Unknown Unit"

        # Split at ", Unit" or " Unit" and keep the facility name
        split_match = re.split(r",\s*Unit|\s+Unit", selected_line)
        facility_name = split_match[0].strip() if split_match else "Not Found"

        # Remove trailing '-' or '.' to ensure alphabet ending
        facility_name = re.sub(r'[\-\.]+$', '', facility_name).strip()

        # Final validation
        if not re.search(r'[a-zA-Z]', facility_name) or not re.match(r'^[\w\s.,&()\-]+$', facility_name):
            facility_name = "Not Found"
            unit = "Unknown Unit"

    # === Title ===
    title = extract_multi_line_section(
        lines, 
        "4. Title", 
        ["5 Event Date", "5. Event Date", ". Event Date", "Event Date"]
    )

    # === Abstract ===
    abstract = extract_abstract(lines)

    # === CFR ===
    cfr = extract_cfr(lines)

    # === Narrative ===
    narrative = extract_narrative(lines)

    # === Event Date ===
    date_ler_pattern = re.compile(r"\b(\d{2})\s+(\d{2})\s+(\d{4})\s+(\d{4})(?:\s*-?\s*)(\d{3})(?:\s*-?\s*)(\d{2})\b")
    for l in lines:
        m = date_ler_pattern.search(l)
        if m:
            mm, dd, yyyy = m.group(1), m.group(2), m.group(3)
            event_date = f"{mm}-{dd}-{yyyy}"
            break

    return {
        "Facility Name": facility_name,
        "Unit": unit,
        "Title": title,
        "Event Date": event_date,
        "Abstract": abstract,
        # "CFR": cfr,
        "Narrative": narrative,
        "File Name": file_name
    }


def process_all_txt(txt_dir, output_csv_path):
    # Process all text files in the directory and save results to a CSV file
    txt_files = [f for f in os.listdir(txt_dir) if f.lower().endswith(".txt")]
    extracted_data = []

    for txt_file in tqdm(txt_files, desc="Processing TXT files", unit="file"):
        txt_path = os.path.join(txt_dir, txt_file)
        fields = process_txt_file(txt_path)
        extracted_data.append(fields)

    df = pd.DataFrame(extracted_data)

    # Remove "Not Found" rows
    not_found_mask = df.eq("Not Found").any(axis=1)
    df = df[~not_found_mask]
    df.to_csv(output_csv_path, index=False, encoding="utf-8")

# Execute
process_all_txt(LER_TEXT_DIR, OUTPUT_CSV_PATH)
