import os
import re
import pandas as pd
from tqdm import tqdm
import csv
import json
from rapidfuzz import fuzz

LER_TEXT_DIR = "../../data/10y/processed/ler_text"
OUTPUT_CSV_PATH = "../../data/10y/processed/ler_structured.csv"


with open("../../data/facility_names.json", "r", encoding="utf-8") as f:
    PLANTS = json.load(f)["facility_names"]

def clean_excel_text(text):
    if not isinstance(text, str):
        return text
    return re.sub(r"[\x00-\x1F\x7F]", "", text)

def match_plant(text):
    best = ("Not Found", 0)

    for plant in PLANTS:
        score = fuzz.partial_ratio(plant.lower(), text.lower())
        if score > best[1]:
            best = (plant, score)

    return best[0] if best[1] > 80 else "Not Found"

def normalize(s):
    return re.sub(r"\s+", " ", s).strip().lower()

def find_line(keyword, lines):
    keyword_norm = normalize(keyword)
    for i, l in enumerate(lines):
        if keyword_norm in normalize(l):
            return i
    return None

def extract_multi_line_section(lines, start_keyword, stop_keywords):
    stop_keywords = sorted(stop_keywords, key=len, reverse=True)

    # Find start index (supports list or single keyword)
    if isinstance(start_keyword, list):
        start_idx = None
        for key in start_keyword:
            start_idx = find_line(key, lines)
            if start_idx is not None:
                break
    else:
        start_idx = find_line(start_keyword, lines)

    if start_idx is None:
        return "Not Found"

    # Check if the target content is on the same line as the start keyword
    start_line = lines[start_idx]
    keywords = start_keyword if isinstance(start_keyword, list) else [start_keyword]

    for key in keywords:
        pattern = re.compile(re.escape(key), re.IGNORECASE)
        match = pattern.search(start_line)
        if match:
            same_line_content = start_line[match.end():].strip()
            if same_line_content:
                return same_line_content

    start_idx += 1

    extracted = []
    for line in lines[start_idx:]:
        if any(stop.lower() in line.lower() for stop in stop_keywords):
            break
        extracted.append(line)

    return " ".join(extracted).strip() if extracted else "Not Found"


def extract_abstract(lines):
    abs_idx = None

    # 1. First priority: exact "16. Abstract"
    abs_idx = find_line("16. Abstract", lines)

    # 2. If not found, fallback
    if abs_idx is None:
        candidates = []

        for i, l in enumerate(lines):
            l_low = l.lower()

            if "abstract" in l_low:

                # exclude checkbox
                if "specify in abstract below" in l_low:
                    continue

                candidates.append(i)

        # choose first valid candidate
        if len(candidates) == 1:
            abs_idx = candidates[0]

        elif len(candidates) > 1:
            # prefer line that starts with ABSTRACT
            for idx in candidates:
                if lines[idx].strip().lower().startswith("abstract"):
                    abs_idx = idx
                    break

            # if still none, just take first
            if abs_idx is None:
                abs_idx = candidates[0]

    if abs_idx is None:
        return "Not Found"

    # extract content
    extracted = []
    for l in lines[abs_idx+1:]:

        if "NARRATIVE" in l.upper():
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
        lines = [re.sub(r"\s+", " ", line.replace("(cid:9)", " ")).strip() for line in f]

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
        # === Facility fallback using dictionary ===
        if facility_name == "Not Found":
            text_top = " ".join(lines[:50])
            matched = match_plant(text_top)
            if matched != "Not Found":
                facility_name = matched

    # === Title ===
    title = extract_multi_line_section(
    lines, 
    ["4. Title", "4.Title", "4 TITLE", "4. TITLE", "4. nTLE"], 
    ["5 Event Date", "5. Event Date", ". Event Date", "Event Date"]
    )

    # === Abstract ===
    abstract = extract_abstract(lines)

    # === CFR ===
    cfr = extract_cfr(lines)

    # === Narrative ===
    narrative = extract_narrative(lines)

    # === Event Date ===
    date_patterns = [
    r"\b(\d{1,2})\s+(\d{1,2})\s+(\d{4})\b",          # 01 20 2016
    r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b",         # 1/20/2016
    r"\b([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})\b"   # January 20, 2016
    ]

    month_map = {
        "january": "01", "february": "02", "march": "03",
        "april": "04", "may": "05", "june": "06",
        "july": "07", "august": "08", "september": "09",
        "october": "10", "november": "11", "december": "12"
    }

    for l in lines:
        for pattern in date_patterns:
            m = re.search(pattern, l)
            if m:
                try:
                    if m.group(1).isalpha():
                        mm = month_map[m.group(1).lower()]
                        dd = m.group(2).zfill(2)
                        yyyy = m.group(3)
                    else:
                        mm = m.group(1).zfill(2)
                        dd = m.group(2).zfill(2)
                        yyyy = m.group(3)

                    event_date = f"{mm}-{dd}-{yyyy}"
                    break
                except:
                    continue
        if event_date != "Not Found":
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
    # not_found_mask = df.eq("Not Found").any(axis=1)
    # df = df[~not_found_mask]

    total_rows = len(df)

    not_found_counts = (df == "Not Found").sum()
    not_found_ratio = ((df == "Not Found").mean() * 100).round(2)

    # Create row-level missing count first
    df["NotFound_Count"] = (df == "Not Found").sum(axis=1)

    # Compute ratio for rows with exactly 5 missing fields
    count_5 = (df["NotFound_Count"] == 5).sum()
    ratio_5 = round(count_5 / total_rows * 100, 2)

    print("\n=== Row-level Missing (NotFound_Count == 5) ===")
    print(f"Count: {count_5} / {total_rows} ({ratio_5}%)")

    print("\n=== Not Found Statistics ===")
    for col in df.columns:
        # Skip the helper column
        if col == "NotFound_Count":
            continue
        count = not_found_counts[col]
        ratio = not_found_ratio[col]
        print(f"{col}: {count} / {total_rows} ({ratio}%)")

    print("\n=== Worst Columns ===")
    print(not_found_counts.sort_values(ascending=False))

    # Tag bad rows (exactly 5 missing fields)
    df["Quality"] = df["NotFound_Count"].apply(
    lambda x: "good" if x == 0 else ("bad" if x >= 5 else "ok")
    )
    # Split datasets
    clean_df = df[df["Quality"] == "ok"]
    bad_df = df[df["Quality"] == "bad"]

    print("\n=== Quality Summary ===")
    print(df["Quality"].value_counts())

    # Clean illegal characters
    df = df.applymap(clean_excel_text)

    # Save full dataset
    df.to_excel(output_csv_path.replace(".csv", ".xlsx"), index=False)

    # Save only GOOD rows
    good_df = df[df["Quality"] == "good"].drop(columns=["NotFound_Count", "Quality"])
    good_df.to_excel(output_csv_path.replace(".csv", "_good.xlsx"), index=False)

# Execute
process_all_txt(LER_TEXT_DIR, OUTPUT_CSV_PATH)
