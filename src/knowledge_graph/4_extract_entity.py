import os
import json
import pandas as pd
import openai
from tqdm import tqdm
from dotenv import load_dotenv
import time

# Load API key
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# File paths
MERGED_CSV_PATH = "../../data/10y/processed/ler_structured_good.xlsx"
OUTPUT_JSONL_PATH = "../../data/10y/processed/ler_kg.jsonl"

MAX_ROWS = None


# ---------------------------
# Robust JSON parsing
# ---------------------------
def safe_json_load(content):
    try:
        return json.loads(content)
    except:
        content = content.strip()
        if "}" in content:
            content = content[:content.rfind("}") + 1]
        try:
            return json.loads(content)
        except:
            return None


# ---------------------------
# Keep only one value per field
# ---------------------------
def enforce_single(value):
    if isinstance(value, list):
        return value[:1]
    return []


# ---------------------------
# GPT-based extraction (UPDATED)
# ---------------------------
def extract_attributes(text):
    prompt = f"""
You are an expert in extracting structured information from nuclear incident reports (LER).

Extract ONLY failure analysis and explicit operator actions.

Rules:
- Extract exact phrases from the text (no paraphrasing)
- If unclear → return []
- Do NOT hallucinate
- Prefer missing over incorrect
- Return ONLY ONE item per field
- Keep each extracted phrase concise (prefer short phrases)

- Outcome must represent final system impact; if not explicitly stated, use clear system state changes (e.g., loss, breach, restoration)
- Do NOT include automatic system responses (e.g., "auto start", "valve actuation")

- Human_Action must be direct operator intervention (physical or control action)
- Exclude approvals, reporting, or administrative actions

- CorrectiveAction must be a repair, replacement, or modification action
- Do NOT include status declarations (e.g., "declared operable")
Definitions:

- Power_State: reactor condition (e.g., "100 percent power", "cold shutdown")
- Component: primary equipment directly associated with the failure
- Procedure: technical_specification or operating_procedure
- Human_Action: explicit operator action ONLY (e.g., "declared inoperable", "started system", "replaced component")
- Outcome: final system-level impact (e.g., safety system failure, shutdown)
- Cause: root cause phrase (e.g., "solenoid failure")
- CorrectiveAction: follow-up repair or mitigation action

STRICT RULES:
- Do NOT infer intent or decision
- Do NOT create actions not explicitly written
- Outcome must NOT repeat state like "inoperable"

Additionally extract generalized categories:

- Component_Type:
  Generalized equipment category (1–2 words, lowercase, snake_case)
  Examples:
    "Auxiliary Feedwater Pump" → pump
    "Main Steam Relief Valves" → relief_valve
    "Reactor Protection System" → protection_system

- Cause_Type:
  Generalized cause category (1–2 words, lowercase, snake_case)
  Examples:
    "corrosion bonding" → corrosion
    "through-wall leak" → leakage
    "operator failed to follow procedure" → human_error
    "setpoint drift" → calibration_error

Rules for *_Type:
- Must be abstract (NOT raw text)
- Must be consistent across similar cases
- If unclear → return []
- If Component is extracted, Component_Type should also be provided if possible
- If Cause is extracted, Cause_Type should also be provided if possible

Incident:
"{text}"

Return JSON:
{{
  "Power_State": [],
  "Component": [],
  "Component_Type": [],
  "Procedure": [],
  "Human_Action": [],
  "Outcome": [],
  "Cause": [],
  "Cause_Type": [],
  "CorrectiveAction": []
}}
"""

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Extract structured failure and action information."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=220
        )

        content = response["choices"][0]["message"]["content"].strip()

        if "```" in content:
            content = content.split("```")[1].strip()

        if not content.startswith("{"):
            print("\n[INVALID RESPONSE]")
            print(content)
            return None

        data = safe_json_load(content)

        if data is None:
            print("\n[JSON PARSE FAIL]")
            print(content)
            return None

        return data

    except Exception as e:
        print(f"[API ERROR] {e}")
        return None


# ---------------------------
# Normalize Procedure
# ---------------------------
def clean_procedure(v):
    if not v:
        return []

    s = v[0].lower()

    if "technical" in s:
        return ["technical_specification"]

    return ["operating_procedure"]


# ---------------------------
# Clean Outcome
# ---------------------------
def clean_outcome(v):
    if not v:
        return []

    s = v[0].lower()

    if "inoperable" in s:
        return []

    return v


# ---------------------------
# Main execution
# ---------------------------
def main():
    df = pd.read_excel(MERGED_CSV_PATH)

    if MAX_ROWS is not None:
        df = df.head(MAX_ROWS)

    TEMP_OUTPUT_PATH = OUTPUT_JSONL_PATH + ".tmp"

    with open(TEMP_OUTPUT_PATH, "w", encoding="utf-8") as out_file:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Extracting"):

            combined_text = " ".join([
                str(row.get("Title", "")),
                str(row.get("Narrative", ""))
            ])[:3000]

            attributes = extract_attributes(combined_text)

            time.sleep(0.3)

            
            if attributes:
                node = {
                    "filename": row.get("File Name", ""),
                    "attributes": {
                        "Power_State": enforce_single(attributes.get("Power_State", [])),
                        "Component": enforce_single(attributes.get("Component", [])),
                        "Component_Type": enforce_single(attributes.get("Component_Type", [])),
                        "Procedure": clean_procedure(enforce_single(attributes.get("Procedure", []))),
                        "Human_Action": enforce_single(attributes.get("Human_Action", [])),
                        "Outcome": clean_outcome(enforce_single(attributes.get("Outcome", []))),
                        "Cause": enforce_single(attributes.get("Cause", [])),
                        "Cause_Type": enforce_single(attributes.get("Cause_Type", [])),
                        "CorrectiveAction": enforce_single(attributes.get("CorrectiveAction", [])),
                    },
                    "metadata": {
                        "facility": {
                            "name": row.get("Facility Name", "Unknown"),
                            "unit": row.get("Unit", "Unknown")
                        },
                        "event_date": row.get("Event Date", ""),
                        "title": row.get("Title", "")
                    }
                }

                out_file.write(json.dumps(node, ensure_ascii=False) + "\n")

    os.replace(TEMP_OUTPUT_PATH, OUTPUT_JSONL_PATH)
    print(f"\nSaved to {OUTPUT_JSONL_PATH}")


if __name__ == "__main__":
    main()