import os
import json
import pandas as pd
import openai
from tqdm import tqdm
from dotenv import load_dotenv
import time

# Load OpenAI API key
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# File paths
MERGED_CSV_PATH = "/../../../../../data/processed/ler_structured_with_cfr.csv"
OUTPUT_JSONL_PATH = "/../../../../../data/processed/ler_kg.jsonl"

# GPT-based attribute extraction
def extract_attributes(text):
    prompt = f""" 
    You are an expert in extracting structured and generalized information from incident reports. 

    Your task is to extract the following entities from the provided text, summarizing each with a **single generalized keyword** inside a JSON array. 

    Entity Definitions:
    - "Detection": How the incident was discovered (e.g., "alarm", "inspection", "operator-observation") 
    - "Equipment": Primary equipment involved in the incident (e.g., "pump", "valve", "sensor") 
    - "Failure_Type": Type of equipment failure (e.g., "mechanical", "electrical", "software") 
    - "Corrective_Actions": Actions taken to address the incident (e.g., "repair", "replacement", "procedure-change") 
    - "Cause_Category": Primary cause classification (e.g., "equipment", "human", "administrative", "external") 
    - "Causes": Specific root causes identified (e.g., "wear", "miscommunication", "design-flaw") 
    - "Impacts": Consequences of the incident (e.g., "shutdown", "delay", "safety-concern") 

    Incident Description: 
    "{text}" 

    Respond strictly in **valid JSON format** as below: 
    {{
    "Detection": ["<one keyword>"], 
    "Equipment": [] // or["<one keyword>"], 
    "Failure_Type": ["<one keyword>"], 
    "Corrective_Actions": [] // or ["<one keyword>"], 
    "Cause_Category": ["<one keyword>"], 
    "Causes": [] // or ["<one keyword>"], 
    "Impacts": [] // or ["<one keyword>"], 
    }} 
    """

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",  # 
            messages=[
                {"role": "system", "content": "You are an expert in extracting structured and generalized information from complex texts for efficient pattern detection and risk analysis."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=150
        )
        content = response["choices"][0]["message"]["content"].strip()
        if content.startswith("```json"):
            content = content[7:]
        if content.endswith("```"):
            content = content[:-3]

        return json.loads(content)
    except json.JSONDecodeError as e:
        print(f"[JSON Decode Error] {e}")
        print("GPT Response:", content)
        return None
    except Exception as e:
        print(f"[GPT Error] {e}")
        return None

# Main execution
def main():
    df = pd.read_csv(MERGED_CSV_PATH, encoding="utf-8")

    TEMP_OUTPUT_PATH = OUTPUT_JSONL_PATH + ".tmp"

    with open(TEMP_OUTPUT_PATH, "w", encoding="utf-8") as out_file:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Extracting attributes"):
            combined_text = " ".join(str(row.get(col, "")) for col in ["Title", "Abstract", "Narrative"])
            attributes = extract_attributes(combined_text)
            time.sleep(1)  # Rate limiting

            if attributes:
                node = {
                    "filename": row.get("File Name", ""),
                    "attributes": {
                        "Detection": attributes.get("Detection", "Unknown"),
                        "Equipment": attributes.get("Equipment", "Unknown"),
                        "Failure_Type": attributes.get("Failure_Type", "Unknown"),
                        "Corrective_Actions": attributes.get("Corrective_Actions", "None"),
                        "Cause_Category": attributes.get("Cause_Category", "Unknown"),
                        "Causes": attributes.get("Causes", "Unknown"),
                        "Impacts": attributes.get("Impacts", "Unknown"),
                    },
                    "metadata": {
                        "facility": {
                            "name": row.get("Facility Name", "Unknown Facility"),
                            "unit": row.get("Unit", "Unknown Unit")
                        },
                        "event_date": row.get("Event Date", ""),
                        "title": row.get("Title", ""),
                        "clause": row.get("CFR", "None")
                    }
                }
                out_file.write(json.dumps(node, ensure_ascii=False) + "\n")

    os.replace(TEMP_OUTPUT_PATH, OUTPUT_JSONL_PATH)
    print(f"\nKnowledge graph saved to {OUTPUT_JSONL_PATH}")

if __name__ == "__main__":
    main()
