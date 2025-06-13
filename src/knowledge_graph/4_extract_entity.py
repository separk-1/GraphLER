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
MERGED_CSV_PATH = "../../data/processed/ler_structured_with_cfr.csv"
OUTPUT_JSONL_PATH = "../../data/processed/ler_kg.jsonl"

# GPT-based attribute extraction
def extract_attributes(text):
    prompt = f"""
    You are an expert in extracting structured and generalized information from incident reports.

    Your task is to extract the following six attributes from the provided text, summarizing each with a **single generalized keyword** inside a JSON array.

    Definitions:
    - "Task": The general type of activity that was being performed when the incident occurred.
    - "Event": What happened during the incident — summarize as a broad event type.
    - "Cause": The primary reason the incident happened — summarize as a single cause.
    - "Influence": The main consequence or impact caused by the event.
    - "Corrective Actions": The general action taken in response to the incident.
    - "Similar Events": Other incidents of a similar nature, if known; otherwise, return an empty list.

    Incident Description:
    "{text}"

    Respond strictly in **valid JSON format** as below:
    {{
    "Task": ["<one keyword>"],
    "Event": ["<one keyword>"],
    "Cause": ["<one keyword>"],
    "Influence": ["<one keyword>"],
    "Corrective Actions": ["<one keyword>"],
    "Similar Events": []  // or ["<one keyword>"]
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
                        "Task": attributes.get("Task", "Unknown"),
                        "Event": attributes.get("Event", "Unknown"),
                        "Cause": attributes.get("Cause", "Unknown"),
                        "Influence": attributes.get("Influence", "Unknown"),
                        "Corrective Actions": attributes.get("Corrective Actions", "None"),
                        "Similar Events": attributes.get("Similar Events", [])
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
