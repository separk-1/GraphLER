# merge_cfr.py
import pandas as pd

LER_DF_PATH = "/../../../../../data/processed/ler_structured.csv"
CLAUSE_CSV_PATH = "/../../../../../data/raw/ler_cfr_map.csv"
MERGED_OUTPUT_PATH = "/../../../../../data/processed/ler_structured_with_cfr.csv"

def merge_cfr_data(ler_path, clause_path, output_path):
    ler_df = pd.read_csv(ler_path, encoding="utf-8")
    clause_df = pd.read_csv(clause_path, encoding="utf-8")
    
    # Merge on "File Name" (left) and "filename" (right)
    merged_df = pd.merge(ler_df, clause_df, left_on="File Name", right_on="filename", how="left")

    # Drop the redundant 'filename' column
    if 'filename' in merged_df.columns:
        merged_df.drop(columns=['filename'], inplace=True)
    
    # Save result
    merged_df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Merged data saved to {output_path}")
    print(merged_df.head())


if __name__ == "__main__":
    merge_cfr_data(LER_DF_PATH, CLAUSE_CSV_PATH, MERGED_OUTPUT_PATH)
