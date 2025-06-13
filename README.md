# GraphLER: Knowledge Graphs from Nuclear Incident Reports

[![Python](https://img.shields.io/badge/Python-3.10-blue.svg)](https://www.python.org/)

## 🎯 Overview
**GraphLER** is a lightweight pipeline that transforms U.S. Nuclear Licensee Event Reports (LERs) into structured **knowledge graphs**, capturing relationships between events, causes, corrective actions, and referenced regulations (CFRs).  
The resulting graph can be explored and queried in a **Neo4j graph database**, enabling deeper insight into incident structures and regulatory relevance.

## 📁 Project Structure
```
├── data/
│   ├── raw/               # Original LER (PDF) and CFR data
│   │   ├── ler/  
│   │   ├── cfr.csv
│   │   └── ler_cfr_map.csv
│   ├── processed/         # Cleaned and structured data
│   │   └── ler_text/
│   └── knowledge_graph/   # Final graph data
├── src/
│   ├── preprocessing/
│   │   ├── 1_ler_to_text.py
│   │   ├── 2_text_to_df.py
│   │   └── 3_merge_cfr.py
│   └── knowledge_graph/
│       ├── 4_extract_entity.py
│       └── 5_build_graph.py
├── README.md
└── requirements.txt
```

## ⚙️ Installation

### 1. Clone the repository
```bash
git clone https://github.com/separk-1/GraphLER.git
cd GraphLER
```

### 2. Create and activate the conda environment
```bash
conda create -n graphler python=3.10
conda activate graphler
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```
If you encounter errors, try upgrading pip:
```bash
python -m pip install --upgrade pip
```

## 🔧 Configuration

### 1. Create and configure `.env` file
```bash
cp .env.example .env
```

Then edit `.env` with your credentials:
```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password_here

LOG_LEVEL=INFO
DATA_DIR=data

OPENAI_API_KEY=your_openai_api_key
```

## 🧪 Usage

### Step 1: Extract Text from LER PDFs
```bash
python src/preprocessing/1_ler_to_text.py
```
- **Input**: Raw LER PDFs (`data/raw/ler/`)
- **Output**: Extracted text files (`data/processed/ler_text/`)

### Step 2: Clean and Structure the Data
```bash
python src/preprocessing/2_text_to_df.py
```
- **Input**: Text files from previous step
- **Output**: Structured CSV (`data/processed/ler_structured.csv`)

```bash
python src/preprocessing/3_merge_cfr.py
```
- **Input**: `ler_structured.csv`, `ler_cfr_map.csv`
- **Output**: Merged file with CFRs (`data/processed/ler_structured_with_cfr.csv`)

### Step 3: Entity Extraction via GPT
```bash
python src/knowledge_graph/4_extract_entity.py
```
- **Input**: `data/processed/ler_structured_with_cfr.csv`
- **Output**: Entity-enriched JSONL file (`data/processed/ler_kg.jsonl`)

### Step 4: Build the Knowledge Graph
Before running:
- Open Neo4j Desktop
- Create and run a database (e.g., `ler50`)
- Update `data/processed/neo4j.conf` or `.env` with its credentials

```bash
python src/knowledge_graph/5_build_graph.py
```
- **Input**: `ler_kg.jsonl`
- **Output**: Populated graph in Neo4j, CSV of similar incidents (`data/processed/linked_incidents.csv`)

## 🧠 Future Improvements
- Improve prompt engineering in `4_extract_entity.py` (current results may be overly generalized)

## 📝 Notes
- This demo uses 50 sample records due to GPT API cost constraints.
- You can experiment with fewer records for testing or expand to full datasets if budget allows.

---

MIT License © 2025 Seongeun Park