import os
import json
from neo4j import GraphDatabase
from tqdm import tqdm
from dotenv import load_dotenv

# ---------------------------
# CONFIG
# ---------------------------
INPUT_JSONL_PATH = "../../data/10y/processed/ler_kg.jsonl"

load_dotenv("../../.env")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

RESET_GRAPH = False  # ⚠ True로 하면 데이터 전체 삭제됨

# ---------------------------
# LOAD DATA
# ---------------------------
data = []
with open(INPUT_JSONL_PATH, "r", encoding="utf-8") as f:
    for line in f:
        data.append(json.loads(line.strip()))

# ---------------------------
# INIT NEO4J
# ---------------------------
driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

# ---------------------------
# CLEAR GRAPH
# ---------------------------
def clear_graph(tx):
    tx.run("MATCH (n) DETACH DELETE n")

if RESET_GRAPH:
    with driver.session() as session:
        session.execute_write(clear_graph)
    print("Graph cleared")

# ---------------------------
# INSERT EVENT (OPTIMIZED)
# ---------------------------
def insert_event(tx, event):

    filename = event["filename"]
    title = event["metadata"]["title"]
    date = event["metadata"]["event_date"]
    facility = event["metadata"]["facility"]

    attrs = event["attributes"]

    # --- Incident (once) ---
    tx.run("""
        MERGE (i:Incident {filename: $filename})
        SET i.title = $title,
            i.date = $date
    """, filename=filename, title=title, date=date)

    # --- Component + Type ---
    tx.run("""
        UNWIND range(0, size($components)-1) AS idx
        WITH $components[idx] AS val, $types[idx] AS t, $filename AS filename
        WHERE val IS NOT NULL AND val <> ""

        MATCH (i:Incident {filename: filename})

        MERGE (c:Component {name: val})
        MERGE (i)-[:HAS_COMPONENT]->(c)

        FOREACH (_ IN CASE WHEN t IS NOT NULL AND t <> "" THEN [1] ELSE [] END |
            MERGE (ct:ComponentType {name: t})
            MERGE (c)-[:IS_A]->(ct)
        )
    """, 
    components=attrs.get("Component", []),
    types=attrs.get("Component_Type", []),
    filename=filename)

    # --- Cause + Type ---
    tx.run("""
        UNWIND range(0, size($causes)-1) AS idx
        WITH $causes[idx] AS val, $types[idx] AS t, $filename AS filename
        WHERE val IS NOT NULL AND val <> ""

        MATCH (i:Incident {filename: filename})

        MERGE (c:Cause {name: val})
        MERGE (i)-[:HAS_CAUSE]->(c)

        FOREACH (_ IN CASE WHEN t IS NOT NULL AND t <> "" THEN [1] ELSE [] END |
            MERGE (ct:CauseType {name: t})
            MERGE (c)-[:IS_A]->(ct)
        )
    """, 
    causes=attrs.get("Cause", []),
    types=attrs.get("Cause_Type", []),
    filename=filename)

    # --- Outcome ---
    tx.run("""
        UNWIND $outcomes AS val
        WITH val WHERE val IS NOT NULL AND val <> ""
        MATCH (i:Incident {filename: $filename})
        MERGE (o:Outcome {name: val})
        MERGE (i)-[:RESULTED_IN]->(o)
    """, outcomes=attrs.get("Outcome", []), filename=filename)

    # --- Human Action ---
    tx.run("""
        UNWIND $actions AS val
        WITH val WHERE val IS NOT NULL AND val <> ""
        MATCH (i:Incident {filename: $filename})
        MERGE (h:HumanAction {name: val})
        MERGE (i)-[:ACTION]->(h)
    """, actions=attrs.get("Human_Action", []), filename=filename)

    # --- Corrective Action ---
    tx.run("""
        UNWIND $fixes AS val
        WITH val WHERE val IS NOT NULL AND val <> ""
        MATCH (i:Incident {filename: $filename})
        MERGE (c:CorrectiveAction {name: val})
        MERGE (i)-[:FIXED_BY]->(c)
    """, fixes=attrs.get("CorrectiveAction", []), filename=filename)

    # --- Facility ---
    tx.run("""
        MATCH (i:Incident {filename: $filename})
        MERGE (f:Facility {name: $name, unit: $unit})
        MERGE (i)-[:OCCURRED_AT]->(f)
    """, name=facility["name"], unit=facility["unit"], filename=filename)


# ---------------------------
# INSERT ALL EVENTS
# ---------------------------
with driver.session() as session:
    for event in tqdm(data, desc="Inserting incidents"):
        session.execute_write(insert_event, event)

driver.close()

print("Graph construction complete")