import os
import json
import pandas as pd
from sentence_transformers import SentenceTransformer, util
from neo4j import GraphDatabase
from tqdm import tqdm
import configparser

# Load Neo4j configuration
config = configparser.ConfigParser()
conf_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config/neo4j.conf"))
print("Loading config from:", conf_path)

config.read(conf_path)
uri = config.get("NEO4J", "uri")
username = config.get("NEO4J", "username")
password = config.get("NEO4J", "password")
driver = GraphDatabase.driver(uri, auth=(username, password))
equipment_weight = float(config.get("SIMILARITY_WEIGHTS", "equipment", fallback=0.5))
cause_weight = float(config.get("SIMILARITY_WEIGHTS", "cause", fallback=0.3))
impact_weight = float(config.get("SIMILARITY_WEIGHTS", "impact", fallback=0.2))

# Configurable paths
INPUT_JSONL_PATH = "/../../../../../data/processed/ler_kg.jsonl"
CFR_DATA_PATH = "/../../../../../data/raw/cfr.csv"
OUTPUT_CSV_PATH = "/../../../../../data/processed/linked_incidents.csv"
SIMILARITY_THRESHOLD = 0.5
RESET_GRAPH = True  # Set to True to clear all existing graph data

# Load LER data from JSONL
data = []
with open(INPUT_JSONL_PATH, "r", encoding="utf-8") as f:
    for line in f:
        data.append(json.loads(line.strip()))

# Load CFR dictionary
cfr_data = pd.read_csv(CFR_DATA_PATH)
cfr_dict = cfr_data.set_index("CFR")[["class_1", "class_2"]].to_dict(orient="index")

# Load embedding model
model = SentenceTransformer("all-MiniLM-L6-v2")

# Step 1: Clear Neo4j graph only if requested
if RESET_GRAPH:
    def delete_all(driver):
        with driver.session(database="ler50") as session:
            session.run("MATCH (n) DETACH DELETE n")
    delete_all(driver)
    print("✅ Graph cleared before insertion.")
else:
    print("ℹ️ Existing graph will be preserved. New nodes and relationships will be added.")

# Step 2: Insert CFR node
def insert_cfr(tx, cfr, upper, lower):
    tx.run("MERGE (c:CFR {cfr: $cfr}) SET c.upper = $upper, c.lower = $lower", cfr=cfr, upper=upper, lower=lower)

# Step 3: Insert Incident & attributes
def insert_event(tx, event):
    tx.run("MERGE (i:Incident {filename: $filename}) SET i.title = $title, i.date = $date",
           filename=event["filename"], title=event["metadata"]["title"], date=event["metadata"]["event_date"])
    for attr, rel in [
        ("Detection", "detected_by"),
        ("Equipment", "involved"),
        ("Corrective_Actions", "resolved_by"),
        ("Causes", "caused_by"),
        ("Impacts", "resulted_in")
    ]:
        for val in event["attributes"].get(attr, []):
            tx.run(f"MERGE (a:{attr.replace(' ', '')} {{description: $val}}) "
                   f"MERGE (i:Incident {{filename: $filename}}) "
                   f"MERGE (i)-[:{rel}]->(a)", val=val, filename=event["filename"])

    # Facility
    facility = event["metadata"]["facility"]
    tx.run("MERGE (f:Facility {name: $name, unit: $unit}) "
           "MERGE (i:Incident {filename: $filename}) "
           "MERGE (i)-[:OCCURRED_AT]->(f)",
           name=facility["name"], unit=facility["unit"], filename=event["filename"])

    # Indirect relationships
    for eq in event["attributes"].get("Equipment", []):
        for ft in event["attributes"].get("Failure_Type", []):
            tx.run("""
                MERGE (e:Equipment {description: $eq})
                MERGE (f:FailureType {description: $ft})
                MERGE (e)-[:failed_because]->(f)
            """, eq=eq, ft=ft)

    for ca in event["attributes"].get("Corrective_Actions", []):
        for ft in event["attributes"].get("Failure_Type", []):
            tx.run("""
                MERGE (c:CorrectiveAction {description: $ca})
                MERGE (f:FailureType {description: $ft})
                MERGE (c)-[:prevents]->(f)
            """, ca=ca, ft=ft)

    for c in event["attributes"].get("Causes", []):
        for cc in event["attributes"].get("Cause_Category", []):
            tx.run("""
                MERGE (c:Causes {description: $c})
                MERGE (cc:CauseCategory {description: $cc})
                MERGE (c)-[:cause_type]->(cc)
            """, c=c, cc=cc)

    for clause in event["metadata"].get("clause", "").split(", "):
        if clause in cfr_dict:
            upper = cfr_dict[clause]["class_1"]
            lower = cfr_dict[clause]["class_2"]
            tx.run("MERGE (c:CFR {cfr: $clause}) SET c.upper = $upper, c.lower = $lower "
                   "MERGE (i:Incident {filename: $filename}) "
                   "MERGE (i)-[:REGULATED_BY]->(c)",
                   clause=clause, upper=upper, lower=lower, filename=event["filename"])

# Step 4: Similarity functions
def calculate_similarity(text1, text2):
    if not text1 or not text2:
        return 0.0
    emb1 = model.encode(text1, convert_to_tensor=True)
    emb2 = model.encode(text2, convert_to_tensor=True)
    return util.pytorch_cos_sim(emb1, emb2).item()

def insert_equipment_similarity(tx, f1, f2, eq1, eq2, score):
    tx.run("""
        MATCH (e1:Incident {filename: $f1}), (e2:Incident {filename: $f2}),
              (q1:Equipment {description: $eq1}), (q2:Equipment {description: $eq2})
        MERGE (e1)-[r:SIMILAR_EQUIPMENT]->(e2)
        ON CREATE SET
            r.similarity = $score,
            r.equipment1 = $eq1,
            r.equipment2 = $eq2
    """, f1=f1, f2=f2, eq1=eq1, eq2=eq2, score=score)

def insert_cause_similarity(tx, f1, f2, c1, c2, score):
    tx.run("""
        MATCH (e1:Incident {filename: $f1}), (e2:Incident {filename: $f2}),
              (c1:Cause {description: $c1}), (c2:Cause {description: $c2})
        MERGE (e1)-[r:SIMILAR_CAUSE]->(e2)
        ON CREATE SET
            r.similarity = $score,
            r.cause1 = $c1,
            r.cause2 = $c2
    """, f1=f1, f2=f2, c1=c1, c2=c2, score=score)

def insert_impact_similarity(tx, f1, f2, i1, i2, score):
    tx.run("""
        MATCH (e1:Incident {filename: $f1}), (e2:Incident {filename: $f2}),
              (i1:Impact {description: $i1}), (i2:Impact {description: $i2})
        MERGE (e1)-[r:SIMILAR_IMPACT]->(e2)
        ON CREATE SET
            r.similarity = $score,
            r.impact1 = $i1,
            r.impact2 = $i2
    """, f1=f1, f2=f2, i1=i1, i2=i2, score=score)

def insert_overall_similarity(tx, f1, f2, score):
    tx.run("""
        MATCH (e1:Incident {filename: $f1}), (e2:Incident {filename: $f2})
        MERGE (e1)-[r:SIMILAR_OVERALL]->(e2)
        ON CREATE SET r.similarity = $score
    """, f1=f1, f2=f2, score=score)

# Step 5: Attribute-level logical relationship construction
def restructure_graph_relationships(tx):
    queries = [
        # Incident → Detection
        """
        MATCH (i:Incident)-[:detected_by]->(d:Detection)
        MERGE (i)-[:detected_by]->(d)
        """,

        # Incident → Equipment
        """
        MATCH (i:Incident)-[:involved]->(e:Equipment)
        MERGE (i)-[:involved]->(e)
        """,

        # Incident → Corrective Actions
        """
        MATCH (i:Incident)-[:resolved_by]->(c:CorrectiveAction)
        MERGE (i)-[:resolved_by]->(c)
        """,

        # Incident → Causes
        """
        MATCH (i:Incident)-[:caused_by]->(c:Causes)
        MERGE (i)-[:caused_by]->(c)
        """,

        # Incident → Impacts
        """
        MATCH (i:Incident)-[:resulted_in]->(imp:Impact)
        MERGE (i)-[:resulted_in]->(imp)
        """
    ]
    for q in queries:
        tx.run(q)

with driver.session(database="ler50") as session:
    for cfr, val in cfr_dict.items():
        session.write_transaction(insert_cfr, cfr, val["class_1"], val["class_2"])
    for event in tqdm(data, desc="Inserting incidents"):
        session.write_transaction(insert_event, event)

linked_records = []
with driver.session(database="ler50") as session:
    for i in tqdm(range(len(data)), desc="Linking similar incidents"):
        e1 = data[i]
        f1 = e1["filename"]
        eq1 = " ".join(e1["attributes"].get("Equipment", []))
        c1 = " ".join(e1["attributes"].get("Causes", []))
        i1 = " ".join(e1["attributes"].get("Impacts", []))
        for j in range(i + 1, len(data)):
            e2 = data[j]
            f2 = e2["filename"]
            eq2 = " ".join(e2["attributes"].get("Equipment", []))
            c2 = " ".join(e2["attributes"].get("Causes", []))
            i2 = " ".join(e2["attributes"].get("Impacts", []))
            equipment_sim = calculate_similarity(eq1, eq2)
            cause_sim = calculate_similarity(c1, c2)
            impact_sim = calculate_similarity(i1, i2)
            total_sim = ( 
                equipment_weight * equipment_sim + 
                cause_weight * cause_sim + 
                impact_weight * impact_sim
            )

            if equipment_sim >= SIMILARITY_THRESHOLD:
                session.write_transaction(insert_equipment_similarity, f1, f2, eq1, eq2, equipment_sim)
            if cause_sim >= SIMILARITY_THRESHOLD:
                session.write_transaction(insert_cause_similarity, f1, f2, c1, c2, cause_sim)
            if impact_sim >= SIMILARITY_THRESHOLD:
                session.write_transaction(insert_impact_similarity, f1, f2, i1, i2, impact_sim)
            if total_sim >= SIMILARITY_THRESHOLD:
                session.write_transaction(insert_overall_similarity, f1, f2, total_sim)

            if any(sim >= SIMILARITY_THRESHOLD for sim in [equipment_sim, cause_sim, impact_sim, total_sim]):
                linked_records.append({
                    "filename_1": f1,
                    "filename_2": f2,
                    "equipment_similarity": equipment_sim,
                    "cause_similarity": cause_sim,
                    "impact_similarity": impact_sim,
                    "total_similarity": total_sim
                })
    # Final step: restructure attribute relationships
    session.write_transaction(restructure_graph_relationships)

# Save linked incident pairs
pd.DataFrame(linked_records).to_csv(OUTPUT_CSV_PATH, index=False, encoding="utf-8")
print(f"Graph construction and relationship restructuring complete. Saved {len(linked_records)} links to {OUTPUT_CSV_PATH}")
driver.close()
