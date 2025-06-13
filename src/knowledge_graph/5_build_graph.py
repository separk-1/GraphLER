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

# Configurable paths
INPUT_JSONL_PATH = "../../data/processed/ler_kg.jsonl"
CFR_DATA_PATH = "../../data/raw/cfr.csv"
OUTPUT_CSV_PATH = "../../data/processed/linked_incidents.csv"
SIMILARITY_THRESHOLD = 0.8
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
    for attr, rel in [("Task", "RELATED_TO_TASK"), ("Event", "HAS_EVENT"), ("Cause", "HAS_CAUSE"),
                      ("Influence", "HAS_INFLUENCE"), ("Corrective Actions", "HAS_CORRECTIVE_ACTIONS")]:
        for val in event["attributes"].get(attr, []):
            tx.run(f"MERGE (a:{attr.replace(' ', '')} {{description: $val}}) "
                   f"MERGE (i:Incident {{filename: $filename}}) "
                   f"MERGE (i)-[:{rel}]->(a)", val=val, filename=event["filename"])

    facility = event["metadata"]["facility"]
    tx.run("MERGE (f:Facility {name: $name, unit: $unit}) "
           "MERGE (i:Incident {filename: $filename}) "
           "MERGE (i)-[:OCCURRED_AT]->(f)",
           name=facility["name"], unit=facility["unit"], filename=event["filename"])

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

# Relationship: SIMILAR_TASK (use MERGE to avoid duplication)
def insert_similarity(tx, f1, f2, t1, t2, sims):
    tx.run("""
        MATCH (e1:Incident {filename: $f1}), (e2:Incident {filename: $f2}),
              (t1:Task {description: $t1}), (t2:Task {description: $t2})
        MERGE (e1)-[r:SIMILAR_TASK]->(e2)
        ON CREATE SET
            r.task_similarity = $task_sim,
            r.cause_similarity = $cause_sim,
            r.event_similarity = $event_sim,
            r.influence_similarity = $influence_sim,
            r.task1 = $t1,
            r.task2 = $t2
    """, f1=f1, f2=f2, t1=t1, t2=t2,
         task_sim=sims["task_similarity"],
         cause_sim=sims["cause_similarity"],
         event_sim=sims["event_similarity"],
         influence_sim=sims["influence_similarity"])


# Step 5: Attribute-level logical relationship construction
def restructure_graph_relationships(tx):
    queries = [
        # Task → Cause
        """
        MATCH (i:Incident)-[:RELATED_TO_TASK]->(t:Task), (i)-[:HAS_CAUSE]->(c:Cause)
        MERGE (t)-[:CAUSES]->(c)
        """,
        # Cause → Event
        """
        MATCH (i:Incident)-[:HAS_CAUSE]->(c:Cause), (i)-[:HAS_EVENT]->(e:Event)
        MERGE (c)-[:TRIGGERS]->(e)
        """,
        # Event → Influence
        """
        MATCH (i:Incident)-[:HAS_EVENT]->(e:Event), (i)-[:HAS_INFLUENCE]->(inf:Influence)
        MERGE (e)-[:IMPACTS]->(inf)
        """,
        # Influence → CorrectiveAction
        """
        MATCH (i:Incident)-[:HAS_INFLUENCE]->(inf:Influence), (i)-[:HAS_CORRECTIVE_ACTIONS]->(ca:CorrectiveAction)
        MERGE (inf)-[:ADDRESSED_BY]->(ca)
        """
    ]
    for q in queries:
        tx.run(q)

with driver.session(database="ler50") as session:
    for cfr, val in cfr_dict.items():
        session.write_transaction(insert_cfr, cfr, val["class_1"], val["class_2"])
    for event in tqdm(data, desc="Inserting incidents"):
        session.write_transaction(insert_event, event)

embedding_cache = {}
for e in data:
    task_text = " ".join(e["attributes"].get("Task", []))
    if task_text:
        embedding_cache[e["filename"]] = model.encode(task_text, convert_to_tensor=True)

linked_records = []
with driver.session(database="ler50") as session:
    for i in tqdm(range(len(data)), desc="Linking similar incidents"):
        e1 = data[i]
        f1 = e1["filename"]
        if f1 not in embedding_cache:
            continue
        for j in range(i + 1, len(data)):
            e2 = data[j]
            f2 = e2["filename"]
            if f2 not in embedding_cache:
                continue
            task_sim = util.pytorch_cos_sim(embedding_cache[f1], embedding_cache[f2]).item()
            if task_sim >= SIMILARITY_THRESHOLD:
                cause_sim = calculate_similarity(" ".join(e1["attributes"].get("Cause", [])),
                                                 " ".join(e2["attributes"].get("Cause", [])))
                event_sim = calculate_similarity(" ".join(e1["attributes"].get("Event", [])),
                                                 " ".join(e2["attributes"].get("Event", [])))
                infl_sim = calculate_similarity(" ".join(e1["attributes"].get("Influence", [])),
                                                " ".join(e2["attributes"].get("Influence", [])))
                session.write_transaction(insert_similarity, f1, f2,
                                          " ".join(e1["attributes"]["Task"]),
                                          " ".join(e2["attributes"]["Task"]),
                                          {
                                              "task_similarity": task_sim,
                                              "cause_similarity": cause_sim,
                                              "event_similarity": event_sim,
                                              "influence_similarity": infl_sim
                                          })
                linked_records.append({
                    "filename_1": f1,
                    "filename_2": f2,
                    "task_similarity": task_sim,
                    "cause_similarity": cause_sim,
                    "event_similarity": event_sim,
                    "influence_similarity": infl_sim
                })
    # Final step: restructure attribute relationships
    session.write_transaction(restructure_graph_relationships)

# Save linked incident pairs
pd.DataFrame(linked_records).to_csv(OUTPUT_CSV_PATH, index=False, encoding="utf-8")
print(f"Graph construction and relationship restructuring complete. Saved {len(linked_records)} links to {OUTPUT_CSV_PATH}")
driver.close()
