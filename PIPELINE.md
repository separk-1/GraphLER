# GraphLER Pipeline Documentation

This document explains the main code structure and data flow of **GraphLER**, a pipeline for converting Nuclear Licensee Event Reports (LERs) into a Neo4j knowledge graph.

The README provides installation and usage instructions. This document focuses on what each script does, what data it produces, and how the graph structure is built.

---

## 1. Overall Pipeline

GraphLER follows this high-level workflow:

```text
Raw LER PDFs
   ↓
Text extraction
   ↓
Structured LER table
   ↓
GPT-based entity extraction
   ↓
JSONL knowledge graph records
   ↓
Neo4j graph construction
   ↓
Graph querying and analysis
```

The current graph focuses on incident structure rather than regulatory mapping. CFR-related fields may exist in earlier preprocessing files, but the current graph-building step does not rely on CFR nodes.

---

## 2. Data Flow

### Input Data

The raw input is a collection of NRC Licensee Event Reports, usually stored as PDF files.

Expected raw data location:

```text
data/raw/ler/
```

Each LER contains narrative descriptions of events, causes, corrective actions, and plant conditions.

---

### Processed Data

After preprocessing, the pipeline generates structured tabular files such as:

```text
data/10y/processed/ler_structured_good.xlsx
```

This file is used as the main input to entity extraction.

Important columns include:

```text
File Name
Facility Name
Unit
Event Date
Title
Narrative
```

The `Narrative` field is the most important input for extraction because it contains the actual event description.

---

### Knowledge Graph JSONL

The entity extraction script produces:

```text
data/10y/processed/ler_kg.jsonl
```

Each line is one incident record in JSON format.

Example structure:

```json
{
  "filename": "MLXXXXXXX",
  "attributes": {
    "Power_State": [],
    "Component": [],
    "Component_Type": [],
    "Procedure": [],
    "Human_Action": [],
    "Outcome": [],
    "Cause": [],
    "Cause_Type": [],
    "CorrectiveAction": []
  },
  "metadata": {
    "facility": {
      "name": "Facility Name",
      "unit": "Unit 1"
    },
    "event_date": "MM-DD-YYYY",
    "title": "LER title"
  }
}
```

---

## 3. Script-by-Script Explanation

## `1_ler_to_text.py`

### Purpose

Extracts raw text from LER PDF files.

### Input

```text
data/raw/ler/*.pdf
```

### Output

```text
data/processed/ler_text/*.txt
```

### Role in Pipeline

This script converts PDFs into text so that later scripts can parse and structure the content.

---

## `2_text_to_df.py`

### Purpose

Converts extracted LER text files into a structured dataframe.

### Input

```text
data/processed/ler_text/
```

### Output

```text
data/processed/ler_structured.csv
```

or, in the current workflow:

```text
data/10y/processed/ler_structured_good.xlsx
```

### Main Function

This script extracts key metadata and narrative sections from each LER.

Typical fields include:

```text
File Name
Facility Name
Unit
Event Date
Title
Narrative
```

---

## `3_merge_cfr.py`

### Purpose

Earlier versions of the pipeline merged LER records with CFR references.

### Current Status

This step is optional in the current graph design.

The current Neo4j graph focuses on:

```text
Incident
Component
Component_Type
Cause
Cause_Type
Outcome
Human_Action
CorrectiveAction
Procedure
Facility
Power_State
```

CFR is not currently used as a graph node in the latest version.

---

## `4_extract_entity.py`

### Purpose

Uses GPT to extract structured attributes from each LER narrative.

This is the most important semantic extraction step.

### Input

```text
data/10y/processed/ler_structured_good.xlsx
```

### Output

```text
data/10y/processed/ler_kg.jsonl
```

---

## 4.1 Extracted Fields

### `Power_State`

Reactor or plant condition at the time of the event.

Examples:

```text
100 percent power
cold shutdown
Mode 1
```

---

### `Component`

Raw equipment phrase extracted directly from the LER text.

Examples:

```text
Main Steam Relief Valves
High Pressure Coolant Injection System
Reactor Recirculation Pump
```

This field preserves traceability to the original report.

---

### `Component_Type`

Normalized equipment category.

Examples:

```text
Main Steam Relief Valves → relief_valve
High Pressure Coolant Injection System → hpci_system
Reactor Recirculation Pump → recirculation_pump
```

This field is added because raw component names are often too specific. Without normalization, similar equipment types appear as separate graph nodes.

---

### `Procedure`

General procedure category.

Current allowed values:

```text
technical_specification
operating_procedure
```

This field indicates whether the event involved a technical specification or operating procedure context.

---

### `Human_Action`

Explicit operator or personnel action described in the text.

Examples:

```text
declared inoperable
started standby gas treatment system
secured and returned to automatic
```

This field does not infer operator intent. It only stores actions explicitly stated in the LER.

---

### `Outcome`

Final system impact or clear system state change.

Examples:

```text
Safety System Functional Failure
loss of secondary containment pressure
shutdown
```

Automatic system responses are excluded unless they represent final system impact.

---

### `Cause`

Raw cause phrase extracted directly from the text.

Examples:

```text
intermittent failure of a solenoid
through-wall leak
lift settings outside of technical specifications
```

---

### `Cause_Type`

Normalized cause category.

Examples:

```text
corrosion bonding → corrosion
through-wall leak → leakage
setpoint drift → calibration_error
operator failed to follow procedure → human_error
```

This field supports pattern analysis across many LERs.

---

### `CorrectiveAction`

Repair, replacement, or modification action.

Examples:

```text
solenoids were replaced
replaced seals
calibrated damper control circuit
```

Status declarations such as `declared operable` are excluded.

---

## 4.2 Why Raw and Type Fields Both Exist

The pipeline keeps both raw and normalized fields because they serve different purposes.

### Raw fields

Raw fields preserve the original wording from the report.

They support:

```text
traceability
case-level review
manual validation
citation back to source text
```

### Type fields

Type fields support higher-level analysis.

They allow similar concepts to be grouped together.

For example:

```text
Main Steam Relief Valves
Relief Valve
Safety Relief Valve
```

can all be analyzed under:

```text
relief_valve
```

This makes frequency analysis and pattern mining more meaningful.

---

## 5. Entity Normalization Layer

Entity normalization is the process of mapping detailed text phrases into stable concept categories.

### Example

```text
Raw Component: Main Steam Relief Valves
Normalized Type: relief_valve
```

### Why It Matters

Without normalization, the graph becomes fragmented.

For example:

```text
Main Steam Relief Valves
Main Steam Relief Valve
Relief Valves
Safety Relief Valve
```

may all represent similar equipment, but Neo4j stores them as different nodes if no normalized type exists.

The normalization layer allows queries such as:

```cypher
MATCH (i)-[:HAS_COMPONENT]->(:Component)-[:IS_A]->(t:ComponentType)
RETURN t.name, count(*) AS freq
ORDER BY freq DESC
```

This provides more useful aggregate patterns than raw text alone.

---

## 6. `5_build_graph.py`

### Purpose

Builds the Neo4j knowledge graph from the JSONL file.

### Input

```text
data/10y/processed/ler_kg.jsonl
```

### Output

A populated Neo4j graph database.

---

## 6.1 Main Graph Structure

Each LER becomes an `Incident` node.

The graph connects each incident to extracted entities:

```text
Incident
 ├── HAS_COMPONENT → Component
 │                    └── IS_A → ComponentType
 ├── HAS_CAUSE → Cause
 │              └── IS_A → CauseType
 ├── RESULTED_IN → Outcome
 ├── ACTION → HumanAction
 ├── FIXED_BY → CorrectiveAction
 ├── USES_PROCEDURE → Procedure
 ├── HAS_POWER_STATE → PowerState
 └── OCCURRED_AT → Facility
```

---

## 6.2 Node Types

### `Incident`

Represents one LER event.

Properties:

```text
filename
title
date
```

---

### `Component`

Raw equipment phrase.

Property:

```text
name
```

---

### `ComponentType`

Normalized equipment category.

Property:

```text
name
```

---

### `Cause`

Raw cause phrase.

Property:

```text
name
```

---

### `CauseType`

Normalized cause category.

Property:

```text
name
```

---

### `Outcome`

Final system-level impact or state change.

---

### `HumanAction`

Explicit operator or personnel action.

---

### `CorrectiveAction`

Repair, replacement, or modification.

---

### `Procedure`

Procedure context.

---

### `PowerState`

Plant or reactor operating state.

---

### `Facility`

Facility and unit where the incident occurred.

---

## 6.3 Relationship Types

### `HAS_COMPONENT`

Connects an incident to the involved raw component.

```text
Incident → Component
```

---

### `IS_A`

Connects a raw entity to its normalized type.

```text
Component → ComponentType
Cause → CauseType
```

---

### `HAS_CAUSE`

Connects an incident to its raw cause.

```text
Incident → Cause
```

---

### `RESULTED_IN`

Connects an incident to its outcome.

```text
Incident → Outcome
```

---

### `ACTION`

Connects an incident to explicit operator or personnel action.

```text
Incident → HumanAction
```

---

### `FIXED_BY`

Connects an incident to corrective action.

```text
Incident → CorrectiveAction
```

---

### `USES_PROCEDURE`

Connects an incident to a procedure context.

```text
Incident → Procedure
```

---

### `HAS_POWER_STATE`

Connects an incident to the plant power state.

```text
Incident → PowerState
```

---

### `OCCURRED_AT`

Connects an incident to facility metadata.

```text
Incident → Facility
```

---

## 7. Example Queries

### Count node types

```cypher
MATCH (n)
RETURN labels(n), count(*)
ORDER BY count(*) DESC
```

---

### Count relationship types

```cypher
MATCH ()-[r]->()
RETURN type(r), count(*)
ORDER BY count(*) DESC
```

---

### Top raw components

```cypher
MATCH (i)-[:HAS_COMPONENT]->(c)
RETURN c.name, count(*) AS freq
ORDER BY freq DESC
LIMIT 20
```

---

### Top normalized component types

```cypher
MATCH (i)-[:HAS_COMPONENT]->(:Component)-[:IS_A]->(t:ComponentType)
RETURN t.name, count(*) AS freq
ORDER BY freq DESC
LIMIT 20
```

---

### Top raw causes

```cypher
MATCH (i)-[:HAS_CAUSE]->(c)
RETURN c.name, count(*) AS freq
ORDER BY freq DESC
LIMIT 20
```

---

### Top normalized cause types

```cypher
MATCH (i)-[:HAS_CAUSE]->(:Cause)-[:IS_A]->(t:CauseType)
RETURN t.name, count(*) AS freq
ORDER BY freq DESC
LIMIT 20
```

---

### Component type to cause type pattern

```cypher
MATCH (i)-[:HAS_COMPONENT]->(:Component)-[:IS_A]->(ct:ComponentType),
      (i)-[:HAS_CAUSE]->(:Cause)-[:IS_A]->(ca:CauseType)
RETURN ct.name AS component_type,
       ca.name AS cause_type,
       count(*) AS freq
ORDER BY freq DESC
LIMIT 30
```

---

### Cause type to outcome pattern

```cypher
MATCH (i)-[:HAS_CAUSE]->(:Cause)-[:IS_A]->(ca:CauseType),
      (i)-[:RESULTED_IN]->(o:Outcome)
RETURN ca.name AS cause_type,
       o.name AS outcome,
       count(*) AS freq
ORDER BY freq DESC
LIMIT 30
```

---

### Human action to outcome pattern

```cypher
MATCH (i)-[:ACTION]->(a:HumanAction),
      (i)-[:RESULTED_IN]->(o:Outcome)
RETURN a.name AS action,
       o.name AS outcome,
       count(*) AS freq
ORDER BY freq DESC
LIMIT 30
```

---

### Facility-level cause patterns

```cypher
MATCH (i)-[:OCCURRED_AT]->(f:Facility),
      (i)-[:HAS_CAUSE]->(:Cause)-[:IS_A]->(ct:CauseType)
RETURN f.name AS facility,
       ct.name AS cause_type,
       count(*) AS freq
ORDER BY freq DESC
LIMIT 30
```

---

## 8. Current Design Choice

The current design intentionally avoids forcing full decision-making interpretation from LERs.

LERs usually describe:

```text
what happened
what caused it
what actions were taken
what corrective actions followed
```

They usually do not explicitly describe full cognitive decision-making processes.

Therefore, the current pipeline extracts:

```text
failure structure
operator actions
corrective actions
normalized equipment and cause types
```

rather than inventing decision intent.

This keeps the graph traceable and reduces hallucination risk.

---

## 9. Recommended Next Improvements

### 1. Add controlled vocabularies

Over time, create stable vocabularies for:

```text
Component_Type
Cause_Type
Outcome
Human_Action
```

This will improve consistency across the graph.

---

### 2. Add validation scripts

A validation script can check:

```text
empty fields
raw/type mismatch
rare normalized labels
duplicate concepts
```

---

### 3. Add graph export

Export graph query results to CSV for plotting and publication.

---

### 4. Add manual review sample

A small manually validated subset can be used to estimate extraction quality.

---

## 10. Summary

GraphLER currently performs the following tasks:

```text
1. Convert LER reports into structured text records
2. Extract raw entities using GPT
3. Add normalized type layers for components and causes
4. Build a Neo4j knowledge graph
5. Support pattern queries over nuclear incident structures
```

The key design principle is:

```text
Preserve raw text for traceability,
add normalized type nodes for analysis.
```

