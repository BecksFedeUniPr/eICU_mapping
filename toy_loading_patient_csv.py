import pandas as pd
import json
import numpy as np
import re
from collections import defaultdict
from neo4j import GraphDatabase
from datetime import datetime, timedelta

# GOAL: load patient.csv fully driven by eICU_mapping.json (nodes + relationships)

URI = "neo4j://localhost:7687"
USER = "neo4j"
PASSWORD = "Mediverse"

# Unique prefix per node type to avoid source_reference collisions
# (e.g. Encounter and HospitalAdmission both reference patienthealthsystemstayid)
NODE_PREFIX = {
    "Patient":                  "PAT",
    "Encounter":                "ENC",
    "Encounter_Segment":        "SEG",
    "HospitalAdmission":        "HADM",
    "HospitalDischarge":        "HDIS",
    "ICU_Admit":                "ICUA",
    "ICU_Discharge":            "ICUD",
    "Location_Hospital":        "LHOSP",
    "Location_Ward":            "LWARD",
    "Location_admit_discharge": "LADM"
}

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def load_mapping_config(mapping_path):
    """Load the mapping JSON file"""
    with open(mapping_path, 'r') as f:
        return json.load(f)

def resolve_template(template, row):
    """Resolve templates like HOSP_{hospitalid} from CSV row values"""
    def replace(match):
        field = match.group(1)
        val = row.get(field)
        return str(int(val)) if val is not None else "UNKNOWN"
    return re.sub(r'\{(\w+)\}', replace, template)

def parse_time_expr(expr, row):
    """
    Parse mapping time expressions like 'hospitaladmittime24 + hospitaladmitoffset'.
    Returns ISO timestamp string or None.
    """
    parts = [p.strip() for p in expr.split('+')]
    time_val   = row.get(parts[0])
    offset_val = row.get(parts[1]) if len(parts) > 1 else 0
    return parse_time_with_offset(time_val, offset_val)

def parse_time_with_offset(time_24h, offset_minutes):
    """
    Calculate the correct timestamp considering offset in minutes (can be negative).
    time_24h: string HH:MM:SS
    offset_minutes: int or float
    Returns: ISO timestamp string
    """
    if time_24h is None:
        return None
    try:
        base = datetime.strptime(str(time_24h).strip(), '%H:%M:%S')
        if offset_minutes is not None:
            base = base + timedelta(minutes=int(offset_minutes))
        return base.isoformat()
    except Exception as e:
        print(f"Error parsing time {time_24h} + offset {offset_minutes}: {e}")
        return None

def build_payload(row, field_list):
    """Build JSON payload dict from a list of CSV field names"""
    payload = {f: row[f] for f in field_list if f in row and row[f] is not None}
    return json.dumps(payload) if payload else None

def get_node_source_ref(node_name, node_def, row):
    """
    Return a globally unique source_reference for this node type + row.
    Uses NODE_PREFIX to disambiguate nodes sharing the same CSV key.
    """
    raw = node_def.get('source_reference', '')
    resolved = resolve_template(raw, row) if '{' in raw else str(row.get(raw, ''))
    if not resolved or resolved == 'None':
        return None
    prefix = NODE_PREFIX.get(node_name, node_name[:3].upper())
    return f"{prefix}_{resolved}"

# ─────────────────────────────────────────────
# DATA PREPARATION  (driven by mapping JSON)
# ─────────────────────────────────────────────

def load_and_prep_data(csv_path, mapping_config):
    """
    Read the CSV and build, for each row:
      - one dict per node type (with source_reference, properties, payload, timestamps)
      - one list of relationships (from_ref, to_ref, type)
    Everything is driven by eICU_mapping.json.
    """
    print("Reading CSV file...")
    df = pd.read_csv(csv_path)
    df = df.replace({np.nan: None})

    nodes_map   = mapping_config['nodes']
    rel_defs    = mapping_config['relationships']

    print(f"Mapping: {len(nodes_map)} node types | {len(rel_defs)} relationship types")

    records = []
    skipped = 0

    for _, raw_row in df.iterrows():
        row = dict(raw_row)

        # Skip rows missing any critical ID
        if None in (row.get('uniquepid'),
                    row.get('patienthealthsystemstayid'),
                    row.get('patientunitstayid')):
            skipped += 1
            continue

        record = {}

        # ── Build node data from mapping ──
        for node_name, node_def in nodes_map.items():
            src_ref = get_node_source_ref(node_name, node_def, row)
            if src_ref is None:
                continue

            node_data = {
                "source_reference": src_ref,
                "type":             node_def.get('type', node_name),
                "concept_label":    node_def.get('concept_label', node_name),
            }

            # Timestamps
            for t_key in ('start_time', 'end_time', 'timestamp'):
                if t_key in node_def:
                    node_data[t_key] = parse_time_expr(node_def[t_key], row)

            # Payload
            payload_fields = node_def.get('payload', [])
            if payload_fields:
                node_data['payload'] = build_payload(row, payload_fields)

            # Direct property mappings (e.g. Patient: age, gender, ethnicity)
            skip_keys = {'source_reference', 'type', 'concept_id', 'concept_label',
                         'payload', 'start_time', 'end_time', 'timestamp',
                         'unit_visit_number', 'contained_events'}
            for prop, csv_col in node_def.items():
                if prop not in skip_keys and isinstance(csv_col, str) and csv_col in row:
                    node_data[prop] = row[csv_col]

            record[node_name] = node_data

        # ── Build relationships from mapping ──
        rel_list = []
        for rel in rel_defs:
            from_name = rel['from']
            to_name   = rel['to']
            from_def  = nodes_map.get(from_name)
            to_def    = nodes_map.get(to_name)
            if from_def is None or to_def is None:
                continue
            from_ref = get_node_source_ref(from_name, from_def, row)
            to_ref   = get_node_source_ref(to_name,   to_def,   row)
            if from_ref and to_ref:
                rel_list.append({
                    "from_label": from_name,
                    "from_ref":   from_ref,
                    "to_label":   to_name,
                    "to_ref":     to_ref,
                    "type":       rel['type']
                })

        record['_relationships'] = rel_list
        records.append(record)

    if skipped:
        print(f"⚠️  Skipped {skipped} rows with missing critical IDs")
    return records

# ─────────────────────────────────────────────
# NEO4J WRITERS
# ─────────────────────────────────────────────

def create_nodes_tx(tx, batch, node_label):
    """Batch-MERGE all nodes of a given label"""
    node_batch = [rec[node_label] for rec in batch if node_label in rec]
    if not node_batch:
        return
    query = f"""
    UNWIND $batch AS props
    MERGE (n:{node_label} {{source_reference: props.source_reference}})
    ON CREATE SET n += props
    """
    tx.run(query, batch=node_batch)

def create_relationships_tx(tx, batch):
    """Batch-MERGE all relationships, grouped by (from_label, to_label, type)"""
    all_rels = []
    for rec in batch:
        all_rels.extend(rec.get('_relationships', []))
    if not all_rels:
        return

    grouped = defaultdict(list)
    for rel in all_rels:
        grouped[(rel['from_label'], rel['to_label'], rel['type'])].append(rel)

    for (from_label, to_label, rel_type), rels in grouped.items():
        query = f"""
        UNWIND $rels AS rel
        MATCH (a:{from_label} {{source_reference: rel.from_ref}})
        MATCH (b:{to_label}   {{source_reference: rel.to_ref}})
        MERGE (a)-[:{rel_type}]->(b)
        """
        tx.run(query, rels=rels)

def setup_constraints(tx, mapping_config):
    """Create uniqueness constraints for every node type in the mapping"""
    for node_label in mapping_config['nodes'].keys():
        tx.run(
            f"CREATE CONSTRAINT {node_label.lower()}_src_ref IF NOT EXISTS "
            f"FOR (n:{node_label}) REQUIRE n.source_reference IS UNIQUE;"
        )

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    csv_file     = "patient.csv"
    mapping_file = "eICU_mapping.json"

    mapping_config = load_mapping_config(mapping_file)
    print(f"Loaded mapping : {mapping_config['mapping_metadata']}")
    print(f"Node types     : {list(mapping_config['nodes'].keys())}")
    print(f"Relationships  : {[r['type'] for r in mapping_config['relationships']]}")

    data       = load_and_prep_data(csv_file, mapping_config)
    total_rows = len(data)
    print(f"Records ready  : {total_rows}")

    BATCH_SIZE   = 5000
    node_labels  = list(mapping_config['nodes'].keys())

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as session:

        session.execute_write(setup_constraints, mapping_config)
        print("✅ Constraints created")

        for i in range(0, total_rows, BATCH_SIZE):
            chunk = data[i: i + BATCH_SIZE]

            # 1. Create all node types
            for label in node_labels:
                session.execute_write(create_nodes_tx, chunk, label)

            # 2. Create all relationships
            session.execute_write(create_relationships_tx, chunk)

            print(f"✅ Processed {min(i + BATCH_SIZE, total_rows)}/{total_rows} rows")

    driver.close()
    print("Done! Graph fully loaded from eICU_mapping.json 🚀")


URI = "neo4j://localhost:7687"
USER = "neo4j"
PASSWORD = "Mediverse"

def load_mapping_config(mapping_path):
    """Load the mapping JSON file"""
    with open(mapping_path, 'r') as f:
        return json.load(f)

def parse_time_with_offset(time_24h, offset_minutes):
    """
    Calculate the correct timestamp considering the offset in minutes.
    time_24h: string in HH:MM:SS format
    offset_minutes: offset in minutes (can be negative)
    Returns: ISO timestamp string
    """
    if pd.isna(time_24h) or time_24h is None:
        return None
    
    try:
        # Parse time as if it were today
        base_time = datetime.strptime(str(time_24h), '%H:%M:%S')
        
        # Add offset if present
        if not pd.isna(offset_minutes) and offset_minutes is not None:
            base_time = base_time + timedelta(minutes=int(offset_minutes))
        
        return base_time.isoformat()
    except Exception as e:
        print(f"Error parsing time {time_24h} with offset {offset_minutes}: {e}")
        return None

def build_payload(row, field_list):
    """Build the JSON payload from specified fields"""
    payload = {}
    for field in field_list:
        value = row.get(field)
        if not pd.isna(value) and value is not None:
            payload[field] = value
    return json.dumps(payload) if payload else None

def load_and_prep_data(csv_path, mapping_config):
    """Prepare data according to the JSON mapping"""
    print("Reading CSV file...")
    df = pd.read_csv(csv_path)
    df = df.replace({np.nan: None})
    
    print("Mapping data according to eICU_mapping.json...")
    records = []
    
    for _, row in df.iterrows():
        # Calculate timestamp with offset for Encounter
        enc_start = parse_time_with_offset(
            row.get('hospitaladmittime24'), 
            row.get('hospitaladmitoffset')
        )
        enc_end = parse_time_with_offset(
            row.get('hospitaldischargetime24'), 
            row.get('hospitaldischargeoffset')
        )
        
        # Calculate timestamp with offset for Encounter_Segment
        seg_start = parse_time_with_offset(
            row.get('unitadmittime24'),
            0  # No offset specified in mapping for unit admit
        )
        seg_end = parse_time_with_offset(
            row.get('unitdischargetime24'),
            row.get('unitdischargeoffset')
        )
        
        # Build payloads according to mapping
        encounter_payload = build_payload(row, [
            "admissionheight", "admissionweight", "dischargeweight",
            "hospitaladmitsource", "hospitaldischargeyear",
            "hospitaldischargestatus", "hospitaldischargelocation"
        ])
        
        segment_payload = build_payload(row, [
            "unitadmitsource", "unitstaytype", "unitdischargelocation",
            "unitdischargestatus", "apache_dissimions"
        ])
        
        record = {
            # Patient
            "patient_ref": str(row['uniquepid']),
            "age": row.get('age'),
            "gender": row.get('gender'),
            "ethnicity": row.get('ethnicity'),
            
            # Encounter
            "encounter_ref": str(row['patienthealthsystemstayid']),
            "encounter_start": enc_start,
            "encounter_end": enc_end,
            "encounter_payload": encounter_payload,
            
            # Encounter_Segment
            "segment_ref": str(row['patientunitstayid']),
            "segment_start": seg_start,
            "segment_end": seg_end,
            "segment_unittype": row.get('unittype'),
            "segment_unitvisitnumber": row.get('unitvisitnumber'),
            "segment_payload": segment_payload,
            "segment_label": row.get('apacheadmissiondx') or "Unknown",
            
            # Locations
            "hospital_ref": f"HOSP_{row['hospitalid']}",
            "ward_ref": f"HOSP_{row['hospitalid']}_Ward_{row['wardid']}"
        }
        records.append(record)
    
    return records

def create_graph(tx, batch):
    # Use separate MERGEs for nodes and then link them specifically
    # This prevents Neo4j from "recycling" wrong relationships
    query = """
    UNWIND $batch AS row
    
    // 1. Create Patient Nodes
    MERGE (p:Patient {source_reference: row.patient_ref})
    ON CREATE SET p.gender = row.gender, p.age = row.age, p.ethnicity = row.ethnicity
    
    // 2. Create Location Nodes (Hospital and Ward distinct)
    MERGE (loc_hosp:Location {source_reference: row.hospital_ref})
    MERGE (loc_ward:Location {source_reference: row.ward_ref})
    
    // 3. Create Encounter Event (Hospital)
    MERGE (enc:Event {source_reference: row.encounter_ref})
    ON CREATE SET enc.type = 'encounter',
                  enc.concept_label = 'Hospital Stay',
                  enc.start_time = row.encounter_start, 
                  enc.end_time = row.encounter_end, 
                  enc.payload = row.encounter_payload
    
    // 4. Create Segment Event (ICU)
    MERGE (seg:Event {source_reference: row.segment_ref})
    ON CREATE SET seg.type = 'encounter_segment', 
                  seg.concept_label = row.segment_label,
                  seg.start_time = row.segment_start, 
                  seg.end_time = row.segment_end, 
                  seg.payload = row.segment_payload,
                  seg.unittype = row.segment_unittype,
                  seg.unitvisitnumber = row.segment_unitvisitnumber

    // 5. Explicit Links (Has_Event)
    MERGE (p)-[:has_event]->(enc)
    MERGE (p)-[:has_event]->(seg)

    //LOAD_THE_RELATIONSHIP FROM JSON FILE
    // 6. Explicit Links to Location
    MERGE (enc)-[:has_location]->(loc_hosp)
    MERGE (seg)-[:has_location]->(loc_ward)
    """
    tx.run(query, batch=batch)

def setup_constraints(tx):
    queries = [
        "CREATE CONSTRAINT patient_ref IF NOT EXISTS FOR (p:Patient) REQUIRE p.source_reference IS UNIQUE;",
        "CREATE CONSTRAINT event_ref IF NOT EXISTS FOR (e:Event) REQUIRE e.source_reference IS UNIQUE;",
        "CREATE CONSTRAINT loc_ref IF NOT EXISTS FOR (l:Location) REQUIRE l.source_reference IS UNIQUE;"
    ]
    for q in queries:
        tx.run(q)

if __name__ == "__main__":
    csv_file = "patient.csv"
    mapping_file = "eICU_mapping.json"
    
    # Load mapping configuration
    mapping_config = load_mapping_config(mapping_file)
    print(f"Loaded mapping config: {mapping_config['mapping_metadata']}")
    
    # Prepare data
    data = load_and_prep_data(csv_file, mapping_config)
    total_rows = len(data)
    print("Number of records: ", total_rows)
    BATCH_SIZE = 5000
    
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))   
    with driver.session() as session:
        session.execute_write(setup_constraints)
        # BATCH_SIZE is necessary 'cause the loading process has a strict limit of RAM Space Source: test on my PC
        for i in range(0, total_rows, BATCH_SIZE):
            chunk = data[i : i + BATCH_SIZE]
            session.execute_write(create_graph, chunk)
            print(f"✅ Processed {min(i + BATCH_SIZE, total_rows)}/{total_rows} rows")
    driver.close()
    print("Done! Now the Locations are correctly separated. 🚀")