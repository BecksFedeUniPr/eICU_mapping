import pandas as pd
import json
import numpy as np
import re
import os
from collections import defaultdict
from neo4j import GraphDatabase
from datetime import datetime, timedelta

# GOAL: load patient.csv fully driven by mapping.json (nodes + relationships)

URI      = os.getenv("NEO4J_URI",      "neo4j://localhost:7687")
USER     = os.getenv("NEO4J_USER",     "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "Mediverse")

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

def is_multi_source(node_def):
    """True for categorical location nodes that use source_reference_fields."""
    return 'source_reference_fields' in node_def

def get_multi_source_nodes(node_name, node_def, row):
    """
    For categorical nodes (e.g. Location_admit_discharge):
    create one node dict per source field that has a non-null value in this row.
    e.g. hospitaladmitsource='Emergency Department' -> LADM_Emergency Department
    """
    prefix = NODE_PREFIX.get(node_name, node_name[:3].upper())
    nodes = []
    for field in node_def['source_reference_fields']:
        val = row.get(field)
        # Guard against None, float NaN, empty string
        if val is None:
            continue
        if isinstance(val, float) and np.isnan(val):
            continue
        val_str = str(val).strip()
        if not val_str or val_str.lower() == 'nan':
            continue
        nodes.append({
            "source_reference": f"{prefix}_{val_str}",
            "name": val_str
        })
    return nodes

def get_multi_source_ref(node_name, field_value):
    """Build the source_reference for a multi-source node from a single field value."""
    if field_value is None:
        return None
    prefix = NODE_PREFIX.get(node_name, node_name[:3].upper())
    return f"{prefix}_{field_value}"

# ─────────────────────────────────────────────
# DATA PREPARATION  (driven by mapping JSON)
# ─────────────────────────────────────────────

def load_and_prep_data(csv_path, mapping_config):
    """
    Read the CSV and build, for each row:
      - one dict per node type (with source_reference, properties, payload, timestamps)
      - one list of relationships (from_ref, to_ref, type)
    Everything is driven by mapping.json.
    """
    print("Reading CSV file...")
    df = pd.read_csv(csv_path)
    df = df.replace({np.nan: None})

    nodes_map = mapping_config['nodes']
    rel_defs  = mapping_config['relationships']

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

            # Categorical / multi-source nodes (e.g. Location_admit_discharge)
            if is_multi_source(node_def):
                multi_nodes = get_multi_source_nodes(node_name, node_def, row)
                if multi_nodes:
                    record[node_name] = multi_nodes  # list of node dicts
                continue

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

            if is_multi_source(to_def):
                # join_key is the CSV field whose value IS the target node
                join_key    = rel.get('join_key')
                field_value = row.get(join_key) if isinstance(join_key, str) else None
                to_ref      = get_multi_source_ref(to_name, field_value)
            else:
                to_ref = get_node_source_ref(to_name, to_def, row)

            if from_ref and to_ref:
                rel_list.append({
                    "from_label": from_name,
                    "from_ref":   from_ref,
                    "to_label":   to_name,
                    "to_ref":     to_ref,
                    "type":       rel['type']
                })

        # ── HAS_CONCEPT → Local_Concept (one shared node per type, cheap) ──
        for node_name, node_def in nodes_map.items():
            concept_id = node_def.get('concept_id')
            if not concept_id or concept_id == 'None':
                continue
            concept_ref = f"CONCEPT_{node_name}"
            if is_multi_source(node_def):
                for entry in record.get(node_name, []):
                    from_ref = entry.get('source_reference')
                    if from_ref:
                        rel_list.append({
                            "from_label": node_name,
                            "from_ref":   from_ref,
                            "to_label":   "Local_Concept",
                            "to_ref":     concept_ref,
                            "type":       "HAS_CONCEPT"
                        })
            else:
                entity_data = record.get(node_name)
                if not isinstance(entity_data, dict):
                    continue
                from_ref = entity_data.get('source_reference')
                if from_ref:
                    rel_list.append({
                        "from_label": node_name,
                        "from_ref":   from_ref,
                        "to_label":   "Local_Concept",
                        "to_ref":     concept_ref,
                        "type":       "HAS_CONCEPT"
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
    seen   = {}
    for rec in batch:
        entry = rec.get(node_label)
        if entry is None:
            continue
        entries = entry if isinstance(entry, list) else [entry]
        for node in entries:
            ref = node.get('source_reference')
            if ref and ref not in seen:
                seen[ref] = node
    node_batch = list(seen.values())
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

def create_local_concepts_tx(tx, mapping_config):
    """Create exactly one Local_Concept node per node type (cheap: ~10 nodes total)."""
    tx.run(
        "CREATE CONSTRAINT local_concept_src_ref IF NOT EXISTS "
        "FOR (n:Local_Concept) REQUIRE n.source_reference IS UNIQUE;"
    )
    concepts = []
    for node_name, node_def in mapping_config['nodes'].items():
        concept_id    = node_def.get('concept_id')
        concept_label = node_def.get('concept_label')
        if not concept_id or concept_id == 'None':
            continue
        concepts.append({
            "source_reference": f"CONCEPT_{node_name}",
            "concept_id":       concept_id,
            "concept_label":    concept_label,
            "name":             node_name
        })
    if not concepts:
        return
    tx.run("""
    UNWIND $concepts AS c
    MERGE (lc:Local_Concept {source_reference: c.source_reference})
    ON CREATE SET lc += c
    """, concepts=concepts)

def cleanup_stale_relationships(session, mapping_config):
    """
    Delete any relationship types that exist in Neo4j but are no longer
    defined in the current mapping (stale types from previous mapping versions).
    """
    # All valid types: mapping-defined + hardcoded HAS_CONCEPT
    valid_types = {r['type'] for r in mapping_config['relationships']}
    valid_types.add('HAS_CONCEPT')

    existing = session.run(
        "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
    ).data()
    existing_types = {r['relationshipType'] for r in existing}

    stale = existing_types - valid_types
    if not stale:
        print("✅ No stale relationship types found")
        return

    for rel_type in stale:
        print(f"  🗑️  Removing stale relationship type: {rel_type}")
        session.run(f"MATCH ()-[r:`{rel_type}`]->() DELETE r")
    print(f"✅ Removed {len(stale)} stale relationship type(s): {stale}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    csv_file     = os.getenv("CSV_FILE",     "/data/patient.csv")
    mapping_file = os.getenv("MAPPING_FILE", "mapping.json")

    mapping_config = load_mapping_config(mapping_file)
    print(f"Loaded mapping : {mapping_config['mapping_metadata']}")
    print(f"Node types     : {list(mapping_config['nodes'].keys())}")
    print(f"Relationships  : {[r['type'] for r in mapping_config['relationships']]}")

    data       = load_and_prep_data(csv_file, mapping_config)
    total_rows = len(data)
    print(f"Records ready  : {total_rows}")

    BATCH_SIZE  = 5000
    node_labels = list(mapping_config['nodes'].keys())

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as session:

        print("🧹 Cleaning up stale relationship types...")
        cleanup_stale_relationships(session, mapping_config)

        session.execute_write(setup_constraints, mapping_config)
        print("✅ Constraints created")

        session.execute_write(create_local_concepts_tx, mapping_config)
        print("✅ Local_Concept nodes created")

        for i in range(0, total_rows, BATCH_SIZE):
            chunk = data[i: i + BATCH_SIZE]

            # 1. Create all node types
            for label in node_labels:
                session.execute_write(create_nodes_tx, chunk, label)

            # 2. Create all relationships
            session.execute_write(create_relationships_tx, chunk)

            print(f"✅ Processed {min(i + BATCH_SIZE, total_rows)}/{total_rows} rows")

    driver.close()
    print("Done! Graph fully loaded from mapping.json 🚀")
