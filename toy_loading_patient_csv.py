import pandas as pd
import json
import numpy as np
import os
from collections import defaultdict
from neo4j import GraphDatabase
from datetime import datetime, timedelta

# GOAL: load patient.csv fully driven by mapping.json (nodes + relationships)

URI      = os.getenv("NEO4J_URI",      "neo4j://localhost:7687")
USER     = os.getenv("NEO4J_USER",     "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "Mediverse")

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def load_mapping_config(mapping_path):
    """Load the mapping JSON file"""
    with open(mapping_path, 'r') as f:
        return json.load(f)

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

# ─────────────────────────────────────────────
# SOURCE REFERENCE RESOLUTION
# ─────────────────────────────────────────────

def is_derived_node(node_def):
    """True for nodes whose source_reference is derived from column values (Local_Concept, Location)."""
    return node_def.get('source_reference') == '__derived_from_column_value__'

def get_node_source_ref(node_name, node_def, row):
    """
    Return a globally unique source_reference for this node type + row.
    Returns None for derived nodes (they are created separately).
    """
    raw = node_def.get('source_reference', '')
    if not raw or raw == '__derived_from_column_value__':
        return None
    val = row.get(raw)
    if val is None:
        return None
    return f"{node_name.upper()}_{val}"

# ─────────────────────────────────────────────
# LOCAL CONCEPT NODES (Patient/Encounter columns → concept nodes)
# ─────────────────────────────────────────────

# Columns to map to Local_Concept nodes for each node type
LOCAL_CONCEPT_COLUMNS = {
    "Patient":           ["gender", "ethnicity"],
    "Encounter":         ["hospitaldischargestatus", "hospitaldischargelocation"],
    "Encounter_Segment": ["unittype", "unitadmitsource", "unitstaytype",
                          "unitdischargelocation", "unitdischargestatus"],
}

def get_local_concept_nodes(row):
    """
    Build a list of {source_reference, concept_label, name} dicts
    for every categorical column that has a non-null value.
    source_reference = LOCALCONCEPT_{column_name}_{value}
    """
    nodes = {}
    for node_type, columns in LOCAL_CONCEPT_COLUMNS.items():
        for col in columns:
            val = row.get(col)
            if val is None:
                continue
            if isinstance(val, float) and np.isnan(val):
                continue
            val_str = str(val).strip()
            if not val_str or val_str.lower() == 'nan':
                continue
            ref = f"LOCALCONCEPT_{col}_{val_str}"
            if ref not in nodes:
                nodes[ref] = {
                    "source_reference": ref,
                    "concept_label": col,
                    "name": val_str
                }
    return list(nodes.values())

def get_local_concept_rels(node_name, node_def, row):
    """
    Build HAS_LOCAL_CONCEPT relationships from a node to its Local_Concept nodes.
    The relationship carries a 'type' property = column_name.
    """
    src_ref = get_node_source_ref(node_name, node_def, row)
    if not src_ref:
        return []
    rels = []
    for col in LOCAL_CONCEPT_COLUMNS.get(node_name, []):
        val = row.get(col)
        if val is None:
            continue
        if isinstance(val, float) and np.isnan(val):
            continue
        val_str = str(val).strip()
        if not val_str or val_str.lower() == 'nan':
            continue
        concept_ref = f"LOCALCONCEPT_{col}_{val_str}"
        rels.append({
            "from_label":  node_name,
            "from_ref":    src_ref,
            "to_label":    "Local_Concept",
            "to_ref":      concept_ref,
            "type":        "HAS_LOCAL_CONCEPT",
            "props":       {"type": col}
        })
    return rels

# ─────────────────────────────────────────────
# LOCATION NODES
#   - Hospital  : identified by hospitalid alone   → LOCATION_H_{hospitalid}
#   - Ward      : identified by hospitalid+wardid  → LOCATION_HW_{hospitalid}_{wardid}
# ─────────────────────────────────────────────

def _safe_str(val):
    """Return stripped string or None if val is null/nan/empty."""
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    s = str(val).strip()
    return s if s and s.lower() != 'nan' else None

def get_location_nodes(row):
    """
    Build Location nodes from the current row.
      - Encounter         → hospital node  (hospitalid)
      - Encounter_Segment → ward node      (hospitalid + wardid)
    """
    nodes = {}

    # Hospital-level location (owned by Encounter)
    hosp = _safe_str(row.get('hospitalid'))
    if hosp:
        ref = f"LOCATION_H_{hosp}"
        if ref not in nodes:
            nodes[ref] = {
                "source_reference": ref,
                "location_type":    "hospital",
                "hospitalid":       hosp,
            }

    # Ward-level location (owned by Encounter_Segment, scoped to hospital)
    ward = _safe_str(row.get('wardid'))
    if hosp and ward:
        ref = f"LOCATION_HW_{hosp}_{ward}"
        if ref not in nodes:
            nodes[ref] = {
                "source_reference": ref,
                "location_type":    "ward",
                "hospitalid":       hosp,
                "wardid":           ward,
            }

    return list(nodes.values())

def get_location_rels(node_name, node_def, row):
    """Build HAS_LOCATION relationships from a node to its Location nodes."""
    src_ref = get_node_source_ref(node_name, node_def, row)
    if not src_ref:
        return []

    hosp = _safe_str(row.get('hospitalid'))
    ward = _safe_str(row.get('wardid'))
    rels = []

    if node_name == "Encounter" and hosp:
        rels.append({
            "from_label": "Encounter",
            "from_ref":   src_ref,
            "to_label":   "Location",
            "to_ref":     f"LOCATION_H_{hosp}",
            "type":       "HAS_LOCATION",
            "props":      {}
        })

    if node_name == "Encounter_Segment" and hosp and ward:
        rels.append({
            "from_label": "Encounter_Segment",
            "from_ref":   src_ref,
            "to_label":   "Location",
            "to_ref":     f"LOCATION_HW_{hosp}_{ward}",
            "type":       "HAS_LOCATION",
            "props":      {}
        })

    return rels

# ─────────────────────────────────────────────
# RELATIONSHIP PROPERTIES
# ─────────────────────────────────────────────

def resolve_rel_properties(rel_def, row):
    """
    Resolve the optional 'properties' block of a relationship definition.
    Values are CSV column names whose values are fetched from the row.
    Returns a dict (may be empty).
    """
    props = {}
    for prop_key, csv_col in rel_def.get('properties', {}).items():
        if csv_col.startswith('__'):
            continue  # handled elsewhere
        val = row.get(csv_col)
        if val is not None and not (isinstance(val, float) and np.isnan(val)):
            props[prop_key] = val
    return props

# ─────────────────────────────────────────────
# DATA PREPARATION  (driven by mapping JSON)
# ─────────────────────────────────────────────

def load_and_prep_data(csv_path, mapping_config):
    """
    Read the CSV and build, for each row:
      - one dict per node type (with source_reference, properties, payload, timestamps)
      - one list of relationships (from_ref, to_ref, type, props)
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
            # Derived nodes (Local_Concept, Location) are built separately
            if is_derived_node(node_def):
                continue

            src_ref = get_node_source_ref(node_name, node_def, row)
            if src_ref is None:
                continue

            node_data = {"source_reference": src_ref}

            # Timestamps
            for t_key in ('start_time', 'end_time', 'timestamp'):
                if t_key in node_def:
                    node_data[t_key] = parse_time_expr(node_def[t_key], row)

            # Payload
            payload_fields = node_def.get('payload', [])
            if payload_fields:
                node_data['payload'] = build_payload(row, payload_fields)

            # Direct property mappings (keys whose values are CSV column names)
            skip_keys = {'source_reference', 'type', 'concept_id', 'concept_label',
                         'payload', 'start_time', 'end_time', 'timestamp'}
            for prop, csv_col in node_def.items():
                if prop not in skip_keys and isinstance(csv_col, str) and csv_col in row:
                    node_data[prop] = row[csv_col]

            record[node_name] = node_data

        # ── Local_Concept nodes ──
        record['Local_Concept'] = get_local_concept_nodes(row)

        # ── Location nodes ──
        record['Location'] = get_location_nodes(row)

        # ── Build relationships from mapping ──
        rel_list = []
        for rel in rel_defs:
            from_name = rel['from']
            to_name   = rel['to']
            rel_type  = rel['type']
            from_def  = nodes_map.get(from_name)
            to_def    = nodes_map.get(to_name)
            if from_def is None or to_def is None:
                continue

            # Derived nodes (Local_Concept, Location) are wired separately below
            if is_derived_node(from_def) or is_derived_node(to_def):
                continue

            from_ref = get_node_source_ref(from_name, from_def, row)
            to_ref   = get_node_source_ref(to_name,   to_def,   row)

            if from_ref and to_ref:
                props = resolve_rel_properties(rel, row)
                rel_list.append({
                    "from_label": from_name,
                    "from_ref":   from_ref,
                    "to_label":   to_name,
                    "to_ref":     to_ref,
                    "type":       rel_type,
                    "props":      props
                })

        # ── HAS_LOCAL_CONCEPT relationships ──
        for node_name, node_def in nodes_map.items():
            if is_derived_node(node_def):
                continue
            rel_list.extend(get_local_concept_rels(node_name, node_def, row))

        # ── HAS_LOCATION relationships ──
        for node_name, node_def in nodes_map.items():
            if is_derived_node(node_def):
                continue
            rel_list.extend(get_location_rels(node_name, node_def, row))

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
        has_props = any(r.get('props') for r in rels)
        if has_props:
            query = f"""
            UNWIND $rels AS rel
            MATCH (a:{from_label} {{source_reference: rel.from_ref}})
            MATCH (b:{to_label}   {{source_reference: rel.to_ref}})
            MERGE (a)-[r:{rel_type}]->(b)
            ON CREATE SET r += rel.props
            """
        else:
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
    # Extra constraint for derived Location nodes
    tx.run(
        "CREATE CONSTRAINT location_src_ref IF NOT EXISTS "
        "FOR (n:Location) REQUIRE n.source_reference IS UNIQUE;"
    )

def cleanup_stale_relationships(session, mapping_config):
    """
    Delete any relationship types that exist in Neo4j but are no longer
    defined in the current mapping (stale types from previous mapping versions).
    """
    valid_types = {r['type'] for r in mapping_config['relationships']}
    valid_types.update({'HAS_LOCAL_CONCEPT', 'HAS_LOCATION'})

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
    # All node labels to write (mapping-defined + derived)
    node_labels = list(mapping_config['nodes'].keys()) + ["Local_Concept", "Location"]
    # Remove duplicates while preserving order
    seen_labels: set = set()
    unique_labels = []
    for lbl in node_labels:
        if lbl not in seen_labels:
            seen_labels.add(lbl)
            unique_labels.append(lbl)
    node_labels = unique_labels

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as session:

        print("🧹 Cleaning up stale relationship types...")
        cleanup_stale_relationships(session, mapping_config)

        session.execute_write(setup_constraints, mapping_config)
        print("✅ Constraints created")

        for i in range(0, total_rows, BATCH_SIZE):
            chunk = data[i: i + BATCH_SIZE]

            # 1. Create all node types (including Local_Concept + Location)
            for label in node_labels:
                session.execute_write(create_nodes_tx, chunk, label)

            # 2. Create all relationships
            session.execute_write(create_relationships_tx, chunk)

            print(f"✅ Processed {min(i + BATCH_SIZE, total_rows)}/{total_rows} rows")

    driver.close()
    print("Done! Graph fully loaded from mapping.json 🚀")
