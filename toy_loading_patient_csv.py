import pandas as pd
import json
import numpy as np
from neo4j import GraphDatabase
from datetime import datetime, timedelta

# GOAL: load patient.csv, mapped with eICU_mapping.json to Neo4j in local

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