import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timedelta
from neo4j import GraphDatabase

# ─────────────────────────────────────────────
# CONFIGURAZIONE DATABASE
# ─────────────────────────────────────────────
URI      = os.getenv("NEO4J_URI",      "neo4j://localhost:7687")
USER     = os.getenv("NEO4J_USER",     "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "Mediverse")

def clear_database(driver):
    """Cancella TUTTI i nodi e le relazioni nel database"""
    print("⚠️ PULIZIA DATABASE IN CORSO...")
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("✅ Database svuotato correttamente.")

def setup_constraints(driver):
    """Crea i vincoli di unicità"""
    queries = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Patient) REQUIRE n.source_reference IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Encounter) REQUIRE n.source_reference IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Encounter_Segment) REQUIRE n.source_reference IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Location) REQUIRE n.source_reference IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Local_Concept) REQUIRE n.source_reference IS UNIQUE"
    ]
    with driver.session() as session:
        for q in queries:
            session.run(q)
    print("✅ Vincoli di unicità impostati.")

def parse_time_with_offset(time_24h, offset_minutes):
    """Calcola l'orario e restituisce SOLO l'ora (HH:MM:SS)"""
    if pd.isna(time_24h) or time_24h is None: return None
    try:
        t_str = str(time_24h).strip()
        # Gestisce sia HH:MM:SS che HH:MM
        try:
            base = datetime.strptime(t_str, '%H:%M:%S')
        except ValueError:
            base = datetime.strptime(t_str, '%H:%M')
        
        if pd.notna(offset_minutes) and offset_minutes is not None:
            base = base + timedelta(minutes=int(float(offset_minutes)))
        
        # RESTITUISCE SOLO L'ORA (HH:MM:SS)
        return base.strftime('%H:%M:%S')
    except: 
        return None

def process_batch(driver, batch):
    """Esegue le query Cypher per un singolo blocco di dati"""
    
    # --- PREPARAZIONE JSON E SOLO ORA ---
    for row in batch:
        payload_enc = {
            "admissionheight": row.get("admissionheight"),
            "hospitaldischargeyear": row.get("hospitaldischargeyear"),
            "admissionweight": row.get("admissionweight"),
            "dischargeweight": row.get("dischargeweight")
        }
        payload_seg = {"unitvisitnumber": row.get("unitvisitnumber")}
        
        row['payload_encounter'] = json.dumps(payload_enc)
        row['payload_segment'] = json.dumps(payload_seg)
        
        # Calcolo orari (restituiscono solo HH:MM:SS)
        row['enc_start_time'] = parse_time_with_offset(row.get('hospitaladmittime24'), row.get('hospitaladmitoffset'))
        row['enc_end_time'] = parse_time_with_offset(row.get('hospitaldischargetime24'), row.get('hospitaldischargeoffset'))
        row['seg_start_time'] = parse_time_with_offset(row.get('unitadmittime24'), 0)
        row['seg_end_time'] = parse_time_with_offset(row.get('unitdischargetime24'), row.get('unitdischargeoffset'))

    with driver.session() as session:
        # FASE 1: BACKBONE
        session.run("""
            UNWIND $batch AS row
            MERGE (p:Patient {source_reference: toString(row.uniquepid)})
            SET p.age = row.age
            
            MERGE (e:Encounter {source_reference: toString(row.patienthealthsystemstayid)})
            SET e.type = 'Encounter', e.concept_label = null, e.concept_id = null,
                e.start_time = row.enc_start_time, e.end_time = row.enc_end_time,
                e.payload = row.payload_encounter
                
            MERGE (s:Encounter_Segment {source_reference: toString(row.patientunitstayid)})
            SET s.type = 'Encounter Segment', s.concept_label = null, s.concept_id = null,
                s.start_time = row.seg_start_time, s.end_time = row.seg_end_time,
                s.payload = row.payload_segment
                
            MERGE (p)-[:HAS_EVENT]->(e)
            MERGE (e)-[:HAS_EVENT]->(s)
            MERGE (p)-[:HAS_EVENT]->(s)
        """, batch=batch)

        session.run("""
            UNWIND $batch AS row
            MATCH (e:Encounter {source_reference: toString(row.patienthealthsystemstayid)})
            
            // 1. Hospital Admit Source
            FOREACH (_ IN CASE WHEN row.hospitaladmitsource IS NOT NULL THEN [1] ELSE [] END |
                MERGE (ha:Location {value: row.hospitaladmitsource})
                MERGE (e)-[r:HAS_LOCATION]->(ha)
                SET r.source = "hospitaladmitsource"
            )
            
            // 2. Hospital Discharge Location
            FOREACH (_ IN CASE WHEN row.hospitaldischargelocation IS NOT NULL THEN [1] ELSE [] END |
                MERGE (hd:Location {value: row.hospitaldischargelocation})
                MERGE (e)-[r:HAS_LOCATION]->(hd)
                SET r.source = "hospitaldischargelocation"
            )
            
            // 3. Unit Admit Source
            FOREACH (_ IN CASE WHEN row.unitadmitsource IS NOT NULL THEN [1] ELSE [] END |
                MERGE (ua:Location {value: row.unitadmitsource})
                MERGE (e)-[r:HAS_LOCATION]->(ua)
                SET r.source = "unitadmitsource"
            )
            
            // 4. Unit Discharge Location
            FOREACH (_ IN CASE WHEN row.unitdischargelocation IS NOT NULL THEN [1] ELSE [] END |
                MERGE (ud:Location {value: row.unitdischargelocation})
                MERGE (e)-[r:HAS_LOCATION]->(ud)
                SET r.source = "unitdischargelocation"
            )
        """, batch=batch)

        # FASE 3: LOCAL CONCEPTS
        p_concepts, e_concepts, s_concepts = [], [], []
        for row in batch:
            for col in ['gender', 'ethnicity']:
                if pd.notna(row.get(col)): p_concepts.append({"pid": str(row['uniquepid']), "type": col, "val": str(row[col])})
            for col in ['hospitaldischargestatus']:
                if pd.notna(row.get(col)): e_concepts.append({"pid": str(row['patienthealthsystemstayid']), "type": col, "val": str(row[col])})
            for col in ['unittype', 'unitstaytype', 'unitdischargestatus', 'apacheadmission', 'apache_dissimions']:
                if pd.notna(row.get(col)): s_concepts.append({"pid": str(row['patientunitstayid']), "type": col, "val": str(row[col])})

        if p_concepts: session.run("UNWIND $c AS c MERGE (n:Local_Concept {source_reference: c.val}) WITH c,n MATCH (p:Patient {source_reference: c.pid}) MERGE (p)-[r:HAS_LOCAL_CONCEPT]->(n) SET r.type = c.type", c=p_concepts)
        if e_concepts: session.run("UNWIND $c AS c MERGE (n:Local_Concept {source_reference: c.val}) WITH c,n MATCH (p:Encounter {source_reference: c.pid}) MERGE (p)-[r:HAS_LOCAL_CONCEPT]->(n) SET r.type = c.type", c=e_concepts)
        if s_concepts: session.run("UNWIND $c AS c MERGE (n:Local_Concept {source_reference: c.val}) WITH c,n MATCH (p:Encounter_Segment {source_reference: c.pid}) MERGE (p)-[r:HAS_LOCAL_CONCEPT]->(n) SET r.type = c.type", c=s_concepts)

def main():
    csv_file = os.getenv("CSV_FILE", "neo4j/import/patient.csv")
    df = pd.read_csv(csv_file, dtype=str).replace({np.nan: None})
    records = df.to_dict('records')
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        clear_database(driver)
        setup_constraints(driver)
        BATCH_SIZE = 5000
        for i in range(0, len(records), BATCH_SIZE):
            process_batch(driver, records[i:i + BATCH_SIZE])
            print(f"⚙️ Processate {min(i + BATCH_SIZE, len(records))} righe...")
    finally:
        driver.close()
        print("🏁 Caricamento completato!")

if __name__ == "__main__":
    main()