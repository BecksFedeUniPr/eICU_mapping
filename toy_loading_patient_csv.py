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
    print("⚠️  PULIZIA DATABASE IN CORSO...")
    with driver.session() as session:
        # DETACH DELETE cancella i nodi e rimuove automaticamente tutte le relazioni collegate
        session.run("MATCH (n) DETACH DELETE n")
    print("✅ Database svuotato correttamente.")

def setup_constraints(driver):
    """Crea i vincoli di unicità basati sul source_reference per TUTTI i nodi"""
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
    if pd.isna(time_24h) or time_24h is None: return None
    try:
        t_str = str(time_24h).strip()
        # Prova con i secondi, se fallisce prova senza
        try:
            base = datetime.strptime(t_str, '%H:%M:%S')
        except ValueError:
            base = datetime.strptime(t_str, '%H:%M')
        if pd.notna(offset_minutes) and offset_minutes is not None:
            base = base + timedelta(minutes=int(float(offset_minutes)))
        return base.isoformat()
    except: 
        return None

def process_batch(driver, batch):
    """Esegue le query Cypher per un singolo blocco di dati"""
    
    # --- PREPARAZIONE JSON E TEMPO ---
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
        row['enc_start_time'] = parse_time_with_offset(row.get('hospitaladmittime24'), row.get('hospitaladmitoffset'))
        row['enc_end_time'] = parse_time_with_offset(row.get('hospitaldischargetime24'), row.get('hospitaldischargeoffset'))
        row['seg_start_time'] = parse_time_with_offset(row.get('unit_admit_time'), 0)
        row['seg_end_time'] = parse_time_with_offset(row.get('unitdischargetime24'), row.get('unitdischargeoffset'))

    with driver.session() as session:
        
        # ==========================================
        # FASE 1: LA BACKBONE
        # ==========================================
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

        # ==========================================
        # FASE 2: LOCATION & RICH EDGES
        # ==========================================
        session.run("""
            UNWIND $batch AS row
            WITH row WHERE row.hospitalid IS NOT NULL
            MATCH (e:Encounter {source_reference: toString(row.patienthealthsystemstayid)})
            MERGE (loc_h:Location {source_reference: 'HOSP_' + toString(row.hospitalid)})
            MERGE (e)-[r_h:HAS_LOCATION]->(loc_h)
            SET r_h.source = row.hospitaladmitsource, 
                r_h.dismission = row.hospitaldischargelocation
            
            WITH row, e
            WHERE row.wardid IS NOT NULL
            MATCH (s:Encounter_Segment {source_reference: toString(row.patientunitstayid)})
            MERGE (loc_w:Location {source_reference: 'HOSP_' + toString(row.hospitalid) + '_WARD_' + toString(row.wardid)})
            MERGE (s)-[r_w:HAS_LOCATION]->(loc_w)
            SET r_w.source = row.unitadmitsource, 
                r_w.dismission = row.unitdischargelocation
        """, batch=batch)

        # ==========================================
        # FASE 3: LOCAL CONCEPTS (Aggiornata!)
        # ==========================================
        p_concepts = []
        e_concepts = []
        s_concepts = []

        for row in batch:
            # Patient: gender, ethnicity
            for col in ['gender', 'ethnicity']:
                if pd.notna(row.get(col)):
                    p_concepts.append({"pid": str(row['uniquepid']), "type": col, "val": str(row[col])})
            
            # Encounter: hospitaldischargestatus
            for col in ['hospitaldischargestatus']:
                if pd.notna(row.get(col)):
                    e_concepts.append({"pid": str(row['patienthealthsystemstayid']), "type": col, "val": str(row[col])})
            
            # Encounter_Segment: unittype, unitstaytype, unitdischargestatus + APACHE
            for col in ['unittype', 'unitstaytype', 'unitdischargestatus', 'apacheadmission', 'apache_dissimions']:
                if pd.notna(row.get(col)):
                    s_concepts.append({"pid": str(row['patientunitstayid']), "type": col, "val": str(row[col])})

        # Query di caricamento
        if p_concepts:
            session.run("UNWIND $c AS c MERGE (n:Local_Concept {source_reference: c.val}) WITH c,n MATCH (p:Patient {source_reference: c.pid}) MERGE (p)-[r:HAS_LOCAL_CONCEPT]->(n) SET r.type = c.type", c=p_concepts)
        if e_concepts:
            session.run("UNWIND $c AS c MERGE (n:Local_Concept {source_reference: c.val}) WITH c,n MATCH (p:Encounter {source_reference: c.pid}) MERGE (p)-[r:HAS_LOCAL_CONCEPT]->(n) SET r.type = c.type", c=e_concepts)
        if s_concepts:
            session.run("UNWIND $c AS c MERGE (n:Local_Concept {source_reference: c.val}) WITH c,n MATCH (p:Encounter_Segment {source_reference: c.pid}) MERGE (p)-[r:HAS_LOCAL_CONCEPT]->(n) SET r.type = c.type", c=s_concepts)

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