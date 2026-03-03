# Mediverse — eICU → Neo4j Loader

Loads the **eICU Collaborative Research Database** `patient.csv` into a Neo4j graph
fully driven by `mapping.json`, with no hard-coded Cypher.

---

## Project Structure

```
project-root/
│
├── mapping.json           # Graph mapping definition (nodes + relationships)
├── README.md              # This file
├── docker-compose.yml     # Neo4j + loader services
├── requirements.txt       # Python dependencies
├── toy_loading_patient_csv.py  # ETL loader script
└── neo4j/
    ├── import/            # Drop sample / full CSV files here
    └── scripts/           # Cypher utility scripts
```

---

## E-R Diagram

```mermaid
graph TD
    PAT["🧑 Patient\n──────\nsource_ref: uniquepid\nage · gender · ethnicity"]
    ENC["🏥 Encounter\n──────\nsource_ref: patienthealthsystemstayid\nstart_time · end_time"]
    SEG["🛏️ Encounter_Segment\n──────\nsource_ref: patientunitstayid\nstart_time · end_time\nunittype · unitstaytype"]
    HADM["📥 HospitalAdmission\n──────\nstart_time\nhospitaladmitsource\nadmissionheight · admissionweight"]
    HDIS["📤 HospitalDischarge\n──────\ntimestamp\nhospitaldischargestatus\nhospitaldischargelocation"]
    ICUA["🚨 ICU_Admit\n──────\ntimestamp\nunitadmitsource · unittype"]
    ICUD["🔔 ICU_Discharge\n──────\ntimestamp\nunitadmitsource · unittype"]
    LHOSP["📍 Location_Hospital\n──────\nsource_ref: HOSP_{hospitalid}"]
    LWARD["📍 Location_Ward\n──────\nsource_ref: HOSP_{hospitalid}_Ward_{wardid}"]
    LADM["📍 Location_admit_discharge\n──────\nhospitaladmitsource\nhospitaldischargelocation"]

    PAT -->|HAS_EVENT| ENC
    PAT -->|HAS_ENCOUNTER_SEGMENT| SEG

    ENC -->|HAS_SEGMENT| SEG
    ENC -->|HAS_LOCATION| LHOSP
    ENC -->|HAS_Hospital_ADMISSION| HADM
    ENC -->|HAS_Hospital_DISMISSION| HDIS

    SEG -->|HAS_LOCATION| LWARD
    SEG -->|HAS_ICU_ADMISSION| ICUA
    SEG -->|HAS_ICU_DISMISSION| ICUD

    ICUA -->|HAS_LOCATION| LWARD
    ICUD -->|HAS_LOCATION| LWARD

    HADM -->|HAS_LOCATION| LADM
    HDIS -->|HAS_LOCATION| LADM
```

---

## Quick Start

```bash
# 1. Drop the eICU patient.csv into neo4j/import/
cp /path/to/eicu/patient.csv neo4j/import/

# 2. Start Neo4j + run the loader
docker compose up

# 3. Open the browser
open http://localhost:7474
```

Default credentials: `neo4j / Mediverse`

---

## Mapping

All graph topology is defined in `mapping.json`:

| Key | Purpose |
|-----|---------|
| `nodes` | Node labels, source CSV keys, properties, timestamps, payloads |
| `relationships` | Edge types, join keys between node pairs |

To extend the model, add entries to `mapping.json` — no code changes needed.

---

## Requirements

- Docker + Docker Compose
- eICU `patient.csv` (place in `neo4j/import/`)
