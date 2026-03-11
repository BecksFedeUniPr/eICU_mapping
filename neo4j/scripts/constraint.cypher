// 1. Vincolo di unicità per i Concetti Locali
// Impedisce che esistano due nodi Local_Concept con la stessa coppia (source_value, type)
CREATE CONSTRAINT unique_concept_source IF NOT EXISTS
FOR (c:Local_Concept) REQUIRE (c.source_value, c.type) IS UNIQUE;

// 2. Vincolo di unicità per i Pazienti (opzionale ma consigliato)
CREATE CONSTRAINT unique_patient_id IF NOT EXISTS
FOR (p:Patient) REQUIRE p.uniquepid IS UNIQUE;

// 3. Vincolo per gli Encounter
CREATE CONSTRAINT unique_encounter_id IF NOT EXISTS
FOR (e:Encounter) REQUIRE e.source_reference IS UNIQUE;