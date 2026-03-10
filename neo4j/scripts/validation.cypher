// A. Conta quanti pazienti e quanti concetti abbiamo
MATCH (p:Patient) WITH count(p) as pazienti
MATCH (c:Local_Concept) WITH pazienti, count(c) as concetti
RETURN pazienti, concetti;

// B. Verifica la distribuzione dei tipi di concetto nelle relazioni
MATCH ()-[r:HAS_LOCAL_CONCEPT]->(c:Local_Concept)
RETURN r.type AS Categoria, count(DISTINCT c) AS Numero_Nodi_Hub, count(r) AS Totale_Collegamenti
ORDER BY Totale_Collegamenti DESC;

// C. Cerca eventuali concetti rimasti "orfani" (senza relazioni)
MATCH (c:Local_Concept)
WHERE NOT (c)<--()
RETURN c.source_value, c.type;