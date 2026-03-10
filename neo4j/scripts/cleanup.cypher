cleanup.cypher// Fonde i nodi Local_Concept duplicati mantenendo tutte le relazioni
MATCH (c:Local_Concept)
WITH c.source_value AS val, collect(c) AS nodes
WHERE size(nodes) > 1
CALL apoc.refactor.mergeNodes(nodes, {properties: "overwrite", mergeRels: true})
YIELD node
RETURN node.source_value AS valore_ripulito, count(*) AS check;