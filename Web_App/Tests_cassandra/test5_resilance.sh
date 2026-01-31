#!/bin/bash

echo "=========================================="
echo "TEST DE RÉSILIENCE ET TOLÉRANCE AUX FAUTES"
echo "=========================================="

# Fonction pour tester avec un consistency level
test_consistency() {
    local cl=$1
    local scenario=$2
    
    echo ""
    echo "▶ Test avec Consistency Level: $cl"
    echo "  Scénario: $scenario"
    echo "----------------------------------------"
    
    curl -X POST http://localhost:5000/cassandra/test/resilience \
      -H "Content-Type: application/json" \
      -d "{\"consistency_level\": \"$cl\", \"num_operations\": 1000}" \
      -o "results_resilience_${cl}_${scenario}.json"
    
    echo ""
    echo "Résultats:"
    cat "results_resilience_${cl}_${scenario}.json"
    
    echo ""
}

# Phase 1: Tous les nœuds UP
echo ""
echo "═══════════════════════════════════════"
echo "PHASE 1: CLUSTER NORMAL (tous nœuds UP)"
echo "═══════════════════════════════════════"

docker exec cassandra1 nodetool status

test_consistency "ONE" "all_up"
test_consistency "QUORUM" "all_up"
test_consistency "ALL" "all_up"

# Phase 2: Arrêter 1 nœud (RF=3, 2 nœuds restants)
echo ""
echo "═══════════════════════════════════════"
echo "PHASE 2: PANNE D'1 NŒUD (cassandra3 DOWN)"
echo "═══════════════════════════════════════"

echo "Arrêt de cassandra3..."
docker stop cassandra3

echo "Attente de détection de la panne (30 sec)..."
sleep 30

docker exec cassandra1 nodetool status

test_consistency "ONE" "1node_down"
test_consistency "QUORUM" "1node_down"
test_consistency "ALL" "1node_down"

# Phase 3: Arrêter 2 nœuds (RF=3, 1 nœud restant)
echo ""
echo "═══════════════════════════════════════"
echo "PHASE 3: PANNE DE 2 NŒUDS (cassandra2&3 DOWN)"
echo "═══════════════════════════════════════"

echo "Arrêt de cassandra2..."
docker stop cassandra2

echo "Attente de détection (30 sec)..."
sleep 30

docker exec cassandra1 nodetool status

test_consistency "ONE" "2nodes_down"
test_consistency "QUORUM" "2nodes_down"
test_consistency "ALL" "2nodes_down"

# Phase 4: Restauration progressive
echo ""
echo "═══════════════════════════════════════"
echo "PHASE 4: RESTAURATION PROGRESSIVE"
echo "═══════════════════════════════════════"

echo "Redémarrage de cassandra2..."
docker start cassandra2
sleep 60

docker exec cassandra1 nodetool status

test_consistency "QUORUM" "recovery_1node"

echo ""
echo "Redémarrage de cassandra3..."
docker start cassandra3
sleep 60

docker exec cassandra1 nodetool status

test_consistency "QUORUM" "recovery_complete"

# Phase 5: Mesure du temps de reprise
echo ""
echo "═══════════════════════════════════════"
echo "PHASE 5: TEMPS DE REPRISE"
echo "═══════════════════════════════════════"

echo "Lancement du repair pour resynchroniser..."
repair_start=$(date +%s)
docker exec cassandra1 nodetool repair projet_bd_rf3
repair_end=$(date +%s)
repair_duration=$((repair_end - repair_start))

echo "Temps de repair: $repair_duration secondes"

# Analyse comparative
echo ""
echo "═══════════════════════════════════════"
echo "ANALYSE COMPARATIVE"
echo "═══════════════════════════════════════"

echo ""
echo "Disponibilité des écritures (%) :"
for f in results_resilience_*.json; do
    scenario=$(basename $f .json | cut -d'_' -f3-)
    availability=$(grep -o '"availability_percent"[[:space:]]*:[[:space:]]*[0-9.]*' "$f" | head -1 | grep -o '[0-9.]*$')
    echo "  $scenario: ${availability:-0}%"
done

echo ""
echo "Disponibilité des lectures (%) :"
for f in results_resilience_*.json; do
    scenario=$(basename $f .json | cut -d'_' -f3-)
    availability=$(grep -o '"availability_percent"[[:space:]]*:[[:space:]]*[0-9.]*' "$f" | tail -1 | grep -o '[0-9.]*$')
    echo "  $scenario: ${availability:-0}%"
done

echo ""
echo "═══════════════════════════════════════"
echo "TEST DE RÉSILIENCE TERMINÉ"
echo "═══════════════════════════════════════"
echo "Temps de reprise total: $repair_duration secondes"