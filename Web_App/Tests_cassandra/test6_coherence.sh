#!/bin/bash

echo "=========================================="
echo "TEST DE COHÉRENCE ET DÉTECTION DE DOUBLONS"
echo "=========================================="

# Fonction pour extraire et afficher les résultats
display_results() {
    local file=$1
    local scenario=$2
    
    echo ""
    echo "Scénario: $scenario"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Extraire les résultats sans jq
    cat "$file" | python3 -c "
import sys, json
data = json.load(sys.stdin)
if data.get('success'):
    print('\nRésultats par Consistency Level:\n')
    for cl in ['ONE', 'QUORUM', 'ALL']:
        if cl in data['results_by_consistency_level']:
            result = data['results_by_consistency_level'][cl]
            print(f'  {cl}:')
            print(f'    Écritures: {result[\"write_performance\"][\"total_writes\"]} en {result[\"write_performance\"][\"write_time_seconds\"]}s')
            print(f'    Latence écriture: {result[\"write_performance\"][\"avg_write_latency_ms\"]} ms (P95: {result[\"write_performance\"][\"p95_write_latency_ms\"]} ms)')
            print(f'    Latence lecture: {result[\"read_performance\"][\"avg_read_latency_ms\"]} ms')
            print(f'    Intégrité: {result[\"consistency_check\"][\"data_integrity\"]}')
            print(f'    Records trouvés: {result[\"consistency_check\"][\"found_records\"]}/{result[\"consistency_check\"][\"expected_records\"]}')
            print(f'    Doublons: {result[\"consistency_check\"][\"duplicate_records\"]}')
            print(f'    Manquants: {result[\"consistency_check\"][\"missing_records\"]}')
            print()
else:
    print(f'Erreur: {data.get(\"error\", \"Unknown error\")}')
"
}

# Test 1: Cohérence en conditions normales (tous nœuds UP)
echo ""
echo "═══════════════════════════════════════"
echo "TEST 1: COHÉRENCE EN CONDITIONS NORMALES"
echo "═══════════════════════════════════════"

docker exec cassandra1 nodetool status

echo ""
echo "Exécution du test avec 1000 écritures..."
curl -X POST http://localhost:5000/cassandra/test/consistency \
  -H "Content-Type: application/json" \
  -d '{"num_writes": 1000}' \
  -o "results_consistency_normal.json"

display_results "results_consistency_normal.json" "Normal (tous nœuds UP)"

# Test 2: Cohérence avec panne d'un nœud
echo ""
echo "═══════════════════════════════════════"
echo "TEST 2: COHÉRENCE AVEC PANNE D'UN NŒUD"
echo "═══════════════════════════════════════"

echo "Arrêt de cassandra3..."
docker stop cassandra3
sleep 30

docker exec cassandra1 nodetool status

echo ""
echo "Exécution du test avec 1000 écritures (1 nœud down)..."
curl -X POST http://localhost:5000/cassandra/test/consistency \
  -H "Content-Type: application/json" \
  -d '{"num_writes": 1000}' \
  -o "results_consistency_1node_down.json"

display_results "results_consistency_1node_down.json" "1 nœud DOWN"

# Test 3: Cohérence avec 2 nœuds down
echo ""
echo "═══════════════════════════════════════"
echo "TEST 3: COHÉRENCE AVEC 2 NŒUDS DOWN"
echo "═══════════════════════════════════════"

echo "Arrêt de cassandra2..."
docker stop cassandra2
sleep 30

docker exec cassandra1 nodetool status

echo ""
echo "Exécution du test avec 1000 écritures (2 nœuds down)..."
curl -X POST http://localhost:5000/cassandra/test/consistency \
  -H "Content-Type: application/json" \
  -d '{"num_writes": 1000}' \
  -o "results_consistency_2nodes_down.json"

display_results "results_consistency_2nodes_down.json" "2 nœuds DOWN"

# Redémarrer les nœuds
echo ""
echo "═══════════════════════════════════════"
echo "RESTAURATION DES NŒUDS"
echo "═══════════════════════════════════════"

echo "Redémarrage de cassandra2..."
docker start cassandra2
sleep 60

echo "Redémarrage de cassandra3..."
docker start cassandra3
sleep 60

docker exec cassandra1 nodetool status

# Test 4: Vérification après récupération
echo ""
echo "═══════════════════════════════════════"
echo "TEST 4: VÉRIFICATION APRÈS RÉCUPÉRATION"
echo "═══════════════════════════════════════"

echo "Lancement du repair..."
docker exec cassandra1 nodetool repair projet_bd_rf3 2>/dev/null || echo "Repair terminé (ou erreur ignorée)"

echo ""
echo "Exécution du test avec 1000 écritures (après recovery)..."
curl -X POST http://localhost:5000/cassandra/test/consistency \
  -H "Content-Type: application/json" \
  -d '{"num_writes": 1000}' \
  -o "results_consistency_after_recovery.json"

display_results "results_consistency_after_recovery.json" "Après récupération"

# Analyse comparative finale
echo ""
echo "═══════════════════════════════════════"
echo "ANALYSE COMPARATIVE GLOBALE"
echo "═══════════════════════════════════════"

python3 << 'EOF'
import json
import os

files = [
    ("Normal", "results_consistency_normal.json"),
    ("1 nœud DOWN", "results_consistency_1node_down.json"),
    ("2 nœuds DOWN", "results_consistency_2nodes_down.json"),
    ("Après recovery", "results_consistency_after_recovery.json")
]

print("\n📊 LATENCES D'ÉCRITURE PAR CONSISTENCY LEVEL (ms)")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print(f"{'Scénario':<20} {'ONE':<12} {'QUORUM':<12} {'ALL':<12}")
print("─" * 60)

for scenario, filename in files:
    if os.path.exists(filename):
        with open(filename) as f:
            data = json.load(f)
            if data.get('success'):
                results = data['results_by_consistency_level']
                one_lat = results['ONE']['write_performance']['avg_write_latency_ms']
                quorum_lat = results['QUORUM']['write_performance']['avg_write_latency_ms']
                all_lat = results['ALL']['write_performance']['avg_write_latency_ms']
                print(f"{scenario:<20} {one_lat:<12.2f} {quorum_lat:<12.2f} {all_lat:<12.2f}")

print("\n\n🔍 INTÉGRITÉ DES DONNÉES")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print(f"{'Scénario':<20} {'CL':<8} {'Statut':<10} {'Doublons':<10} {'Manquants':<10}")
print("─" * 60)

for scenario, filename in files:
    if os.path.exists(filename):
        with open(filename) as f:
            data = json.load(f)
            if data.get('success'):
                results = data['results_by_consistency_level']
                for cl in ['ONE', 'QUORUM', 'ALL']:
                    check = results[cl]['consistency_check']
                    print(f"{scenario:<20} {cl:<8} {check['data_integrity']:<10} {check['duplicate_records']:<10} {check['missing_records']:<10}")

print("\n\n⚖️  COMPROMIS LATENCE VS COHÉRENCE")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print("CL=ONE    : Latence minimale, cohérence éventuelle")
print("CL=QUORUM : Équilibre latence/cohérence (recommandé)")
print("CL=ALL    : Latence maximale, cohérence stricte")

EOF

echo ""
echo "═══════════════════════════════════════"
echo "TEST DE COHÉRENCE TERMINÉ"
echo "═══════════════════════════════════════"
echo "Fichiers de résultats:"
echo "  - results_consistency_normal.json"
echo "  - results_consistency_1node_down.json"
echo "  - results_consistency_2nodes_down.json"
echo "  - results_consistency_after_recovery.json"