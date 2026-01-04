#!/bin/bash

echo "=========================================="
echo "TEST DE SCALABILITÉ HORIZONTALE"
echo "=========================================="

# Fonction pour extraire les valeurs JSON avec Python
parse_json() {
    python3 -c "import json, sys; data=json.load(sys.stdin); print($1)" 2>/dev/null || echo "0"
}

# Fonction pour tester les performances
test_performance() {
    local num_nodes=$1
    local phase=$2
    
    echo ""
    echo "▶ Phase $phase : Test avec $num_nodes nœuds"
    echo "----------------------------------------"
    
    # Attendre que le cluster soit stable
    echo "Attente de stabilisation du cluster..."
    sleep 30
    
    # Vérifier le statut du cluster
    echo "Statut du cluster :"
    docker exec cassandra1 nodetool status
    
    # Test Write
    echo ""
    echo "Test d'écriture (5000 ops)..."
    curl -s -X POST http://localhost:5000/cassandra/test/scalability \
      -H "Content-Type: application/json" \
      -d '{"test_type": "write", "num_operations": 5000}' \
      -o "results_write_${num_nodes}nodes.json"
    
    if [ $? -eq 0 ]; then
        echo "Résultats écriture:"
        cat "results_write_${num_nodes}nodes.json" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    write = data.get('results', {}).get('write_test', {})
    print(f\"  - Operations: {write.get('total_operations', 'N/A')} ops\")
    print(f\"  - Throughput: {write.get('throughput_ops_per_sec', 'N/A')} ops/sec\")
    print(f\"  - Latence moyenne: {write.get('avg_latency_ms', 'N/A')} ms\")
    print(f\"  - Latence P95: {write.get('p95_latency_ms', 'N/A')} ms\")
    print(f\"  - Latence P99: {write.get('p99_latency_ms', 'N/A')} ms\")
except Exception as e:
    print(f'Erreur parsing: {e}')
"
    else
        echo "❌ Erreur lors du test d'écriture"
    fi
    
    # Test Read
    echo ""
    echo "Test de lecture (500 ops)..."
    curl -s -X POST http://localhost:5000/cassandra/test/scalability \
      -H "Content-Type: application/json" \
      -d '{"test_type": "read", "num_operations": 500}' \
      -o "results_read_${num_nodes}nodes.json"
    
    if [ $? -eq 0 ]; then
        echo "Résultats lecture:"
        cat "results_read_${num_nodes}nodes.json" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    read = data.get('results', {}).get('read_test', {})
    print(f\"  - Operations: {read.get('total_operations', 'N/A')} ops\")
    print(f\"  - Throughput: {read.get('throughput_ops_per_sec', 'N/A')} ops/sec\")
    print(f\"  - Latence moyenne: {read.get('avg_latency_ms', 'N/A')} ms\")
    print(f\"  - Latence P95: {read.get('p95_latency_ms', 'N/A')} ms\")
    print(f\"  - Latence P99: {read.get('p99_latency_ms', 'N/A')} ms\")
except Exception as e:
    print(f'Erreur parsing: {e}')
"
    else
        echo "❌ Erreur lors du test de lecture"
    fi
    
    # Test Mixte
    echo ""
    echo "Test mixte (3000 ops)..."
    curl -s -X POST http://localhost:5000/cassandra/test/scalability \
      -H "Content-Type: application/json" \
      -d '{"test_type": "mixed", "num_operations": 3000}' \
      -o "results_mixed_${num_nodes}nodes.json"
    
    if [ $? -eq 0 ]; then
        echo "Résultats mixte:"
        cat "results_mixed_${num_nodes}nodes.json" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    results = data.get('results', {})
    cluster = results.get('cluster_info', {})
    cpu_mem = results.get('cpu_memory', [])
    
    print(f\"  - Nombre de nœuds: {cluster.get('num_nodes', 'N/A')}\")
    print(f\"  - Replication factor: {cluster.get('replication_factor', 'N/A')}\")
    
    if cpu_mem:
        print(\"  - CPU/Mémoire par nœud:\")
        for node in cpu_mem:
            print(f\"    * {node.get('node', 'N/A')}: CPU {node.get('cpu_percent', 'N/A')}%, MEM {node.get('memory_percent', 'N/A')}%\")
except Exception as e:
    print(f'Erreur parsing: {e}')
"
    else
        echo "❌ Erreur lors du test mixte"
    fi
    
    # Métriques cluster
    echo ""
    echo "Statistiques du cluster :"
    docker exec cassandra1 nodetool tablestats projet_bd_rf3.trips_by_borough_time > "stats_${num_nodes}nodes.txt" 2>&1
    docker exec cassandra1 nodetool tpstats > "tpstats_${num_nodes}nodes.txt" 2>&1
    
    echo "✓ Phase $phase terminée"
    echo ""
}

# Baseline : 3 nœuds
echo ""
echo "═══════════════════════════════════════"
echo "BASELINE : 3 NŒUDS"
echo "═══════════════════════════════════════"
test_performance 3 "1-BASELINE"

# Ajouter le 4ème nœud
echo ""
echo "═══════════════════════════════════════"
echo "AJOUT DU 4ÈME NŒUD"
echo "═══════════════════════════════════════"
docker-compose up -d cassandra4
echo "Attente du démarrage du nœud 4 (120 secondes)..."
sleep 120

echo "Vérification du statut..."
docker exec cassandra1 nodetool status

echo "Lancement du repair pour redistribuer les données..."
docker exec cassandra1 nodetool repair projet_bd_rf3

test_performance 4 "2-AFTER-NODE4"

# Ajouter le 5ème nœud
echo ""
echo "═══════════════════════════════════════"
echo "AJOUT DU 5ÈME NŒUD"
echo "═══════════════════════════════════════"
docker-compose up -d cassandra5
echo "Attente du démarrage du nœud 5 (120 secondes)..."
sleep 120

echo "Vérification du statut..."
docker exec cassandra1 nodetool status

echo "Lancement du repair..."
docker exec cassandra1 nodetool repair projet_bd_rf3

test_performance 5 "3-AFTER-NODE5"

# Ajouter le 6ème nœud
echo ""
echo "═══════════════════════════════════════"
echo "AJOUT DU 6ÈME NŒUD"
echo "═══════════════════════════════════════"
docker-compose up -d cassandra6
echo "Attente du démarrage du nœud 6 (120 secondes)..."
sleep 120

echo "Vérification du statut..."
docker exec cassandra1 nodetool status

echo "Lancement du repair..."
docker exec cassandra1 nodetool repair projet_bd_rf3

test_performance 6 "4-AFTER-NODE6"

# Analyse comparative
echo ""
echo "═══════════════════════════════════════"
echo "ANALYSE COMPARATIVE"
echo "═══════════════════════════════════════"

echo ""
echo "Création du rapport comparatif..."

python3 << 'EOF'
import json
import glob

print("\n📊 RAPPORT DE SCALABILITÉ\n")
print("="*60)

# Collecte des résultats
results_summary = {}

for filepath in sorted(glob.glob("results_write_*nodes.json")):
    try:
        nodes = filepath.split("_")[2].replace("nodes.json", "")
        with open(filepath, 'r') as f:
            data = json.load(f)
            write_test = data.get('results', {}).get('write_test', {})
            
            if nodes not in results_summary:
                results_summary[nodes] = {}
            
            results_summary[nodes]['write'] = {
                'throughput': write_test.get('throughput_ops_per_sec', 0),
                'latency': write_test.get('avg_latency_ms', 0),
                'p95': write_test.get('p95_latency_ms', 0)
            }
    except Exception as e:
        print(f"Erreur lecture {filepath}: {e}")

for filepath in sorted(glob.glob("results_read_*nodes.json")):
    try:
        nodes = filepath.split("_")[2].replace("nodes.json", "")
        with open(filepath, 'r') as f:
            data = json.load(f)
            read_test = data.get('results', {}).get('read_test', {})
            
            if nodes not in results_summary:
                results_summary[nodes] = {}
            
            results_summary[nodes]['read'] = {
                'throughput': read_test.get('throughput_ops_per_sec', 0),
                'latency': read_test.get('avg_latency_ms', 0),
                'p95': read_test.get('p95_latency_ms', 0)
            }
    except Exception as e:
        print(f"Erreur lecture {filepath}: {e}")

# Affichage
print("\n📝 THROUGHPUT D'ÉCRITURE (ops/sec)")
print("-" * 60)
for nodes in sorted(results_summary.keys(), key=int):
    throughput = results_summary[nodes].get('write', {}).get('throughput', 0)
    print(f"  {nodes} nœuds: {throughput:.2f} ops/sec")

print("\n📝 LATENCE MOYENNE D'ÉCRITURE (ms)")
print("-" * 60)
for nodes in sorted(results_summary.keys(), key=int):
    latency = results_summary[nodes].get('write', {}).get('latency', 0)
    print(f"  {nodes} nœuds: {latency:.2f} ms")

print("\n📝 LATENCE P95 D'ÉCRITURE (ms)")
print("-" * 60)
for nodes in sorted(results_summary.keys(), key=int):
    p95 = results_summary[nodes].get('write', {}).get('p95', 0)
    print(f"  {nodes} nœuds: {p95:.2f} ms")

print("\n📝 THROUGHPUT DE LECTURE (ops/sec)")
print("-" * 60)
for nodes in sorted(results_summary.keys(), key=int):
    throughput = results_summary[nodes].get('read', {}).get('throughput', 0)
    print(f"  {nodes} nœuds: {throughput:.2f} ops/sec")

print("\n📝 LATENCE MOYENNE DE LECTURE (ms)")
print("-" * 60)
for nodes in sorted(results_summary.keys(), key=int):
    latency = results_summary[nodes].get('read', {}).get('latency', 0)
    print(f"  {nodes} nœuds: {latency:.2f} ms")

# Calcul amélioration
if '3' in results_summary and '6' in results_summary:
    print("\n📈 AMÉLIORATION 3→6 NŒUDS")
    print("-" * 60)
    
    write_improvement = (
        (results_summary['6']['write']['throughput'] - results_summary['3']['write']['throughput']) 
        / results_summary['3']['write']['throughput'] * 100
    ) if results_summary['3']['write']['throughput'] > 0 else 0
    
    read_improvement = (
        (results_summary['6']['read']['throughput'] - results_summary['3']['read']['throughput']) 
        / results_summary['3']['read']['throughput'] * 100
    ) if results_summary['3']['read']['throughput'] > 0 else 0
    
    print(f"  Throughput écriture: {write_improvement:+.1f}%")
    print(f"  Throughput lecture: {read_improvement:+.1f}%")

print("\n" + "="*60)
EOF

echo ""
echo "═══════════════════════════════════════"
echo "TEST DE SCALABILITÉ TERMINÉ"
echo "═══════════════════════════════════════"
echo "Résultats sauvegardés dans:"
echo "  - results_*nodes.json (résultats tests)"
echo "  - stats_*nodes.txt (statistiques tables)"
echo "  - tpstats_*nodes.txt (thread pool stats)"