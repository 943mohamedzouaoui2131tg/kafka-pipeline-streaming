#!/bin/bash

echo "=========================================="
echo "TEST DE COHÉRENCE ET DÉTECTION DE DOUBLONS"
echo "         MongoDB (Sharded Cluster)        "
echo "=========================================="

# ─── Conteneurs ───────────────────────────────────────────────────────────
S1A="shard1a";  S1B="shard1b"
S2A="shard2a";  S2B="shard2b"
S3A="shard3a";  S3B="shard3b"
MONGOS="mongos"

WAIT_STOP=35
WAIT_START=60

# Timeout curl : 10 writes * 3s timeout max par write = ~3000s worst case
# On met 600s (10 min) pour être large mais pas infini
CURL_TIMEOUT=600

# ─── Affichage ────────────────────────────────────────────────────────────
display_results() {
    local file=$1
    local scenario=$2

    echo ""
    echo "Scénario: $scenario"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Vérifie que le fichier existe et n'est pas vide
    if [ ! -s "$file" ]; then
        echo "  ⚠️  Pas de réponse reçue (fichier vide ou introuvable)"
        return
    fi

    python3 -c "
import sys, json
try:
    with open('$file') as f:
        data = json.load(f)
    if data.get('success'):
        r  = data['results']
        wp = r['write_performance']
        rp = r['read_performance']
        cc = r['consistency_check']
        print(f'  Écritures réussies : {wp[\"total_writes\"]}   |  Échecs : {wp[\"failed_writes\"]}')
        print(f'  Temps total écriture : {wp[\"write_time_seconds\"]}s')
        print(f'  Latence écriture     : moy {wp[\"avg_write_latency_ms\"]} ms   |  P95 {wp[\"p95_write_latency_ms\"]} ms')
        print(f'  Latence lecture      : {rp[\"avg_read_latency_ms\"]} ms')
        if rp.get('read_error'):
            print(f'  ⚠️  Lecture partielle (un ou plusieurs shards injoignables)')
        print(f'  Intégrité            : {cc[\"data_integrity\"]}')
        print(f'  Records trouvés      : {cc[\"found_records\"]} / {cc[\"expected_records\"]}')
        print(f'  Doublons             : {cc[\"duplicate_records\"]}')
        print(f'  Manquants            : {cc[\"missing_records\"]}')
    else:
        print(f'  Erreur : {data.get(\"error\", \"Unknown\")}')
except Exception as e:
    print(f'  ⚠️  Erreur de parsing : {e}')
"
}

# ─── Statut conteneurs ────────────────────────────────────────────────────
show_status() {
    echo ""
    for c in $MONGOS $S1A $S1B $S2A $S2B $S3A $S3B; do
        STATE=$(docker ps --format "{{.Names}}:{{.State}}" --filter "name=^${c}$" 2>/dev/null | cut -d: -f2)
        [ "$STATE" == "running" ] && echo "  ✅ $c" || echo "  ❌ $c"
    done
    echo ""
}

# ═══════════════════════════════════════════════════════════════════════════
# TEST 1 — Normal
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════"
echo "TEST 1: COHÉRENCE EN CONDITIONS NORMALES"
echo "═══════════════════════════════════════"
show_status

echo "Exécution du test avec 10 écritures..."
curl -s --max-time $CURL_TIMEOUT -X POST http://localhost:5000/mongo/test/consistency \
  -H "Content-Type: application/json" \
  -d '{"num_writes": 10}' \
  -o results_consistency_normal.json

display_results results_consistency_normal.json "Normal (tous shards UP)"

# ═══════════════════════════════════════════════════════════════════════════
# TEST 2 — 1 shard down
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════"
echo "TEST 2: 1 SHARD DOWN (sh1)"
echo "═══════════════════════════════════════"

echo "Arrêt de shard1 (shard1a + shard1b)..."
docker stop $S1A $S1B
echo "Attente de ${WAIT_STOP}s..."
sleep $WAIT_STOP
show_status

echo "Exécution du test avec 10 écritures..."
curl -s --max-time $CURL_TIMEOUT -X POST http://localhost:5000/mongo/test/consistency \
  -H "Content-Type: application/json" \
  -d '{"num_writes": 10}' \
  -o results_consistency_1shard_down.json

display_results results_consistency_1shard_down.json "1 shard DOWN (sh1)"

# ═══════════════════════════════════════════════════════════════════════════
# TEST 3 — 2 shards down
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════"
echo "TEST 3: 2 SHARDS DOWN (sh1 + sh2)"
echo "═══════════════════════════════════════"

echo "Arrêt de shard2 (shard2a + shard2b)..."
docker stop $S2A $S2B
echo "Attente de ${WAIT_STOP}s..."
sleep $WAIT_STOP
show_status

echo "Exécution du test avec 10 écritures..."
curl -s --max-time $CURL_TIMEOUT -X POST http://localhost:5000/mongo/test/consistency \
  -H "Content-Type: application/json" \
  -d '{"num_writes": 10}' \
  -o results_consistency_2shards_down.json

display_results results_consistency_2shards_down.json "2 shards DOWN (sh1 + sh2)"

# ═══════════════════════════════════════════════════════════════════════════
# RESTAURATION
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════"
echo "RESTAURATION DES SHARDS"
echo "═══════════════════════════════════════"

echo "Redémarrage shard1..."
docker start $S1A; sleep 5; docker start $S1B
echo "Attente de ${WAIT_START}s (resync sh1)..."
sleep $WAIT_START

echo "Redémarrage shard2..."
docker start $S2A; sleep 5; docker start $S2B
echo "Attente de ${WAIT_START}s (resync sh2)..."
sleep $WAIT_START

show_status

# ═══════════════════════════════════════════════════════════════════════════
# TEST 4 — Après récupération
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════"
echo "TEST 4: APRÈS RÉCUPÉRATION"
echo "═══════════════════════════════════════"

echo "Exécution du test avec 10 écritures..."
curl -s --max-time $CURL_TIMEOUT -X POST http://localhost:5000/mongo/test/consistency \
  -H "Content-Type: application/json" \
  -d '{"num_writes": 10}' \
  -o results_consistency_after_recovery.json

display_results results_consistency_after_recovery.json "Après récupération"

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSE COMPARATIVE
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════"
echo "ANALYSE COMPARATIVE GLOBALE"
echo "═══════════════════════════════════════"

python3 << 'EOF'
import json, os

files = [
    ("Normal",           "results_consistency_normal.json"),
    ("1 shard DOWN",     "results_consistency_1shard_down.json"),
    ("2 shards DOWN",    "results_consistency_2shards_down.json"),
    ("Après recovery",   "results_consistency_after_recovery.json"),
]

print("\n📊 LATENCES D'ÉCRITURE (ms)")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print(f"{'Scénario':<22} {'Moy':<12} {'P95':<12} {'Lecture':<12}")
print("─" * 60)

for label, fname in files:
    if not os.path.exists(fname) or os.path.getsize(fname) == 0:
        print(f"{label:<22} pas de données")
        continue
    try:
        with open(fname) as f:
            data = json.load(f)
        if data.get("success"):
            wp = data["results"]["write_performance"]
            rp = data["results"]["read_performance"]
            print(f"{label:<22} {wp['avg_write_latency_ms']:<12} {wp['p95_write_latency_ms']:<12} {rp['avg_read_latency_ms']:<12}")
        else:
            print(f"{label:<22} ERREUR: {data.get('error','?')}")
    except Exception as e:
        print(f"{label:<22} parse error: {e}")

print("\n\n🔍 INTÉGRITÉ DES DONNÉES")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print(f"{'Scénario':<22} {'Statut':<10} {'Trouvés':<12} {'Attendus':<12} {'Doublons':<10} {'Manquants':<10}")
print("─" * 78)

for label, fname in files:
    if not os.path.exists(fname) or os.path.getsize(fname) == 0:
        print(f"{label:<22} pas de données")
        continue
    try:
        with open(fname) as f:
            data = json.load(f)
        if data.get("success"):
            cc = data["results"]["consistency_check"]
            print(f"{label:<22} {cc['data_integrity']:<10} {cc['found_records']:<12} {cc['expected_records']:<12} {cc['duplicate_records']:<10} {cc['missing_records']:<10}")
        else:
            print(f"{label:<22} ERREUR")
    except Exception as e:
        print(f"{label:<22} parse error: {e}")

EOF

echo ""
echo "═══════════════════════════════════════"
echo "TEST TERMINÉ"
echo "═══════════════════════════════════════"
echo "Fichiers de résultats :"
echo "  - results_consistency_normal.json"
echo "  - results_consistency_1shard_down.json"
echo "  - results_consistency_2shards_down.json"
echo "  - results_consistency_after_recovery.json"