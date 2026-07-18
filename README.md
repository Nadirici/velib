# Vélib' — Pipeline temps réel & dashboard

Pipeline de données temps réel qui collecte l'état des ~1 400 stations Vélib' Métropole, détecte les changements (vélos pris/rendus), et les visualise sur un dashboard cartographique en direct.

## Architecture

```
┌─────────────┐     ┌───────────┐     ┌───────────┐     ┌──────────────┐
│  API Vélib' │────▶│  Producer │────▶│   Kafka   │────▶│   Consumer   │
│  (open data)│     │  (Python) │     │  (broker) │     │    Redis     │
└─────────────┘     └───────────┘     └─────┬─────┘     └──────┬───────┘
                                            │                  │
                                            ▼                  ▼
                                     ┌─────────────┐   ┌─────────────┐
                                     │  Dashboard  │◀──│    Redis     │
                                     │  (FastAPI)  │   │   (photo)   │
                                     │  + SSE push │   └─────────────┘
                                     └──────┬──────┘
                                            │
                                            ▼
                                     ┌─────────────┐
                                     │  Navigateur  │
                                     │  Leaflet +   │
                                     │  graphiques  │
                                     └─────────────┘
```

**Architecture Lambda** — deux couches complémentaires :

| Couche | Source | Rôle | Module |
|--------|--------|------|--------|
| **Speed** (temps réel) | Kafka → SSE | Événements du jour, agrégés en RAM par tranches de 1 à 60 min | `activity.py` |
| **Batch** (historique) | Kafka → Parquet → DuckDB | Archives quotidiennes, requêtées en SQL | `archiver.py` |

## Stack technique

| Composant | Technologie |
|-----------|-------------|
| Source de données | [API open data Vélib' Métropole](https://www.velib-metropole.fr/donnees-open-data-gbfs-702c02b3-ec5e-4db8-824f-13787e1e7dfd) |
| Message broker | Apache Kafka 4.1 (mode KRaft, sans ZooKeeper) |
| State store | Redis 7 (vue matérialisée de l'état courant) |
| Backend API | FastAPI + Uvicorn |
| Frontend | HTML/CSS/JS vanilla + Leaflet.js |
| Archivage | Parquet (via DuckDB) |
| Requêtes SQL | DuckDB (in-process, directement sur les fichiers Parquet) |
| Gestion de projet | uv (packaging Python) |

## Prérequis

- **Python ≥ 3.13**
- **Docker** et **Docker Compose** (pour Kafka + Redis)
- **uv** (gestionnaire de paquets Python) — [installation](https://docs.astral.sh/uv/)

## Installation

```bash
# Cloner le projet
git clone <url-du-repo>
cd velib

# Installer les dépendances Python
uv sync

# Démarrer l'infrastructure (Kafka + Redis + UI)
docker compose up -d
```

## Lancement

Le pipeline nécessite **3 terminaux** :

### 1. Producer — collecte les données Vélib' et publie dans Kafka

```bash
uv run python -m velib.kafka_producer
```

Interroge l'API Vélib' toutes les ~60 s, détecte les changements d'état des stations, et publie les événements dans le topic `velib.station.changes`.

### 2. Consumer Redis — matérialise la photo dans Redis

```bash
uv run python -m velib.redis_consumer
```

Lit le flux Kafka et écrit l'état courant de chaque station dans Redis (vue matérialisée). Le dashboard lira cette photo au démarrage.

### 3. Dashboard — carte temps réel

```bash
uv run python -m velib.dashboard
```

Ouvre le dashboard sur **http://localhost:8000** :
- **Carte Leaflet** avec les ~1 400 stations (couleur = remplissage, animation = événement en direct)
- **KPIs** en bandeau : vélos disponibles, bornettes libres, stations vides/pleines/fermées
- **Panneau BI** (bouton 📊 Activité) : graphiques d'activité du jour + historique

## Commandes utiles

```bash
# Archiver les événements d'hier en Parquet (couche batch)
uv run python -m velib.archiver

# Archiver une date précise
uv run python -m velib.archiver 2026-07-19

# Lancer les tests
uv run pytest

# Accéder aux UI d'admin
# Kafka UI :       http://localhost:8080
# Redis Insight :  http://localhost:5540
```

## Structure du projet

```
velib/
├── docker-compose.yml          # Kafka (KRaft) + Redis + UI admin
├── pyproject.toml              # Métadonnées projet & dépendances
├── uv.lock                    # Lock file des dépendances
├── data/                       # Données générées (Parquet)
│   └── events/
│       └── date=YYYY-MM-DD/
│           └── events.parquet
├── src/velib/
│   ├── models.py               # Dataclasses : StationInfo, StationState, StationChangeEvent
│   ├── velib_api.py            # Client HTTP de l'API open data Vélib'
│   ├── producer.py             # Logique pure de détection de changements
│   ├── state_store.py          # Store d'état précédent (en mémoire)
│   ├── kafka_producer.py       # Boucle temps réel : API → Kafka
│   ├── kafka_consumer.py       # Consumer moniteur (logs console)
│   ├── redis_consumer.py       # Consumer Redis (vue matérialisée)
│   ├── activity.py             # Agrégation en RAM (couche speed)
│   ├── archiver.py             # Archivage Parquet (couche batch)
│   ├── dashboard.py            # Backend FastAPI (SSE + API REST)
│   └── static/
│       └── dashboard.html      # Frontend (carte Leaflet + graphiques SVG)
└── tests/
    ├── conftest.py
    ├── test_models.py
    ├── test_producer.py
    ├── test_state_store.py
    └── test_velib_api.py
```

## Concepts clés

### Kafka
- **Topic** `velib.station.changes` : 3 partitions, clé = `station_id` (garantit l'ordre par station)
- **Groupes de consumers** : chaque service (Redis, dashboard, archiver) lit indépendamment
- **Rétention** : 7 jours par défaut — suffisant pour reconstruire toute la vue Redis si nécessaire

### Redis (vue matérialisée)
- `velib:station:{id}` → HASH de l'état complet de la station
- `velib:stations` → SET des station_id connus
- `velib:last_ts` → fraîcheur de la dernière écriture
- **Idempotent** : rejouer un événement ne corrompt rien

### Dashboard
- **Snapshot** au chargement : GET `/api/stations` lit la photo Redis
- **SSE** en continu : le backend relaie chaque événement Kafka aux navigateurs connectés
- **Carte** : marqueurs circulaires (taille ∝ capacité, couleur = remplissage), animation « radar » sur chaque changement
- **Panneau BI** : graphiques SVG faits à la main (sans librairie externe)
