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
- **Carte Leaflet** avec les ~1 500 stations (couleur = remplissage, animation « radar » = événement en direct, badge « EN DIRECT » reflétant l'état réel du flux)
- **Stats** en bandeau : vélos disponibles, bornettes libres, stations vides/pleines/fermées
- **Panneau BI** (bouton 📊 Activité), trois onglets :
  - **Indicateurs** — KPIs instantanés (taux de remplissage, part électrique, stations sous tension, stations en service) et du jour (rotations, flux net, rythme/min avec sparkline, heure de pointe, station la plus active, comparaison à la moyenne archivée), tous mis à jour en direct par le flux SSE ;
  - **Aujourd'hui (direct)** — courbe vélos pris/rendus du jour, granularité ajustable 1 min → 1 h, reconstruite depuis Kafka au démarrage du serveur et alimentée en continu ;
  - **Historique (archives)** — requêtes DuckDB sur les Parquet : volumes par jour, profil horaire moyen (signature jour/nuit).

## Commandes utiles

```bash
# Archiver les événements d'hier en Parquet (couche batch)
uv run python -m velib.archiver

# Archiver une date précise
uv run python -m velib.archiver 2026-07-19

# Ingérer la météo horaire (Open-Meteo → Parquet, pour le futur ML)
uv run python -m velib.weather                        # hier
uv run python -m velib.weather 2026-07-18 2026-07-25  # backfill d'une plage

# Lancer les tests (couverture minimale exigée : 90 %)
uv run pytest

# Accéder aux UI d'admin
# Kafka UI :       http://localhost:8080
# Redis Insight :  http://localhost:5540
# Airflow :        http://localhost:8081
```

## Orchestration (Airflow)

Les jobs batch sont orchestrés par **Apache Airflow 3** (conteneur `velib-airflow`,
mode standalone — UI sur http://localhost:8081, sans login en local).

Le DAG [`velib_daily`](dags/velib_daily.py) tourne chaque nuit à 00h15 (Europe/Paris)
et lance en parallèle, avec 3 retries chacun :
- `archive_events` — `python -m velib.archiver {{ ds }}` (les événements de la veille → Parquet) ;
- `ingest_weather` — `python -m velib.weather {{ ds }}` (la météo de la veille → Parquet).

`{{ ds }}` est la *logical date* d'Airflow (le début de l'intervalle couvert par le
run) : relancer un vieux run archive la bonne date historique, sans calcul de « hier »
dans le code. Le conteneur monte le repo entier ; le code parle à Kafka via le
listener interne (`VELIB_BOOTSTRAP_SERVERS=kafka:19092`). Prochaine étape prévue :
une tâche de chargement PostgreSQL en aval des deux archives.

## Conteneurisation & déploiement (GCP)

Une **image Docker unique** ([Dockerfile](Dockerfile), build uv multi-couches) porte tous
les rôles — dashboard par défaut, producer/consumer/archiver/weather via la commande.
Les adresses (Kafka, Redis, port) viennent de l'environnement : le même code tourne sur
l'hôte, dans Docker et sur Cloud Run.

- **Prod sur une VM GCE** : `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d`
  (ajoute producer + consumer conteneurisés, l'adresse annoncée de Kafka, l'envoi des
  Parquet vers GCS depuis Airflow).
- **Dashboard sur Cloud Run** : jamais éteint, Parquet lus depuis le bucket monté en
  volume, mode dégradé automatique si Kafka est injoignable.
- **CI/CD GitHub Actions** ([.github/workflows/ci.yml](.github/workflows/ci.yml)) :
  push → tests (gate 90 %) → build → Artifact Registry → déploiement Cloud Run,
  authentifié par Workload Identity Federation (zéro clé stockée).

Guide complet pas à pas : [docs/DEPLOY-GCP.md](docs/DEPLOY-GCP.md).

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

## Décisions de conception

- **Clé de partition = `station_id`** — Kafka ne garantit l'ordre qu'au sein d'une partition ;
  avec la clé, tous les événements d'une même station restent ordonnés, ce qui rend `bikes_delta` fiable en aval.
- **« Au moins une fois » + idempotence** — le consumer commit *après* traitement (aucune perte possible),
  et les écritures Redis remplacent l'état complet : rejouer un événement est sans effet. Le couple
  transforme la garantie faible de Kafka en résultat exact côté vue.
- **Messages empoisonnés** — un événement inparsable est écarté et loggé (partition + offset) au lieu
  de bloquer la partition à l'infini. Vécu en conditions réelles : un `stationCode: null` transitoire de
  l'API, propagé à ~1 400 événements par la jointure du producer (le défaut de `dict.get()` ne couvre pas
  les valeurs `null`).
- **Event sourcing** — le journal Kafka est la seule source de vérité : la vue Redis comme l'activité du
  jour du dashboard sont des projections jetables, reconstructibles par replay (`FLUSHDB` + reset des
  offsets ; `offsets_for_times` à chaque démarrage du dashboard).
- **Producer idempotent** (`enable.idempotence`, `acks=all`) — les retries réseau ne créent pas de doublons.
- **Push plutôt que polling** — SSE (unidirectionnel, reconnexion native) plutôt que WebSocket ;
  le navigateur ne redemande jamais rien, il resynchronise sa photo après une coupure.
- **Couleurs de la carte** — pas de dégradé rouge→vert (indiscernable en cas de daltonisme) : rampes
  séquentielles monochromes validées par un contrôle de contraste automatisé, rouge réservé au seul
  état critique (station vide / pleine), gris pour les stations fermées.

## Limites connues & pistes

- Le store « état précédent » du producer est en mémoire : chaque redémarrage réémet un snapshot complet
  (~1 500 événements à `bikes_delta=0`). Piste : `RedisStateStore` via le Protocol `PreviousStateStore` existant.
- Le contrat de schéma est implicite (`to_json`/`parse_event`). Version industrielle : Avro + Schema
  Registry, et une dead letter queue à la place du log d'écartement.
- L'agrégation d'activité n'est pas idempotente en cas de replay partiel (tolérable pour des tendances).
- L'archiveur se lance à la main — à planifier (Planificateur de tâches Windows / cron).
- À venir : fenêtres glissantes sur `bikes_delta`, croisement météo (les horodatages sont volontairement
  restés en epoch UTC pour ça), prédiction de disponibilité par station.
