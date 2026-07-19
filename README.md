# Vélib' — Pipeline temps réel & dashboard

[![CI/CD](https://github.com/Nadirici/velib/actions/workflows/ci.yml/badge.svg)](https://github.com/Nadirici/velib/actions/workflows/ci.yml)

Pipeline de données temps réel qui collecte l'état des ~1 500 stations Vélib' Métropole, détecte les changements (vélos pris/rendus), et les visualise sur un dashboard cartographique en direct.

## ▶ Démo en ligne

**https://velib-dashboard-564200084105.europe-west1.run.app**

Aucune installation : le pipeline tourne en continu sur Google Cloud (VM GCE pour le cœur streaming, Cloud Run pour le dashboard public), et chaque merge sur `main` redéploie automatiquement.

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
| Source de données | [API open data Vélib' Métropole](https://www.velib-metropole.fr/donnees-open-data-gbfs-702c02b3-ec5e-4db8-824f-13787e1e7dfd) (GBFS) |
| Message broker | Apache Kafka 4.1 (mode KRaft, sans ZooKeeper) |
| State store | Redis 7 (vue matérialisée de l'état courant) |
| Backend API | FastAPI + Uvicorn (+ SSE pour le push temps réel) |
| Frontend | HTML/CSS/JS vanilla + Leaflet.js (graphiques SVG faits main) |
| Archivage & requêtes | Parquet (partitionné par date) + DuckDB (SQL in-process) |
| Ingestion météo | [Open-Meteo](https://open-meteo.com) → Parquet (pour le futur ML) |
| Orchestration | Apache Airflow 3 (DAG batch quotidien) |
| Conteneurisation | Docker (image unique multi-rôles) + Docker Compose |
| Cloud | GCE (VM cœur streaming), Cloud Run (dashboard), Cloud Storage (Parquet) |
| CI/CD | GitHub Actions + Workload Identity Federation (déploiement sans clé) |
| Tests | pytest (couverture ≥ 90 % imposée), fakeredis, transports HTTP simulés |
| Gestion de projet | uv (packaging & dépendances Python) |

## Le dashboard

Servi par FastAPI, poussé en temps réel via **SSE** (le navigateur ne fait aucun polling : il reçoit chaque événement Kafka à la milliseconde) :

- **Carte Leaflet** des ~1 500 stations — couleur = remplissage, taille ∝ capacité, animation « radar » à chaque changement, badge « EN DIRECT » reflétant l'état réel du flux (passe à « FLUX INTERROMPU » si la donnée vieillit).
- **Deux modes** : « trouver un vélo » (couleur = vélos dispo) / « rendre un vélo » (couleur = bornettes libres).
- **Bandeau de stats** : vélos disponibles, bornettes libres, stations vides/pleines/fermées.
- **Panneau BI** (bouton 📊 Activité), cinq onglets :
  - **Indicateurs** — KPIs instantanés (taux de remplissage, part électrique, stations sous tension) et du jour (rotations, flux net, rythme/min avec sparkline, heure de pointe), en direct via SSE ;
  - **Arrondissements** — carte choroplèthe vectorielle (SVG natif) de l'activité intra-muros, calcul 100 % local en temps réel des classements de stations (plus actives, en tension) globalement ou par arrondissement cliqué ;
  - **Métier** — vue exploitant : demande & revenus estimés, priorités de rééquilibrage (stations en défaut triées par la demande qu'elles portent), puits/sources (flux net par station) ;
  - **Aujourd'hui (direct)** — courbe vélos pris/rendus, granularité ajustable 1 min → 1 h, reconstruite depuis Kafka au démarrage puis alimentée en continu ;
  - **Historique (archives)** — requêtes DuckDB sur les Parquet : volumes par jour, profil horaire moyen (signature jour/nuit).

## Déploiement (GCP)

Une **image Docker unique** ([Dockerfile](Dockerfile), build uv multi-couches) porte tous les rôles — dashboard par défaut, producer/consumer/archiver/weather via la commande. Les adresses (Kafka, Redis, port) viennent de l'environnement : le même code tourne dans Docker et sur Cloud Run.

- **Cœur streaming sur une VM GCE** — Kafka, Redis, producer et consumer conteneurisés (`docker-compose.yml` + `docker-compose.prod.yml`), redémarrage automatique. Les Parquet produits sont poussés vers Cloud Storage.
- **Dashboard sur Cloud Run** — public et toujours disponible, l'historique lu depuis le bucket monté en volume, mode dégradé automatique si Kafka est injoignable.
- **CI/CD GitHub Actions** ([.github/workflows/ci.yml](.github/workflows/ci.yml)) — sur `main` : tests (gate 90 %) → build → Artifact Registry → déploiement Cloud Run, authentifié par **Workload Identity Federation** (zéro clé stockée).

Guide complet pas à pas : [docs/DEPLOY-GCP.md](docs/DEPLOY-GCP.md).

## Orchestration (Airflow)

Les jobs batch sont orchestrés par **Apache Airflow 3**. Le DAG [`velib_daily`](dags/velib_daily.py) tourne chaque nuit à 00h15 (Europe/Paris) et lance en parallèle, avec 3 retries chacun :

- `archive_events` — les événements de la veille → Parquet ;
- `ingest_weather` — la météo de la veille (Open-Meteo) → Parquet.

`{{ ds }}` est la *logical date* d'Airflow (le début de l'intervalle couvert par le run) : relancer un vieux run archive la bonne date historique, sans calcul de « hier » dans le code. Prochaine étape prévue : un chargement PostgreSQL analytique en aval des deux archives.

## Structure du projet

```
velib/
├── docker-compose.yml           # Infra : Kafka (KRaft) + Redis + Airflow + UIs
├── docker-compose.prod.yml      # Overlay VM GCE : producer/consumer conteneurisés, envoi GCS
├── Dockerfile                   # Image unique multi-rôles (build uv multi-couches)
├── pyproject.toml               # Métadonnées projet & dépendances
├── uv.lock                      # Lock des dépendances
├── .github/workflows/ci.yml     # CI/CD : tests → build → Cloud Run (via WIF)
├── dags/
│   └── velib_daily.py           # DAG Airflow : archivage événements + météo (nuit)
├── docs/
│   └── DEPLOY-GCP.md            # Guide de déploiement GCP pas à pas
├── data/                        # Données générées (Parquet, hors git) : events/ et weather/
│   └── {events,weather}/date=YYYY-MM-DD/*.parquet
├── src/velib/
│   ├── models.py                # Dataclasses : StationInfo, StationState, StationChangeEvent
│   ├── velib_api.py             # Client HTTP de l'API open data Vélib' (GBFS)
│   ├── producer.py              # Logique pure de détection de changements
│   ├── state_store.py           # Store d'état précédent (Protocol + impl. en mémoire)
│   ├── kafka_producer.py        # Boucle temps réel : API → Kafka
│   ├── kafka_consumer.py        # Boucle de consommation générique (moniteur console)
│   ├── redis_consumer.py        # Consumer Redis (vue matérialisée)
│   ├── activity.py              # Agrégation en RAM du jour (couche speed)
│   ├── archiver.py              # Archivage Parquet des événements (couche batch)
│   ├── weather.py               # Ingestion météo Open-Meteo → Parquet
│   ├── gcs.py                   # Envoi optionnel des Parquet vers un bucket GCS
│   ├── dashboard.py             # Backend FastAPI (snapshot + SSE + API BI)
│   └── static/dashboard.html    # Frontend (carte Leaflet + graphiques SVG)
└── tests/                       # 90 %+ de couverture, sans infra réelle
    ├── conftest.py
    ├── test_models.py           test_producer.py        test_state_store.py
    ├── test_velib_api.py        test_activity.py        test_kafka_producer.py
    ├── test_kafka_consumer.py   test_redis_consumer.py  test_archiver.py
    └── test_weather.py          test_gcs.py             test_dashboard.py
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
- La VM est un point unique de défaillance (single-node Kafka/Redis) — en production on passerait à
  un cluster Kafka managé + Redis répliqué ; le déploiement ne met à jour que le dashboard (Cloud Run),
  les composants VM se mettent à jour à la main (`git pull` + `docker compose up -d`).
- Airflow tourne en mode `standalone` (base SQLite) — suffisant pour le batch quotidien, à séparer
  (executor distribué + Postgres) pour un vrai environnement de production.
- À venir : chargement PostgreSQL analytique,
  croisement météo (les horodatages sont volontairement restés en epoch UTC pour ça), prédiction de
  disponibilité par station, agent conversationnel LLM sur les données.

## Développement local

Le code de développement, l'infrastructure locale (Docker Compose) et les instructions pour lancer le
pipeline sur sa machine vivent sur la branche [`dev`](https://github.com/Nadirici/velib/tree/dev).
Le détail du déploiement cloud est dans [docs/DEPLOY-GCP.md](docs/DEPLOY-GCP.md).
