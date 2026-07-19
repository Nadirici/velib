# Déploiement GCP — guide pas à pas

Architecture cible (décidée le 2026-07-19) :

```
                    ┌────────────── GCP ───────────────────────────────┐
                    │                                                  │
 GitHub ── Actions ─┼─► Artifact Registry ──► Cloud Run (dashboard)    │
 (merge → main)     │        (images)         │ lit Redis+Kafka via IP │
                    │                         │ interne, Parquet via   │
                    │                         │ volume GCS monté       │
                    │   VM GCE (e2-medium) ◄──┘                        │
                    │   docker compose :                               │
                    │   kafka, redis, producer, redis-consumer,        │
                    │   airflow (archive nocturne → bucket GCS)        │
                    └──────────────────────────────────────────────────┘
```

- **Cloud Run** : le dashboard, public, jamais éteint, redéployé à chaque push.
- **VM GCE** : le cœur streaming (Kafka/Redis/producer/consumer) + Airflow,
  via `docker-compose.yml` + `docker-compose.prod.yml`.
- **GCS** : les Parquet, poussés par les jobs Airflow (`VELIB_GCS_BUCKET`),
  montés en lecture dans Cloud Run (zéro code GCS dans le dashboard).
- **CI/CD** : GitHub Actions, auth par Workload Identity Federation (sans clé).

Coûts (ordre de grandeur) : VM e2-medium ≈ 25-30 €/mois (couverte par les
300 $ de crédits d'essai), Cloud Run et GCS ≈ 0 à ce volume. Tout s'arrête
avec `gcloud compute instances stop`.

---

## 0. Prérequis

- Un compte GCP avec facturation (ou les crédits d'essai), le SDK `gcloud` installé.
- Le repo poussé sur GitHub (`git push -u origin main && git push -u origin dev`).
- Flux à deux environnements : on travaille sur `dev`, on ouvre une PR `dev → main`,
  le merge déploie. Protéger `main` sur GitHub (Settings → Branches → branch
  protection : require PR + require status check « tests »).

```bash
gcloud auth login
export PROJECT=velib-pipeline REGION=europe-west1
gcloud projects create $PROJECT && gcloud config set project $PROJECT
# (associer la facturation au projet via la console)
gcloud services enable run.googleapis.com artifactregistry.googleapis.com \
  compute.googleapis.com storage.googleapis.com iamcredentials.googleapis.com
```

## 1. Bucket + registre d'images

```bash
gcloud storage buckets create gs://$PROJECT-data --location=$REGION
gcloud artifacts repositories create velib --repository-format=docker --location=$REGION
```

## 2. La VM (cœur du pipeline)

```bash
gcloud compute instances create velib-core \
  --zone=$REGION-b --machine-type=e2-medium \
  --image-family=debian-12 --image-project=debian-cloud \
  --scopes=storage-rw
gcloud compute ssh velib-core --zone=$REGION-b
```

Sur la VM :

```bash
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git
sudo usermod -aG docker $USER && newgrp docker
git clone https://github.com/<toi>/velib.git && cd velib
cat > .env <<EOF
VM_INTERNAL_IP=$(hostname -I | awk '{print $1}')
VELIB_GCS_BUCKET=<PROJECT>-data
EOF
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

Vérifier : `docker compose ps` (kafka, redis, producer, redis-consumer,
airflow… tous Up), et dans le bucket, les Parquet apparaissent après le
premier run nocturne du DAG (ou un trigger manuel).

**Firewall** : Kafka (9092) et Redis (6379) ne doivent être joignables que
depuis le VPC (Cloud Run y accède par IP interne) — ne PAS créer de règle
les ouvrant à Internet ; le firewall GCP les bloque par défaut, c'est bien.

## 3. Cloud Run (création initiale, une seule fois)

D'abord une image dans le registre (le premier push viendra ensuite de la CI) :

```bash
gcloud auth configure-docker $REGION-docker.pkg.dev
docker build -t $REGION-docker.pkg.dev/$PROJECT/velib/velib:init .
docker push $REGION-docker.pkg.dev/$PROJECT/velib/velib:init
```

Puis le service — env vers la VM (IP interne), volume GCS monté sur
`/app/data` (le dashboard lit l'historique comme des fichiers locaux), et
sortie réseau directe dans le VPC :

```bash
VM_IP=$(gcloud compute instances describe velib-core --zone=$REGION-b \
  --format='get(networkInterfaces[0].networkIP)')

gcloud run deploy velib-dashboard \
  --image=$REGION-docker.pkg.dev/$PROJECT/velib/velib:init \
  --region=$REGION --allow-unauthenticated \
  --network=default --subnet=default \
  --add-volume=name=data,type=cloud-storage,bucket=$PROJECT-data,readonly=true \
  --add-volume-mount=volume=data,mount-path=/app/data \
  --set-env-vars=VELIB_BOOTSTRAP_SERVERS=$VM_IP:9092,VELIB_REDIS_HOST=$VM_IP,TZ=Europe/Paris
```

Notes :
- `min-instances` reste à 0 : l'instance vit tant qu'un navigateur est
  connecté au SSE, et un démarrage à froid reconstruit l'activité du jour en
  rejouant Kafka (c'est prévu pour). Passer à 1 (~quelques €/mois) si tu veux
  zéro latence de premier chargement.
- Si Kafka est injoignable, le dashboard démarre quand même en mode dégradé
  (photo Redis + historique, sans temps réel) — comportement volontaire.

## 4. CI/CD — Workload Identity Federation

Créer le service account de déploiement et le pool d'identité GitHub :

```bash
gcloud iam service-accounts create github-deployer
for role in roles/run.admin roles/artifactregistry.writer roles/iam.serviceAccountUser; do
  gcloud projects add-iam-policy-binding $PROJECT \
    --member="serviceAccount:github-deployer@$PROJECT.iam.gserviceaccount.com" --role=$role
done

gcloud iam workload-identity-pools create github --location=global
gcloud iam workload-identity-pools providers create-oidc github-oidc \
  --location=global --workload-identity-pool=github \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository=='<toi>/velib'"

PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format='get(projectNumber)')
gcloud iam service-accounts add-iam-policy-binding \
  github-deployer@$PROJECT.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/github/attribute.repository/<toi>/velib"
```

Côté GitHub (Settings → Secrets and variables → Actions) :

| Type | Nom | Valeur |
|---|---|---|
| secret | `GCP_PROJECT_ID` | l'ID du projet |
| secret | `GCP_SA_EMAIL` | `github-deployer@<projet>.iam.gserviceaccount.com` |
| secret | `GCP_WIF_PROVIDER` | `projects/<num>/locations/global/workloadIdentityPools/github/providers/github-oidc` |
| variable | `GCP_REGION` | `europe-west1` |

Ensuite : chaque merge sur `main` → tests (gate 90 %) → build → push →
déploiement. Un push sur `dev` ou une PR ne déclenchent que les tests.

## 5. Exploitation

```bash
gcloud run services describe velib-dashboard --region=$REGION --format='get(status.url)'
gcloud compute instances stop velib-core --zone=$REGION-b   # pause (stoppe la facturation CPU)
```

Limites connues à garder en tête : la VM est un SPOF assumé (un vrai cluster
Kafka managé serait l'étape « entreprise ») ; l'auth du dashboard est publique
(c'est un portfolio) ; Airflow standalone sur la VM reste du mode dev.
