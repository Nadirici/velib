# Image unique pour tous les rôles du pipeline : le process lancé est choisi
# par la commande (dashboard par défaut ; producer, redis_consumer, archiver,
# weather via `docker run <image> python -m velib.<module>`). Une image = un
# artefact versionné unique à tester et déployer — le cœur de la CI/CD.

FROM python:3.13-slim AS runtime

# uv, copié depuis son image officielle (pas de curl | sh).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Couche dépendances d'abord : elle n'est reconstruite que si le lock change,
# pas à chaque modification du code (cache Docker).
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Puis le code, et l'installation du package lui-même.
COPY src ./src
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH" \
    # Conteneur = on écoute sur toutes les interfaces (Cloud Run l'exige) ;
    # le port réel vient de $PORT (posé par Cloud Run, 8000 sinon).
    HOST=0.0.0.0 \
    PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["python", "-m", "velib.dashboard"]
