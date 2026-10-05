# Image unique pour le pipeline ET le notebook : mêmes versions de librairies
# dans les deux cas, donc une exploration et une production cohérentes.
FROM python:3.11-slim

# Pas de fichiers .pyc, logs Python affichés immédiatement (sans tampon).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dépendances installées AVANT de copier le code : cette couche Docker reste
# en cache tant que requirements.txt ne change pas => reconstructions rapides.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Le code (le .env est exclu par .dockerignore : le secret n'est jamais
# intégré à l'image, il est injecté au lancement via env_file).
COPY . .

ENTRYPOINT ["python", "-m", "pipeline"]
CMD ["--help"]
