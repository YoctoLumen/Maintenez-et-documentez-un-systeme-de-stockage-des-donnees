FROM python:3.11-slim

WORKDIR /app

# Installer les dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copier requirements.txt (depuis la racine ou depuis app/)
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copier tous les fichiers Python depuis le dossier app/
COPY app/ ./

# Forcer l'affichage immédiat des prints Python
ENV PYTHONUNBUFFERED=1

# Script par défaut
CMD ["python", "migrer_data.py"]
