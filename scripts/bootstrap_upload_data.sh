#!/usr/bin/env bash
# bootstrap_upload_data.sh
set -euo pipefail

BUCKET="${1:-p11-fruits-pipeline}"
REGION="${2:-eu-west-1}"
DATA_URL="${3:-https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/Data_Scientist_P8/fruits.zip}"

RAW_PREFIX="raw/fruits"
TMP_DIR="$(mktemp -d)"

cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

echo "Bucket: $BUCKET | Region: $REGION"
echo "Téléchargement: $DATA_URL"
echo "Tmp: $TMP_DIR"

command -v aws >/dev/null 2>&1 || { echo "❌ aws CLI manquant"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "❌ curl manquant"; exit 1; }
command -v unzip >/dev/null 2>&1 || { echo "❌ unzip manquant"; exit 1; }

echo "🔎 Vérification du bucket…"
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "🪣 Bucket absent, création…"
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
  else
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
  echo "✅ Bucket créé."
else
  echo "✅ Bucket déjà existant."
fi

ZIP_PATH="$TMP_DIR/fruits.zip"
echo "⬇️  Téléchargement du dataset…"
curl -L "$DATA_URL" -o "$ZIP_PATH"

echo "🗜️  Décompression…"
unzip -q "$ZIP_PATH" -d "$TMP_DIR"

SRC_DIR="$(find "$TMP_DIR" -maxdepth 2 -type d -name 'fruits*' | head -n1 || true)"
if [ -z "${SRC_DIR:-}" ]; then
  # fallback: si le zip contient directement des sous-dossiers d'images
  SRC_DIR="$TMP_DIR"
fi
echo "📁 Dossier source: $SRC_DIR"

# ---------- Sync vers S3 ----------
DEST="s3://$BUCKET/$RAW_PREFIX/"
echo "☁️  Upload vers $DEST …"
aws s3 sync "$SRC_DIR" "$DEST" \
  --region "$REGION" \
  --only-show-errors \
  --exclude ".DS_Store" \
  --storage-class STANDARD \
  --sse AES256

echo "✅ Terminé. Données disponibles dans $DEST"