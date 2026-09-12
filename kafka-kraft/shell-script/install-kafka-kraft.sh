#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================================
# VishwaTech Labs
# Apache Kafka 4.0.2 - KRaft Single EC2 Installer
#
# Intended for a fresh Amazon Linux 2023 EC2 lab.
# Runs everything as ec2-user. No separate Kafka user.
#
# Usage:
#   chmod +x install-kafka-kraft.sh
#   ./install-kafka-kraft.sh
# ============================================================

KAFKA_VERSION="${KAFKA_VERSION:-4.0.2}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_DIR="${HOME}/kafka/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
DATA_DIR="${HOME}/kafka/data/kafka-logs"
CLUSTER_ID_FILE="${HOME}/kafka-cluster-id.txt"
SERVICE_FILE="/etc/systemd/system/kafka-kraft.service"

echo "============================================================"
echo " VishwaTech Labs - Kafka ${KAFKA_VERSION} KRaft Installer"
echo "============================================================"

# ---------- Preconditions ----------
if [[ "$(id -un)" != "ec2-user" ]]; then
  echo "ERROR: Run this script as ec2-user."
  echo "Current user: $(id -un)"
  exit 1
fi

if [[ ! -f /etc/os-release ]]; then
  echo "ERROR: Cannot identify operating system."
  exit 1
fi

source /etc/os-release
echo "OS: ${PRETTY_NAME:-unknown}"
echo "User: $(id -un)"

# ---------- Packages ----------
echo
echo "==> Updating Amazon Linux packages..."
sudo dnf update -y

echo
echo "==> Installing Java 17..."
sudo dnf install -y java-17-amazon-corretto wget tar

echo
echo "==> Java version:"
java -version

# ---------- Kafka download ----------
mkdir -p "${HOME}/kafka"
cd "${HOME}/kafka"

ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
DOWNLOAD_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${ARCHIVE}"

if [[ ! -d "${KAFKA_DIR}" ]]; then
  echo
  echo "==> Downloading Kafka ${KAFKA_VERSION}..."
  if [[ ! -f "${ARCHIVE}" ]]; then
    wget -O "${ARCHIVE}" "${DOWNLOAD_URL}"
  fi

  echo
  echo "==> Extracting Kafka..."
  tar -xzf "${ARCHIVE}"
else
  echo
  echo "==> Kafka directory already exists: ${KAFKA_DIR}"
fi

cd "${KAFKA_DIR}"

# ---------- Directories ----------
echo
echo "==> Creating Kafka data directories..."
mkdir -p "${DATA_DIR}"

# ---------- Configuration backup ----------
if [[ -f config/server.properties && ! -f config/server.properties.bak ]]; then
  cp config/server.properties config/server.properties.bak
fi

# ---------- KRaft configuration ----------
echo
echo "==> Writing single-node KRaft configuration..."

cat > config/server.properties <<EOF
process.roles=broker,controller
node.id=1

controller.quorum.voters=1@localhost:9093

listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092

listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT

log.dirs=${DATA_DIR}
EOF

# ---------- Cluster ID ----------
if [[ -f "${CLUSTER_ID_FILE}" ]]; then
  KAFKA_CLUSTER_ID="$(cat "${CLUSTER_ID_FILE}")"
  echo "==> Existing Cluster ID found: ${KAFKA_CLUSTER_ID}"
else
  echo "==> Generating KRaft Cluster ID..."
  KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
  printf '%s\n' "${KAFKA_CLUSTER_ID}" > "${CLUSTER_ID_FILE}"
  chmod 600 "${CLUSTER_ID_FILE}"
  echo "==> Cluster ID: ${KAFKA_CLUSTER_ID}"
fi

# ---------- Storage format ----------
META_FILE="${DATA_DIR}/meta.properties"

if [[ ! -f "${META_FILE}" ]]; then
  echo
  echo "==> Formatting KRaft storage..."
  bin/kafka-storage.sh format \
    --standalone \
    -t "${KAFKA_CLUSTER_ID}" \
    -c config/server.properties
else
  echo
  echo "==> Existing KRaft metadata detected."
  echo "    Skipping storage formatting."
fi

# ---------- Permissions ----------
echo
echo "==> Ensuring ec2-user owns Kafka directories..."
chown -R ec2-user:ec2-user "${HOME}/kafka"

# ---------- systemd ----------
echo
echo "==> Creating ${SERVICE_FILE}..."

sudo tee "${SERVICE_FILE}" > /dev/null <<EOF
[Unit]
Description=Apache Kafka KRaft
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=${KAFKA_DIR}

ExecStart=${KAFKA_DIR}/bin/kafka-server-start.sh ${KAFKA_DIR}/config/server.properties
ExecStop=${KAFKA_DIR}/bin/kafka-server-stop.sh

Restart=on-failure
RestartSec=10

LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

echo
echo "==> Reloading systemd..."
sudo systemctl daemon-reload

echo
echo "==> Enabling Kafka service..."
sudo systemctl enable kafka-kraft

echo
echo "==> Starting Kafka service..."
sudo systemctl restart kafka-kraft

echo
echo "==> Waiting for Kafka to initialize..."
sleep 8

# ---------- Validation ----------
echo
echo "============================================================"
echo " Validation"
echo "============================================================"

echo
echo "[1] User"
whoami

echo
echo "[2] Java"
java -version

echo
echo "[3] systemd status"
sudo systemctl --no-pager --full status kafka-kraft || true

echo
echo "[4] Java processes"
jps || true

echo
echo "[5] Broker port 9092"
if ss -lnt | grep -q ':9092 '; then
  echo "PASS: Kafka broker is listening on 9092."
else
  echo "WARNING: 9092 is not listening yet. Check:"
  echo "  sudo journalctl -u kafka-kraft -n 100 --no-pager"
fi

echo
echo "[6] Controller port 9093"
if ss -lnt | grep -q ':9093 '; then
  echo "PASS: KRaft controller is listening on 9093."
else
  echo "WARNING: 9093 is not listening yet. Check:"
  echo "  sudo journalctl -u kafka-kraft -n 100 --no-pager"
fi

echo
echo "[7] Cluster ID"
cat "${CLUSTER_ID_FILE}"

echo
echo "============================================================"
echo " Installation complete"
echo "============================================================"
echo
echo "Kafka directory:"
echo "  ${KAFKA_DIR}"
echo
echo "Service:"
echo "  sudo systemctl status kafka-kraft"
echo
echo "Logs:"
echo "  sudo journalctl -u kafka-kraft -f"
echo
echo "Kafka client endpoint for this single-host lab:"
echo "  localhost:9092"
echo
echo "Controller endpoint:"
echo "  localhost:9093"
echo
echo "Next recommended test:"
echo "  ${KAFKA_DIR}/bin/kafka-topics.sh --list --bootstrap-server localhost:9092"
echo
