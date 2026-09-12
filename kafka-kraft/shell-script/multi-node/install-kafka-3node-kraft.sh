#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# VishwaTech Labs
# Apache Kafka 4.0.2 - 3 Node KRaft Combined Server
# Amazon Linux 2023 / ec2-user
#
# REQUIRED environment variables:
#   NODE_ID
#   NODE_PRIVATE_IP
#   NODE_PUBLIC_IP
#   KAFKA_CLUSTER_ID
#   CONTROLLER_VOTERS
#
# Example:
# export NODE_ID=1
# export NODE_PRIVATE_IP=10.0.1.11
# export NODE_PUBLIC_IP=18.x.x.11
# export KAFKA_CLUSTER_ID="YOUR_CLUSTER_ID"
# export CONTROLLER_VOTERS="1@10.0.1.11:9093,2@10.0.1.12:9093,3@10.0.1.13:9093"
#
# Run:
#   sudo -E ./install-kafka-3node-kraft.sh
#
# IMPORTANT:
# - Run once per NEW Kafka EC2 node.
# - The same KAFKA_CLUSTER_ID must be used on all 3 nodes.
# - Do not run the script against an existing broker unless you
#   understand the storage-format implications.
# ============================================================

KAFKA_VERSION="${KAFKA_VERSION:-4.0.2}"
KAFKA_SCALA_VERSION="${KAFKA_SCALA_VERSION:-2.13}"
KAFKA_TGZ="kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"

KAFKA_HOME="/home/ec2-user/kafka"
KAFKA_VERSION_DIR="/home/ec2-user/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_DATA_DIR="/var/lib/kafka/data"
KAFKA_CONFIG="/home/ec2-user/kafka/config/kraft/server.properties"
SYSTEMD_FILE="/etc/systemd/system/kafka-kraft.service"

: "${NODE_ID:?Set NODE_ID, e.g. NODE_ID=1}"
: "${NODE_PRIVATE_IP:?Set NODE_PRIVATE_IP, e.g. 10.0.1.11}"
: "${NODE_PUBLIC_IP:?Set NODE_PUBLIC_IP, e.g. 18.x.x.11}"
: "${KAFKA_CLUSTER_ID:?Set the SAME KAFKA_CLUSTER_ID on all three brokers}"
: "${CONTROLLER_VOTERS:?Set CONTROLLER_VOTERS for all three controllers}"

if [[ "$EUID" -ne 0 ]]; then
  echo "ERROR: Run with sudo -E:"
  echo "  sudo -E ./install-kafka-3node-kraft.sh"
  exit 1
fi

echo "============================================================"
echo " VishwaTech Kafka 3-Node KRaft Installer"
echo "============================================================"
echo "Kafka version       : ${KAFKA_VERSION}"
echo "Node ID             : ${NODE_ID}"
echo "Private IP          : ${NODE_PRIVATE_IP}"
echo "Public IP           : ${NODE_PUBLIC_IP}"
echo "Controller voters   : ${CONTROLLER_VOTERS}"
echo "Cluster ID           : ${KAFKA_CLUSTER_ID}"
echo "============================================================"

echo "[1/10] Installing packages..."
dnf install -y java-17-amazon-corretto wget tar gzip curl nc

echo "[2/10] Java validation..."
java -version

echo "[3/10] Creating directories..."
mkdir -p /home/ec2-user
mkdir -p "${KAFKA_DATA_DIR}"
mkdir -p /etc/kafka
chown -R ec2-user:ec2-user /home/ec2-user
chown -R ec2-user:ec2-user /var/lib/kafka

echo "[4/10] Downloading Kafka if required..."
if [[ ! -f "/home/ec2-user/${KAFKA_TGZ}" ]]; then
  sudo -u ec2-user wget -q --show-progress -O "/home/ec2-user/${KAFKA_TGZ}" "${KAFKA_URL}"
fi

echo "[5/10] Extracting Kafka..."
if [[ ! -d "${KAFKA_VERSION_DIR}" ]]; then
  sudo -u ec2-user tar -xzf "/home/ec2-user/${KAFKA_TGZ}" -C /home/ec2-user
fi

ln -sfn "${KAFKA_VERSION_DIR}" "${KAFKA_HOME}"
chown -h ec2-user:ec2-user "${KAFKA_HOME}"

echo "[6/10] Writing KRaft configuration..."

cat > "${KAFKA_CONFIG}" <<EOF
# ============================================================
# VishwaTech Labs - Kafka 4.0.2 KRaft
# Node ID: ${NODE_ID}
# ============================================================

process.roles=broker,controller
node.id=${NODE_ID}

# ------------------------------------------------------------
# Listeners
# ------------------------------------------------------------
# INTERNAL = private VPC broker-to-broker / management traffic
# EXTERNAL = public client listener
# CONTROLLER = KRaft metadata quorum
# ------------------------------------------------------------
listeners=INTERNAL://${NODE_PRIVATE_IP}:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://${NODE_PRIVATE_IP}:9093

advertised.listeners=INTERNAL://${NODE_PRIVATE_IP}:19092,EXTERNAL://${NODE_PUBLIC_IP}:9092

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER

# ------------------------------------------------------------
# Static KRaft controller quorum
# ------------------------------------------------------------
controller.quorum.voters=${CONTROLLER_VOTERS}

# ------------------------------------------------------------
# Storage
# ------------------------------------------------------------
log.dirs=${KAFKA_DATA_DIR}

# ------------------------------------------------------------
# Replication / durability for 3 brokers
# ------------------------------------------------------------
default.replication.factor=3
min.insync.replicas=2

offsets.topic.replication.factor=3

transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# ------------------------------------------------------------
# Topic behavior
# ------------------------------------------------------------
auto.create.topics.enable=false

# ------------------------------------------------------------
# Performance defaults suitable for this training lab
# ------------------------------------------------------------
num.network.threads=3
num.io.threads=8
num.replica.fetchers=2

socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

num.partitions=3

# ------------------------------------------------------------
# Consumer group
# ------------------------------------------------------------
group.initial.rebalance.delay.ms=0
EOF

chown ec2-user:ec2-user "${KAFKA_CONFIG}"

echo "[7/10] Checking existing Kafka metadata..."
if [[ -f "${KAFKA_DATA_DIR}/meta.properties" ]]; then
  echo "WARNING: ${KAFKA_DATA_DIR}/meta.properties already exists."
  echo "The broker appears to have already been formatted."
  echo "The script will NOT reformat existing storage."
else
  echo "[8/10] Formatting NEW KRaft storage..."
  sudo -u ec2-user "${KAFKA_HOME}/bin/kafka-storage.sh" format \
    --cluster-id "${KAFKA_CLUSTER_ID}" \
    --config "${KAFKA_CONFIG}"
fi

echo "[9/10] Creating systemd service..."

cat > "${SYSTEMD_FILE}" <<EOF
[Unit]
Description=Apache Kafka 4.0.2 KRaft - VishwaTech
Documentation=https://kafka.apache.org/40/operations/kraft/
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
Group=ec2-user
Environment="KAFKA_HEAP_OPTS=-Xms1G -Xmx1G"
Environment="KAFKA_JVM_PERFORMANCE_OPTS=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true"
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_CONFIG}
ExecStop=${KAFKA_HOME}/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

chmod 644 "${SYSTEMD_FILE}"

systemctl daemon-reload
systemctl enable kafka-kraft

echo "[10/10] Starting Kafka..."
systemctl restart kafka-kraft

echo "Waiting for Kafka to start..."
sleep 15

echo "============================================================"
echo " Kafka service status"
echo "============================================================"
systemctl --no-pager --full status kafka-kraft || true

echo "============================================================"
echo " Listening ports"
echo "============================================================"
ss -lntp | grep -E ':9092|:9093|:19092' || true

echo "============================================================"
echo " Installer complete"
echo "============================================================"
echo "Node ID        : ${NODE_ID}"
echo "Internal Kafka : ${NODE_PRIVATE_IP}:19092"
echo "External Kafka : ${NODE_PUBLIC_IP}:9092"
echo "Controller     : ${NODE_PRIVATE_IP}:9093"
echo
echo "Next validation:"
echo "  ${KAFKA_HOME}/bin/kafka-metadata-quorum.sh --bootstrap-server ${NODE_PRIVATE_IP}:19092 describe --status"
echo
echo "Logs:"
echo "  sudo journalctl -u kafka-kraft -n 200 --no-pager"
echo "============================================================"
