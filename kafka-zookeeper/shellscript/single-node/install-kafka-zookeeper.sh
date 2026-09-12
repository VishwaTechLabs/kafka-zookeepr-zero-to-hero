#!/usr/bin/env bash
set -Eeuo pipefail

# VishwaTech Labs - Apache Kafka 3.8.1 + ZooKeeper single EC2 installer
# Target: Amazon Linux 2023
# User: ec2-user (no separate Kafka/ZooKeeper Linux user)

KAFKA_VERSION="3.8.1"
KAFKA_SCALA="2.13"
KAFKA_DIR="/home/ec2-user/kafka"
KAFKA_HOME="${KAFKA_DIR}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}"
KAFKA_TGZ="kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"
DATA_DIR="${KAFKA_DIR}/data"
ZK_DATA_DIR="${DATA_DIR}/zookeeper"
KAFKA_LOG_DIR="${DATA_DIR}/kafka-logs"

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

[[ "$(id -un)" == "ec2-user" ]] || fail "Run this script as ec2-user. Do not run it as root."

if [[ -f /etc/os-release ]]; then
  . /etc/os-release
  [[ "${ID:-}" == "amzn" ]] || echo "WARNING: This script was designed for Amazon Linux 2023; detected ID=${ID:-unknown}."
fi

log "Updating Amazon Linux packages"
sudo dnf update -y

log "Installing Java 17 and required utilities"
sudo dnf install -y java-17-amazon-corretto curl tar gzip

log "Checking Java"
java -version

log "Creating Kafka directories"
mkdir -p "${KAFKA_DIR}" "${ZK_DATA_DIR}" "${KAFKA_LOG_DIR}"

if [[ ! -d "${KAFKA_HOME}" ]]; then
  log "Downloading Apache Kafka ${KAFKA_VERSION}"
  cd "${KAFKA_DIR}"
  curl -fL --retry 3 -o "${KAFKA_TGZ}" "${KAFKA_URL}"

  log "Extracting Kafka"
  tar -xzf "${KAFKA_TGZ}"
fi

[[ -x "${KAFKA_HOME}/bin/kafka-server-start.sh" ]] || fail "Kafka installation not found at ${KAFKA_HOME}"
[[ -x "${KAFKA_HOME}/bin/zookeeper-server-start.sh" ]] || fail "ZooKeeper script not found in ${KAFKA_HOME}"

log "Backing up Kafka configuration files"
cd "${KAFKA_HOME}"
[[ -f config/zookeeper.properties.bak ]] || cp config/zookeeper.properties config/zookeeper.properties.bak
[[ -f config/server.properties.bak ]] || cp config/server.properties config/server.properties.bak

log "Configuring ZooKeeper"
# Remove active dataDir/clientPort entries, then append our lab values.
sed -i '/^[[:space:]]*dataDir[[:space:]]*=.*/d;/^[[:space:]]*clientPort[[:space:]]*=.*/d' config/zookeeper.properties
cat >> config/zookeeper.properties <<EOF_ZK

dataDir=${ZK_DATA_DIR}
clientPort=2181
EOF_ZK

log "Configuring Kafka broker"
# Keep this lab intentionally simple: one broker, local client access, ZooKeeper mode.
sed -i '/^[[:space:]]*broker.id[[:space:]]*=.*/d;/^[[:space:]]*listeners[[:space:]]*=.*/d;/^[[:space:]]*advertised.listeners[[:space:]]*=.*/d;/^[[:space:]]*log.dirs[[:space:]]*=.*/d;/^[[:space:]]*zookeeper.connect[[:space:]]*=.*/d' config/server.properties
cat >> config/server.properties <<EOF_KAFKA

# VishwaTech Labs - single broker ZooKeeper lab
broker.id=0
listeners=PLAINTEXT://:9092
log.dirs=${KAFKA_LOG_DIR}
zookeeper.connect=localhost:2181
EOF_KAFKA

log "Creating ZooKeeper systemd service"
sudo tee /etc/systemd/system/zookeeper.service >/dev/null <<EOF_SERVICE_ZK
[Unit]
Description=Apache ZooKeeper - VishwaTech Labs
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=${KAFKA_HOME}
ExecStart=${KAFKA_HOME}/bin/zookeeper-server-start.sh ${KAFKA_HOME}/config/zookeeper.properties
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF_SERVICE_ZK

log "Creating Kafka systemd service"
sudo tee /etc/systemd/system/kafka.service >/dev/null <<EOF_SERVICE_KAFKA
[Unit]
Description=Apache Kafka Broker - VishwaTech Labs
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=ec2-user
WorkingDirectory=${KAFKA_HOME}
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF_SERVICE_KAFKA

log "Reloading systemd"
sudo systemctl daemon-reload

log "Enabling services at boot"
sudo systemctl enable zookeeper
sudo systemctl enable kafka

log "Stopping any existing lab services before clean startup"
sudo systemctl stop kafka 2>/dev/null || true
sudo systemctl stop zookeeper 2>/dev/null || true

log "Starting ZooKeeper"
sudo systemctl start zookeeper
sleep 5

log "Starting Kafka"
sudo systemctl start kafka
sleep 10

log "Checking service status"
sudo systemctl --no-pager --full status zookeeper || true
sudo systemctl --no-pager --full status kafka || true

log "Checking Java processes"
jps || true

log "Checking ports"
ss -lntp | grep -E ':2181|:9092' || true

log "Final configuration summary"
echo "Kafka home : ${KAFKA_HOME}"
echo "ZooKeeper  : localhost:2181"
echo "Kafka      : localhost:9092"
echo "Kafka data : ${KAFKA_LOG_DIR}"
echo "ZK data    : ${ZK_DATA_DIR}"
echo "Linux user : $(id -un)"

echo
log "Installation complete. Next: create a topic and test producer/consumer."
echo "Example:"
echo "  cd ${KAFKA_HOME}"
echo "  bin/kafka-topics.sh --create --topic my-first-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"
echo "  bin/kafka-console-producer.sh --topic my-first-topic --bootstrap-server localhost:9092"
echo "  bin/kafka-console-consumer.sh --topic my-first-topic --bootstrap-server localhost:9092 --from-beginning"
