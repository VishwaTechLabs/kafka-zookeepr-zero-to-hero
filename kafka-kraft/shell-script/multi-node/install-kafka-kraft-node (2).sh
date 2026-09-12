#!/bin/bash
# ============================================================
# VishwaTech Kafka 4.0.2 KRaft Lab
# 2 Dedicated Controllers + 2 Dedicated Brokers
# Amazon Linux 2023 / ec2-user
#
# IMPORTANT:
#   This script installs/configures ONE node.
#   Run it separately on each EC2 instance with the correct
#   NODE_ROLE / NODE_ID / IP variables.
# ============================================================

set -euo pipefail

: "${NODE_ROLE:?Set NODE_ROLE=controller or broker}"
: "${NODE_ID:?Set NODE_ID}"
: "${NODE_PRIVATE_IP:?Set NODE_PRIVATE_IP}"
: "${CTRL1_PRIVATE_IP:?Set CTRL1_PRIVATE_IP}"
: "${CTRL2_PRIVATE_IP:?Set CTRL2_PRIVATE_IP}"
: "${KAFKA_CLUSTER_ID:?Set the SAME KAFKA_CLUSTER_ID on all nodes}"

KAFKA_VERSION="4.0.2"
KAFKA_DIR="/home/ec2-user/kafka"
DATA_DIR="/home/ec2-user/kafka-data"
KAFKA_TGZ="/home/ec2-user/kafka_2.13-${KAFKA_VERSION}.tgz"
CONTROLLERS="${NODE_ID}" # placeholder; overwritten below

echo "============================================================"
echo "VishwaTech Kafka KRaft Installer"
echo "ROLE              : ${NODE_ROLE}"
echo "NODE ID           : ${NODE_ID}"
echo "PRIVATE IP        : ${NODE_PRIVATE_IP}"
echo "CONTROLLER-1 IP   : ${CTRL1_PRIVATE_IP}"
echo "CONTROLLER-2 IP   : ${CTRL2_PRIVATE_IP}"
echo "CLUSTER ID        : ${KAFKA_CLUSTER_ID}"
echo "============================================================"

sudo dnf install -y java-17-amazon-corretto wget tar gzip curl nc

java -version

mkdir -p "${KAFKA_DIR}" "${DATA_DIR}"
chown -R ec2-user:ec2-user "${KAFKA_DIR}" "${DATA_DIR}"

if [ ! -x "${KAFKA_DIR}/bin/kafka-server-start.sh" ]; then
  cd /home/ec2-user

  if [ ! -f "${KAFKA_TGZ}" ]; then
    wget "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz"
  fi

  rm -rf "/home/ec2-user/kafka_2.13-${KAFKA_VERSION}"
  tar -xzf "${KAFKA_TGZ}"

  rm -rf "${KAFKA_DIR}"
  mv "/home/ec2-user/kafka_2.13-${KAFKA_VERSION}" "${KAFKA_DIR}"
  chown -R ec2-user:ec2-user "${KAFKA_DIR}"
fi

mkdir -p "${KAFKA_DIR}/config/kraft"
chown -R ec2-user:ec2-user "${KAFKA_DIR}"

QUORUM="1@${CTRL1_PRIVATE_IP}:9093,2@${CTRL2_PRIVATE_IP}:9093"

if [ "${NODE_ROLE}" = "controller" ]; then

cat > "${KAFKA_DIR}/config/kraft/controller.properties" <<EOF
process.roles=controller
node.id=${NODE_ID}

listeners=CONTROLLER://${NODE_PRIVATE_IP}:9093
controller.listener.names=CONTROLLER

controller.quorum.voters=${QUORUM}

listener.security.protocol.map=CONTROLLER:PLAINTEXT

log.dirs=${DATA_DIR}
EOF

chown ec2-user:ec2-user "${KAFKA_DIR}/config/kraft/controller.properties"

cat > /tmp/kafka-controller.service <<EOF
[Unit]
Description=Apache Kafka KRaft Controller
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
Group=ec2-user
Environment="KAFKA_HEAP_OPTS=-Xms512M -Xmx512M"
ExecStart=${KAFKA_DIR}/bin/kafka-server-start.sh ${KAFKA_DIR}/config/kraft/controller.properties
ExecStop=${KAFKA_DIR}/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

sudo mv /tmp/kafka-controller.service /etc/systemd/system/kafka-controller.service

else

: "${NODE_PUBLIC_IP:?Set NODE_PUBLIC_IP on broker nodes}"

cat > "${KAFKA_DIR}/config/kraft/server.properties" <<EOF
process.roles=broker
node.id=${NODE_ID}

listeners=INTERNAL://${NODE_PRIVATE_IP}:19092,EXTERNAL://0.0.0.0:9092
advertised.listeners=INTERNAL://${NODE_PRIVATE_IP}:19092,EXTERNAL://${NODE_PUBLIC_IP}:9092

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER

controller.quorum.voters=${QUORUM}

log.dirs=${DATA_DIR}

num.partitions=2
default.replication.factor=2
min.insync.replicas=1

offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1

auto.create.topics.enable=false
EOF

chown ec2-user:ec2-user "${KAFKA_DIR}/config/kraft/server.properties"

cat > /tmp/kafka.service <<EOF
[Unit]
Description=Apache Kafka KRaft Broker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
Group=ec2-user
Environment="KAFKA_HEAP_OPTS=-Xms1G -Xmx1G"
ExecStart=${KAFKA_DIR}/bin/kafka-server-start.sh ${KAFKA_DIR}/config/kraft/server.properties
ExecStop=${KAFKA_DIR}/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

sudo mv /tmp/kafka.service /etc/systemd/system/kafka.service

fi

echo
echo "Installation/configuration complete."
echo
echo "NEXT STEP:"
echo "  Controller nodes: format with --initial-controllers"
echo "  Broker nodes    : format with --no-initial-controllers"
echo
echo "DO NOT format an already-initialized data directory again."
