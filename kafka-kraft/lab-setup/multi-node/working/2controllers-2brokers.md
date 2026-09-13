# 🚀 VishwaTechLabs — Apache Kafka KRaft Cluster on AWS EC2

<p align="center">

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-EC2-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Amazon Linux](https://img.shields.io/badge/Amazon%20Linux-2023-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)
![Java](https://img.shields.io/badge/Java-17-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Architecture](https://img.shields.io/badge/Architecture-2%20Brokers%20%2B%202%20Controllers-blue?style=for-the-badge)
![Mode](https://img.shields.io/badge/Mode-KRaft-success?style=for-the-badge)
![Lab](https://img.shields.io/badge/Type-Hands--On%20Lab-purple?style=for-the-badge)

</p>

<p align="center">
  <b>Build a real Apache Kafka KRaft cluster using 2 dedicated Kafka Brokers and 2 dedicated KRaft Controllers on AWS EC2.</b>
</p>

---

## 📚 Table of Contents

- [🎯 Lab Objective](#-lab-objective)
- [💡 Why This Lab Is Useful](#-why-this-lab-is-useful)
- [🏢 Real-World Use Cases](#-real-world-use-cases)
- [🧠 Concepts You Will Learn](#-concepts-you-will-learn)
- [🏗️ Architecture](#️-architecture)
- [🖥️ EC2 Inventory](#️-ec2-inventory)
- [🌐 Private IP vs Public IP](#-private-ip-vs-public-ip)
- [🔐 AWS Security Group Rules](#-aws-security-group-rules)
- [☕ Prerequisites](#-prerequisites)
- [📁 Kafka Directory Structure](#-kafka-directory-structure)
- [⚙️ Controller-1 Configuration](#️-controller-1-configuration)
- [⚙️ Controller-2 Configuration](#️-controller-2-configuration)
- [⚙️ Broker-1 Configuration](#️-broker-1-configuration)
- [⚙️ Broker-2 Configuration](#️-broker-2-configuration)
- [🆔 Generate the KRaft Cluster ID](#-generate-the-kraft-cluster-id)
- [💾 Format Kafka Storage](#-format-kafka-storage)
- [▶️ Start the Cluster](#️-start-the-cluster)
- [✅ Validate the Cluster](#-validate-the-cluster)
- [🧪 Create a Test Topic](#-create-a-test-topic)
- [📤 Producer Test](#-producer-test)
- [📥 Consumer Test](#-consumer-test)
- [💻 Connect from Offset Explorer](#-connect-from-offset-explorer)
- [🔍 How the Listener Design Works](#-how-the-listener-design-works)
- [🛠️ Troubleshooting](#️-troubleshooting)
- [⚠️ Important Production Notes](#️-important-production-notes)
- [🎓 Learning Outcomes](#-learning-outcomes)
- [🔗 Useful References](#-useful-references)

---

# 🎯 Lab Objective

The objective of this lab is to build a **fully working Apache Kafka cluster in KRaft mode** using:

- 🎛️ **2 dedicated KRaft Controllers**
- 📦 **2 dedicated Kafka Brokers**
- ☁️ **AWS EC2**
- 🐧 **Amazon Linux**
- ☕ **Java 17**
- 🌐 **Private networking inside AWS**
- 💻 **External Kafka access from a laptop**
- 🔭 **Offset Explorer connectivity**

This architecture helps us understand how Kafka works when the **controller role and broker role are separated**.

---

# 💡 Why This Lab Is Useful

This lab is valuable because it gives practical experience with the same core Kafka concepts used in enterprise environments.

After completing this lab, you will understand:

✅ Kafka KRaft architecture  
✅ Difference between a Kafka Broker and KRaft Controller  
✅ Kafka cluster metadata management  
✅ Controller quorum  
✅ Kafka listeners  
✅ Advertised listeners  
✅ Private vs public networking  
✅ Kafka broker-to-broker communication  
✅ Broker-to-controller communication  
✅ Topic creation  
✅ Partitioning  
✅ Replication  
✅ ISR — In-Sync Replicas  
✅ Producer and consumer testing  
✅ External client connectivity  
✅ AWS Security Groups  
✅ Troubleshooting Kafka networking issues  

---

# 🏢 Real-World Use Cases

Kafka is widely used as an **event streaming platform**.

Typical enterprise use cases include:

### 🛒 E-Commerce

```text
Customer Order
      ↓
Kafka Topic
      ↓
 ┌───────────────┬────────────────┬────────────────┐
 ↓               ↓                ↓
Payment       Inventory       Notification
Service       Service          Service
```

Kafka can publish an `order-created` event and multiple systems can consume it independently.

---

### 🏦 Banking

Kafka can handle events such as:

```text
Transaction Created
        ↓
      Kafka
        ↓
 ┌─────────────┬───────────────┬──────────────────┐
 ↓             ↓               ↓
Fraud       Ledger          Notification
Engine      System          Service
```

Possible use cases:

- Transaction processing
- Fraud detection
- Audit events
- Customer notifications
- Account activity streams
- Security monitoring

---

### 🔐 Cybersecurity

Kafka is commonly used for security event pipelines.

```text
Applications
Servers
Firewalls
Cloud Logs
Endpoints
      │
      ▼
    Kafka
      │
      ├──► SIEM
      ├──► SOC Analytics
      ├──► Threat Detection
      └──► Data Lake
```

---

### 📊 Real-Time Analytics

```text
Applications
     ↓
Kafka
     ↓
Stream Processing
     ↓
Dashboard / Analytics
```

Possible technologies:

- Apache Flink
- Spark Streaming
- Kafka Streams
- Elasticsearch
- OpenSearch

---

### 🤖 AI / ML Pipelines

Kafka can also feed real-time data into:

```text
Applications
     ↓
Kafka Topics
     ↓
Feature Pipeline
     ↓
ML / AI Model
     ↓
Prediction / Decision
```

---

# 🧠 Concepts You Will Learn

## Kafka Broker

A Kafka Broker:

- Stores messages
- Hosts topic partitions
- Handles producers
- Handles consumers
- Replicates partition data

In this lab:

```text
Broker-1 = node.id 3
Broker-2 = node.id 4
```

---

## KRaft Controller

A KRaft Controller manages Kafka metadata.

It handles information such as:

- Brokers
- Topics
- Partitions
- Leaders
- Replicas
- Cluster metadata

KRaft replaces the ZooKeeper dependency used by older Kafka architectures.

In this lab:

```text
Controller-1 = node.id 1
Controller-2 = node.id 2
```

---

# 🏗️ Architecture

```text
                          ┌──────────────────────────┐
                          │       YOUR LAPTOP        │
                          │                          │
                          │     Offset Explorer      │
                          └────────────┬─────────────┘
                                       │
                              Internet / TCP 19092
                                       │
                 ┌─────────────────────┴─────────────────────┐
                 │                                           │
                 ▼                                           ▼
      ┌─────────────────────┐                    ┌─────────────────────┐
      │      BROKER-1       │                    │      BROKER-2       │
      │                     │                    │                     │
      │ node.id = 3         │◄──── TCP 9092 ───►│ node.id = 4         │
      │                     │                    │                     │
      │ Private             │                    │ Private             │
      │ 172.31.36.36        │                    │ 172.31.42.7         │
      │                     │                    │                     │
      │ Public              │                    │ Public              │
      │ 34.253.224.235      │                    │ 18.203.251.147      │
      └──────────┬──────────┘                    └──────────┬──────────┘
                 │                                           │
                 └────────────────┬──────────────────────────┘
                                  │
                           Controller Traffic
                               TCP 9093
                                  │
                 ┌────────────────┴────────────────┐
                 │                                 │
                 ▼                                 ▼
       ┌────────────────────┐           ┌────────────────────┐
       │    CONTROLLER-1    │           │    CONTROLLER-2    │
       │                    │◄─────────►│                    │
       │ node.id = 1        │ TCP 9093  │ node.id = 2        │
       │                    │           │                    │
       │ 172.31.35.220      │           │ 172.31.34.34       │
       └────────────────────┘           └────────────────────┘
```

---

# 🖥️ EC2 Inventory

| Server | Role | Node ID | Private IP | Public IP | Kafka Port |
|---|---|---:|---|---|---|
| `controller1` | KRaft Controller | `1` | `172.31.35.220` | `3.251.97.0` | `9093` |
| `contoller2` | KRaft Controller | `2` | `172.31.34.34` | `108.133.98.249` | `9093` |
| `broker1` | Kafka Broker | `3` | `172.31.36.36` | `34.253.224.235` | `9092 / 19092` |
| `broker2` | Kafka Broker | `4` | `172.31.42.7` | `18.203.251.147` | `9092 / 19092` |

---

# 🌐 Private IP vs Public IP

This is one of the most important concepts in the lab.

## 🟢 Private IP

Use private IP addresses for communication **inside AWS**.

```text
Controller-1
172.31.35.220

Controller-2
172.31.34.34

Broker-1
172.31.36.36

Broker-2
172.31.42.7
```

Private IPs are used for:

- Controller quorum
- Broker → Controller
- Controller → Controller
- Broker → Broker
- Internal Kafka clients

---

## 🔵 Public IP

Public IP addresses are used only when an external client needs to access Kafka.

For this lab:

```text
Broker-1
34.253.224.235:19092

Broker-2
18.203.251.147:19092
```

These are used by:

```text
Laptop
Offset Explorer
External Kafka Clients
```

---

## 🚨 Golden Rule

```text
PRIVATE IP
   ↓
Internal Kafka Communication

PUBLIC IP
   ↓
External Client Communication
```

---

# 🔐 AWS Security Group Rules

Recommended inbound rules:

| Port | Protocol | Source | Purpose |
|---:|---|---|---|
| `22` | TCP | Your Laptop Public IP `/32` | SSH |
| `9092` | TCP | Kafka Security Group | Internal brokers |
| `9093` | TCP | Kafka Security Group | KRaft controllers |
| `19092` | TCP | Your Laptop Public IP `/32` | External Kafka |

Do **not** expose these to the whole internet:

```text
9092 → 0.0.0.0/0 ❌

9093 → 0.0.0.0/0 ❌
```

For the lab, external Kafka access should ideally be:

```text
19092 → YOUR_PUBLIC_IP/32
```

---

# ☕ Prerequisites

Run on **all four EC2 servers**.

```bash
sudo dnf update -y
```

Install Java:

```bash
sudo dnf install java-17-amazon-corretto -y
```

Verify:

```bash
java -version
```

Kafka installation assumed:

```text
/opt/kafka
```

Check:

```bash
cd /opt/kafka
ls
```

Expected:

```text
bin
config
libs
LICENSE
NOTICE
```

Check Kafka version:

```bash
bin/kafka-server-start.sh --version
```

---

# 📁 Kafka Directory Structure

```text
/opt/kafka
│
├── bin
│
├── config
│   ├── controller.properties
│   └── server.properties
│
├── libs
└── logs
```

Data locations used in this lab:

```text
Controllers
/var/lib/kafka/controller-data

Brokers
/var/lib/kafka/data
```

---

# ⚙️ Controller-1 Configuration

Server:

```text
controller1
```

Private IP:

```text
172.31.35.220
```

Create storage directory:

```bash
sudo mkdir -p /var/lib/kafka/controller-data
sudo chown -R ec2-user:ec2-user /var/lib/kafka
```

Edit:

```bash
cd /opt/kafka

cp config/controller.properties \
   config/controller.properties.backup

vim config/controller.properties
```

Use:

```properties
#############################################
# VishwaTechLabs - KRaft Controller-1
#############################################

process.roles=controller

node.id=1

listeners=CONTROLLER://172.31.35.220:9093

controller.listener.names=CONTROLLER

listener.security.protocol.map=CONTROLLER:PLAINTEXT

controller.quorum.voters=1@172.31.35.220:9093,2@172.31.34.34:9093

log.dirs=/var/lib/kafka/controller-data
```

---

# ⚙️ Controller-2 Configuration

Server:

```text
contoller2
```

Private IP:

```text
172.31.34.34
```

Create storage:

```bash
sudo mkdir -p /var/lib/kafka/controller-data
sudo chown -R ec2-user:ec2-user /var/lib/kafka
```

Edit:

```bash
cd /opt/kafka

cp config/controller.properties \
   config/controller.properties.backup

vim config/controller.properties
```

Use:

```properties
#############################################
# VishwaTechLabs - KRaft Controller-2
#############################################

process.roles=controller

node.id=2

listeners=CONTROLLER://172.31.34.34:9093

controller.listener.names=CONTROLLER

listener.security.protocol.map=CONTROLLER:PLAINTEXT

controller.quorum.voters=1@172.31.35.220:9093,2@172.31.34.34:9093

log.dirs=/var/lib/kafka/controller-data
```

---

# ⚙️ Broker-1 Configuration

Server:

```text
broker1
```

Private IP:

```text
172.31.36.36
```

Public IP:

```text
34.253.224.235
```

Create data directory:

```bash
sudo mkdir -p /var/lib/kafka/data
sudo chown -R ec2-user:ec2-user /var/lib/kafka
```

Edit:

```bash
cd /opt/kafka

cp config/server.properties \
   config/server.properties.backup

vim config/server.properties
```

Configuration:

```properties
##################################################
# VishwaTechLabs - Kafka Broker-1
##################################################

process.roles=broker

node.id=3


##################################################
# KRaft Controllers
##################################################

controller.quorum.voters=1@172.31.35.220:9093,2@172.31.34.34:9093

controller.listener.names=CONTROLLER


##################################################
# Kafka Listeners
##################################################

listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:19092


##################################################
# Advertised Listeners
##################################################

advertised.listeners=INTERNAL://172.31.36.36:9092,EXTERNAL://34.253.224.235:19092


##################################################
# Listener Protocols
##################################################

listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT


##################################################
# Inter-Broker Communication
##################################################

inter.broker.listener.name=INTERNAL


##################################################
# Storage
##################################################

log.dirs=/var/lib/kafka/data


##################################################
# Performance
##################################################

num.network.threads=3

num.io.threads=8

socket.send.buffer.bytes=102400

socket.receive.buffer.bytes=102400

socket.request.max.bytes=104857600


##################################################
# Topic Defaults
##################################################

num.partitions=3

default.replication.factor=2


##################################################
# Kafka Internal Topics
##################################################

offsets.topic.replication.factor=2

transaction.state.log.replication.factor=2

transaction.state.log.min.isr=1

min.insync.replicas=1


##################################################
# Retention
##################################################

log.retention.hours=168

log.segment.bytes=1073741824

log.retention.check.interval.ms=300000
```

---

# ⚙️ Broker-2 Configuration

Server:

```text
broker2
```

Private IP:

```text
172.31.42.7
```

Public IP:

```text
18.203.251.147
```

Create directory:

```bash
sudo mkdir -p /var/lib/kafka/data
sudo chown -R ec2-user:ec2-user /var/lib/kafka
```

Edit:

```bash
cd /opt/kafka

cp config/server.properties \
   config/server.properties.backup

vim config/server.properties
```

Configuration:

```properties
##################################################
# VishwaTechLabs - Kafka Broker-2
##################################################

process.roles=broker

node.id=4


##################################################
# KRaft Controllers
##################################################

controller.quorum.voters=1@172.31.35.220:9093,2@172.31.34.34:9093

controller.listener.names=CONTROLLER


##################################################
# Kafka Listeners
##################################################

listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:19092


##################################################
# Advertised Listeners
##################################################

advertised.listeners=INTERNAL://172.31.42.7:9092,EXTERNAL://18.203.251.147:19092


##################################################
# Listener Protocol
##################################################

listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT


##################################################
# Inter-Broker Communication
##################################################

inter.broker.listener.name=INTERNAL


##################################################
# Storage
##################################################

log.dirs=/var/lib/kafka/data


##################################################
# Performance
##################################################

num.network.threads=3

num.io.threads=8

socket.send.buffer.bytes=102400

socket.receive.buffer.bytes=102400

socket.request.max.bytes=104857600


##################################################
# Topic Defaults
##################################################

num.partitions=3

default.replication.factor=2


##################################################
# Kafka Internal Topics
##################################################

offsets.topic.replication.factor=2

transaction.state.log.replication.factor=2

transaction.state.log.min.isr=1

min.insync.replicas=1


##################################################
# Retention
##################################################

log.retention.hours=168

log.segment.bytes=1073741824

log.retention.check.interval.ms=300000
```

---

# 🆔 Generate the KRaft Cluster ID

Generate the cluster ID **only once**.

Run on Controller-1:

```bash
cd /opt/kafka

bin/kafka-storage.sh random-uuid
```

Example output:

```text
pvIHXL1QSuKVdXbZG2zjMQ
```

> ⚠️ Your actual cluster ID will be different.

The same cluster ID must be used on:

```text
Controller-1
Controller-2
Broker-1
Broker-2
```

---

## Node IDs vs Cluster ID

Node IDs must be different:

```text
Controller-1 = 1
Controller-2 = 2
Broker-1     = 3
Broker-2     = 4
```

Cluster ID must be the same:

```text
Controller-1 ─┐
Controller-2 ─┤
Broker-1 ─────┼── SAME CLUSTER ID
Broker-2 ─────┘
```

---

# 💾 Format Kafka Storage

Export the same cluster ID on every server:

```bash
export CLUSTER_ID='YOUR_REAL_CLUSTER_ID'
```

---

## Controller-1

```bash
cd /opt/kafka

bin/kafka-storage.sh format \
  -t "$CLUSTER_ID" \
  -c config/controller.properties
```

---

## Controller-2

```bash
cd /opt/kafka

bin/kafka-storage.sh format \
  -t "$CLUSTER_ID" \
  -c config/controller.properties
```

---

## Broker-1

```bash
cd /opt/kafka

bin/kafka-storage.sh format \
  -t "$CLUSTER_ID" \
  -c config/server.properties
```

---

## Broker-2

```bash
cd /opt/kafka

bin/kafka-storage.sh format \
  -t "$CLUSTER_ID" \
  -c config/server.properties
```

---

## ⚠️ Important

Storage formatting is a **cluster initialization step**.

Do not repeatedly run:

```bash
kafka-storage.sh format
```

during every startup.

Normal Kafka startup uses:

```bash
kafka-server-start.sh
```

---

# ▶️ Start the Cluster

Recommended startup order:

```text
1️⃣ Controller-1
        ↓
2️⃣ Controller-2
        ↓
3️⃣ Broker-1
        ↓
4️⃣ Broker-2
```

---

## Start Controller-1

```bash
cd /opt/kafka

bin/kafka-server-start.sh \
  -daemon config/controller.properties
```

Verify:

```bash
ps -ef | grep kafka

ss -lntp | grep 9093
```

---

## Start Controller-2

```bash
cd /opt/kafka

bin/kafka-server-start.sh \
  -daemon config/controller.properties
```

Verify:

```bash
ps -ef | grep kafka

ss -lntp | grep 9093
```

---

## Start Broker-1

```bash
cd /opt/kafka

bin/kafka-server-start.sh \
  -daemon config/server.properties
```

Verify:

```bash
ss -lntp | grep -E '9092|19092'
```

---

## Start Broker-2

```bash
cd /opt/kafka

bin/kafka-server-start.sh \
  -daemon config/server.properties
```

Verify:

```bash
ss -lntp | grep -E '9092|19092'
```

---

# ✅ Validate the Cluster

## Controller-1 → Controller-2

```bash
nc -vz 172.31.34.34 9093
```

---

## Controller-2 → Controller-1

```bash
nc -vz 172.31.35.220 9093
```

---

## Broker-1 → Controllers

```bash
nc -vz 172.31.35.220 9093

nc -vz 172.31.34.34 9093
```

---

## Broker-2 → Controllers

```bash
nc -vz 172.31.35.220 9093

nc -vz 172.31.34.34 9093
```

---

## Broker-1 → Broker-2

```bash
nc -vz 172.31.42.7 9092
```

---

## Broker-2 → Broker-1

```bash
nc -vz 172.31.36.36 9092
```

---

# 👑 Check KRaft Metadata Quorum

Run:

```bash
cd /opt/kafka

bin/kafka-metadata-quorum.sh \
  --bootstrap-controller 172.31.35.220:9093 \
  describe \
  --status
```

Expected information includes:

```text
ClusterId
LeaderId
LeaderEpoch
HighWatermark
CurrentVoters
```

This confirms that the KRaft control plane is functioning.

---

# 🧪 Create a Test Topic

Run from Broker-1:

```bash
cd /opt/kafka

bin/kafka-topics.sh \
  --bootstrap-server 172.31.36.36:9092 \
  --create \
  --topic vishwatech-topic \
  --partitions 4 \
  --replication-factor 2
```

---

## Describe Topic

```bash
bin/kafka-topics.sh \
  --bootstrap-server 172.31.36.36:9092 \
  --describe \
  --topic vishwatech-topic
```

Expected conceptually:

```text
Partition 0
Leader: 3
Replicas: 3,4
ISR: 3,4

Partition 1
Leader: 4
Replicas: 4,3
ISR: 4,3
```

This demonstrates:

- Partition distribution
- Leader election
- Replication
- In-Sync Replicas

---

# 📤 Producer Test

On Broker-1:

```bash
bin/kafka-console-producer.sh \
  --bootstrap-server 172.31.36.36:9092 \
  --topic vishwatech-topic
```

Enter messages:

```text
Hello Kafka
Hello KRaft
Welcome to VishwaTechLabs
Kafka cluster is working
```

---

# 📥 Consumer Test

On Broker-2:

```bash
bin/kafka-console-consumer.sh \
  --bootstrap-server 172.31.42.7:9092 \
  --topic vishwatech-topic \
  --from-beginning
```

Expected:

```text
Hello Kafka
Hello KRaft
Welcome to VishwaTechLabs
Kafka cluster is working
```

---

# 💻 Connect from Offset Explorer

Your laptop cannot directly use AWS private IP addresses unless it has VPC connectivity such as VPN.

Therefore do not use:

```text
172.31.36.36:9092 ❌
172.31.42.7:9092  ❌
```

Use the public broker listeners:

```text
34.253.224.235:19092

18.203.251.147:19092
```

Bootstrap servers:

```text
34.253.224.235:19092,18.203.251.147:19092
```

---

## Offset Explorer Settings

```text
Cluster Name:
VishwaTechLabs-KRaft

Security:
PLAINTEXT

Bootstrap Brokers:
34.253.224.235:19092
18.203.251.147:19092
```

For this learning lab:

```text
SSL  = No
SASL = No
```

> 🚨 PLAINTEXT is appropriate only for a controlled learning lab. Production Kafka should use authentication and encryption.

---

# 🔍 How the Listener Design Works

This is one of the most important Kafka concepts.

Broker-1:

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:19092
```

This means:

> Listen on ports 9092 and 19092 on the server.

But:

```properties
advertised.listeners=INTERNAL://172.31.36.36:9092,EXTERNAL://34.253.224.235:19092
```

means:

> Tell clients which addresses they should actually use.

---

## Internal Client Flow

```text
Kafka Client inside AWS
        │
        ▼
172.31.36.36:9092
```

---

## External Client Flow

```text
Laptop
  │
  ▼
Internet
  │
  ▼
34.253.224.235:19092
```

The same principle applies to Broker-2.

---

# 🛠️ Troubleshooting

## Kafka is not running

Check:

```bash
ps -ef | grep kafka
```

Then inspect logs:

```bash
cd /opt/kafka

ls logs
```

Try:

```bash
tail -100 logs/server.log
```

---

## Controller port not listening

```bash
ss -lntp | grep 9093
```

Check:

```properties
listeners=CONTROLLER://PRIVATE_IP:9093
```

---

## Broker ports not listening

```bash
ss -lntp | grep -E '9092|19092'
```

---

## Broker cannot connect to controller

Test:

```bash
nc -vz 172.31.35.220 9093

nc -vz 172.31.34.34 9093
```

Check:

- Security Group
- Controller process
- Private IP
- Port 9093
- `controller.quorum.voters`

---

## Broker cannot connect to Broker

```bash
nc -vz 172.31.42.7 9092
```

or:

```bash
nc -vz 172.31.36.36 9092
```

Check:

```properties
inter.broker.listener.name=INTERNAL
```

---

## Offset Explorer cannot connect

Check all of the following:

```text
✔ Broker process running
✔ Port 19092 listening
✔ Security Group allows 19092 from laptop
✔ advertised.listeners contains PUBLIC IP
✔ Public IP has not changed
```

Test from your laptop:

```bash
nc -vz 34.253.224.235 19092
```

and:

```bash
nc -vz 18.203.251.147 19092
```

---

# ⚠️ Important Production Notes

## 1. Two Controllers Are Lab-Oriented

This lab intentionally uses:

```text
2 Controllers
```

This is useful for understanding KRaft.

However, a production deployment normally uses an **odd number** of controllers such as:

```text
3 Controllers ✅

5 Controllers ✅
```

A 2-controller quorum does not provide useful controller failure tolerance because a majority is required.

---

## 2. Use Elastic IP or DNS for External Brokers

The current public IPs are:

```text
34.253.224.235
18.203.251.147
```

Normal EC2 public IPv4 addresses can change after stop/start.

For a more stable lab:

```text
Elastic IP
     ↓
Broker
```

Even better for production:

```text
DNS Name
   ↓
Load Balancing / Network Design
   ↓
Kafka Broker
```

---

## 3. Do Not Use PLAINTEXT in Production

This lab uses:

```text
PLAINTEXT
```

Production Kafka should consider:

```text
TLS
SASL_SSL
SASL/SCRAM
mTLS
ACLs
RBAC
Secrets Management
```

---

## 4. Do Not Expose Kafka to the Whole Internet

Avoid:

```text
19092 → 0.0.0.0/0
```

Prefer:

```text
19092 → trusted IPs only
```

or use:

```text
VPN
Private Networking
AWS Transit Gateway
Direct Connect
PrivateLink-style architecture
```

depending on the design.

---

# 🎓 Learning Outcomes

After completing this lab, students should be able to explain:

### Architecture

```text
What is Kafka?

What is a Kafka Broker?

What is KRaft?

What is a KRaft Controller?

Why did Kafka move away from ZooKeeper?
```

### Networking

```text
What is a listener?

What is advertised.listeners?

Why do we need internal and external listeners?

Why use private IP internally?

Why use public IP externally?
```

### KRaft

```text
What is node.id?

What is cluster.id?

Why must node IDs be unique?

Why must all nodes share the same cluster ID?

What is controller.quorum.voters?
```

### Kafka

```text
What is a topic?

What is a partition?

What is replication factor?

What is leader?

What is follower?

What is ISR?

What happens if one broker fails?
```

### Operations

Students will know how to:

```text
Install Kafka
Configure KRaft
Configure brokers
Format storage
Start controllers
Start brokers
Check ports
Check logs
Create topics
Run producers
Run consumers
Connect Offset Explorer
Troubleshoot networking
```

---

# 🧩 Complete Lab Flow

```text
Launch 4 EC2 Instances
          │
          ▼
Install Java
          │
          ▼
Install Kafka
          │
          ▼
Configure Security Groups
          │
          ▼
Configure Controller-1
          │
          ▼
Configure Controller-2
          │
          ▼
Configure Broker-1
          │
          ▼
Configure Broker-2
          │
          ▼
Generate ONE Cluster ID
          │
          ▼
Format all 4 Kafka nodes
          │
          ▼
Start Controller-1
          │
          ▼
Start Controller-2
          │
          ▼
Verify KRaft Quorum
          │
          ▼
Start Broker-1
          │
          ▼
Start Broker-2
          │
          ▼
Create Topic
          │
          ▼
Run Producer
          │
          ▼
Run Consumer
          │
          ▼
Connect Offset Explorer
          │
          ▼
🎉 KAFKA KRAFT LAB COMPLETE
```

---

# 📌 Quick Reference

```text
CONTROLLER-1
Node ID     : 1
Private IP  : 172.31.35.220
Port        : 9093


CONTROLLER-2
Node ID     : 2
Private IP  : 172.31.34.34
Port        : 9093


BROKER-1
Node ID     : 3
Private IP  : 172.31.36.36
Public IP   : 34.253.224.235
Internal    : 9092
External    : 19092


BROKER-2
Node ID     : 4
Private IP  : 172.31.42.7
Public IP   : 18.203.251.147
Internal    : 9092
External    : 19092
```

---

## Controller Quorum

```properties
controller.quorum.voters=1@172.31.35.220:9093,2@172.31.34.34:9093
```

---

## External Bootstrap Servers

```text
34.253.224.235:19092,18.203.251.147:19092
```

---

# 🏆 Lab Achievement

By completing this lab you have successfully built:

![Kafka](https://img.shields.io/badge/Kafka-Cluster-success?style=for-the-badge&logo=apachekafka)
![KRaft](https://img.shields.io/badge/KRaft-Enabled-success?style=for-the-badge)
![Brokers](https://img.shields.io/badge/Brokers-2-blue?style=for-the-badge)
![Controllers](https://img.shields.io/badge/Controllers-2-orange?style=for-the-badge)
![AWS](https://img.shields.io/badge/AWS-EC2-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![External](https://img.shields.io/badge/External%20Access-Enabled-purple?style=for-the-badge)

```text
             🎉 VishwaTechLabs 🎉

         Apache Kafka KRaft Cluster

             2 Controllers
                   +
              2 Brokers
                   +
                AWS EC2
                   +
         External Client Access
```

---

# 🔗 Useful References

- 🌐 [Apache Kafka](https://kafka.apache.org/)
- 📘 [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- ⚙️ [Kafka KRaft](https://kafka.apache.org/documentation/#kraft)
- ☁️ [AWS EC2 Documentation](https://docs.aws.amazon.com/ec2/)
- 🔐 [AWS Security Groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html)
- ☕ [Amazon Corretto](https://aws.amazon.com/corretto/)
- 🔭 [Offset Explorer](https://www.kafkatool.com/)

---

# 👨‍💻 VishwaTechLabs

> **Learn → Build → Break → Troubleshoot → Understand → Master**

This lab is designed for:

- Kafka Administrators
- DevOps Engineers
- Platform Engineers
- Cloud Engineers
- SRE Engineers
- Data Engineers
- Security Engineers
- Students preparing for Kafka interviews
- Engineers learning distributed systems

---

<p align="center">

### ⭐ If this lab helped you, keep practicing by breaking the cluster and troubleshooting it.

![Hands On](https://img.shields.io/badge/Learning-Hands--On-brightgreen?style=for-the-badge)
![Practice](https://img.shields.io/badge/Practice-Real%20World-blue?style=for-the-badge)
![VishwaTechLabs](https://img.shields.io/badge/VishwaTechLabs-Kafka%20Lab-orange?style=for-the-badge)

</p>
