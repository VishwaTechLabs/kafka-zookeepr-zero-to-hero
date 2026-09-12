# 🚀 VishwaTech Labs — Kafbat UI Setup on Existing Single-Node Kafka KRaft

![Kafbat UI](https://img.shields.io/badge/Kafbat%20UI-1.5.0-2ea44f?logo=apachekafka)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.0.2-black?logo=apachekafka)
![KRaft](https://img.shields.io/badge/Kafka-KRaft-blue)
![AWS](https://img.shields.io/badge/AWS-EC2-orange?logo=amazonaws)
![Amazon Linux](https://img.shields.io/badge/Amazon%20Linux-2023-orange?logo=linux)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker)
![Java](https://img.shields.io/badge/Java-17-red?logo=openjdk)
![Open Source](https://img.shields.io/badge/Open%20Source-Yes-success)
![VishwaTech](https://img.shields.io/badge/VishwaTech-Labs-1f6feb)

> 🎓 **VishwaTech Labs — Apache Kafka GUI Practical Lab**
>
> This guide installs **Kafbat UI** on the **existing single-node Apache Kafka KRaft EC2 lab**.
>
> Kafka remains unchanged. Kafbat UI runs separately as a Docker container and connects to the existing Kafka broker on `9092`.

---

# 📚 Table of Contents

- [1. 🎯 Lab Objective](#1--lab-objective)
- [2. 🧠 What is Kafbat UI](#2--what-is-kafbat-ui)
- [3. ⭐ Why Kafbat UI](#3--why-kafbat-ui)
- [4. 🏗️ Existing Kafka Lab](#4--existing-kafka-lab)
- [5. 🏆 Final Architecture](#5--final-architecture)
- [6. 🔌 Port Map](#6--port-map)
- [7. ☁️ AWS EC2 Requirements](#7--aws-ec2-requirements)
- [8. 🔐 Security Group](#8--security-group)
- [9. 🐳 Why Docker](#9--why-docker)
- [10. 📦 Kafbat Image](#10--kafbat-image)
- [11. 🐳 Install Docker](#11--install-docker)
- [12. 📁 Create Kafbat Directory](#12--create-kafbat-directory)
- [13. 🧪 Pre-Flight Kafka Validation](#13--pre-flight-kafka-validation)
- [14. ⚙️ Kafbat Configuration](#14--kafbat-configuration)
- [15. 🚀 Start Kafbat](#15--start-kafbat)
- [16. 🔎 Validate Container](#16--validate-container)
- [17. 🌐 Open Kafbat UI](#17--open-kafbat-ui)
- [18. 🔗 How Kafbat Connects to Kafka](#18--how-kafbat-connects-to-kafka)
- [19. 🧠 Why Host Networking](#19--why-host-networking)
- [20. 🖥️ Kafbat UI Areas](#20--kafbat-ui-areas)
- [21. 🟦 KRaft Architecture](#21--kraft-architecture)
- [22. 🧪 Lab 01 — Connect Cluster](#22--lab-01--connect-cluster)
- [23. 🧪 Lab 02 — Create Topic](#23--lab-02--create-topic)
- [24. 🧪 Lab 03 — Partitions](#24--lab-03--partitions)
- [25. 🧪 Lab 04 — Produce Messages](#25--lab-04--produce-messages)
- [26. 🧪 Lab 05 — Browse Messages](#26--lab-05--browse-messages)
- [27. 🧪 Lab 06 — Consumer Groups](#27--lab-06--consumer-groups)
- [28. 🧪 Lab 07 — Consumer Lag](#28--lab-07--consumer-lag)
- [29. 🧪 Lab 08 — Topic Configuration](#29--lab-08--topic-configuration)
- [30. 🧪 Lab 09 — KRaft/Broker Visibility](#30--lab-09--kraftbroker-visibility)
- [31. 🧪 Lab 10 — Multi-Cluster Concept](#31--lab-10--multi-cluster-concept)
- [32. 🧪 Lab 11 — Schema Registry](#32--lab-11--schema-registry)
- [33. 🧪 Lab 12 — Kafka Connect](#33--lab-12--kafka-connect)
- [34. 🔐 Kafbat Security](#34--kafbat-security)
- [35. 🛠️ Troubleshooting](#35--troubleshooting)
- [36. 📜 Container Logs](#36--container-logs)
- [37. 🔄 Restart/Reboot](#37--restartreboot)
- [38. 🧹 Uninstall](#38--uninstall)
- [39. 🏭 Single Node vs Production](#39--single-node-vs-production)
- [40. 🔗 Official Resources](#40--official-resources)
- [41. ✅ Final Validation](#41--final-validation)
- [42. 🏆 VishwaTech Learning Outcome](#42--vishwatech-learning-outcome)

---

# 1. 🎯 Lab Objective

We already have:

```text
AWS EC2
│
└── Apache Kafka 4.0.2
      │
      ├── Broker      :9092
      └── KRaft       :9093
```

Now we add:

```text
                    🌐 Browser
                        │
                        │ :8080
                        ▼
                 ┌──────────────┐
                 │  Kafbat UI   │
                 │   Docker     │
                 └──────┬───────┘
                        │
                        │ Kafka API
                        ▼
                 ┌──────────────┐
                 │ Kafka Broker │
                 │    :9092    │
                 └──────────────┘
```

### 🎓 Goal

Students should be able to:

- connect Kafbat to Kafka
- inspect brokers
- create topics
- inspect partitions
- produce messages
- consume/view messages
- inspect offsets
- inspect consumer groups
- understand consumer lag
- modify topic configuration
- understand KRaft architecture
- compare CLI and GUI operations
- troubleshoot Kafka using GUI + CLI

---

# 2. 🧠 What is Kafbat UI?

**Kafbat UI** is a free, open-source web UI for monitoring and managing Apache Kafka clusters.

The official project describes it as a lightweight web UI for Kafka with visibility into brokers, topics, partitions, production/consumption, consumer groups and message browsing. citeturn1search1

### Simple definition

> **Kafbat UI = Visual control panel for Kafka.**

Instead of only using:

```bash
kafka-topics.sh
kafka-console-producer.sh
kafka-console-consumer.sh
kafka-consumer-groups.sh
```

you can visualize Kafka through a browser.

---

# 3. ⭐ Why Kafbat UI?

The official project currently highlights:

- 📚 Topic insights
- ⚙️ Topic configuration
- 🖥️ Broker overview
- 👥 Consumer groups
- 📉 Consumer lag
- 🔎 Message browser
- 🧩 Multi-cluster management
- 🧬 Schema Registry
- 🔌 Kafka Connect
- 📊 Metrics integrations

Kafbat's current GitHub documentation also lists dynamic topic management and support for JSON, plain text and Avro message browsing. citeturn1search1

### CLI vs Kafbat

| Operation | CLI | Kafbat |
|---|---|---|
| List topics | ✅ | ✅ |
| Create topic | ✅ | ✅ |
| Delete topic | ✅ | ✅ |
| Partitions | ✅ | ✅ |
| Produce messages | ✅ | ✅ |
| Browse messages | ✅ | ⭐⭐⭐⭐⭐ |
| Consumer groups | ✅ | ⭐⭐⭐⭐⭐ |
| Consumer lag | ✅ | ⭐⭐⭐⭐⭐ |
| Configuration | ✅ | ⭐⭐⭐⭐⭐ |
| Visual learning | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| Automation | ⭐⭐⭐⭐⭐ | ⭐⭐ |

### 🧠 Golden rule

> **CLI teaches Kafka. Kafbat visualizes Kafka.**

---

# 4. 🏗️ Existing Kafka Lab

This README assumes the previous VishwaTech KRaft lab is already installed.

Expected:

```text
OS
└── Amazon Linux 2023

Linux user
└── ec2-user

Java
└── 17

Kafka
└── Apache Kafka 4.0.2

Mode
└── KRaft

Kafka service
└── kafka-kraft.service

Broker
└── 9092

Controller
└── 9093
```

Expected Kafka process:

```bash
jps
```

Example:

```text
Kafka
Jps
```

No:

```text
QuorumPeerMain
```

because KRaft does not use ZooKeeper.

---

# 5. 🏆 Final Architecture

## 🌟 Recommended single-EC2 architecture

```text
                         🌐 STUDENT LAPTOP
                                │
                                │ HTTP :8080
                                ▼
                  ┌─────────────────────────┐
                  │        AWS EC2          │
                  │                         │
                  │   ┌─────────────────┐   │
                  │   │   Kafbat UI     │   │
                  │   │     Docker      │   │
                  │   │      :8080      │   │
                  │   └────────┬────────┘   │
                  │            │            │
                  │            │ Kafka API  │
                  │            ▼            │
                  │   ┌─────────────────┐   │
                  │   │ Kafka KRaft     │   │
                  │   │                 │   │
                  │   │ Broker :9092    │   │
                  │   │ Ctrl   :9093   │   │
                  │   └─────────────────┘   │
                  │                         │
                  └─────────────────────────┘
```

### Important

Kafbat connects to:

```text
Kafka Broker :9092
```

It does **not** need to connect directly to:

```text
KRaft Controller :9093
```

---

# 6. 🔌 Port Map

| Port | Component | Purpose | Public? |
|---:|---|---|---|
| `22` | SSH | EC2 administration | 🔒 Your IP only |
| `8080` | Kafbat | Web UI | 🔒 Your IP only |
| `9092` | Kafka | Broker/client traffic | ⚠️ Only if required |
| `9093` | Kafka | KRaft controller | ❌ Never public |

### Recommended Security Group

```text
TCP 22
Source: YOUR_IP/32

TCP 8080
Source: YOUR_IP/32

TCP 9092
Only if remote Kafka clients are required

TCP 9093
DO NOT expose
```

If Kafbat and Kafka are on the same EC2 host, Kafbat can use local `9092`, so there is normally no reason to expose `9092` publicly merely for Kafbat.

---

# 7. ☁️ AWS EC2 Requirements

### Existing lab

```text
Instance:
t3.medium

CPU:
2 vCPU

Memory:
4 GB

Storage:
30 GB gp3

OS:
Amazon Linux 2023
```

### Recommended if running several Kafka GUIs

```text
t3.large
2 vCPU
8 GB RAM
```

For a simple Kafbat-only lab, `t3.medium` can be sufficient, but monitor memory.

---

# 8. 🔐 Security Group

Open AWS:

```text
EC2
 ↓
Security Groups
 ↓
Inbound rules
```

Add:

```text
Custom TCP
Port: 8080
Source: YOUR_PUBLIC_IP/32
```

Example:

```text
203.0.113.10/32
```

Do **not** use:

```text
0.0.0.0/0
```

for an administrative Kafka UI unless you intentionally understand and secure the exposure.

---

# 9. 🐳 Why Docker?

Kafbat officially supports running through a pre-built Docker image, and the project documentation provides Docker/Compose examples. citeturn1search1turn1search0

Architecture:

```text
Kafka
  │
  │ Existing installation
  ▼
Kafbat
  │
  │ Separate Docker container
  ▼
Browser
```

### Benefits

- ✅ Kafka installation remains untouched
- ✅ Easy start/stop
- ✅ Easy upgrade
- ✅ Easy rollback
- ✅ Clean lab
- ✅ Repeatable
- ✅ Easy to remove

---

# 10. 📦 Kafbat Image

Official image:

```text
ghcr.io/kafbat/kafka-ui
```

The current official GitHub repository shows **1.5.0** as the latest release as of this guide's creation. citeturn1search1

For a reproducible lab, you can pin:

```text
ghcr.io/kafbat/kafka-ui:1.5.0
```

instead of using:

```text
latest
```

### Why pin the version?

```text
latest
  │
  ├── version changes
  ├── behavior can change
  └── lab may become non-reproducible

1.5.0
  │
  └── reproducible
```

For teaching environments, version pinning is generally preferable.

---

# 11. 🐳 Install Docker

Run as `ec2-user`.

Update:

```bash
sudo dnf update -y
```

Install:

```bash
sudo dnf install -y docker
```

Start:

```bash
sudo systemctl enable --now docker
```

Verify:

```bash
sudo systemctl status docker
```

Expected:

```text
Active: active (running)
```

Check:

```bash
sudo docker version
```

---

# 12. 📁 Create Kafbat Directory

```bash
mkdir -p /home/ec2-user/kafbat-ui
```

Move:

```bash
cd /home/ec2-user/kafbat-ui
```

Verify:

```bash
pwd
```

Expected:

```text
/home/ec2-user/kafbat-ui
```

---

# 13. 🧪 Pre-Flight Kafka Validation

Before starting Kafbat, make sure Kafka is healthy.

## Check service

```bash
sudo systemctl status kafka-kraft
```

Expected:

```text
Active: active (running)
```

---

## Check processes

```bash
jps
```

Expected:

```text
Kafka
Jps
```

---

## Check ports

```bash
ss -lntp | grep -E '9092|9093'
```

Expected:

```text
LISTEN ... :9092
LISTEN ... :9093
```

---

## Test Kafka

```bash
cd /home/ec2-user/kafka/kafka_2.13-4.0.2
```

Run:

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

If this works, Kafka is ready for Kafbat.

---

# 14. ⚙️ Kafbat Configuration

There are two good approaches.

## Option A — Environment variables

Best for the first lab.

Official examples use:

```text
KAFKA_CLUSTERS_0_NAME
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS
```

The official Compose example demonstrates exactly this configuration pattern. citeturn1search0

Create:

```bash
nano /home/ec2-user/kafbat-ui/kafbat.env
```

Put:

```dotenv
KAFKA_CLUSTERS_0_NAME=VishwaTech-Kafka-KRaft

KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=127.0.0.1:9092

DYNAMIC_CONFIG_ENABLED=true

SWAGGER_UI_ENABLED=false
```

Save.

Secure:

```bash
chmod 600 /home/ec2-user/kafbat-ui/kafbat.env
```

---

## Option B — Dynamic configuration

Kafbat supports a persistent dynamic configuration file, and the official documentation shows mounting a configuration file and enabling dynamic configuration. citeturn1search1turn1search3

For your first lab, **Option A is simpler**.

Later we can build a full:

```text
config.yml
```

for multiple clusters, Schema Registry, Kafka Connect and authentication.

---

# 15. 🚀 Start Kafbat

Because Kafka is already running on the same EC2 host and advertises:

```text
localhost:9092
```

the simplest lab deployment is host networking.

Run:

```bash
sudo docker run -d \
  --name kafbat-ui \
  --restart unless-stopped \
  --network host \
  --env-file /home/ec2-user/kafbat-ui/kafbat.env \
  ghcr.io/kafbat/kafka-ui:1.5.0
```

### What this means

| Option | Purpose |
|---|---|
| `-d` | Run in background |
| `--name kafbat-ui` | Container name |
| `--restart unless-stopped` | Restart after Docker/EC2 reboot |
| `--network host` | Use EC2 host network |
| `--env-file` | Load Kafka configuration |
| `ghcr.io/...:1.5.0` | Kafbat UI image |

---

# 16. 🔎 Validate Container

Run:

```bash
sudo docker ps
```

Expected:

```text
CONTAINER ID   IMAGE                              STATUS
xxxxxxxx       ghcr.io/kafbat/kafka-ui:1.5.0    Up ...
```

Detailed:

```bash
sudo docker ps -a
```

---

## Check Kafbat port

```bash
ss -lntp | grep 8080
```

Expected:

```text
LISTEN ... :8080
```

---

## Check health endpoint

Kafbat documents:

```text
/actuator/health
```

as its liveness/readiness endpoint. citeturn1search1

Run:

```bash
curl http://127.0.0.1:8080/actuator/health
```

Expected response similar to:

```json
{"status":"UP"}
```

---

# 17. 🌐 Open Kafbat UI

Find the EC2 public IP:

```bash
curl http://checkip.amazonaws.com
```

Suppose:

```text
54.xx.xx.xx
```

Open in your browser:

```text
http://54.xx.xx.xx:8080
```

You should see:

```text
┌─────────────────────────────────────────┐
│             Kafbat UI                   │
├─────────────────────────────────────────┤
│                                         │
│  VishwaTech-Kafka-KRaft                 │
│                                         │
│  Brokers                                │
│  Topics                                 │
│  Consumers                              │
│  Messages                               │
│                                         │
└─────────────────────────────────────────┘
```

---

# 18. 🔗 How Kafbat Connects to Kafka

This is one of the most important concepts.

```text
                     AWS EC2
┌─────────────────────────────────────────────┐
│                                             │
│  ┌─────────────────────┐                    │
│  │ Kafbat UI           │                    │
│  │ Docker              │                    │
│  │                     │                    │
│  │ Bootstrap:          │                    │
│  │ 127.0.0.1:9092      │                    │
│  └──────────┬──────────┘                    │
│             │                               │
│             │ Kafka protocol                │
│             ▼                               │
│  ┌─────────────────────┐                    │
│  │ Kafka Broker        │                    │
│  │ :9092               │                    │
│  └─────────────────────┘                    │
│             │                               │
│             ▼                               │
│      KRaft Controller                      │
│          :9093                             │
│                                             │
└─────────────────────────────────────────────┘
```

Kafbat is a **Kafka client/application**, not another Kafka broker.

---

# 19. 🧠 Why Host Networking?

Normally:

```text
Docker container localhost
       ≠
EC2 host localhost
```

So:

```text
Kafbat
  │
  └── localhost:9092
        │
        ❌ container localhost
```

But with:

```bash
--network host
```

we get:

```text
Kafbat
  │
  └── 127.0.0.1:9092
           │
           ▼
       EC2 Kafka
```

### Why this is useful for your existing lab

Your Kafka configuration already uses:

```properties
advertised.listeners=PLAINTEXT://localhost:9092
```

Therefore host networking makes the Kafbat connection straightforward.

### Important production note

Do not treat host networking as mandatory for production. In a real deployment you would normally design explicit container networking, DNS, TLS, authentication and network policy.

---

# 20. 🖥️ Kafbat UI Areas

Once connected, students should explore:

```text
VishwaTech-Kafka-KRaft
│
├── 🖥 Brokers
│
├── 📚 Topics
│   ├── Messages
│   ├── Partitions
│   └── Configuration
│
├── 👥 Consumer Groups
│
├── 🔌 Kafka Connect
│
└── 🧬 Schema Registry
```

The official Kafbat project highlights topic insights, broker overview, consumer groups/lag, message browsing and dynamic topic management. citeturn1search1

---

# 21. 🟦 KRaft Architecture

Your existing Kafka:

```text
                    Kafka JVM
                       │
          ┌────────────┴────────────┐
          │                         │
       BROKER                   CONTROLLER
       :9092                      :9093
          │                         │
          │                         │
          └────────── KRaft ────────┘
```

Kafbat connects to:

```text
Kafbat
  │
  ▼
Broker :9092
```

Not:

```text
Kafbat
  │
  ▼
Controller :9093
```

### Student mental model

```text
Browser
   │
   ▼
Kafbat
   │
   ▼
Kafka Broker
   │
   ├── Topics
   ├── Partitions
   ├── Messages
   └── Consumer Groups
```

---

# 22. 🧪 LAB 01 — Connect Cluster

Open:

```text
Kafbat UI
```

Look for:

```text
VishwaTech-Kafka-KRaft
```

Validate:

```text
Status → Connected
```

### Expected

```text
Cluster
   │
   ├── Broker: 1
   ├── Topics
   └── Consumer Groups
```

---

# 23. 🧪 LAB 02 — Create Topic

Create:

```text
vishwatech-orders
```

Set:

```text
Partitions:
3

Replication Factor:
1
```

Why replication factor 1?

Because:

```text
Single-node Kafka

Broker 1
   │
   ├── P0
   ├── P1
   └── P2
```

There is no Broker 2 or Broker 3.

---

# 24. 🧪 LAB 03 — Partitions

Open:

```text
Topics
 ↓
vishwatech-orders
 ↓
Partitions
```

Understand:

```text
Topic: vishwatech-orders

P0
P1
P2
```

### Key concept

A topic is a logical name.

Partitions provide the physical/logical parallelism mechanism.

```text
Topic
 │
 ├── Partition 0
 ├── Partition 1
 └── Partition 2
```

---

# 25. 🧪 LAB 04 — Produce Messages

Kafbat can produce messages through its UI.

Example:

```json
{
  "orderId": 1001,
  "customer": "Vishwa",
  "amount": 5000
}
```

Another:

```json
{
  "orderId": 1002,
  "customer": "Rahul",
  "amount": 7200
}
```

### Student flow

```text
Kafbat Producer
       │
       ▼
Kafka Broker
       │
       ▼
vishwatech-orders
       │
       ▼
Partition
```

Kafbat's official project explicitly supports sending/writing messages to Kafka topics from the UI. citeturn1search1

---

# 26. 🧪 LAB 05 — Browse Messages

Open:

```text
Topics
 ↓
vishwatech-orders
 ↓
Messages
```

Inspect:

- Key
- Value
- Offset
- Partition
- Timestamp
- Headers where applicable

### Example

```text
Partition 0

Offset 0
   │
   └── order-1001

Offset 1
   │
   └── order-1002
```

Kafbat supports message browsing and supports JSON/plain-text/Avro message views according to the project documentation. citeturn1search1

---

# 27. 🧪 LAB 06 — Consumer Groups

Start a CLI consumer:

```bash
cd /home/ec2-user/kafka/kafka_2.13-4.0.2

bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic vishwatech-orders \
  --group vishwa-orders-group
```

Then Kafbat:

```text
Consumer Groups
       │
       ▼
vishwa-orders-group
```

Students should understand:

```text
Topic
 │
 ├── P0
 ├── P1
 └── P2
       │
       ▼
Consumer Group
       │
       └── Consumer
```

---

# 28. 🧪 LAB 07 — Consumer Lag

This is one of the most important Kafbat labs.

Produce many messages:

```bash
for i in {1..100}; do
  echo "order-$i"
done | \
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic vishwatech-orders
```

Then inspect:

```text
Consumer Groups
 ↓
vishwa-orders-group
 ↓
Lag
```

Concept:

```text
Producer
   │
   │ 100 messages
   ▼
Kafka
   │
   ▼
Consumer
   │
   │ 60 processed
   ▼

Lag = 40
```

### Student question

> Why is lag increasing?

Possible causes:

- consumer stopped
- consumer too slow
- downstream API slow
- expensive processing
- insufficient consumer parallelism
- partition/consumer assignment limitations

Kafbat's current project specifically highlights combined and partition-specific consumer lag. citeturn1search1

---

# 29. 🧪 LAB 08 — Topic Configuration

Open:

```text
Topics
 ↓
vishwatech-orders
 ↓
Configuration
```

Study:

```text
retention.ms
cleanup.policy
segment.bytes
compression.type
max.message.bytes
```

### Teaching diagram

```text
Kafka Topic
│
├── Partitions
├── Replication
├── Retention
├── Segments
└── Configuration
```

---

# 30. 🧪 LAB 09 — KRaft/Broker Visibility

Open:

```text
Brokers
```

Inspect:

- Broker ID
- Controller status
- Partition assignments
- Broker information

Kafbat's project highlights broker overview and controller status. citeturn1search1

Compare with CLI:

```bash
bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

### Learning

```text
CLI
 │
 └── KRaft metadata quorum

        VS

Kafbat
 │
 └── Broker/controller visualization
```

---

# 31. 🧪 LAB 10 — Multi-Cluster Concept

Kafbat supports multiple Kafka clusters in one UI. citeturn1search1

Your future architecture:

```text
                    Kafbat UI
                        │
        ┌───────────────┼───────────────┐
        │               │               │
        ▼               ▼               ▼
     DEV Kafka       QA Kafka        PROD Kafka
        │               │               │
      :9092           :9092           :9092
```

For your first lab:

```text
Cluster 1
└── VishwaTech-Kafka-KRaft
```

Later:

```text
Cluster 2
└── VishwaTech-Kafka-ZooKeeper
```

This is an excellent exercise for your students.

---

# 32. 🧪 LAB 11 — Schema Registry

Kafbat supports Schema Registry integration, including Avro, JSON Schema and Protobuf according to the official project documentation. citeturn1search1

Future architecture:

```text
Producer
   │
   ▼
Schema Registry
   │
   ▼
Kafka
   │
   ▼
Kafbat
```

Example:

```text
orders-value
     │
     ▼
Avro / JSON Schema / Protobuf
```

> Schema Registry is **not part of the current single-node Kafka lab** unless you install it separately.

---

# 33. 🧪 LAB 12 — Kafka Connect

Kafbat can integrate with Kafka Connect.

Architecture:

```text
Database
   │
   ▼
Kafka Connect
   │
   ▼
Kafka
   │
   ▼
Kafbat
```

Future example:

```text
PostgreSQL
    │
    ▼
Debezium
    │
    ▼
Kafka Connect
    │
    ▼
Kafka
```

> Kafka Connect is also **not installed by this README**. Add it as a separate lab.

---

# 34. 🔐 Kafbat Security

For this training lab:

```text
Kafka:
PLAINTEXT

Kafbat:
HTTP

AWS:
Security Group IP restriction
```

This is acceptable for a controlled learning environment.

It is **not a production security design**.

### Production

```text
Browser
   │
 HTTPS
   ▼
Kafbat
   │
 TLS/SASL
   ▼
Kafka
```

Kafbat supports authentication integrations and RBAC configuration; the official documentation provides examples for OAuth2/LDAP and role-based access control. citeturn1search6turn1search11

---

# 35. 🛠️ Troubleshooting

## ❌ Kafbat container not running

```bash
sudo docker ps -a
```

Then:

```bash
sudo docker logs kafbat-ui
```

---

## ❌ Kafbat cannot connect to Kafka

Check:

```bash
sudo systemctl status kafka-kraft
```

Then:

```bash
ss -lntp | grep 9092
```

Test:

```bash
nc -vz 127.0.0.1 9092
```

Expected:

```text
succeeded
```

---

## ❌ Kafbat UI opens but cluster is unavailable

Check:

```bash
cat /home/ec2-user/kafbat-ui/kafbat.env
```

Expected:

```text
KAFKA_CLUSTERS_0_NAME=VishwaTech-Kafka-KRaft
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=127.0.0.1:9092
```

Restart:

```bash
sudo docker restart kafbat-ui
```

---

## ❌ Browser cannot open port 8080

Check:

```bash
ss -lntp | grep 8080
```

Then check AWS Security Group:

```text
TCP 8080
Source: YOUR_IP/32
```

---

## ❌ Topics visible from CLI but not Kafbat

CLI:

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Then inspect:

```bash
sudo docker logs kafbat-ui
```

---

## ❌ Kafbat uses wrong Kafka address

Check:

```bash
grep '^advertised.listeners' \
/home/ec2-user/kafka/kafka_2.13-4.0.2/config/server.properties
```

For this same-host lab:

```text
advertised.listeners=PLAINTEXT://localhost:9092
```

is compatible with the host-network deployment.

---

# 36. 📜 Container Logs

Follow:

```bash
sudo docker logs -f kafbat-ui
```

Last 100 lines:

```bash
sudo docker logs --tail 100 kafbat-ui
```

With timestamps:

```bash
sudo docker logs -t --tail 100 kafbat-ui
```

Look for:

```text
Kafka connection
Cluster configuration
Spring Boot
HTTP server
Exceptions
Authentication
Serialization
```

---

# 37. 🔄 Restart/Reboot

## Restart Kafbat

```bash
sudo docker restart kafbat-ui
```

---

## Restart Kafka

```bash
sudo systemctl restart kafka-kraft
```

Then:

```bash
sudo docker restart kafbat-ui
```

---

## Reboot EC2

```bash
sudo reboot
```

Reconnect.

Check:

```bash
sudo systemctl is-active docker
```

Expected:

```text
active
```

Kafka:

```bash
sudo systemctl is-active kafka-kraft
```

Expected:

```text
active
```

Kafbat:

```bash
sudo docker ps --filter name=kafbat-ui
```

Expected:

```text
Up ...
```

Because the container was started with:

```text
--restart unless-stopped
```

Docker should automatically restart it after the daemon starts.

---

# 38. 🧹 Uninstall

Stop:

```bash
sudo docker stop kafbat-ui
```

Remove:

```bash
sudo docker rm kafbat-ui
```

Remove image:

```bash
sudo docker rmi ghcr.io/kafbat/kafka-ui:1.5.0
```

Remove configuration:

```bash
rm -rf /home/ec2-user/kafbat-ui
```

### Important

This does **not** delete Kafka topics.

Kafbat is only the GUI.

Your Kafka data remains under the Kafka installation/data directory.

---

# 39. 🏭 Single Node vs Production

## Training

```text
                Kafbat
                  │
                  ▼
              Kafka-1
             ┌───────┐
             │Broker │
             │Ctrl   │
             └───────┘
```

Excellent for:

- 🎓 Training
- 🧪 Development
- 🧰 POC
- 👨‍💻 Learning
- 🧑‍🏫 Demonstrations

But:

```text
❌ No HA
❌ One broker
❌ One EC2 failure = outage
❌ Replication factor > 1 impossible
```

---

## Production

```text
                       Kafbat
                          │
              ┌───────────┼───────────┐
              │           │           │
              ▼           ▼           ▼
           Kafka-1     Kafka-2     Kafka-3
              │           │           │
              └───────────┼───────────┘
                          │
                     KRaft quorum
```

Add:

- TLS
- SASL
- RBAC
- private networking
- HTTPS
- monitoring
- auditing
- backups/recovery strategy
- multi-node Kafka
- replication
- alerting

---

# 40. 🔗 Official Resources

## 🏆 Kafbat UI

🔗 [Official Kafbat GitHub](https://github.com/kafbat/kafka-ui)

## 🐳 Docker Compose Example

🔗 [Official Kafbat Docker Compose](https://github.com/kafbat/kafka-ui/blob/main/documentation/compose/kafbat-ui.yaml)

## ⚙️ Configuration

🔗 [Kafbat Configuration Documentation](https://github.com/kafbat/ui-docs/blob/main/configuration/configuration-file.md)

## 🔐 RBAC

🔗 [Kafbat RBAC Documentation](https://github.com/kafbat/ui-docs/blob/main/configuration/rbac-role-based-access-control/README.md)

## 🔒 SSL

🔗 [Kafbat Kafka SSL Example](https://github.com/kafbat/kafka-ui/blob/main/documentation/compose/kafka-ssl.yml)

## ☁️ AWS IAM / MSK

🔗 [Kafbat AWS IAM Authentication](https://github.com/kafbat/ui-docs/blob/main/configuration/authentication/for-kafka/aws-iam.md)

## 🏗️ Apache Kafka

🔗 [Apache Kafka](https://kafka.apache.org/)

---

# 41. ✅ Final Validation

Run all checks.

## 1️⃣ Docker

```bash
sudo systemctl is-active docker
```

Expected:

```text
active
```

---

## 2️⃣ Kafka

```bash
sudo systemctl is-active kafka-kraft
```

Expected:

```text
active
```

---

## 3️⃣ Kafka Broker

```bash
ss -lntp | grep 9092
```

Expected:

```text
LISTEN ... :9092
```

---

## 4️⃣ KRaft Controller

```bash
ss -lntp | grep 9093
```

Expected:

```text
LISTEN ... :9093
```

---

## 5️⃣ Kafbat

```bash
sudo docker ps --filter name=kafbat-ui
```

Expected:

```text
kafbat-ui ... Up ...
```

---

## 6️⃣ Kafbat Port

```bash
ss -lntp | grep 8080
```

Expected:

```text
LISTEN ... :8080
```

---

## 7️⃣ Health

```bash
curl http://127.0.0.1:8080/actuator/health
```

Expected:

```json
{"status":"UP"}
```

---

## 8️⃣ Kafka CLI

```bash
cd /home/ec2-user/kafka/kafka_2.13-4.0.2

bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

---

## 9️⃣ KRaft

```bash
bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

---

## 🔟 Browser

```text
http://EC2_PUBLIC_IP:8080
```

---

# 42. 🏆 VishwaTech Learning Outcome

After completing this lab, students should be able to explain:

```text
                  👨‍🎓 KAFKA ENGINEER
                         │
              ┌──────────┴──────────┐
              │                     │
             CLI                 Kafbat UI
              │                     │
              ▼                     ▼
       Deep Kafka knowledge    Visual Kafka
                              operations
              │                     │
              └──────────┬──────────┘
                         │
                         ▼
                  🏆 KAFKA SKILL
```

### Student should understand:

```text
Producer
   │
   ▼
Kafka Broker
   │
   ▼
Topic
   │
   ├── Partition 0
   ├── Partition 1
   └── Partition 2
          │
          ▼
   Consumer Group
          │
          ▼
       Consumer
          │
          ▼
      Consumer Lag
```

And:

```text
Kafbat
   │
   ▼
Kafka Broker :9092
   │
   ├── Topics
   ├── Partitions
   ├── Messages
   ├── Consumer Groups
   ├── Offsets
   └── Configuration

Kafka KRaft Controller :9093
   │
   └── Metadata quorum
```

---

# 🚀 VishwaTech Kafka GUI Roadmap

You now have the following sequence:

```text
                  VISHWATECH KAFKA
                         │
                         ▼
                  1️⃣ Kafka CLI
                         │
                         ▼
                  2️⃣ Kafbat UI
                         │
                         ▼
                    3️⃣ Kpow
                         │
                         ▼
               4️⃣ Redpanda Console
                         │
                         ▼
                   5️⃣ Conduktor
                         │
                         ▼
               6️⃣ Enterprise Kafka
```

### 🥇 Kafbat

Best first GUI for:

```text
Students
   │
   ├── Topics
   ├── Partitions
   ├── Messages
   ├── Consumers
   └── Lag
```

### 🥈 Kpow

Then introduce:

```text
Operations
   │
   ├── Monitoring
   ├── Consumer lag
   ├── KRaft
   ├── Topics
   └── Production concepts
```

---

# 🏁 FINAL ARCHITECTURE

```text
                           🌐 BROWSER
                               │
                               │ HTTP :8080
                               ▼
                      ┌─────────────────┐
                      │   KAFBAT UI     │
                      │     Docker      │
                      │      :8080      │
                      └────────┬────────┘
                               │
                               │ Kafka API
                               ▼
               ┌──────────────────────────────┐
               │        KAFKA KRaft           │
               │                              │
               │   ┌──────────────────────┐   │
               │   │ Broker               │   │
               │   │ :9092                │   │
               │   └──────────────────────┘   │
               │                              │
               │   ┌──────────────────────┐   │
               │   │ Controller            │   │
               │   │ :9093                │   │
               │   └──────────────────────┘   │
               │                              │
               │        Single EC2            │
               └──────────────────────────────┘
```

## 🎓 VishwaTech Golden Rule

> **Kafka CLI = understand Kafka.**
>
> **Kafbat UI = visualize Kafka.**
>
> **Kpow = operate and troubleshoot Kafka.**
>
> **Together = strong Kafka engineering skills.**
