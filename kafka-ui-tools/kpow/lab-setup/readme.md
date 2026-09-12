
# 🚀 VishwaTech Labs — Kpow Setup on Existing Single-Node Apache Kafka

![Kpow](https://img.shields.io/badge/Kpow-Kafka%20UI%20%26%20Toolkit-111827)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.0.2-black?logo=apachekafka)
![KRaft](https://img.shields.io/badge/Kafka-KRaft-blue)
![AWS](https://img.shields.io/badge/AWS-EC2-orange?logo=amazonaws)
![Amazon Linux](https://img.shields.io/badge/Amazon%20Linux-2023-orange?logo=linux)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker)
![Java](https://img.shields.io/badge/Java-17-red?logo=openjdk)
![VishwaTech](https://img.shields.io/badge/VishwaTech-Labs-1f6feb)
![Lab](https://img.shields.io/badge/Lab-Single%20Node-green)

> 🎓 **VishwaTech Labs — Production-style Kafka GUI Practice**
>
> This guide installs **Kpow for Apache Kafka** alongside the **existing single-node Kafka KRaft EC2 lab**.
>
> The Kafka broker remains the existing service. Kpow runs as a Docker container and connects to Kafka through the broker's `9092` listener.

---

## 📚 Table of Contents

- [1. 🎯 Lab Objective](#1--lab-objective)
- [2. 🧠 What is Kpow](#2--what-is-kpow)
- [3. 🏗️ Existing Lab Assumption](#3--existing-lab-assumption)
- [4. 🧩 Final Architecture](#4--final-architecture)
- [5. 🔌 Port Map](#5--port-map)
- [6. ☁️ AWS EC2 Requirements](#6--aws-ec2-requirements)
- [7. 🔐 Security Group](#7--security-group)
- [8. 🐳 Why Docker for Kpow](#8--why-docker-for-kpow)
- [9. 📜 Kpow Version and License](#9--kpow-version-and-license)
- [10. 🐳 Install Docker](#10--install-docker)
- [11. 📁 Create Kpow Directory](#11--create-kpow-directory)
- [12. 🔑 Obtain Kpow Community License](#12--obtain-kpow-community-license)
- [13. ⚙️ Create Kpow Environment File](#13--create-kpow-environment-file)
- [14. ⚠️ Critical Single-Node Configuration](#14--critical-single-node-configuration)
- [15. 🚀 Start Kpow](#15--start-kpow)
- [16. 🔎 Verify Container](#16--verify-container)
- [17. 🌐 Open Kpow UI](#17--open-kpow-ui)
- [18. 🔗 How Kpow Connects to Kafka](#18--how-kpow-connects-to-kafka)
- [19. 🧠 Why We Use host Network](#19--why-we-use-host-network)
- [20. 📊 What Kpow Shows](#20--what-kpow-shows)
- [21. 🟦 KRaft Visibility](#21--kraft-visibility)
- [22. 🧪 Lab 01 — Verify Kafka Cluster](#22--lab-01--verify-kafka-cluster)
- [23. 🧪 Lab 02 — Create Topic from Kpow](#23--lab-02--create-topic-from-kpow)
- [24. 🧪 Lab 03 — Produce Messages](#24--lab-03--produce-messages)
- [25. 🧪 Lab 04 — Inspect Messages](#25--lab-04--inspect-messages)
- [26. 🧪 Lab 05 — Consumer Groups](#26--lab-05--consumer-groups)
- [27. 🧪 Lab 06 — Consumer Lag](#27--lab-06--consumer-lag)
- [28. 🧪 Lab 07 — Topic Configuration](#28--lab-07--topic-configuration)
- [29. 🧪 Lab 08 — Partition and Replica View](#29--lab-08--partition-and-replica-view)
- [30. 🧪 Lab 09 — KRaft Broker/Quorum View](#30--lab-09--kraft-brokerquorum-view)
- [31. 🧪 Lab 10 — Topic Troubleshooting](#31--lab-10--topic-troubleshooting)
- [32. 🔧 Kpow Container Operations](#32--kpow-container-operations)
- [33. 📝 Kpow Logs](#33--kpow-logs)
- [34. 🔄 Restart and Reboot Validation](#34--restart-and-reboot-validation)
- [35. 🛠️ Troubleshooting](#35--troubleshooting)
- [36. 🔐 Security Hardening](#36--security-hardening)
- [37. 🏭 Single Node vs Production](#37--single-node-vs-production)
- [38. 📈 Resource Planning](#38--resource-planning)
- [39. 🧹 Uninstall](#39--uninstall)
- [40. 📚 Official Documentation](#40--official-documentation)
- [41. ✅ Final Validation Checklist](#41--final-validation-checklist)
- [42. 🏆 VishwaTech Learning Outcome](#42--vishwatech-learning-outcome)

---

# 1. 🎯 Lab Objective

We already have a **single-node Kafka KRaft broker** running on AWS EC2.

Now we add:

```text
                         🌐 Browser
                             │
                             │ HTTP :3000
                             ▼
                    ┌──────────────────┐
                    │      Kpow        │
                    │  Kafka UI/API    │
                    │    Docker        │
                    └────────┬─────────┘
                             │
                             │ Kafka protocol
                             ▼
                    ┌──────────────────┐
                    │ Kafka Single Node│
                    │                  │
                    │ Broker :9092     │
                    │ Controller :9093 │
                    └──────────────────┘
```

### 🎓 What students will learn

- What Kpow is
- How a Kafka GUI connects to a broker
- Kpow container deployment
- Kafka topic management
- Message inspection
- Consumer-group monitoring
- Consumer lag
- Partition/replica inspection
- KRaft visibility
- Broker configuration
- Kafka troubleshooting
- GUI vs CLI operations
- AWS Security Group considerations
- Docker networking
- Single-node limitations
- Basic production security

---

# 2. 🧠 What is Kpow?

**Kpow** is a Kafka UI and engineering toolkit for Apache Kafka.

Kpow provides visibility into Kafka resources such as:

```text
Kpow
│
├── 🖥 Brokers
├── 📚 Topics
├── 🧩 Partitions
├── 👥 Consumer Groups
├── 📍 Offsets
├── 📉 Consumer Lag
├── 🔐 Kafka configuration/security
├── 🔌 Kafka Connect
├── 🧬 Schema Registry
├── 🟦 KRaft information
└── 📊 Operational insights
```

Kpow's current documentation describes it as a Kafka UI/toolkit that snapshots Kafka resources periodically and stores the required operational data in Kafka internal topics. It uses Kafka AdminClient, Producer and Consumer APIs, and can also integrate with Schema Registry and Kafka Connect. citeturn1search8turn0search10

### Official resources

🔗 [Kpow Documentation](https://docs.factorhouse.io/kpow/)

🔗 [Kpow GitHub](https://github.com/factorhouse/kpow)

🔗 [Kpow Docker Hub](https://hub.docker.com/r/factorhouse/kpow)

---

# 3. 🧠 Existing Lab Assumption

This guide assumes the previous VishwaTech single-node **Kafka KRaft** lab is already installed.

Expected environment:

```text
OS
└── Amazon Linux 2023

User
└── ec2-user

Java
└── Java 17

Kafka
└── Apache Kafka 4.0.2

Kafka mode
└── KRaft

Kafka process
└── broker + controller

Kafka broker
└── :9092

KRaft controller
└── :9093

Kafka service
└── kafka-kraft.service
```

### Expected Kafka configuration

```properties
process.roles=broker,controller
node.id=1

listeners=PLAINTEXT://:9092,CONTROLLER://:9093

controller.listener.names=CONTROLLER

controller.quorum.voters=1@localhost:9093

inter.broker.listener.name=PLAINTEXT

advertised.listeners=PLAINTEXT://localhost:9092
```

> ⚠️ If your existing Kafka configuration uses a different advertised hostname/IP, adjust the Kpow `BOOTSTRAP` value accordingly.

---

# 4. 🏗️ Final Architecture

## 🌟 Single EC2 Architecture

```text
                              🌐 Student Laptop
                                      │
                                      │ HTTP :3000
                                      ▼
                         ┌────────────────────────┐
                         │       AWS EC2          │
                         │                        │
                         │  ┌──────────────────┐  │
                         │  │      Kpow        │  │
                         │  │     Docker       │  │
                         │  │      :3000       │  │
                         │  └────────┬─────────┘  │
                         │           │            │
                         │           │ :9092      │
                         │           ▼            │
                         │  ┌──────────────────┐  │
                         │  │  Kafka KRaft     │  │
                         │  │                  │  │
                         │  │ Broker :9092     │  │
                         │  │ Controller :9093 │  │
                         │  └──────────────────┘  │
                         │                        │
                         └────────────────────────┘
```

---

# 5. 🔌 Port Map

| Port | Component | Purpose | Public Internet? |
|---:|---|---|---|
| `22` | SSH | EC2 administration | ❌ IP restricted |
| `9092` | Kafka Broker | Kafka clients/Kpow | ⚠️ Only when required |
| `9093` | KRaft Controller | Controller quorum | ❌ Never public |
| `3000` | Kpow | Web UI | ⚠️ Your IP only |

### Important

Kpow connects to:

```text
Kafka Broker :9092
```

Kpow does **not** need browser access to:

```text
KRaft Controller :9093
```

Keep `9093` private.

---

# 6. ☁️ AWS EC2 Requirements

For the existing training lab:

### Minimum practical lab

```text
Instance:
t3.medium

CPU:
2 vCPU

Memory:
4 GB

Disk:
30 GB gp3

OS:
Amazon Linux 2023
```

### Better Kpow + Kafka lab

```text
t3.large

CPU:
2 vCPU

Memory:
8 GB
```

Kpow's current system requirements recommend **2 CPU and 8 GB memory for production**, while the documentation notes that around **1 GB can be suitable for small/development environments**. For a t3.medium training machine, keep Kpow resource limits conservative and monitor memory. citeturn1search8

---

# 7. 🔐 Security Group

Add:

| Type | Port | Source | Reason |
|---|---:|---|---|
| SSH | 22 | Your IP `/32` | Administration |
| Custom TCP | 3000 | Your IP `/32` | Kpow UI |
| Custom TCP | 9092 | Your IP/app subnet only if required | External Kafka clients |
| Custom TCP | 9093 | ❌ Do not expose | KRaft controller |

### Recommended

If only Kpow runs on the same EC2 instance:

```text
Internet
   │
   └── :3000 → Kpow

Kpow
   │
   └── localhost:9092 → Kafka
```

In that case, **9092 does not need to be publicly accessible merely for Kpow**.

---

# 8. 🐳 Why Docker for Kpow?

Kpow can be deployed as a Docker container or JAR, and Factor House provides official Docker images. citeturn0search0turn0search2

For this lab:

```text
Existing Kafka
       │
       │ stays unchanged
       ▼
Kafka KRaft service
       │
       │
       ▼
Docker
       │
       ▼
Kpow
```

### Advantages

- ✅ No modification to Kafka installation
- ✅ Easy start/stop
- ✅ Easy upgrade
- ✅ Easy rollback
- ✅ Isolated application
- ✅ Good for training
- ✅ Easy to remove
- ✅ Matches common deployment patterns

---

# 9. 📜 Kpow Version and License

At the time this guide was created, Factor House documentation is on the **96.4** documentation version, and the official Docker repository lists `96.4` as a current tag. citeturn1search7turn0search10

For this lab we use:

```text
Kpow Community Edition
factorhouse/kpow-ce:96.4
```

### License

Kpow is proprietary software, but Factor House provides a **free Community License** for learning, development and non-production/ephemeral use. The Enterprise edition is available through a trial/commercial licensing model. citeturn0search4

> ⚠️ **Do not copy example license values from documentation into your lab.**
>
> Request your own current Community/Trial license from Factor House and keep the values private.

---

# 10. 🐳 Install Docker

Run as `ec2-user`:

```bash
sudo dnf update -y
```

Install Docker:

```bash
sudo dnf install -y docker
```

Start Docker:

```bash
sudo systemctl enable --now docker
```

Check:

```bash
sudo systemctl status docker
```

Expected:

```text
● docker.service - Docker Application Container Engine
   Loaded: loaded
   Active: active (running)
```

Test:

```bash
sudo docker version
```

---

# 11. 📁 Create Kpow Directory

Create a dedicated directory:

```bash
mkdir -p /home/ec2-user/kpow
```

Move into it:

```bash
cd /home/ec2-user/kpow
```

Expected:

```bash
pwd
```

Output:

```text
/home/ec2-user/kpow
```

---

# 12. 🔑 Obtain Kpow Community License

Use the current Factor House licensing/trial process.

🔗 [Kpow Documentation](https://docs.factorhouse.io/kpow/)

🔗 [Kpow GitHub](https://github.com/factorhouse/kpow)

Kpow's Docker documentation requires valid license information in the environment configuration. citeturn1search3

You will receive values similar to:

```text
LICENSE_ID
LICENSE_CODE
LICENSEE
LICENSE_EXPIRY
LICENSE_SIGNATURE
```

### ⚠️ Never publish these values

Do not put your real license values into:

- GitHub
- public README
- screenshots
- public Slack messages
- public tutorials
- shell history where avoidable

---

# 13. ⚙️ Create Kpow Environment File

Create:

```bash
nano /home/ec2-user/kpow/kpow.env
```

Use this structure:

```dotenv
ENVIRONMENT_NAME=VishwaTech-Kafka-Single-Node

BOOTSTRAP=127.0.0.1:9092

REPLICATION_FACTOR=1

NUM_PARTITIONS=1

PERSISTENCE_MODE=full

ALLOW_TOPIC_CREATE=true
ALLOW_TOPIC_DELETE=true
ALLOW_TOPIC_EDIT=true
ALLOW_TOPIC_INSPECT=true
ALLOW_TOPIC_PRODUCE=true

LICENSE_ID=<YOUR_LICENSE_ID>
LICENSE_CODE=<YOUR_LICENSE_CODE>
LICENSEE=<YOUR_LICENSEE>
LICENSE_EXPIRY=<YOUR_LICENSE_EXPIRY>
LICENSE_SIGNATURE=<YOUR_LICENSE_SIGNATURE>
```

Then secure it:

```bash
chmod 600 /home/ec2-user/kpow/kpow.env
```

Verify permissions:

```bash
ls -l /home/ec2-user/kpow/kpow.env
```

Expected:

```text
-rw------- 1 ec2-user ec2-user ... kpow.env
```

---

# 14. ⚠️ Critical Single-Node Configuration

This is one of the **most important sections** of the lab.

Kpow maintains internal topics for its own operational state/metrics/audit/streaming computation.

Current Kpow documentation identifies internal topics including:

```text
__oprtr_metric_pt1m
__oprtr_snapshot_state
__oprtr_audit_log
oprtr.compute.metrics.v2-oprtr_metric_v2_pt1m-changelog
oprtr.compute.snapshots.v2-oprtr_snaphot_state_v2-changelog
```

Kpow also uses internal streaming compute applications. citeturn1search1

### Default replication factor

Kpow's documented default for internal topics is:

```text
REPLICATION_FACTOR=3
```

and the default number of partitions is:

```text
NUM_PARTITIONS=12
```

For a **single-node Kafka lab**, replication factor 3 cannot work because only one broker exists. citeturn1search12

Therefore:

```dotenv
REPLICATION_FACTOR=1
NUM_PARTITIONS=1
```

### Why?

Your cluster:

```text
Kafka Cluster
     │
     └── Broker 1
```

You cannot create:

```text
Replication Factor = 3
```

because:

```text
Broker 1
Broker 2 ❌
Broker 3 ❌
```

Instead:

```text
Topic
 │
 └── Replica → Broker 1
```

This is correct for the lab but **not highly available**.

---

# 15. 🚀 Start Kpow

## Recommended method for this EC2 lab

Because Kafka and Kpow are on the **same EC2 host**, use Docker host networking.

This allows:

```text
Kpow container
     │
     └── 127.0.0.1:9092
              │
              ▼
       Host Kafka broker
```

Start:

```bash
sudo docker run -d \
  --name kpow \
  --restart unless-stopped \
  --network host \
  --env-file /home/ec2-user/kpow/kpow.env \
  -m 1G \
  factorhouse/kpow-ce:96.4
```

### What each option means

| Option | Meaning |
|---|---|
| `-d` | Detached/background |
| `--name kpow` | Container name |
| `--restart unless-stopped` | Restart after Docker/host restart |
| `--network host` | Container shares EC2 network namespace |
| `--env-file` | Load Kpow configuration |
| `-m 1G` | Limit container memory |
| `factorhouse/kpow-ce:96.4` | Kpow Community Edition |

Kpow's official Docker instructions use the Community Edition image and a memory limit; the exact resource allocation should be chosen for the environment. citeturn1search3turn1search8

---

# 16. 🔎 Verify Container

Check:

```bash
sudo docker ps
```

Expected:

```text
CONTAINER ID   IMAGE                       STATUS
xxxxxxxxxxxx   factorhouse/kpow-ce:96.4   Up ...
```

Check all containers:

```bash
sudo docker ps -a
```

---

# 17. 🌐 Open Kpow UI

Find EC2 public IP:

```bash
curl http://checkip.amazonaws.com
```

Suppose:

```text
54.xx.xx.xx
```

Open:

```text
http://54.xx.xx.xx:3000
```

Architecture:

```text
Your Browser
     │
     │ HTTP :3000
     ▼
AWS Security Group
     │
     ▼
EC2
     │
     ▼
Kpow
```

### 🔐 Security reminder

Only allow port `3000` from your own IP during the lab.

---

# 18. 🔗 How Kpow Connects to Kafka

This is critical to understand.

```text
                    EC2
┌──────────────────────────────────────────────┐
│                                              │
│  Docker Container                            │
│  ┌──────────────────────┐                    │
│  │ Kpow                 │                    │
│  │                      │                    │
│  │ BOOTSTRAP=           │                    │
│  │ 127.0.0.1:9092       │                    │
│  └──────────┬───────────┘                    │
│             │                                │
│             ▼                                │
│      ┌──────────────┐                        │
│      │ Kafka Broker │                        │
│      │    :9092     │                        │
│      └──────────────┘                        │
│                                              │
│      KRaft Controller :9093                  │
│                                              │
└──────────────────────────────────────────────┘
```

Kpow connects using Kafka's normal client APIs. Current Kpow documentation states that it uses Kafka AdminClient, Consumer and Producer resources to inspect/manage Kafka. citeturn1search8

---

# 19. 🧠 Why We Use `host` Network

Normally a Docker container has its own network namespace.

Therefore:

```text
Container
localhost
   ≠
EC2 host
localhost
```

That causes this common mistake:

```text
Kpow container
BOOTSTRAP=localhost:9092
          │
          ❌
          │
Kafka on EC2 host
```

With:

```bash
--network host
```

we get:

```text
Kpow
 │
 └── 127.0.0.1:9092
          │
          ▼
Kafka on same EC2 host
```

### Why this is convenient for the lab

- Simple
- No custom Docker bridge configuration
- No extra host gateway mapping
- Works naturally with `advertised.listeners=localhost:9092`
- Keeps Kpow-to-Kafka traffic local to the EC2 host

> This is a **lab convenience**, not a universal production recommendation.

---

# 20. 📊 What Kpow Shows

Once connected, Kpow can provide visibility into:

```text
Kpow
│
├── 🖥 Brokers
│
├── 📚 Topics
│   ├── Partitions
│   ├── Offsets
│   ├── Replicas
│   └── Configuration
│
├── 👥 Groups
│   ├── Consumers
│   ├── Assignments
│   └── Lag
│
├── 🟦 KRaft
│   └── Quorum information
│
├── 🔌 Kafka Connect
│
└── 🧬 Schema Registry
```

Kpow's current broker documentation specifically includes KRaft visibility and allows viewing Raft quorum voters/observers from the Brokers area. citeturn1search9

---

# 21. 🟦 KRaft Visibility

Your architecture:

```text
Kafka JVM
│
├── Broker
│    └── :9092
│
└── KRaft Controller
     └── :9093
```

Kpow can show KRaft-related broker/quorum information.

This gives students a powerful visual connection:

```text
CLI
 │
 └── kafka-metadata-quorum.sh

              VS

GUI
 │
 └── Kpow → Brokers → KRaft
```

### CLI

```bash
cd /home/ec2-user/kafka/kafka_2.13-4.0.2

bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

### GUI

Open:

```text
Kpow
  ↓
Brokers
  ↓
KRaft
```

---

# 22. 🧪 LAB 01 — Verify Kafka Cluster

Before troubleshooting Kpow, verify Kafka.

```bash
sudo systemctl status kafka-kraft
```

Then:

```bash
jps
```

Expected approximately:

```text
Kafka
Jps
```

Verify ports:

```bash
ss -lntp | grep -E '9092|9093'
```

Expected:

```text
LISTEN ... :9092
LISTEN ... :9093
```

Test Kafka:

```bash
cd /home/ec2-user/kafka/kafka_2.13-4.0.2

bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

---

# 23. 🧪 LAB 02 — Create Topic from Kpow

Create:

```text
vishwatech-orders
```

Suggested lab values:

```text
Partitions:
3

Replication Factor:
1
```

### Concept

```text
vishwatech-orders
       │
       ├── Partition 0
       ├── Partition 1
       └── Partition 2
```

Kpow's topic management documentation supports topic creation, configuration and other topic operations. citeturn1search0

---

# 24. 🧪 LAB 03 — Produce Messages

Using Kafka CLI:

```bash
cd /home/ec2-user/kafka/kafka_2.13-4.0.2

bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic vishwatech-orders
```

Enter:

```text
order-1001
order-1002
order-1003
order-1004
```

Then inspect through Kpow.

### Learning objective

Students understand:

```text
CLI Producer
      │
      ▼
Kafka Topic
      │
      ▼
Kpow
      │
      ▼
Visual Message Inspection
```

---

# 25. 🧪 LAB 04 — Inspect Messages

In Kpow:

```text
Topics
  ↓
vishwatech-orders
  ↓
Inspect Data
```

Students should identify:

- Key
- Value
- Offset
- Partition
- Timestamp
- Headers where applicable
- Consumer position where applicable

### Example mental model

```text
Partition 0

Offset 0 → order-1001
Offset 1 → order-1002
Offset 2 → order-1003
Offset 3 → order-1004
```

---

# 26. 🧪 LAB 05 — Consumer Groups

Create a consumer group:

```bash
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic vishwatech-orders \
  --group vishwa-orders-group
```

Then inspect in Kpow:

```text
Groups
  ↓
vishwa-orders-group
```

Students should understand:

```text
Topic
 │
 ├── Partition 0
 ├── Partition 1
 └── Partition 2
        │
        ▼
Consumer Group
```

---

# 27. 🧪 LAB 06 — Consumer Lag

This is one of the most valuable Kpow labs.

### Generate many messages

```bash
for i in {1..100}; do
  echo "order-$i"
done | \
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic vishwatech-orders
```

Now inspect the consumer group.

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
   │ only consumed 60
   ▼

Lag = 40
```

### Student question

> Why is consumer lag increasing?

Possible answers:

- Consumer is stopped
- Consumer is slow
- Processing is expensive
- Consumer has insufficient parallelism
- Partition/consumer assignment constraints
- Downstream dependency is slow

---

# 28. 🧪 LAB 07 — Topic Configuration

Select:

```text
Topics
  ↓
vishwatech-orders
  ↓
Configuration
```

Study settings such as:

```text
retention.ms
cleanup.policy
segment.bytes
compression.type
max.message.bytes
```

### Teaching concept

Kafka topic:

```text
Topic
 │
 ├── Data
 │
 ├── Retention
 │
 ├── Partitions
 │
 ├── Replication
 │
 └── Configuration
```

---

# 29. 🧪 LAB 08 — Partition and Replica View

Create a topic:

```text
payments
```

with:

```text
Partitions = 3
Replication Factor = 1
```

Because this is a single broker:

```text
Broker 1
│
├── payments-P0
├── payments-P1
└── payments-P2
```

### Production comparison

Single node:

```text
Replication Factor = 1
```

Production cluster:

```text
Broker 1
Broker 2
Broker 3

Replication Factor = 3
```

This is an excellent teaching opportunity to explain **partitioning vs replication**.

---

# 30. 🧪 LAB 09 — KRaft Broker/Quorum View

Open Kpow:

```text
Brokers
   ↓
KRaft
```

Compare with:

```bash
bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

### Learning objective

Students connect:

```text
KRaft CLI
      +
Kpow GUI
      +
Kafka Controller
```

into one mental model.

Kpow's current documentation explicitly provides KRaft UI visibility from the Brokers area. citeturn1search9

---

# 31. 🧪 LAB 10 — Topic Troubleshooting

Create a topic:

```text
orders-test
```

Then intentionally stop the consumer.

Generate messages.

Observe:

```text
Messages ↑
Consumer position → stopped
Lag ↑
```

Start the consumer again.

Observe:

```text
Consumer resumes
       │
       ▼
Lag ↓
```

This is a very good real-world operations exercise.

---

# 32. 🔧 Kpow Container Operations

## Start

```bash
sudo docker start kpow
```

## Stop

```bash
sudo docker stop kpow
```

## Restart

```bash
sudo docker restart kpow
```

## Status

```bash
sudo docker ps
```

## Detailed inspection

```bash
sudo docker inspect kpow
```

## Resource usage

```bash
sudo docker stats kpow
```

---

# 33. 📝 Kpow Logs

Follow logs:

```bash
sudo docker logs -f kpow
```

Last 100 lines:

```bash
sudo docker logs --tail 100 kpow
```

With timestamps:

```bash
sudo docker logs -t --tail 100 kpow
```

### Look for

```text
Kafka connection
License
Internal topics
Broker discovery
Snapshot jobs
Metrics
Errors
Exceptions
```

---

# 34. 🔄 Restart and Reboot Validation

## Restart Kpow

```bash
sudo docker restart kpow
```

Check:

```bash
sudo docker ps
```

---

## Reboot EC2

```bash
sudo reboot
```

Reconnect.

Check Kafka:

```bash
sudo systemctl status kafka-kraft
```

Check Docker:

```bash
sudo systemctl status docker
```

Check Kpow:

```bash
sudo docker ps
```

Because we used:

```bash
--restart unless-stopped
```

Docker should automatically restart the Kpow container after the Docker daemon starts.

---

# 35. 🛠️ Troubleshooting

## ❌ Problem 1 — Kpow container exits

Run:

```bash
sudo docker ps -a
```

Then:

```bash
sudo docker logs kpow
```

Look for:

```text
license error
connection error
Kafka error
configuration error
```

---

## ❌ Problem 2 — Kpow cannot connect to Kafka

Check Kafka:

```bash
sudo systemctl status kafka-kraft
```

Check broker:

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

Check Kpow configuration:

```bash
grep '^BOOTSTRAP=' /home/ec2-user/kpow/kpow.env
```

Expected:

```text
BOOTSTRAP=127.0.0.1:9092
```

---

## ❌ Problem 3 — Internal topics cannot be created

This is common when running Kpow against a single broker.

Check:

```bash
grep -E '^(REPLICATION_FACTOR|NUM_PARTITIONS)=' \
/home/ec2-user/kpow/kpow.env
```

Expected:

```text
REPLICATION_FACTOR=1
NUM_PARTITIONS=1
```

If you accidentally used:

```text
REPLICATION_FACTOR=3
```

the single broker cannot satisfy it.

Restart:

```bash
sudo docker rm -f kpow
```

Then start again with the corrected env file.

---

## ❌ Problem 4 — Browser cannot open port 3000

Check:

```bash
sudo ss -lntp | grep 3000
```

Check container:

```bash
sudo docker ps
```

Check AWS Security Group:

```text
TCP 3000
Source = YOUR_PUBLIC_IP/32
```

---

## ❌ Problem 5 — Kpow shows no topics

Check Kafka CLI:

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

If CLI sees topics but Kpow doesn't:

```bash
sudo docker logs kpow
```

Then verify Kpow's `BOOTSTRAP`.

---

## ❌ Problem 6 — Kafka advertises localhost incorrectly

This matters for external Kafka clients.

Check:

```bash
grep '^advertised.listeners' \
/home/ec2-user/kafka/kafka_2.13-4.0.2/config/server.properties
```

For the same-host Kpow setup:

```text
advertised.listeners=PLAINTEXT://localhost:9092
```

works because Kpow is using host networking.

For a remote application:

```text
localhost
```

would be wrong.

You would need a reachable DNS name/private IP/public endpoint appropriate to your architecture.

---

# 36. 🔐 Security Hardening

This lab uses:

```text
PLAINTEXT
```

for Kafka because it is a learning environment.

That is **not a production security design**.

### Production-style architecture

```text
Browser
   │
 HTTPS
   │
   ▼
Kpow
   │
 TLS/SASL
   │
   ▼
Kafka
```

Use:

- 🔐 TLS
- 🔑 SASL
- 🧑‍💼 RBAC
- 🔥 Network controls
- 🔒 HTTPS
- 📝 Audit
- 🛡️ Least privilege
- 🔐 Secrets management

Kpow supports multiple authentication/security approaches and enterprise authorization capabilities; exact features depend on edition/configuration. citeturn0search7

---

# 37. 🏭 Single Node vs Production

## Our training lab

```text
┌─────────────────────┐
│ EC2                 │
│                     │
│ Kafka Broker        │
│ Kafka Controller    │
│ Kpow                │
│                     │
└─────────────────────┘
```

### Advantages

- Cheap
- Simple
- Easy to understand
- Great for students
- Excellent for demos

### Disadvantages

- ❌ No HA
- ❌ One broker
- ❌ One controller
- ❌ One EC2 failure stops everything
- ❌ Replication factor cannot exceed 1

---

## Production

```text
                   Kpow
                     │
       ┌─────────────┼─────────────┐
       │             │             │
   Kafka-1       Kafka-2       Kafka-3
       │             │             │
       └─────────────┼─────────────┘
                     │
                KRaft Quorum
```

Typical production goals:

```text
Broker Count:
3+

Replication:
3

Controller Quorum:
3 or more according to design

Security:
TLS + SASL/RBAC

Monitoring:
Prometheus/Grafana/etc.
```

---

# 38. 📈 Resource Planning

Kpow documentation recommends 8 GB memory and 2 CPU for a production installation, while 1 GB can be appropriate for smaller/dev installations. Kpow also stores operational data in Kafka internal topics, with default retention that can consume broker disk over time. citeturn1search8

### Lab

```text
Kafka
   +
Kpow
   +
Docker
```

For a 4 GB EC2 machine:

```text
Kpow:
~1 GB container limit

Kafka:
remaining memory

OS:
remaining memory
```

Monitor:

```bash
free -h
```

and:

```bash
sudo docker stats kpow
```

### Better training machine

```text
t3.large
8 GB RAM
```

This gives more breathing room for:

```text
Kafka
+
Kpow
+
CLI
+
Docker
+
student labs
```

---

# 39. 🧹 Uninstall

Stop:

```bash
sudo docker stop kpow
```

Remove:

```bash
sudo docker rm kpow
```

Remove image:

```bash
sudo docker rmi factorhouse/kpow-ce:96.4
```

Remove configuration:

```bash
rm -rf /home/ec2-user/kpow
```

### Important

Removing the Kpow container does **not automatically mean Kafka internal Kpow topics are removed**.

If you want to inspect them:

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Look for:

```text
__oprtr_metric_pt1m
__oprtr_snapshot_state
__oprtr_audit_log
oprtr.compute.metrics.v2-oprtr_metric_v2_pt1m-changelog
oprtr.compute.snapshots.v2-oprtr_snaphot_state_v2-changelog
```

Only delete them if you intentionally want to reset the Kpow lab state and understand the consequences.

---

# 40. 📚 Official Documentation

## 🏆 Kpow

🔗 [Kpow Documentation](https://docs.factorhouse.io/kpow/)

## 🐳 Docker Installation

🔗 [Kpow Docker Installation](https://docs.factorhouse.io/kpow/installation/docker)

## ⚙️ Environment Variables

🔗 [Kpow Environment Variables](https://docs.factorhouse.io/kpow/configuration/config-kpow/environment-variables)

## 🖥 System Requirements

🔗 [Kpow System Requirements](https://docs.factorhouse.io/kpow/faq/system-requirements)

## 🔐 Minimum Kafka ACL Permissions

🔗 [Kpow Minimum ACL Permissions](https://docs.factorhouse.io/kpow/faq/minimum-acl-permissions)

## 🟦 KRaft Broker Management

🔗 [Kpow Brokers / KRaft Management](https://docs.factorhouse.io/kpow/management/brokers)

## 📚 Topic Management

🔗 [Kpow Topic Management](https://docs.factorhouse.io/kpow/management/topics)

## 🐙 Kpow GitHub

🔗 [Factor House Kpow GitHub](https://github.com/factorhouse/kpow)

## 🐳 Docker Hub

🔗 [Kpow Docker Hub](https://hub.docker.com/r/factorhouse/kpow)

---

# 41. ✅ Final Validation Checklist

Run:

```bash
sudo systemctl is-active docker
```

Expected:

```text
active
```

---

Kafka:

```bash
sudo systemctl is-active kafka-kraft
```

Expected:

```text
active
```

---

Kafka broker:

```bash
ss -lntp | grep 9092
```

Expected:

```text
LISTEN ... 9092
```

---

KRaft controller:

```bash
ss -lntp | grep 9093
```

Expected:

```text
LISTEN ... 9093
```

---

Kpow:

```bash
sudo docker ps --filter name=kpow
```

Expected:

```text
kpow ... Up ...
```

---

Kpow port:

```bash
ss -lntp | grep 3000
```

Expected:

```text
LISTEN ... 3000
```

---

Kafka CLI:

```bash
cd /home/ec2-user/kafka/kafka_2.13-4.0.2

bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

---

KRaft:

```bash
bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

---

Kpow logs:

```bash
sudo docker logs --tail 100 kpow
```

---

Browser:

```text
http://EC2_PUBLIC_IP:3000
```

---

# 42. 🏆 VishwaTech Learning Outcome

After completing this lab, students should be able to explain:

### Kafka

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
       Consumer Lag
```

### KRaft

```text
Kafka
│
├── Broker
│    └── :9092
│
└── Controller
     └── :9093
```

### Kpow

```text
Browser
   │
   ▼
Kpow :3000
   │
   ▼
Kafka :9092
   │
   ├── Brokers
   ├── Topics
   ├── Partitions
   ├── Messages
   ├── Groups
   ├── Offsets
   ├── Lag
   └── KRaft
```

---

# 🎓 VishwaTech Golden Rule

> **CLI teaches you how Kafka works.**
>
> **Kpow shows you what Kafka is doing.**
>
> **Together they create a strong Kafka operations skillset.**

```text
             👨‍🎓 KAFKA ENGINEER
                    │
          ┌─────────┴─────────┐
          │                   │
        CLI                  Kpow
          │                   │
          ▼                   ▼
     Deep Kafka         Visual Kafka
     knowledge          operations
          │                   │
          └─────────┬─────────┘
                    │
                    ▼
             🏆 REAL KAFKA SKILL
```

---

# 🚀 Recommended VishwaTech Kafka GUI Path

Your complete learning sequence should now be:

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

For the **single-node KRaft lab**, Kpow is particularly valuable because it lets students compare the same Kafka concepts through both CLI and GUI, while its current documentation also provides KRaft-specific broker/quorum visibility. citeturn1search9

---

## 🏁 Final Lab Architecture

```text
                         🌐 BROWSER
                             │
                             │ HTTP :3000
                             ▼
                    ┌──────────────────┐
                    │      KPOW        │
                    │   Docker :3000   │
                    └────────┬─────────┘
                             │
                             │ Kafka API
                             │
                             ▼
              ┌─────────────────────────────┐
              │       KAFKA KRAFT           │
              │                             │
              │  ┌───────────────────────┐  │
              │  │ Broker                │  │
              │  │ :9092                 │  │
              │  └───────────────────────┘  │
              │                             │
              │  ┌───────────────────────┐  │
              │  │ Controller             │  │
              │  │ :9093                 │  │
              │  └───────────────────────┘  │
              │                             │
              │  Single EC2 Node            │
              └─────────────────────────────┘
```

### ⭐ Lab Status

```text
Kafka KRaft       ✅
Single Broker     ✅
KRaft Controller  ✅
Docker            ✅
Kpow              ✅
Web UI            :3000
Kafka             :9092
Controller        :9093
Replication       1
```

> ⚠️ **Educational single-node lab only.** For production, use a multi-node Kafka/KRaft design, appropriate replication, TLS/SASL/RBAC, private networking, monitoring and operational controls.
