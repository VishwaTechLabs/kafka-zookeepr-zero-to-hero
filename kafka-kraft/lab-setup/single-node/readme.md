
# 🚀 VishwaTech Labs — Apache Kafka KRaft Single-EC2 Lab

[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.0.2-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![KRaft](https://img.shields.io/badge/KRaft-Enabled-0F766E)](https://kafka.apache.org/documentation/#kraft)
[![AWS EC2](https://img.shields.io/badge/AWS-EC2-FF9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com/ec2/)
[![Amazon Linux](https://img.shields.io/badge/Amazon%20Linux-2023-232F3E?logo=amazonaws&logoColor=white)](https://aws.amazon.com/linux/amazon-linux-2023/)
[![Java](https://img.shields.io/badge/Java-17-007396?logo=openjdk&logoColor=white)](https://openjdk.org/)
[![License](https://img.shields.io/badge/Lab-Educational-blue)](#-license)

> 🎓 **VishwaTech Labs** hands-on guide for installing, configuring, operating, and validating **Apache Kafka in KRaft mode** on a **single AWS EC2 instance**.

---

## 📚 Table of Contents

- [🎯 Objective](#-objective)
- [🏗️ Final Architecture](#️-final-architecture)
- [🧠 What Is KRaft](#-what-is-kraft)
- [🆚 ZooKeeper vs KRaft](#-zookeeper-vs-kraft)
- [☁️ AWS Prerequisites](#️-aws-prerequisites)
- [🔐 Security Group](#-security-group)
- [👤 User Model](#-user-model)
- [☕ Java Installation](#-java-installation)
- [📦 Kafka Installation](#-kafka-installation)
- [🆔 Cluster ID](#-cluster-id)
- [⚙️ KRaft Configuration](#️-kraft-configuration)
- [💾 Format KRaft Storage](#-format-kraft-storage)
- [▶️ Start Kafka](#️-start-kafka)
- [🔄 Background Process](#-background-process)
- [⚙️ systemd Service](#️-systemd-service)
- [🔁 Enable Kafka at Boot](#-enable-kafka-at-boot)
- [🔎 Health Checks](#-health-checks)
- [🧩 Producer and Consumer](#-producer-and-consumer)
- [📌 Topic and Partition](#-topic-and-partition)
- [🧠 KRaft Metadata Quorum](#-kraft-metadata-quorum)
- [📜 Logs](#-logs)
- [🛑 Stop / Start / Restart](#-stop--start--restart)
- [🔄 Reboot Validation](#-reboot-validation)
- [🧯 Troubleshooting](#-troubleshooting)
- [🧑‍🏫 Concepts Students Must Know](#-concepts-students-must-know)
- [🏭 Production Architecture](#-production-architecture)
- [🤖 Automation Script](#-automation-script)
- [📚 Official References](#-official-references)

---

## 🎯 Objective

Build this complete environment:

```text
                         ☁️ AWS CLOUD
                              │
                              ▼
                  ┌──────────────────────┐
                  │      AWS VPC          │
                  │                       │
                  │  ┌─────────────────┐  │
                  │  │ EC2             │  │
                  │  │                 │  │
                  │  │ Amazon Linux    │  │
                  │  │ Java 17         │  │
                  │  │                 │  │
                  │  │   Kafka JVM      │  │
                  │  │  ┌───────────┐  │  │
                  │  │  │  BROKER   │  │  │
                  │  │  │   :9092   │  │  │
                  │  │  └─────┬─────┘  │  │
                  │  │        │         │  │
                  │  │  ┌─────▼─────┐   │  │
                  │  │  │CONTROLLER │   │  │
                  │  │  │   :9093   │   │  │
                  │  │  └───────────┘   │  │
                  │  └─────────────────┘  │
                  └────────────────────────┘
```

### Lab characteristics

| Item | Value |
|---|---|
| Cloud | AWS |
| Compute | EC2 |
| Suggested instance | `t3.medium` |
| vCPU | 2 |
| RAM | 4 GiB |
| OS | Amazon Linux 2023 |
| Linux user | `ec2-user` |
| Java | 17 |
| Kafka | Apache Kafka 4.0.2 |
| Mode | KRaft |
| ZooKeeper | ❌ Not used |
| Broker listener | `9092` |
| Controller listener | `9093` |
| Broker count | 1 |
| Controller count | 1 |
| Controller role | Combined with broker |

> ⚠️ This is a **learning/development/POC topology**, not a highly available production cluster.

---

# 🧠 What Is KRaft?

**KRaft** means Kafka's metadata management architecture based on the **Raft consensus protocol**.

Historically Kafka used ZooKeeper for cluster metadata and coordination.

KRaft removes that external dependency.

### ZooKeeper model

```text
                 ZooKeeper
                    │
                    │ metadata / coordination
                    ▼
Producer ───────► Kafka Broker ───────► Consumer
```

### KRaft model

```text
                    Kafka
              ┌───────────────┐
              │               │
              │  Controller   │
              │     :9093     │
              │               │
              │      +        │
              │               │
              │    Broker     │
              │     :9092     │
              │               │
              └───────┬───────┘
                      │
               ┌──────┴──────┐
               ▼             ▼
           Producer       Consumer
```

The Kafka process can perform both roles:

```properties
process.roles=broker,controller
```

---

# 🆚 ZooKeeper vs KRaft

| Area | ZooKeeper Architecture | KRaft Architecture |
|---|---|---|
| ZooKeeper | Required in legacy mode | ❌ No |
| Kafka Controller | Kafka + ZooKeeper coordination | Kafka controller quorum |
| Metadata | ZooKeeper/Kafka coordination | Kafka metadata log |
| Controller listener | Not this KRaft model | `9093` |
| Client listener | `9092` | `9092` |
| Storage formatting | Not KRaft-style | Required |
| Cluster UUID | Different workflow | Generated with `kafka-storage.sh` |
| `process.roles` | No | Yes |
| `node.id` | No | Yes |
| `controller.quorum.voters` | No | Yes |
| Metadata quorum CLI | No | Yes |

🔥 **Key learning:**

```text
ZooKeeper
   ↓
External coordination system

KRaft
   ↓
Kafka manages its own metadata/controller quorum
```

---

# 🏗️ Final Architecture

## Single-node combined-role architecture

```text
                           AWS EC2
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│                         ec2-user                             │
│                             │                                │
│                             ▼                                │
│                          systemd                             │
│                             │                                │
│                             ▼                                │
│                    kafka-kraft.service                       │
│                             │                                │
│                             ▼                                │
│                       Java 17 JVM                            │
│                             │                                │
│                ┌────────────┴────────────┐                   │
│                │                         │                   │
│                ▼                         ▼                   │
│             BROKER                  CONTROLLER               │
│             :9092                      :9093                 │
│                │                         │                   │
│                │                         │                   │
│                ▼                         ▼                   │
│        Producer / Consumer          KRaft Metadata           │
│                │                       Quorum                │
│                ▼                                             │
│             Topics                                            │
│                │                                             │
│                ▼                                             │
│           Partitions                                         │
│                │                                             │
│                ▼                                             │
│          Kafka Storage                                       │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

# 👨‍💻 Who Is the Producer?

A **producer is an application that sends records to Kafka**.

For this lab, our producer is:

```text
kafka-console-producer.sh
```

Flow:

```text
YOU
 │
 │ type "Order-1001"
 ▼
Console Producer
 │
 │ send
 ▼
Kafka Broker :9092
 │
 ▼
Topic
```

In real applications, the producer could be:

```text
Python application
Java application
Spring Boot service
Node.js application
Payment service
Order service
IoT device
Log collector
```

---

# 👨‍💻 Who Is the Consumer?

A **consumer is an application that reads records from Kafka**.

For this lab:

```text
kafka-console-consumer.sh
```

Flow:

```text
Kafka Broker
     │
     ▼
Topic
     │
     ▼
Consumer
     │
     ▼
Application
```

---

# ☁️ AWS Prerequisites

## Recommended EC2

Create an EC2 instance with:

```text
AMI       : Amazon Linux 2023
Instance  : t3.medium
CPU       : 2 vCPU
Memory    : 4 GB
Disk      : 30 GB gp3
User      : ec2-user
```

### Why `t3.medium`?

The lab runs:

```text
Amazon Linux
+
Java JVM
+
Kafka Broker
+
KRaft Controller
```

4 GiB gives the single-node training environment reasonable headroom.

---

# 🔐 Security Group

Recommended inbound:

| Protocol | Port | Source | Purpose |
|---|---:|---|---|
| TCP | 22 | My IP | SSH |
| TCP | 9092 | My IP | Kafka client access |

Do **not** expose controller port `9093` publicly for this lab.

```text
Internet
   │
   ├──► 22    SSH
   │
   └──► 9092  Kafka client
              │
              └── 9093 stays internal
```

If all producer/consumer tests run on the EC2 instance itself, `9092` can also remain private.

---

# 👤 User Model

We deliberately use only:

```text
ec2-user
```

No:

```text
❌ kafka
❌ zookeeper
❌ kraft
```

Verify:

```bash
whoami
```

Expected:

```text
ec2-user
```

---

# ☕ Java Installation

Verify first:

```bash
java -version
```

If Java isn't installed:

```bash
sudo dnf install java-17-amazon-corretto -y
```

Verify:

```bash
java -version
```

Expected:

```text
openjdk version "17.x.x"
OpenJDK Runtime Environment Corretto-17...
```

Why Java?

```text
Kafka
  ↓
Java application
  ↓
JVM
  ↓
Linux
```

---

# 📦 Kafka Installation

Create the installation directory:

```bash
mkdir -p ~/kafka
cd ~/kafka
```

Download Apache Kafka 4.0.2 from the official Apache distribution source.

Then verify:

```bash
ls -lh
```

Expected:

```text
kafka_2.13-4.0.2.tgz
```

Extract:

```bash
tar -xzf kafka_2.13-4.0.2.tgz
```

Enter:

```bash
cd ~/kafka/kafka_2.13-4.0.2
```

Verify:

```bash
pwd
```

Expected:

```text
/home/ec2-user/kafka/kafka_2.13-4.0.2
```

---

# 📁 Kafka Directory Structure

```text
kafka_2.13-4.0.2/
│
├── bin/
│   ├── kafka-server-start.sh
│   ├── kafka-server-stop.sh
│   ├── kafka-storage.sh
│   ├── kafka-topics.sh
│   ├── kafka-console-producer.sh
│   ├── kafka-console-consumer.sh
│   └── kafka-metadata-quorum.sh
│
├── config/
│
├── libs/
│
└── logs/
```

### Important scripts

| Script | Purpose |
|---|---|
| `kafka-storage.sh` | Initialize/format storage |
| `kafka-server-start.sh` | Start Kafka |
| `kafka-server-stop.sh` | Stop Kafka |
| `kafka-topics.sh` | Topic administration |
| `kafka-console-producer.sh` | Test producer |
| `kafka-console-consumer.sh` | Test consumer |
| `kafka-metadata-quorum.sh` | Inspect KRaft metadata quorum |

---

# 🆔 Cluster ID

KRaft needs a cluster identity.

Run:

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```

Display:

```bash
echo "$KAFKA_CLUSTER_ID"
```

Expected:

```text
A unique generated identifier
```

Your value will be different.

Save it:

```bash
echo "$KAFKA_CLUSTER_ID" > ~/kafka-cluster-id.txt
```

Verify:

```bash
cat ~/kafka-cluster-id.txt
```

### Why?

Think of it as:

```text
Kafka Cluster
      │
      ▼
Unique Cluster ID
      │
      ▼
"This storage belongs to this cluster"
```

---

# 🆔 Node ID

Our single node:

```properties
node.id=1
```

Conceptually:

```text
Kafka Cluster
      │
      ▼
   Node 1
```

If we had multiple nodes:

```text
Node 1
Node 2
Node 3
```

Every node needs a unique identity.

---

# 🎭 Process Roles

Our Kafka process:

```properties
process.roles=broker,controller
```

Means one JVM performs two roles:

```text
                 Kafka JVM
              ┌──────────────┐
              │              │
              │   Broker     │
              │              │
              │      +       │
              │              │
              │  Controller  │
              │              │
              └──────────────┘
```

### Broker

Handles client traffic:

```text
Producer
   ↓
Broker
   ↓
Topic / Partition
   ↓
Consumer
```

### Controller

Manages cluster metadata/control-plane responsibilities.

---

# 🔌 Listener Architecture

We use two logical listeners:

```text
9092
 │
 └── Broker / client traffic

9093
 │
 └── Controller traffic
```

Diagram:

```text
                 Kafka JVM
              ┌─────────────┐
              │             │
Client ──────►│   Broker    │
   :9092      │             │
              │ Controller  │
              │    :9093    │
              └─────────────┘
```

---

# ⚙️ KRaft Configuration

Create a backup first:

```bash
cp config/server.properties config/server.properties.bak
```

Edit:

```bash
vi config/server.properties
```

For this single-node lab, the important settings are:

```properties
process.roles=broker,controller
node.id=1

controller.quorum.voters=1@localhost:9093

listeners=PLAINTEXT://:9092,CONTROLLER://:9093

advertised.listeners=PLAINTEXT://localhost:9092

listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT

log.dirs=/home/ec2-user/kafka/data/kafka-logs
```

> 💡 If you later connect from another machine, replace the advertised broker address with a reachable DNS name/IP appropriate for your network design.

---

# 🧠 Configuration Explained

### `process.roles`

```properties
process.roles=broker,controller
```

Kafka performs both roles.

### `node.id`

```properties
node.id=1
```

Identifies the node.

### `controller.quorum.voters`

```properties
controller.quorum.voters=1@localhost:9093
```

Our one controller voter is:

```text
Node ID = 1
Host    = localhost
Port    = 9093
```

### `listeners`

```properties
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
```

Kafka opens:

```text
9092 → broker/client
9093 → controller
```

### `advertised.listeners`

Tells clients which broker address to use.

### `controller.listener.names`

Selects the controller listener.

### `inter.broker.listener.name`

Selects the broker-to-broker listener.

### `log.dirs`

Kafka data location.

---

# 💾 Create Kafka Storage

```bash
mkdir -p ~/kafka/data/kafka-logs
```

Verify:

```bash
ls -ld ~/kafka/data/kafka-logs
```

Expected:

```text
... ec2-user ec2-user ... /home/ec2-user/kafka/data/kafka-logs
```

---

# 🧹 Format KRaft Storage

This is a critical KRaft step.

Run:

```bash
bin/kafka-storage.sh format \
  --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c config/server.properties
```

Expected output indicates the storage directory was formatted using the cluster ID.

### What does formatting do?

```text
Before

Kafka
 │
 ▼
Empty storage


After

Kafka
 │
 ├── Cluster identity
 ├── Metadata storage
 ├── Metadata log
 └── Kafka data storage
```

⚠️ **Do not repeatedly format an existing Kafka data directory as a normal startup operation.** Formatting is an initialization/reset operation.

---

# ▶️ Start Kafka

## Foreground mode

```bash
bin/kafka-server-start.sh config/server.properties
```

Kafka occupies the terminal.

Architecture:

```text
Terminal
   │
   ▼
Kafka JVM
   │
   ├── Broker
   └── Controller
```

Stop with the appropriate shutdown method rather than killing the process unnecessarily.

---

# 🔄 Background Process

For a lab, Kafka can be started as a background process:

```bash
bin/kafka-server-start.sh -daemon config/server.properties
```

Now:

```text
Terminal
   │
   └── command returns
            │
            ▼
       Kafka JVM
       background
```

Verify:

```bash
jps
```

Expected:

```text
12345 Kafka
67890 Jps
```

🔥 Notice:

```text
Kafka
```

but **not**:

```text
QuorumPeerMain
```

because KRaft does not use ZooKeeper.

---

# ⚙️ systemd Service

For reliable service management, create:

```text
/etc/systemd/system/kafka-kraft.service
```

Run:

```bash
sudo vi /etc/systemd/system/kafka-kraft.service
```

Use:

```ini
[Unit]
Description=Apache Kafka KRaft
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/kafka/kafka_2.13-4.0.2

ExecStart=/home/ec2-user/kafka/kafka_2.13-4.0.2/bin/kafka-server-start.sh /home/ec2-user/kafka/kafka_2.13-4.0.2/config/server.properties

ExecStop=/home/ec2-user/kafka/kafka_2.13-4.0.2/bin/kafka-server-stop.sh

Restart=on-failure
RestartSec=10

LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
```

### Why `User=ec2-user`?

Because our lab deliberately uses:

```text
ONE USER
   ↓
ec2-user
```

---

# 🔄 Reload systemd

```bash
sudo systemctl daemon-reload
```

---

# ▶️ Start Service

```bash
sudo systemctl start kafka-kraft
```

Check:

```bash
sudo systemctl status kafka-kraft
```

Expected:

```text
● kafka-kraft.service
     Loaded: loaded
     Active: active (running)
```

The key result:

```text
Active: active (running)
```

---

# 🔁 Enable Kafka at Boot

```bash
sudo systemctl enable kafka-kraft
```

Verify:

```bash
sudo systemctl is-enabled kafka-kraft
```

Expected:

```text
enabled
```

Architecture:

```text
EC2 Reboot
    │
    ▼
Linux Boot
    │
    ▼
systemd
    │
    ▼
kafka-kraft.service
    │
    ▼
Kafka
    ├── Broker
    └── Controller
```

---

# 🔎 Health Checks

## Check user

```bash
whoami
```

Expected:

```text
ec2-user
```

## Check Java

```bash
java -version
```

Expected:

```text
Java 17
```

## Check process

```bash
jps
```

Expected:

```text
Kafka
Jps
```

## Check broker port

```bash
ss -lntp | grep 9092
```

Expected:

```text
LISTEN ... :9092 ... java
```

## Check controller port

```bash
ss -lntp | grep 9093
```

Expected:

```text
LISTEN ... :9093 ... java
```

---

# 🧠 KRaft Metadata Quorum

Run:

```bash
bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe \
  --status
```

This command helps inspect the KRaft metadata quorum.

For our one-node lab, conceptually:

```text
KRaft Metadata Quorum

       ┌──────────────┐
       │    Node 1    │
       │              │
       │    Voter     │
       │    Leader    │
       └──────────────┘
```

In production, multiple controller voters provide fault tolerance.

---

# 📌 Topic and Partition

Create a topic:

```bash
bin/kafka-topics.sh \
  --create \
  --topic vishwatech-orders \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Expected:

```text
Created topic vishwatech-orders.
```

List:

```bash
bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Expected:

```text
vishwatech-orders
```

Describe:

```bash
bin/kafka-topics.sh \
  --describe \
  --topic vishwatech-orders \
  --bootstrap-server localhost:9092
```

Expected conceptually:

```text
Topic: vishwatech-orders
PartitionCount: 1
ReplicationFactor: 1
Partition: 0
Leader: 1
Replicas: 1
Isr: 1
```

---

# 🧩 Producer and Consumer

## Start Producer

```bash
bin/kafka-console-producer.sh \
  --topic vishwatech-orders \
  --bootstrap-server localhost:9092
```

Type:

```text
Order-1001
```

Then:

```text
Order-1002
```

---

## Start Consumer

Open another SSH session.

```bash
cd ~/kafka/kafka_2.13-4.0.2
```

Run:

```bash
bin/kafka-console-consumer.sh \
  --topic vishwatech-orders \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

Expected:

```text
Order-1001
Order-1002
```

---

# 🔥 Complete Data Flow

```text
                       USER
                        │
                        │ types message
                        ▼
              ┌──────────────────┐
              │ Console Producer │
              └────────┬─────────┘
                       │
                       │ produce
                       ▼
              ┌──────────────────┐
              │ Kafka Broker     │
              │     :9092        │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ Topic            │
              │ vishwatech-orders│
              └────────┬─────────┘
                       │
                       ▼
                 Partition 0
                       │
                       ▼
                     Disk
                       │
                       │ consume
                       ▼
              ┌──────────────────┐
              │ Console Consumer│
              └────────┬─────────┘
                       │
                       ▼
                     USER
```

And behind the scenes:

```text
              KRaft Controller
                    :9093
                       │
                       ▼
               Cluster Metadata
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
       Topics      Partitions     Leaders
```

---

# 📜 Logs

systemd logs:

```bash
sudo journalctl -u kafka-kraft
```

Follow live:

```bash
sudo journalctl -u kafka-kraft -f
```

Last 100 lines:

```bash
sudo journalctl -u kafka-kraft -n 100 --no-pager
```

Application logs:

```bash
ls -lh ~/kafka/kafka_2.13-4.0.2/logs/
```

---

# 🛑 Stop / Start / Restart

Stop:

```bash
sudo systemctl stop kafka-kraft
```

Start:

```bash
sudo systemctl start kafka-kraft
```

Restart:

```bash
sudo systemctl restart kafka-kraft
```

Status:

```bash
sudo systemctl status kafka-kraft
```

Disable automatic boot:

```bash
sudo systemctl disable kafka-kraft
```

---

# 🔄 Reboot Validation

This is a mandatory lab test.

Before reboot:

```bash
sudo systemctl status kafka-kraft
```

Expected:

```text
active (running)
```

Reboot:

```bash
sudo reboot
```

Reconnect using SSH.

Then:

```bash
sudo systemctl status kafka-kraft
```

Expected:

```text
active (running)
```

Verify:

```bash
jps
```

Expected:

```text
Kafka
Jps
```

Verify ports:

```bash
ss -lntp | grep 9092
ss -lntp | grep 9093
```

Both should be listening.

🔥 This proves systemd successfully restarted Kafka after the EC2 reboot.

---

# 🧯 Troubleshooting

## Kafka service failed

```bash
sudo systemctl status kafka-kraft
```

Then:

```bash
sudo journalctl -u kafka-kraft -n 100 --no-pager
```

---

## Kafka port already in use

```bash
sudo ss -lntp | grep 9092
```

Find the process:

```bash
jps
```

Potential cause:

```text
Kafka was already started manually
+
systemd tried to start another Kafka
```

Don't run the background/manual process and systemd-managed process simultaneously.

---

## Controller port unavailable

```bash
sudo ss -lntp | grep 9093
```

Check logs:

```bash
sudo journalctl -u kafka-kraft -n 100 --no-pager
```

---

## Kafka cannot start after configuration change

Check:

```bash
grep -E '^(process.roles|node.id|controller.quorum.voters|listeners|advertised.listeners|controller.listener.names|inter.broker.listener.name|log.dirs)' \
config/server.properties
```

---

## Permission issue

Check:

```bash
ls -ld ~/kafka
ls -ld ~/kafka/data
ls -ld ~/kafka/data/kafka-logs
```

The lab expects ownership to be associated with:

```text
ec2-user
```

---

## Accidentally formatted storage again

Stop and investigate before doing anything destructive:

```bash
sudo systemctl stop kafka-kraft
```

Then inspect:

```bash
cat ~/kafka/kafka-cluster-id.txt
```

and:

```bash
find ~/kafka/data -maxdepth 2 -type f -name "meta.properties" -print
```

For a disposable training lab, a complete reset can be designed separately. Do not use storage formatting as a routine restart command.

---

# 🧑‍🏫 Concepts Students Must Know

## 1. Kafka

Distributed event streaming platform.

## 2. Broker

Kafka server process that handles client traffic and stores records.

## 3. Controller

Kafka control-plane role responsible for cluster metadata and controller operations.

## 4. KRaft

Kafka's metadata quorum architecture based on Raft.

## 5. Node ID

Unique identity of a Kafka node.

```text
node.id=1
```

## 6. Cluster ID

Unique identity for the Kafka cluster.

## 7. Process Roles

```text
broker
controller
broker,controller
```

## 8. Controller Quorum

The set of controller voters that maintain Kafka metadata consensus.

## 9. Topic

Logical stream/category of records.

Examples:

```text
orders
payments
users
logs
```

## 10. Partition

Ordered log within a topic.

```text
Topic
 ├── Partition 0
 ├── Partition 1
 └── Partition 2
```

## 11. Producer

Writes/sends records.

## 12. Consumer

Reads records.

## 13. Listener

Network endpoint used for Kafka communication.

## 14. Advertised Listener

Address Kafka tells clients to use.

## 15. Metadata Log

Kafka's internal log used for KRaft metadata.

## 16. systemd

Linux service manager.

## 17. Background Process

Kafka runs independently of the interactive terminal.

## 18. Service

systemd manages Kafka's lifecycle.

---

# 🏭 Production Architecture

Our lab:

```text
              ONE EC2
                 │
                 ▼
        ┌─────────────────┐
        │ Kafka JVM       │
        │                 │
        │ Broker          │
        │ Controller      │
        └─────────────────┘
```

A more resilient production architecture could look like:

```text
                 KRaft Controller Quorum

          ┌────────────┐
          │ Controller │
          │     1      │
          └─────┬──────┘
                │
          ┌─────▼──────┐
          │ Controller │
          │     2      │
          └─────┬──────┘
                │
          ┌─────▼──────┐
          │ Controller │
          │     3      │
          └────────────┘


              Kafka Brokers

       ┌──────────┐ ┌──────────┐ ┌──────────┐
       │ Broker 1 │ │ Broker 2 │ │ Broker 3 │
       └──────────┘ └──────────┘ └──────────┘
             │            │            │
             └────────────┼────────────┘
                          │
                    Kafka Topics
```

### Why multiple nodes?

```text
Single node failure
        ↓
No redundancy


Multiple brokers/controllers
        ↓
Fault tolerance
        ↓
High availability
```

---

# 🧠 One Diagram to Remember

```text
                         KRaft Kafka
                    ┌─────────────────┐
                    │                 │
                    │   CONTROLLER    │
                    │     :9093       │
                    │                 │
                    │   Metadata      │
                    │    Quorum       │
                    │        │        │
                    │        ▼        │
                    │      BROKER     │
                    │      :9092      │
                    │        │        │
                    └────────┼────────┘
                             │
                  ┌──────────┴──────────┐
                  ▼                     ▼
              PRODUCER              CONSUMER
                  │                     ▲
                  ▼                     │
                TOPIC ──────────────────┘
                  │
                  ▼
              PARTITION
                  │
                  ▼
                 DISK
```

---

# 🤖 Automation Script

The repository includes:

```text
install-kafka-kraft.sh
```

Run:

```bash
chmod +x install-kafka-kraft.sh
./install-kafka-kraft.sh
```

The script automates the core installation and configuration steps.

### Script responsibilities

```text
✓ Verify ec2-user
✓ Verify Amazon Linux
✓ Install Java 17
✓ Download Kafka
✓ Extract Kafka
✓ Create data directories
✓ Generate Cluster ID
✓ Configure KRaft
✓ Format KRaft storage
✓ Create systemd service
✓ Reload systemd
✓ Enable Kafka
✓ Start Kafka
✓ Validate service
✓ Validate ports
✓ Validate JVM
```

> ⚠️ The script is intended for a **fresh/disposable lab instance**. Review it before running it on an existing Kafka host.

---

# 📚 Official References

- [Apache Kafka](https://kafka.apache.org/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka KRaft Documentation](https://kafka.apache.org/documentation/#kraft)
- [Kafka 4.0 Quick Start](https://kafka.apache.org/40/getting-started/quickstart/)
- [Kafka Configuration](https://kafka.apache.org/documentation/#configuration)
- [AWS EC2](https://aws.amazon.com/ec2/)
- [Amazon Linux](https://aws.amazon.com/linux/amazon-linux-2023/)
- [OpenJDK](https://openjdk.org/)

---

# 🏁 Final Validation Checklist

```text
[ ] EC2 created
[ ] Amazon Linux 2023 verified
[ ] ec2-user verified
[ ] Java 17 installed
[ ] Kafka downloaded
[ ] Kafka extracted
[ ] Cluster ID generated
[ ] node.id configured
[ ] process.roles configured
[ ] controller.quorum.voters configured
[ ] listeners configured
[ ] KRaft storage formatted
[ ] Kafka started
[ ] Kafka process verified
[ ] 9092 verified
[ ] 9093 verified
[ ] systemd service created
[ ] systemd service started
[ ] service enabled
[ ] Metadata quorum verified
[ ] Topic created
[ ] Partition verified
[ ] Producer tested
[ ] Consumer tested
[ ] Message received
[ ] Logs verified
[ ] Stop tested
[ ] Start tested
[ ] Restart tested
[ ] EC2 reboot tested
[ ] Kafka automatically restarted
```

---

## 🎓 VishwaTech Labs — Learning Outcome

After completing this lab, you should be able to explain:

```text
EC2
 ↓
Linux
 ↓
Java
 ↓
Kafka JVM
 ├── Broker
 └── KRaft Controller
      ↓
Controller Quorum
      ↓
Metadata
      ↓
Topics
      ↓
Partitions
      ↓
Producer → Kafka → Consumer
```

**Most important takeaway:**

> 🟢 **ZooKeeper-based Kafka:** Kafka depended on an external ZooKeeper architecture for cluster coordination/metadata.

> 🔵 **KRaft Kafka:** Kafka incorporates the controller quorum and metadata management architecture into Kafka itself.

---

## ⭐ VishwaTech Labs

**Learn → Build → Break → Troubleshoot → Automate → Explain**

**Apache Kafka | KRaft | AWS | Linux | DevOps | Cloud | Security**
