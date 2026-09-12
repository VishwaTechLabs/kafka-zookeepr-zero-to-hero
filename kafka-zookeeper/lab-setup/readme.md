# 🚀 VishwaTech Labs — Apache Kafka + ZooKeeper on a Single AWS EC2 Machine

[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.8.1-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/38/)
[![ZooKeeper](https://img.shields.io/badge/Apache%20ZooKeeper-3.8.1-1A73E8?logo=apache&logoColor=white)](https://zookeeper.apache.org/)
[![Amazon Linux](https://img.shields.io/badge/Amazon%20Linux-2023-FF9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com/linux/amazon-linux-2023/)
[![Java](https://img.shields.io/badge/Java-17-007396?logo=openjdk&logoColor=white)](https://aws.amazon.com/corretto/)
[![AWS EC2](https://img.shields.io/badge/AWS-EC2-FF9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com/ec2/)
[![VishwaTech Labs](https://img.shields.io/badge/VishwaTech-Labs-blueviolet)](https://github.com/)

> **A complete, beginner-friendly hands-on lab for installing, configuring, operating, and testing Apache Kafka with ZooKeeper on one Amazon EC2 instance.**

---

## 📚 What You Will Build

By the end of this lab you will have:

```text
                         ☁️ AWS CLOUD
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│                         VPC                                 │
│                          │                                  │
│                          ▼                                  │
│             ┌───────────────────────────────┐               │
│             │        AWS EC2 Instance       │               │
│             │                               │               │
│             │      Amazon Linux 2023        │               │
│             │      t3.medium                │               │
│             │      2 vCPU / 4 GB RAM        │               │
│             │                               │               │
│             │          ec2-user             │               │
│             │              │                │               │
│             │          systemd              │               │
│             │       ┌──────┴──────┐         │               │
│             │       ▼             ▼         │               │
│             │  ZooKeeper        Kafka       │               │
│             │    :2181          :9092       │               │
│             │       │             │         │               │
│             │       │       ┌─────┴─────┐   │               │
│             │       │       ▼           ▼   │               │
│             │       │   Producer     Consumer               │
│             │       │       │           ▲   │               │
│             │       │       ▼           │   │               │
│             │       │  ┌───────────┐    │   │               │
│             │       └─►│   Topic   │────┘   │               │
│             │          │ Partition │        │               │
│             │          └───────────┘        │               │
│             │                │              │               │
│             │                ▼              │               │
│             │         Kafka log files       │               │
│             └───────────────────────────────┘               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 🎯 Learning Goals

- Create an EC2 instance for Kafka labs
- Use **only `ec2-user`** — no separate Kafka/ZooKeeper Linux user
- Install Java 17
- Install Apache Kafka 3.8.1
- Configure ZooKeeper
- Configure a single Kafka broker
- Run ZooKeeper and Kafka as background processes
- Create proper `systemd` services
- Start, stop, restart, enable and inspect services
- Create Kafka topics
- Produce messages
- Consume messages
- Understand partitions and replication factor
- Understand Producer → Kafka → Consumer
- Understand ZooKeeper's role in this legacy Kafka deployment model
- Inspect JVM processes and listening ports
- Read service logs
- Test automatic service startup after EC2 reboot
- Troubleshoot common failures

---

## ⚠️ Important Architecture Note

This lab intentionally uses **Kafka 3.8.1 with ZooKeeper** because the purpose is to learn the Kafka + ZooKeeper architecture. Apache Kafka's modern direction is KRaft, and ZooKeeper-based deployments are legacy. Apache's Kafka 3.8 documentation explicitly provides a ZooKeeper startup path; use this lab for learning and lab environments, not as a recommendation for a new production Kafka platform. See the official [Kafka 3.8 Quick Start](https://kafka.apache.org/38/getting-started/quickstart/) and [Kafka Downloads](https://kafka.apache.org/community/downloads/).

---

# 🏗️ 1. Final Architecture

```text
                                      ☁️ AWS CLOUD
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│                               VPC                                        │
│                                │                                         │
│                                ▼                                         │
│                    ┌──────────────────────────┐                          │
│                    │       EC2 INSTANCE       │                          │
│                    │                          │                          │
│                    │ Amazon Linux 2023        │                          │
│                    │ t3.medium                │                          │
│                    │ 2 vCPU / 4 GB RAM       │                          │
│                    │ 30 GB gp3                │                          │
│                    │                          │                          │
│                    │        ec2-user           │                          │
│                    │            │              │                          │
│                    │            ▼              │                          │
│                    │          systemd           │                          │
│                    │            │              │                          │
│                    │      ┌─────┴─────┐        │                          │
│                    │      ▼           ▼        │                          │
│                    │ ZooKeeper       Kafka     │                          │
│                    │  :2181          :9092     │                          │
│                    │      │             │       │                          │
│                    │      │       ┌─────┴─────┐ │                          │
│                    │      │       │           │ │                          │
│                    │      │       ▼           ▼ │                          │
│                    │      │   Producer     Consumer                       │
│                    │      │       │           ▲ │                          │
│                    │      │       ▼           │ │                          │
│                    │      │   ┌──────────────┐│ │                          │
│                    │      └──►│ Kafka Topic  │┘ │                          │
│                    │          │ Partition 0 │  │                          │
│                    │          └──────┬───────┘  │                          │
│                    │                 │          │                          │
│                    │                 ▼          │                          │
│                    │      /home/ec2-user/kafka/│                          │
│                    │             data/kafka-logs│                          │
│                    └────────────────────────────┘                          │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

---

# 🧠 2. Who Is Who?

## 🟢 Producer

A **producer is an application that writes/sends records to Kafka**.

For this lab, the producer is Apache Kafka's command-line client:

```bash
bin/kafka-console-producer.sh
```

Example:

```text
You type:

Hello Kafka

        │
        ▼
Kafka Console Producer
        │
        ▼
Kafka Broker
```

In a real system, the producer could be:

- Java/Spring Boot application
- Python application
- Node.js application
- Payment service
- Order service
- IoT application
- Log/event producer
- Microservice

---

## 🔵 Kafka Broker

The **Kafka broker is the server that receives, stores, and serves records**.

Our broker listens on:

```text
localhost:9092
```

It owns the Kafka topic partitions in this single-node lab.

---

## 🟣 Consumer

A **consumer is an application that reads records from Kafka**.

For this lab:

```bash
bin/kafka-console-consumer.sh
```

Flow:

```text
Kafka Broker
     │
     ▼
Kafka Consumer
     │
     ▼
Terminal / Application
```

---

## 🟠 ZooKeeper

ZooKeeper is **not the message pipeline**.

Do NOT think:

```text
Producer → ZooKeeper → Kafka → Consumer
```

Think instead:

```text
                    ZooKeeper
                        │
                        │ cluster coordination / metadata
                        ▼
Producer ───────────► Kafka ◄────────── Consumer
                       │
                       ▼
                     Topic
                       │
                       ▼
                   Partition
                       │
                       ▼
                     Disk
```

Kafka 3.8's official quick start documents ZooKeeper as the coordination component for the ZooKeeper-based startup path. [Official Kafka 3.8 Quick Start](https://kafka.apache.org/38/getting-started/quickstart/)

---

# 🔥 3. Producer → Kafka → Consumer

This is the most important Kafka concept in this lab:

```text
                 PRODUCER
                    │
                    │  "Hello Kafka"
                    ▼
          ┌──────────────────────┐
          │    KAFKA BROKER      │
          │                      │
          │   Topic: orders      │
          │                      │
          │   ┌──────────────┐   │
          │   │ Partition 0  │   │
          │   │              │   │
          │   │ Hello Kafka  │   │
          │   │ Order #1001  │   │
          │   │ Order #1002  │   │
          │   └──────────────┘   │
          └──────────┬───────────┘
                     │
                     │ read
                     ▼
                 CONSUMER
                     │
                     ▼
                  Application
```

---

# 🏦 4. Real-World Example

Imagine an e-commerce platform:

```text
                         Order Service
                              │
                              │ Order Created
                              ▼
                       Kafka Producer
                              │
                              ▼
                    ┌──────────────────┐
                    │  Kafka Cluster   │
                    │                  │
                    │ Topic: orders    │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        Payment         Shipping        Notification
        Consumer        Consumer          Consumer
```

One producer can publish an event and multiple independent consumers can process it.

---

# ☁️ 5. AWS EC2 Prerequisites

| Item | Recommended Lab Value |
|---|---|
| AMI | Amazon Linux 2023 |
| Architecture | x86_64 |
| Instance | `t3.medium` |
| vCPU | 2 |
| RAM | 4 GB |
| Disk | 30 GB gp3 |
| Linux user | `ec2-user` |
| Java | 17 |
| Kafka | 3.8.1 |
| Kafka port | 9092 |
| ZooKeeper port | 2181 |
| Kafka data | `/home/ec2-user/kafka/data/kafka-logs` |
| ZooKeeper data | `/home/ec2-user/kafka/data/zookeeper` |

> 💡 A `t3.medium` is a practical teaching/lab size. For production, size Kafka based on throughput, retention, partitions, replication, network, disk I/O, and workload.

---

# 🔐 6. Security Group

For a single EC2 lab where producer and consumer run on the same EC2 instance:

| Rule | Port | Source | Purpose |
|---|---:|---|---|
| SSH | 22 | **Your IP only** | Administration |
| Kafka | 9092 | **Your IP only, only if external clients are needed** | External Kafka client access |
| ZooKeeper | 2181 | **Do not expose publicly** | Local Kafka ↔ ZooKeeper communication |

### 🔒 Security rule

Do **not** use:

```text
0.0.0.0/0 → 2181
```

for this lab.

If producer/consumer run on the EC2 machine, Kafka can be accessed using:

```text
localhost:9092
```

and ZooKeeper using:

```text
localhost:2181
```

---

# 🖥️ 7. Connect to EC2

From your workstation:

```bash
ssh -i <YOUR_KEY>.pem ec2-user@<EC2_PUBLIC_IP>
```

Verify the user:

```bash
whoami
```

Expected:

```text
ec2-user
```

Verify OS:

```bash
cat /etc/os-release
```

Verify CPU architecture:

```bash
uname -m
```

Expected:

```text
x86_64
```

---

# ☕ 8. Install Java 17

Kafka runs on the Java Virtual Machine.

```text
EC2
 ↓
Amazon Linux
 ↓
Java 17
 ↓
Kafka / ZooKeeper
```

Update packages:

```bash
sudo dnf update -y
```

Install Java 17:

```bash
sudo dnf install java-17-amazon-corretto -y
```

Verify:

```bash
java -version
```

Expected output will contain Java 17 / Corretto 17.

Check path:

```bash
which java
```

---

# 📦 9. Install Apache Kafka 3.8.1

Kafka 3.8.1 was released on October 29, 2024. Apache provides the Scala 2.13 binary and recommends 2.13 unless you specifically need another Scala build. [Kafka Downloads](https://kafka.apache.org/community/downloads/) [Kafka 3.8.1 release](https://kafka.apache.org/blog/2024/10/29/apache-kafka-3.8.1-release-announcement/)

Create the lab directory:

```bash
mkdir -p ~/kafka
cd ~/kafka
```

Download:

```bash
curl -fLO https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz
```

Extract:

```bash
tar -xzf kafka_2.13-3.8.1.tgz
```

Enter Kafka:

```bash
cd ~/kafka/kafka_2.13-3.8.1
```

Verify:

```bash
ls
```

You should see directories such as:

```text
bin
config
libs
logs
```

---

# 📁 10. Understand the Kafka Directory

```text
kafka_2.13-3.8.1/
│
├── bin/                 ← Kafka/ZooKeeper commands
│   ├── kafka-server-start.sh
│   ├── kafka-server-stop.sh
│   ├── kafka-topics.sh
│   ├── kafka-console-producer.sh
│   ├── kafka-console-consumer.sh
│   ├── zookeeper-server-start.sh
│   └── zookeeper-server-stop.sh
│
├── config/              ← Configuration
│   ├── server.properties
│   └── zookeeper.properties
│
├── libs/                ← Kafka libraries
│
└── logs/                ← Kafka application logs
```

---

# 🗄️ 11. Create Data Directories

We will not use `/tmp` for the lab's persistent data.

```bash
mkdir -p ~/kafka/data/zookeeper
mkdir -p ~/kafka/data/kafka-logs
```

Verify:

```bash
ls -ld ~/kafka/data/*
```

Expected:

```text
/home/ec2-user/kafka/data/zookeeper
/home/ec2-user/kafka/data/kafka-logs
```

---

# 🦓 12. Configure ZooKeeper

Backup the original configuration:

```bash
cd ~/kafka/kafka_2.13-3.8.1
cp config/zookeeper.properties config/zookeeper.properties.bak
```

Edit:

```bash
vi config/zookeeper.properties
```

Set:

```properties
dataDir=/home/ec2-user/kafka/data/zookeeper
clientPort=2181
```

Verify:

```bash
grep -E '^(dataDir|clientPort)' config/zookeeper.properties
```

Expected:

```text
dataDir=/home/ec2-user/kafka/data/zookeeper
clientPort=2181
```

### Why port 2181?

`2181` is the ZooKeeper client port used by Kafka to connect to ZooKeeper in this lab.

---

# 🛠️ 13. Configure Kafka Broker

Backup the configuration:

```bash
cp config/server.properties config/server.properties.bak
```

Edit:

```bash
vi config/server.properties
```

For this single-node lab, verify or set the following values:

```properties
broker.id=0
listeners=PLAINTEXT://:9092
log.dirs=/home/ec2-user/kafka/data/kafka-logs
zookeeper.connect=localhost:2181
```

For a local-only lab, these settings allow clients on the EC2 instance to use:

```text
localhost:9092
```

> If you later want applications outside the EC2 instance to connect, configure `advertised.listeners` carefully with the appropriate private/public address and security controls. Do not blindly advertise the public IP in a production environment.

Verify:

```bash
grep -E '^(broker.id|listeners|log.dirs|zookeeper.connect)' config/server.properties
```

---

# 🧩 14. Understand the Configuration

```text
broker.id=0
```

Identifies this broker.

Because we have one broker:

```text
Broker 0
```

---

```text
listeners=PLAINTEXT://:9092
```

Kafka listens on TCP port `9092`.

---

```text
log.dirs=/home/ec2-user/kafka/data/kafka-logs
```

Kafka stores its partition log data here.

---

```text
zookeeper.connect=localhost:2181
```

Kafka connects to ZooKeeper running on the same EC2 machine.

---

# ▶️ 15. Manual Background Startup

Apache's Kafka 3.8 quick start starts ZooKeeper first and then the Kafka broker. [Official Kafka 3.8 Quick Start](https://kafka.apache.org/38/getting-started/quickstart/)

### Start ZooKeeper in background

```bash
cd ~/kafka/kafka_2.13-3.8.1
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
```

Verify:

```bash
jps
```

Expected to contain:

```text
QuorumPeerMain
```

Check port:

```bash
ss -lntp | grep 2181
```

Expected:

```text
LISTEN ... :2181 ... java
```

---

# ▶️ 16. Start Kafka in Background

```bash
bin/kafka-server-start.sh -daemon config/server.properties
```

Verify:

```bash
jps
```

Expected to contain:

```text
QuorumPeerMain
Kafka
Jps
```

Check port:

```bash
ss -lntp | grep 9092
```

Expected:

```text
LISTEN ... :9092 ... java
```

---

# ⚠️ 17. Background Process vs systemd Service

### Background process

```bash
bin/kafka-server-start.sh -daemon config/server.properties
```

Good for:

- Quick labs
- Testing
- Temporary development

But after an EC2 reboot, you may need to start it again.

### systemd service

```bash
sudo systemctl start kafka
```

Better for:

- Automatic startup
- Service status
- Service logs
- Restart policy
- Operational management

For this lab, **we will configure systemd**.

---

# ⚙️ 18. Create ZooKeeper systemd Service

Create:

```bash
sudo vi /etc/systemd/system/zookeeper.service
```

Use:

```ini
[Unit]
Description=Apache ZooKeeper
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/kafka/kafka_2.13-3.8.1
ExecStart=/home/ec2-user/kafka/kafka_2.13-3.8.1/bin/zookeeper-server-start.sh /home/ec2-user/kafka/kafka_2.13-3.8.1/config/zookeeper.properties
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Important

```text
User=ec2-user
```

We intentionally do **not** create another Linux user.

---

# 🔄 19. Reload systemd

```bash
sudo systemctl daemon-reload
```

---

# ▶️ 20. Start ZooKeeper Service

```bash
sudo systemctl start zookeeper
```

Check:

```bash
sudo systemctl status zookeeper --no-pager
```

Expected:

```text
Active: active (running)
```

---

# 🔁 21. Enable ZooKeeper at Boot

```bash
sudo systemctl enable zookeeper
```

Verify:

```bash
sudo systemctl is-enabled zookeeper
```

Expected:

```text
enabled
```

---

# ⚙️ 22. Create Kafka systemd Service

Create:

```bash
sudo vi /etc/systemd/system/kafka.service
```

Use:

```ini
[Unit]
Description=Apache Kafka Broker
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/kafka/kafka_2.13-3.8.1
ExecStart=/home/ec2-user/kafka/kafka_2.13-3.8.1/bin/kafka-server-start.sh /home/ec2-user/kafka/kafka_2.13-3.8.1/config/server.properties
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

The important dependency is:

```ini
Requires=zookeeper.service
After=zookeeper.service
```

Conceptually:

```text
systemd
  │
  ▼
ZooKeeper
  │
  │ available first
  ▼
Kafka
```

---

# 🔄 23. Reload systemd Again

```bash
sudo systemctl daemon-reload
```

---

# ▶️ 24. Start Kafka Service

```bash
sudo systemctl start kafka
```

Check:

```bash
sudo systemctl status kafka --no-pager
```

Expected:

```text
Active: active (running)
```

---

# 🔁 25. Enable Kafka at Boot

```bash
sudo systemctl enable kafka
```

Verify:

```bash
sudo systemctl is-enabled kafka
```

Expected:

```text
enabled
```

---

# 🧠 26. Final systemd Architecture

```text
                         systemd
                            │
              ┌─────────────┴─────────────┐
              │                           │
              ▼                           ▼
     zookeeper.service              kafka.service
              │                           │
              ▼                           ▼
          ZooKeeper                     Kafka
           :2181                         :9092
              │                           │
              │     coordination         │
              └──────────────────────────►
                                          │
                                          ▼
                                  Producer / Consumer
```

---

# 🧪 27. Service Management Commands

## Start

```bash
sudo systemctl start zookeeper
sudo systemctl start kafka
```

## Stop

```bash
sudo systemctl stop kafka
sudo systemctl stop zookeeper
```

Stop Kafka first, then ZooKeeper.

## Restart Kafka

```bash
sudo systemctl restart kafka
```

## Restart ZooKeeper

```bash
sudo systemctl restart zookeeper
```

For a clean full restart:

```bash
sudo systemctl stop kafka
sudo systemctl restart zookeeper
sudo systemctl start kafka
```

## Status

```bash
sudo systemctl status zookeeper
sudo systemctl status kafka
```

## Enable at boot

```bash
sudo systemctl enable zookeeper
sudo systemctl enable kafka
```

## Disable at boot

```bash
sudo systemctl disable kafka
sudo systemctl disable zookeeper
```

---

# 🔍 28. Process Verification

Check Java processes:

```bash
jps
```

Expected:

```text
XXXX QuorumPeerMain
YYYY Kafka
ZZZZ Jps
```

More detailed:

```bash
ps -ef | grep -E 'Kafka|QuorumPeerMain' | grep -v grep
```

---

# 🌐 29. Port Verification

ZooKeeper:

```bash
ss -lntp | grep 2181
```

Kafka:

```bash
ss -lntp | grep 9092
```

Expected conceptually:

```text
2181 → ZooKeeper
9092 → Kafka Broker
```

---

# 🪵 30. Service Logs

ZooKeeper logs:

```bash
sudo journalctl -u zookeeper
```

Follow live:

```bash
sudo journalctl -u zookeeper -f
```

Kafka logs:

```bash
sudo journalctl -u kafka
```

Follow live:

```bash
sudo journalctl -u kafka -f
```

Last 100 lines:

```bash
sudo journalctl -u kafka -n 100 --no-pager
```

---

# 📌 31. Create Your First Kafka Topic

We will create:

```text
my-first-topic
```

Run:

```bash
cd ~/kafka/kafka_2.13-3.8.1

bin/kafka-topics.sh \
  --create \
  --topic my-first-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Expected:

```text
Created topic my-first-topic.
```

---

# 📋 32. List Topics

```bash
bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Expected:

```text
my-first-topic
```

---

# 🔬 33. Describe Topic

```bash
bin/kafka-topics.sh \
  --describe \
  --topic my-first-topic \
  --bootstrap-server localhost:9092
```

Conceptually you should see:

```text
Topic: my-first-topic
PartitionCount: 1
ReplicationFactor: 1
Partition: 0
Leader: 0
Replicas: 0
Isr: 0
```

### Why replication factor 1?

Because our lab has only one broker:

```text
Broker count = 1
Replication factor = 1
```

A production cluster would normally have multiple brokers and appropriate replication.

---

# ✍️ 34. Start the Producer

Open a second SSH session to the same EC2 machine.

```bash
ssh -i <YOUR_KEY>.pem ec2-user@<EC2_PUBLIC_IP>
```

Then:

```bash
cd ~/kafka/kafka_2.13-3.8.1
```

Start producer:

```bash
bin/kafka-console-producer.sh \
  --topic my-first-topic \
  --bootstrap-server localhost:9092
```

Now type:

```text
Hello Kafka
```

Press Enter.

Then:

```text
Kafka is working
```

Press Enter.

---

# 📖 35. Start the Consumer

Open a third SSH session:

```bash
ssh -i <YOUR_KEY>.pem ec2-user@<EC2_PUBLIC_IP>
```

Then:

```bash
cd ~/kafka/kafka_2.13-3.8.1
```

Run:

```bash
bin/kafka-console-consumer.sh \
  --topic my-first-topic \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

Expected:

```text
Hello Kafka
Kafka is working
```

🎉 You have successfully completed:

```text
Producer
   │
   ▼
Kafka Broker
   │
   ▼
Topic
   │
   ▼
Partition
   │
   ▼
Consumer
```

---

# 🔄 36. Live Message Flow

Keep the consumer running.

In the producer terminal type:

```text
Message 1
Message 2
Message 3
```

Consumer should receive:

```text
Message 1
Message 2
Message 3
```

Architecture:

```text
┌──────────────┐
│   Producer   │
└──────┬───────┘
       │
       │ record
       ▼
┌────────────────────┐
│    Kafka Broker    │
│                    │
│ Topic              │
│   └─ Partition 0   │
└─────────┬──────────┘
          │
          │ poll/read
          ▼
┌────────────────────┐
│     Consumer       │
└────────────────────┘
```

---

# 🔁 37. Consumer Groups — First Look

Kafka consumers can work as part of a **consumer group**.

Conceptually:

```text
                   Kafka Topic
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
       Consumer Group A    Consumer Group B
             │                   │
             ▼                   ▼
         Application          Application
```

Multiple consumers in the same group can divide partitions between themselves.

This single-broker lab has one partition initially, so it is intentionally simple.

---

# 💾 38. Kafka Data Storage

Kafka stores records in partition log files.

Our configured location:

```text
/home/ec2-user/kafka/data/kafka-logs
```

Conceptually:

```text
Topic
  │
  ▼
Partition 0
  │
  ▼
Kafka Log Segments
  │
  ▼
Disk
```

Do not confuse Kafka's data log with ordinary application log messages.

---

# 🔄 39. Reboot Test — Most Important Operations Test

First verify both services are enabled:

```bash
sudo systemctl is-enabled zookeeper
sudo systemctl is-enabled kafka
```

Expected:

```text
enabled
enabled
```

Now reboot:

```bash
sudo reboot
```

Wait for SSH to become available again.

Reconnect:

```bash
ssh -i <YOUR_KEY>.pem ec2-user@<EC2_PUBLIC_IP>
```

Check:

```bash
sudo systemctl status zookeeper --no-pager
sudo systemctl status kafka --no-pager
```

Expected:

```text
Active: active (running)
```

Then:

```bash
jps
```

Expected:

```text
QuorumPeerMain
Kafka
Jps
```

🎯 This proves the services are configured to start automatically after an EC2 reboot.

---

# 🧯 40. Troubleshooting Guide

## Kafka is not running

```bash
sudo systemctl status kafka --no-pager
```

Then:

```bash
sudo journalctl -u kafka -n 100 --no-pager
```

---

## ZooKeeper is not running

```bash
sudo systemctl status zookeeper --no-pager
```

Then:

```bash
sudo journalctl -u zookeeper -n 100 --no-pager
```

---

## Port 2181 already in use

```bash
sudo ss -lntp | grep 2181
```

Find the process before starting another ZooKeeper instance.

---

## Port 9092 already in use

```bash
sudo ss -lntp | grep 9092
```

Find the existing Kafka process.

---

## Java not found

```bash
java -version
```

If missing:

```bash
sudo dnf install java-17-amazon-corretto -y
```

---

## Kafka starts but producer cannot connect

Check:

```bash
sudo systemctl status kafka --no-pager
ss -lntp | grep 9092
```

Then verify:

```bash
grep -E '^(listeners|advertised.listeners)' ~/kafka/kafka_2.13-3.8.1/config/server.properties
```

For this local lab, the producer should use:

```text
localhost:9092
```

---

## Kafka cannot connect to ZooKeeper

Check:

```bash
sudo systemctl status zookeeper --no-pager
```

Then:

```bash
ss -lntp | grep 2181
```

Then:

```bash
grep '^zookeeper.connect' ~/kafka/kafka_2.13-3.8.1/config/server.properties
```

Expected:

```text
zookeeper.connect=localhost:2181
```

---

# 🧹 41. Clean Stop

Stop producer and consumer with:

```text
Ctrl+C
```

Then:

```bash
sudo systemctl stop kafka
sudo systemctl stop zookeeper
```

Verify:

```bash
jps
```

Kafka and ZooKeeper JVMs should no longer be running.

---

# 🔁 42. Clean Start

Start in the correct order:

```bash
sudo systemctl start zookeeper
sudo systemctl start kafka
```

Verify:

```bash
sudo systemctl status zookeeper --no-pager
sudo systemctl status kafka --no-pager
```

---

# 🧪 43. One-Command Validation Checklist

Run:

```bash
whoami
java -version
jps
ss -lntp | grep 2181
ss -lntp | grep 9092
sudo systemctl is-enabled zookeeper
sudo systemctl is-enabled kafka
```

Expected concepts:

```text
ec2-user
Java 17
QuorumPeerMain
Kafka
2181 LISTEN
a9092 LISTEN
enabled
enabled
```

---

# 🧰 44. Automated Installation Script

This repository includes:

```text
install-kafka-zookeeper.sh
```

The script automates the base installation and service configuration.

### Download/copy the script to EC2

```bash
chmod +x install-kafka-zookeeper.sh
```

Run:

```bash
./install-kafka-zookeeper.sh
```

### What the script does

```text
1. Verify ec2-user
2. Update packages
3. Install Java 17
4. Download Kafka 3.8.1
5. Extract Kafka
6. Create data directories
7. Configure ZooKeeper
8. Configure Kafka
9. Create systemd services
10. Enable services
11. Start ZooKeeper
12. Start Kafka
13. Validate processes and ports
```

> 🔐 The script uses `sudo` only where operating-system/service privileges are required. Kafka and ZooKeeper themselves run as `ec2-user`.

---

# 🧱 45. Manual vs Automated Installation

| Method | Best For |
|---|---|
| Manual commands | Learning each concept |
| Shell script | Repeatable lab setup |
| systemd | Service operations |
| Console producer | Learning Kafka message flow |
| Console consumer | Learning Kafka message flow |
| Python/Java producer | Application development |
| Python/Java consumer | Application development |

**Recommendation:** Do the manual installation once. Then use the script to reproduce the lab quickly.

---

# 🧠 46. Kafka Concepts You Must Know

```text
Kafka
│
├── Broker
│
├── Topic
│
├── Partition
│
├── Record / Message
│
├── Producer
│
├── Consumer
│
├── Consumer Group
│
├── Offset
│
├── Leader
│
├── Replication
│
├── ISR
│
├── Retention
│
├── Log Segment
│
├── Bootstrap Server
│
├── Serializer / Deserializer
│
├── Key / Value
│
├── Acknowledgement
│
├── Batch
│
├── Compression
│
└── Kafka Connect / Streams
```

---

# 🧠 47. ZooKeeper Concepts for This Lab

```text
ZooKeeper
│
├── Client connection
├── Coordination
├── Broker metadata
├── Cluster state
├── Leader-related coordination
└── Configuration/state for ZooKeeper-based Kafka
```

Remember:

> ZooKeeper is not where your application messages are stored.

Kafka stores the records in its partition logs.

---

# 🏗️ 48. Single Broker vs Production Cluster

## Our lab

```text
EC2
 │
 ├── ZooKeeper
 │
 └── Kafka Broker 0
```

Replication:

```text
RF = 1
```

## Production-style cluster

```text
              ZooKeeper Ensemble / KRaft
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
       Broker 1      Broker 2      Broker 3
          │             │             │
          └─────────────┼─────────────┘
                        │
                      Topics
                        │
              Multiple partitions
                        │
                  Replication
```

A single EC2 broker is a **learning environment**, not a highly available Kafka architecture.

---

# 🔐 49. Security Considerations

This lab intentionally keeps security simple.

For production, consider:

- TLS encryption
- SASL authentication
- ACL authorization
- Private subnets
- Security Groups
- Network ACLs
- IAM/network integration where applicable
- Secrets management
- Disk encryption
- OS hardening
- Monitoring
- Alerting
- Multi-broker architecture
- Replication
- Capacity planning

Never expose Kafka or ZooKeeper broadly to the Internet just to make a lab work.

---

# 📊 50. Operations Cheat Sheet

| Task | Command |
|---|---|
| Start ZooKeeper | `sudo systemctl start zookeeper` |
| Stop ZooKeeper | `sudo systemctl stop zookeeper` |
| Restart ZooKeeper | `sudo systemctl restart zookeeper` |
| ZooKeeper status | `sudo systemctl status zookeeper` |
| Start Kafka | `sudo systemctl start kafka` |
| Stop Kafka | `sudo systemctl stop kafka` |
| Restart Kafka | `sudo systemctl restart kafka` |
| Kafka status | `sudo systemctl status kafka` |
| Enable ZooKeeper | `sudo systemctl enable zookeeper` |
| Enable Kafka | `sudo systemctl enable kafka` |
| JVM processes | `jps` |
| ZooKeeper port | `ss -lntp \| grep 2181` |
| Kafka port | `ss -lntp \| grep 9092` |
| Kafka logs | `sudo journalctl -u kafka -f` |
| ZooKeeper logs | `sudo journalctl -u zookeeper -f` |
| List topics | `bin/kafka-topics.sh --list --bootstrap-server localhost:9092` |
| Describe topic | `bin/kafka-topics.sh --describe --topic my-first-topic --bootstrap-server localhost:9092` |

---

# 🎓 51. Learning Path After This Lab

Once this single-node lab is working, continue in this order:

```text
Level 1
Single Kafka Broker + ZooKeeper
        ↓
Level 2
Multiple Topics
        ↓
Level 3
Multiple Partitions
        ↓
Level 4
Producer Keys
        ↓
Level 5
Consumer Groups
        ↓
Level 6
Offsets
        ↓
Level 7
Replication
        ↓
Level 8
Multiple Kafka Brokers
        ↓
Level 9
Kafka Connect
        ↓
Level 10
Kafka Streams
        ↓
Level 11
Python Producer / Consumer
        ↓
Level 12
Java / Spring Boot Producer / Consumer
        ↓
Level 13
TLS / SASL / ACL
        ↓
Level 14
Monitoring
        ↓
Level 15
KRaft Architecture
```

---

# 🆚 52. ZooKeeper vs KRaft

```text
ZooKeeper Model

Kafka Broker
     │
     ▼
ZooKeeper
```

Modern Kafka direction:

```text
KRaft Model

Kafka Controller Quorum
          │
          ▼
      Kafka Brokers
```

For this lab we intentionally learn the ZooKeeper model first because it is useful for understanding older Kafka environments and interview questions.

---

# 🏁 53. Final Validation

You are done when all of these work:

```bash
whoami
```

```text
ec2-user
```

```bash
java -version
```

```text
Java 17
```

```bash
sudo systemctl status zookeeper --no-pager
```

```text
active (running)
```

```bash
sudo systemctl status kafka --no-pager
```

```text
active (running)
```

```bash
jps
```

```text
QuorumPeerMain
Kafka
Jps
```

```bash
ss -lntp | grep 2181
ss -lntp | grep 9092
```

```text
2181 → ZooKeeper
9092 → Kafka
```

Then:

```text
Producer
   ↓
Kafka
   ↓
Topic
   ↓
Partition
   ↓
Consumer
```

🎉 **VishwaTech Labs Kafka + ZooKeeper single-EC2 lab is working!**

---

# 📚 54. Official References

- [Apache Kafka 3.8 Quick Start](https://kafka.apache.org/38/getting-started/quickstart/)
- [Apache Kafka Downloads](https://kafka.apache.org/community/downloads/)
- [Apache Kafka 3.8.1 Release](https://kafka.apache.org/blog/2024/10/29/apache-kafka-3.8.1-release-announcement/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache ZooKeeper](https://zookeeper.apache.org/)
- [Amazon EC2](https://aws.amazon.com/ec2/)
- [Amazon Linux 2023](https://aws.amazon.com/linux/amazon-linux-2023/)
- [Amazon Corretto](https://aws.amazon.com/corretto/)

---

# 👨‍🏫 VishwaTech Labs Teaching Note

The most important sequence for a student to remember is:

```text
                         KAFKA
                           │
          ┌────────────────┼────────────────┐
          │                │                │
          ▼                ▼                ▼
       Producer          Topic           Consumer
          │                │                ▲
          │                ▼                │
          └──────────► Partition ──────────┘
                           │
                           ▼
                          Disk

                    ZooKeeper
                         │
                         ▼
               Cluster coordination
```

### One-line definitions

| Concept | Simple Definition |
|---|---|
| Producer | Sends records to Kafka |
| Broker | Kafka server that stores/serves records |
| Topic | Named stream/category of records |
| Partition | Ordered log inside a topic |
| Consumer | Reads records from Kafka |
| Consumer Group | Group of consumers sharing work |
| Offset | Position of a record within a partition |
| Replication | Copies partition data across brokers |
| ZooKeeper | Coordination component in this legacy Kafka model |
| KRaft | Modern Kafka metadata/controller architecture |

---

## ⭐ VishwaTech Labs

**Learn → Build → Break → Fix → Automate → Secure → Operate**

> This lab is designed to be followed line-by-line. Execute the manual sections once, verify the expected outputs, and then use `install-kafka-zookeeper.sh` to reproduce the environment.
