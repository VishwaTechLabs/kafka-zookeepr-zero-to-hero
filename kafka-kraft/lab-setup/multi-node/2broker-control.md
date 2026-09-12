# 🚀 Apache Kafka 4.0.2 --- KRaft Multi-Node AWS EC2 Lab

```{=html}
<p align="center">
```
`<img src="https://img.shields.io/badge/Apache%20Kafka-4.0.2-black?style=for-the-badge&logo=apachekafka" alt="Kafka 4.0.2"/>`{=html}
`<img src="https://img.shields.io/badge/KRaft-Enabled-FF6F00?style=for-the-badge" alt="KRaft"/>`{=html}
`<img src="https://img.shields.io/badge/AWS-EC2-FF9900?style=for-the-badge&logo=amazonaws" alt="AWS EC2"/>`{=html}
`<img src="https://img.shields.io/badge/Amazon%20Linux%202023-232F3E?style=for-the-badge&logo=amazonlinux" alt="Amazon Linux 2023"/>`{=html}
`<img src="https://img.shields.io/badge/Offset%20Explorer-Client-4CAF50?style=for-the-badge" alt="Offset Explorer"/>`{=html}
```{=html}
</p>
```
```{=html}
<p align="center">
```
`<b>`{=html}2 Dedicated KRaft Controllers + 2 Dedicated Kafka Brokers +
Public Offset Explorer Access`</b>`{=html}
```{=html}
</p>
```
```{=html}
<p align="center">
```
`<i>`{=html}Production-style architecture for learning, labs, Kafka
administration, networking, replication, KRaft and external client
connectivity.`</i>`{=html}
```{=html}
</p>
```

------------------------------------------------------------------------

## 📚 Table of Contents

-   [🎯 Lab Objective](#-lab-objective)
-   [🏗️ Final Architecture](#️-final-architecture)
-   [🧭 Node Inventory](#-node-inventory)
-   [🌐 Private vs Public IP Design](#-private-vs-public-ip-design)
-   [🔌 Port Matrix](#-port-matrix)
-   [🧠 Kafka Concepts](#-kafka-concepts)
-   [🆔 Cluster ID](#-cluster-id)
-   [🔢 Node IDs](#-node-ids)
-   [🗳️ KRaft Controller Quorum](#️-kraft-controller-quorum)
-   [📁 Kafka Directory Structure](#-kafka-directory-structure)
-   [☕ Java Prerequisites](#-java-prerequisites)
-   [🔍 Verify Kafka Installation](#-verify-kafka-installation)
-   [☁️ AWS Security Group](#️-aws-security-group)
-   [🛠️ Step 1 --- Verify All IPs](#️-step-1--verify-all-ips)
-   [🛠️ Step 2 --- Configure Controller
    1](#️-step-2--configure-controller-1)
-   [🛠️ Step 3 --- Configure Controller
    2](#️-step-3--configure-controller-2)
-   [🛠️ Step 4 --- Configure Broker 1](#️-step-4--configure-broker-1)
-   [🛠️ Step 5 --- Configure Broker 2](#️-step-5--configure-broker-2)
-   [🧪 Step 6 --- Test Network
    Connectivity](#-step-6--test-network-connectivity)
-   [🆔 Step 7 --- Generate Cluster ID](#-step-7--generate-cluster-id)
-   [🧹 Step 8 --- Check Existing Kafka
    Data](#-step-8--check-existing-kafka-data)
-   [💾 Step 9 --- Format KRaft Storage](#-step-9--format-kraft-storage)
-   [🚀 Step 10 --- Start Controllers](#-step-10--start-controllers)
-   [🚀 Step 11 --- Start Brokers](#-step-11--start-brokers)
-   [🔎 Step 12 --- Verify Listening
    Ports](#-step-12--verify-listening-ports)
-   [🗳️ Step 13 --- Verify KRaft
    Quorum](#️-step-13--verify-kraft-quorum)
-   [📦 Step 14 --- Create Replicated
    Topic](#-step-14--create-replicated-topic)
-   [✍️ Step 15 --- Producer Test](#️-step-15--producer-test)
-   [📥 Step 16 --- Consumer Test](#-step-16--consumer-test)
-   [💻 Step 17 --- Offset Explorer](#-step-17--offset-explorer)
-   [🧪 Step 18 --- Failure and Recovery
    Tests](#-step-18--failure-and-recovery-tests)
-   [🔐 Security Notes](#-security-notes)
-   [🚨 Common Mistakes](#-common-mistakes)
-   [🩺 Troubleshooting](#-troubleshooting)
-   [📊 Verification Checklist](#-verification-checklist)
-   [🎓 What This Lab Teaches](#-what-this-lab-teaches)
-   [📖 Official Documentation](#-official-documentation)

------------------------------------------------------------------------

# 🎯 Lab Objective

This lab builds a Kafka **4.0.2** cluster on AWS EC2 using **KRaft
mode**.

The target topology is:

``` text
                    ┌───────────────────────────┐
                    │       YOUR LAPTOP         │
                    │     Offset Explorer       │
                    └─────────────┬─────────────┘
                                  │
                    Internet / AWS Public IPs
                                  │
                   ┌──────────────┴──────────────┐
                   │                             │
            PUBLIC-IP:29092                PUBLIC-IP:29092
                   │                             │
          ┌────────▼────────┐          ┌────────▼────────┐
          │    BROKER 1     │          │    BROKER 2     │
          │     node 101    │          │     node 102    │
          │                 │          │                 │
          │ Internal :9092  │◄────────►│ Internal :9092  │
          │ External :29092 │          │ External :29092 │
          └────────┬────────┘          └────────┬────────┘
                   │                             │
                   └──────────────┬──────────────┘
                                  │
                           KRaft metadata
                                  │
                 ┌────────────────┴────────────────┐
                 │                                 │
          ┌──────▼───────┐                  ┌──────▼───────┐
          │ CONTROLLER 1 │                  │ CONTROLLER 2 │
          │    node 1    │                  │    node 2    │
          │ :9093        │◄────────────────►│ :9093        │
          └──────────────┘                  └──────────────┘
```

### Lab goals

-   Understand KRaft architecture.
-   Run dedicated controllers.
-   Run dedicated brokers.
-   Understand `node.id`.
-   Understand `cluster.id`.
-   Understand controller quorum.
-   Understand private and public IPs.
-   Configure internal and external Kafka listeners.
-   Configure `advertised.listeners`.
-   Connect Offset Explorer from a laptop.
-   Create replicated topics.
-   Test producer/consumer traffic.
-   Observe partition leadership and ISR.
-   Perform broker failure testing.
-   Learn AWS Security Group design.

------------------------------------------------------------------------

# 🏗️ Final Architecture

## Logical architecture

``` text
                          KAFKA CLUSTER
                               │
                  ┌────────────┴────────────┐
                  │                         │
             KRaft Quorum               Brokers
                  │                         │
          ┌───────┴───────┐          ┌──────┴──────┐
          │               │          │             │
       Ctrl-1           Ctrl-2     Broker-1      Broker-2
       ID=1             ID=2       ID=101        ID=102
       :9093            :9093      :9092         :9092
```

## External client architecture

``` text
Laptop
  │
  ├── Broker-1 Public-IP:29092
  │
  └── Broker-2 Public-IP:29092
```

## Internal architecture

``` text
Broker-1 Private-IP:9092
       │
       ├──────── Broker-2 Private-IP:9092
       │
       ├──────── Controller-1 Private-IP:9093
       │
       └──────── Controller-2 Private-IP:9093
```

------------------------------------------------------------------------

# 🧭 Node Inventory

> Replace `<BROKER1_PUBLIC_IP>` and `<BROKER2_PUBLIC_IP>` with the
> actual public IPv4 addresses from EC2.

  -----------------------------------------------------------------------------------------------------------------
  Node         Hostname               Role              Node ID Private IP        Public IP               Port
  ------------ ---------------------- ------------ ------------ ----------------- ----------------------- ---------
  Controller 1 `kafka-controller-1`   Controller            `1` `172.31.39.10`    ❌ Do not use           `9093`

  Controller 2 `kafka-controller-2`   Controller            `2` `172.31.39.223`   ❌ Do not use           `9093`

  Broker 1     `kafka-broker-1`       Broker              `101` `172.31.45.5`     `<BROKER1_PUBLIC_IP>`   `9092`,
                                                                                                          `29092`

  Broker 2     `kafka-broker-2`       Broker              `102` `172.31.39.40`    `<BROKER2_PUBLIC_IP>`   `9092`,
                                                                                                          `29092`
  -----------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------

# 🌐 Private vs Public IP Design

This is one of the most important parts of the lab.

## Private IPs

AWS private IPs are used for:

-   Broker-to-broker traffic.
-   Broker-to-controller traffic.
-   Controller-to-controller traffic.
-   Internal cluster communication.

Example:

``` text
Controller 1 → 172.31.39.10:9093
Controller 2 → 172.31.39.223:9093

Broker 1 → 172.31.45.5:9092
Broker 2 → 172.31.39.40:9092
```

## Public IPs

Public IPs are used only for external clients such as:

-   Offset Explorer.
-   Laptop-based Kafka CLI clients.
-   Other external applications.

Example:

``` text
Broker 1 → <BROKER1_PUBLIC_IP>:29092
Broker 2 → <BROKER2_PUBLIC_IP>:29092
```

## ❌ Never advertise these to your laptop

``` text
172.31.45.5:9092
172.31.39.40:9092
```

Those are AWS VPC private addresses.

## ❌ Never expose controllers to the Internet

Do not configure:

``` text
PUBLIC-IP:9093
```

for controller communication.

------------------------------------------------------------------------

# 🔌 Port Matrix

  ------------------------------------------------------------------------
                   Port Purpose          Server           Exposure
  --------------------- ---------------- ---------------- ----------------
                   `22` SSH              All EC2          Your IP only

                 `9092` Internal broker  Brokers          Private VPC only
                        traffic                           

                 `9093` KRaft controller Controllers      Private VPC only
                        quorum                            

                `29092` External Kafka   Brokers          Your laptop
                        clients                           public IP only
  ------------------------------------------------------------------------

### Traffic flow

``` text
Laptop
   │
   └──── TCP/29092 ────► Broker

Broker
   │
   └──── TCP/9092 ─────► Broker

Broker
   │
   └──── TCP/9093 ─────► Controller

Controller
   │
   └──── TCP/9093 ─────► Controller
```

------------------------------------------------------------------------

# 🧠 Kafka Concepts

## Broker

A Kafka broker stores topic partitions and serves producer/consumer
requests.

``` text
Broker 1
Broker 2
```

## Controller

The KRaft controller manages Kafka cluster metadata and controller
quorum state.

``` text
Controller 1
Controller 2
```

## KRaft

KRaft replaces ZooKeeper with Kafka's own metadata quorum.

``` text
Old Kafka:

Kafka Brokers → ZooKeeper

Modern Kafka:

Kafka Brokers → KRaft Controllers
```

## Partition

A topic is divided into partitions.

``` text
orders
 ├── partition 0
 ├── partition 1
 ├── partition 2
 └── partition 3
```

## Replication

Replication keeps copies of partitions on multiple brokers.

``` text
Partition 0
Leader  → Broker 1
Replica → Broker 2
```

------------------------------------------------------------------------

# 🆔 Cluster ID

The **cluster ID** identifies the Kafka cluster.

It is different from `node.id`.

``` text
                    CLUSTER ID
                        │
             ┌──────────┼──────────┐
             │          │          │
          Node 1     Node 2     Node 101
          Ctrl-1     Ctrl-2     Broker-1
             │          │          │
             └──────────┼──────────┘
                        │
                     Node 102
                     Broker-2
```

All four servers must use the **same cluster ID**.

Generate it once:

``` bash
cd /home/ec2-user/kafka

./bin/kafka-storage.sh random-uuid
```

Example:

``` text
ABC123...
```

Do not generate a different cluster ID for every server.

------------------------------------------------------------------------

# 🔢 Node IDs

Node IDs identify individual Kafka servers.

Our design:

``` text
Controller 1 = 1
Controller 2 = 2

Broker 1     = 101
Broker 2     = 102
```

All node IDs must be unique across the cluster.

------------------------------------------------------------------------

# 🗳️ KRaft Controller Quorum

This lab uses a **static KRaft quorum**.

``` properties
controller.quorum.voters=1@172.31.39.10:9093,2@172.31.39.223:9093
```

Meaning:

``` text
Controller ID 1
       ↓
172.31.39.10:9093

Controller ID 2
       ↓
172.31.39.223:9093
```

All brokers and controllers must have the same controller voter list.

> ⚠️ Two controllers are suitable for a lab, but not ideal for
> production HA. A 2-controller quorum requires both controllers for
> majority. A 3-controller quorum is the usual resilient design.

------------------------------------------------------------------------

# 📁 Kafka Directory Structure

Your existing installation is:

``` text
/home/ec2-user/kafka/
├── bin/
├── config/
│   ├── server.properties
│   ├── controller.properties
│   ├── broker.properties
│   ├── producer.properties
│   ├── consumer.properties
│   └── ...
├── libs/
├── licenses/
└── site-docs/
```

Kafka data:

``` text
/home/ec2-user/kafka-data/
```

### Important

You do **NOT** need to create:

``` text
config/kraft/
```

for this lab.

We intentionally use:

``` text
config/server.properties
```

for brokers and:

``` text
config/controller.properties
```

for controllers.

------------------------------------------------------------------------

# ☕ Java Prerequisites

Kafka 4.0.2 requires Java 17+.

Check:

``` bash
java -version
```

Expected:

``` text
openjdk version "17..."
```

If Java is missing on Amazon Linux 2023:

``` bash
sudo dnf install java-17-amazon-corretto -y
```

Then:

``` bash
java -version
```

------------------------------------------------------------------------

# 🔍 Verify Kafka Installation

On every Kafka server:

``` bash
cd /home/ec2-user/kafka

./bin/kafka-topics.sh --version
```

Expected:

``` text
4.0.2
```

Also:

``` bash
pwd
```

Expected:

``` text
/home/ec2-user/kafka
```

------------------------------------------------------------------------

# ☁️ AWS Security Group

Create/use a Kafka security group.

## Inbound rules

### SSH

``` text
TCP 22
Source: YOUR_PUBLIC_IP/32
```

### Internal Broker

``` text
TCP 9092
Source: Kafka-SG
```

### Controller

``` text
TCP 9093
Source: Kafka-SG
```

### External Kafka

``` text
TCP 29092
Source: YOUR_PUBLIC_IP/32
```

### Recommended

Do NOT use:

``` text
0.0.0.0/0
```

for Kafka ports unless this is a disposable lab and you deliberately
accept the exposure.

## Security Group table

     Port Source     Why
  ------- ---------- ------------------
       22 Your IP    SSH
     9092 Kafka SG   Broker-to-broker
     9093 Kafka SG   KRaft quorum
    29092 Your IP    Offset Explorer

------------------------------------------------------------------------

# 🛠️ Step 1 --- Verify All IPs

## Controller 1

``` bash
hostname -I
```

Expected:

``` text
172.31.39.10
```

## Controller 2

``` bash
hostname -I
```

Expected:

``` text
172.31.39.223
```

## Broker 1

``` bash
hostname -I
```

Expected:

``` text
172.31.45.5
```

## Broker 2

``` bash
hostname -I
```

Expected:

``` text
172.31.39.40
```

Get broker public IP:

``` bash
curl http://169.254.169.254/latest/meta-data/public-ipv4
```

Run this on both brokers.

------------------------------------------------------------------------

# 🛠️ Step 2 --- Configure Controller 1

Server:

``` text
kafka-controller-1
Private IP: 172.31.39.10
Node ID: 1
```

Run:

``` bash
cd /home/ec2-user/kafka/config
sudo cp controller.properties controller.properties.backup
sudo vim controller.properties
```

Use:

``` properties
############################
# KAFKA CONTROLLER 1
############################

process.roles=controller

node.id=1

############################
# CONTROLLER LISTENER
############################

listeners=CONTROLLER://172.31.39.10:9093

controller.listener.names=CONTROLLER

############################
# STATIC KRAFT QUORUM
############################

controller.quorum.voters=1@172.31.39.10:9093,2@172.31.39.223:9093

############################
# BROKER COMMUNICATION
############################

inter.broker.listener.name=INTERNAL

############################
# SECURITY PROTOCOL MAP
############################

listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT

############################
# DATA
############################

log.dirs=/home/ec2-user/kafka-data
```

------------------------------------------------------------------------

# 🛠️ Step 3 --- Configure Controller 2

Server:

``` text
kafka-controller-2
Private IP: 172.31.39.223
Node ID: 2
```

Run:

``` bash
cd /home/ec2-user/kafka/config
sudo cp controller.properties controller.properties.backup
sudo vim controller.properties
```

Use:

``` properties
############################
# KAFKA CONTROLLER 2
############################

process.roles=controller

node.id=2

############################
# CONTROLLER LISTENER
############################

listeners=CONTROLLER://172.31.39.223:9093

controller.listener.names=CONTROLLER

############################
# STATIC KRAFT QUORUM
############################

controller.quorum.voters=1@172.31.39.10:9093,2@172.31.39.223:9093

############################
# BROKER COMMUNICATION
############################

inter.broker.listener.name=INTERNAL

############################
# SECURITY PROTOCOL MAP
############################

listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT

############################
# DATA
############################

log.dirs=/home/ec2-user/kafka-data
```

------------------------------------------------------------------------

# 🛠️ Step 4 --- Configure Broker 1

Server:

``` text
kafka-broker-1
Private IP: 172.31.45.5
Node ID: 101
```

Run:

``` bash
cd /home/ec2-user/kafka/config
sudo cp server.properties server.properties.backup
sudo vim server.properties
```

Use:

``` properties
############################
# KAFKA BROKER 1
############################

process.roles=broker

node.id=101

############################
# LISTENERS
############################

listeners=INTERNAL://172.31.45.5:9092,EXTERNAL://0.0.0.0:29092

############################
# ADVERTISED LISTENERS
############################

advertised.listeners=INTERNAL://172.31.45.5:9092,EXTERNAL://<BROKER1_PUBLIC_IP>:29092

############################
# LISTENER SECURITY
############################

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

############################
# INTER-BROKER COMMUNICATION
############################

inter.broker.listener.name=INTERNAL

############################
# KRAFT
############################

controller.listener.names=CONTROLLER

controller.quorum.voters=1@172.31.39.10:9093,2@172.31.39.223:9093

############################
# DATA
############################

log.dirs=/home/ec2-user/kafka-data

############################
# LAB REPLICATION SETTINGS
############################

default.replication.factor=2

min.insync.replicas=1

offsets.topic.replication.factor=2

transaction.state.log.replication.factor=2

transaction.state.log.min.isr=1
```

Replace:

``` text
<BROKER1_PUBLIC_IP>
```

with the actual EC2 public IPv4.

------------------------------------------------------------------------

# 🛠️ Step 5 --- Configure Broker 2

Server:

``` text
kafka-broker-2
Private IP: 172.31.39.40
Node ID: 102
```

Run:

``` bash
cd /home/ec2-user/kafka/config
sudo cp server.properties server.properties.backup
sudo vim server.properties
```

Use:

``` properties
############################
# KAFKA BROKER 2
############################

process.roles=broker

node.id=102

############################
# LISTENERS
############################

listeners=INTERNAL://172.31.39.40:9092,EXTERNAL://0.0.0.0:29092

############################
# ADVERTISED LISTENERS
############################

advertised.listeners=INTERNAL://172.31.39.40:9092,EXTERNAL://<BROKER2_PUBLIC_IP>:29092

############################
# LISTENER SECURITY
############################

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

############################
# INTER-BROKER COMMUNICATION
############################

inter.broker.listener.name=INTERNAL

############################
# KRAFT
############################

controller.listener.names=CONTROLLER

controller.quorum.voters=1@172.31.39.10:9093,2@172.31.39.223:9093

############################
# DATA
############################

log.dirs=/home/ec2-user/kafka-data

############################
# LAB REPLICATION SETTINGS
############################

default.replication.factor=2

min.insync.replicas=1

offsets.topic.replication.factor=2

transaction.state.log.replication.factor=2

transaction.state.log.min.isr=1
```

Replace:

``` text
<BROKER2_PUBLIC_IP>
```

with the actual EC2 public IPv4.

------------------------------------------------------------------------

# 🧪 Step 6 --- Test Network Connectivity

Do this **before formatting or starting Kafka**.

## From Broker 1

``` bash
nc -zv 172.31.39.10 9093
nc -zv 172.31.39.223 9093
nc -zv 172.31.39.40 9092
```

## From Broker 2

``` bash
nc -zv 172.31.39.10 9093
nc -zv 172.31.39.223 9093
nc -zv 172.31.45.5 9092
```

## From Controller 1

``` bash
nc -zv 172.31.39.223 9093
```

## From Controller 2

``` bash
nc -zv 172.31.39.10 9093
```

At this stage Kafka may not be listening yet, so a failed port check on
an unstarted service is expected. What matters is that the AWS Security
Group/network path allows the traffic.

------------------------------------------------------------------------

# 🆔 Step 7 --- Generate Cluster ID

Do this **once**.

On Controller 1:

``` bash
cd /home/ec2-user/kafka

./bin/kafka-storage.sh random-uuid
```

Example:

``` text
4L5xQz1mQ5q9A2bKJxZ7Mw
```

Save it:

``` bash
export KAFKA_CLUSTER_ID="<YOUR_CLUSTER_ID>"
echo "$KAFKA_CLUSTER_ID"
```

Do not generate a new UUID on each node.

------------------------------------------------------------------------

# 🧹 Step 8 --- Check Existing Kafka Data

Before formatting, check every server:

``` bash
ls -la /home/ec2-user/kafka-data
```

Then:

``` bash
cat /home/ec2-user/kafka-data/meta.properties
```

If it exists, inspect:

``` text
cluster.id=
node.id=
directory.id=
```

## Fresh lab reset

If this is a disposable lab and existing Kafka data is not required:

``` bash
rm -rf /home/ec2-user/kafka-data/*
```

Do this on all four nodes.

> ⚠️ Never delete Kafka data on a cluster containing data you need.

------------------------------------------------------------------------

# 💾 Step 9 --- Format KRaft Storage

Use the **same cluster ID** everywhere.

## Controller 1

``` bash
cd /home/ec2-user/kafka

./bin/kafka-storage.sh format \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --config config/controller.properties
```

## Controller 2

``` bash
cd /home/ec2-user/kafka

./bin/kafka-storage.sh format \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --config config/controller.properties
```

## Broker 1

``` bash
cd /home/ec2-user/kafka

./bin/kafka-storage.sh format \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --config config/server.properties
```

## Broker 2

``` bash
cd /home/ec2-user/kafka

./bin/kafka-storage.sh format \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --config config/server.properties
```

Because this lab explicitly configures `controller.quorum.voters`, it
uses the static quorum model.

------------------------------------------------------------------------

# 🔎 Verify `meta.properties`

On every server:

``` bash
cat /home/ec2-user/kafka-data/meta.properties
```

Expected conceptually:

### Controller 1

``` text
cluster.id=ABC
node.id=1
```

### Controller 2

``` text
cluster.id=ABC
node.id=2
```

### Broker 1

``` text
cluster.id=ABC
node.id=101
```

### Broker 2

``` text
cluster.id=ABC
node.id=102
```

The cluster ID must match.

The node ID must be unique.

------------------------------------------------------------------------

# 🚀 Step 10 --- Start Controllers

Start Controller 1 first.

## Controller 1

``` bash
cd /home/ec2-user/kafka

./bin/kafka-server-start.sh config/controller.properties
```

Keep the terminal running.

## Controller 2

Open another SSH session:

``` bash
cd /home/ec2-user/kafka

./bin/kafka-server-start.sh config/controller.properties
```

Wait until both controllers show successful startup.

------------------------------------------------------------------------

# 🚀 Step 11 --- Start Brokers

## Broker 1

``` bash
cd /home/ec2-user/kafka

./bin/kafka-server-start.sh config/server.properties
```

## Broker 2

``` bash
cd /home/ec2-user/kafka

./bin/kafka-server-start.sh config/server.properties
```

------------------------------------------------------------------------

# 🔎 Step 12 --- Verify Listening Ports

## Broker 1

``` bash
sudo ss -lntp | grep -E '9092|29092'
```

Expected conceptually:

``` text
172.31.45.5:9092
0.0.0.0:29092
```

## Broker 2

``` bash
sudo ss -lntp | grep -E '9092|29092'
```

Expected:

``` text
172.31.39.40:9092
0.0.0.0:29092
```

## Controller 1

``` bash
sudo ss -lntp | grep 9093
```

Expected:

``` text
172.31.39.10:9093
```

## Controller 2

``` bash
sudo ss -lntp | grep 9093
```

Expected:

``` text
172.31.39.223:9093
```

------------------------------------------------------------------------

# 🗳️ Step 13 --- Verify KRaft Quorum

From Broker 1:

``` bash
cd /home/ec2-user/kafka

./bin/kafka-metadata-quorum.sh \
  --bootstrap-server 172.31.45.5:9092 \
  describe --status
```

Look for:

``` text
ClusterId
LeaderId
LeaderEpoch
HighWatermark
CurrentVoters
```

The controller quorum should show both controllers.

------------------------------------------------------------------------

# 📦 Step 14 --- Create Replicated Topic

Create a test topic:

``` bash
cd /home/ec2-user/kafka

./bin/kafka-topics.sh \
  --bootstrap-server 172.31.45.5:9092 \
  --create \
  --topic test-topic \
  --partitions 4 \
  --replication-factor 2
```

Expected:

``` text
Created topic test-topic.
```

Describe:

``` bash
./bin/kafka-topics.sh \
  --bootstrap-server 172.31.45.5:9092 \
  --describe \
  --topic test-topic
```

Expected conceptually:

``` text
Partition 0
Leader: 101
Replicas: 101,102
ISR: 101,102

Partition 1
Leader: 102
Replicas: 102,101
ISR: 102,101
```

Your exact partition leadership may differ.

------------------------------------------------------------------------

# ✍️ Step 15 --- Producer Test

Run:

``` bash
cd /home/ec2-user/kafka

./bin/kafka-console-producer.sh \
  --bootstrap-server 172.31.45.5:9092 \
  --topic test-topic
```

Enter:

``` text
hello kafka
message one
message two
message three
kafka kraft lab
```

------------------------------------------------------------------------

# 📥 Step 16 --- Consumer Test

On another terminal:

``` bash
cd /home/ec2-user/kafka

./bin/kafka-console-consumer.sh \
  --bootstrap-server 172.31.39.40:9092 \
  --topic test-topic \
  --from-beginning
```

You should receive:

``` text
hello kafka
message one
message two
message three
kafka kraft lab
```

This proves the cluster is functioning.

------------------------------------------------------------------------

# 💻 Step 17 --- Offset Explorer

Offset Explorer runs on your Windows laptop.

## Bootstrap servers

Use the **public IPs of both brokers**:

``` text
<BROKER1_PUBLIC_IP>:29092,<BROKER2_PUBLIC_IP>:29092
```

Example format:

``` text
54.x.x.x:29092,3.x.x.x:29092
```

Do not use:

``` text
172.31.45.5:9092
172.31.39.40:9092
```

because those are private VPC addresses.

## Client protocol

For this lab:

``` text
PLAINTEXT
```

No TLS/SASL is configured yet.

## Why advertised.listeners matters

Offset Explorer initially connects to a broker and receives Kafka
metadata.

Kafka must tell Offset Explorer:

``` text
Broker 101 → PUBLIC-IP-1:29092
Broker 102 → PUBLIC-IP-2:29092
```

That is why:

``` properties
advertised.listeners=INTERNAL://PRIVATE-IP:9092,EXTERNAL://PUBLIC-IP:29092
```

is essential.

------------------------------------------------------------------------

# 🧪 Laptop Connectivity Test

From Windows PowerShell:

``` powershell
Test-NetConnection <BROKER1_PUBLIC_IP> -Port 29092
```

and:

``` powershell
Test-NetConnection <BROKER2_PUBLIC_IP> -Port 29092
```

Expected:

``` text
TcpTestSucceeded : True
```

If false, check:

1.  EC2 Security Group.
2.  Network ACL.
3.  EC2 public IP.
4.  Kafka listener.
5.  Kafka advertised listener.
6.  OS firewall if configured.
7.  Whether Kafka is actually running.

------------------------------------------------------------------------

# 🧪 Step 18 --- Failure and Recovery Tests

## Test 1 --- Stop Broker 1

Stop Broker 1 using:

``` text
Ctrl+C
```

Then from Broker 2:

``` bash
./bin/kafka-topics.sh \
  --bootstrap-server 172.31.39.40:9092 \
  --describe \
  --topic test-topic
```

Observe:

``` text
Leader
Replicas
ISR
```

## Test 2 --- Restart Broker 1

``` bash
cd /home/ec2-user/kafka

./bin/kafka-server-start.sh config/server.properties
```

Wait for ISR recovery.

Run:

``` bash
./bin/kafka-topics.sh \
  --bootstrap-server 172.31.39.40:9092 \
  --describe \
  --topic test-topic
```

You should eventually see Broker 1 return to the ISR.

------------------------------------------------------------------------

# 🔐 Security Notes

This lab uses:

``` text
PLAINTEXT
```

That is intentional for learning.

It is **not appropriate for production Internet exposure**.

Production Kafka should normally use:

``` text
TLS
```

and/or:

``` text
SASL_SSL
```

with:

-   Authentication.
-   Encryption.
-   Authorization.
-   ACLs.
-   Restricted Security Groups.
-   Private networking.
-   Monitoring.
-   Secrets management.
-   Certificate management.
-   Strong operational controls.

------------------------------------------------------------------------

# 🚨 Common Mistakes

## ❌ Mistake 1 --- Different cluster IDs

Wrong:

``` text
Controller 1 → UUID-A
Controller 2 → UUID-B
Broker 1     → UUID-C
Broker 2     → UUID-D
```

Correct:

``` text
All four → SAME cluster ID
```

------------------------------------------------------------------------

## ❌ Mistake 2 --- Duplicate node IDs

Wrong:

``` text
Broker 1 = 1
Broker 2 = 1
```

Correct:

``` text
Controller 1 = 1
Controller 2 = 2
Broker 1 = 101
Broker 2 = 102
```

------------------------------------------------------------------------

## ❌ Mistake 3 --- Public controller address

Do not use:

``` text
PUBLIC-IP:9093
```

Use:

``` text
172.31.39.10:9093
172.31.39.223:9093
```

------------------------------------------------------------------------

## ❌ Mistake 4 --- Advertise private IP to Offset Explorer

Wrong:

``` properties
advertised.listeners=PLAINTEXT://172.31.45.5:9092
```

for an external laptop client.

Correct:

``` properties
advertised.listeners=INTERNAL://172.31.45.5:9092,EXTERNAL://<BROKER1_PUBLIC_IP>:29092
```

------------------------------------------------------------------------

## ❌ Mistake 5 --- Use public IP for internal broker traffic

Avoid:

``` properties
INTERNAL://<PUBLIC_IP>:9092
```

Use:

``` properties
INTERNAL://172.31.x.x:9092
```

------------------------------------------------------------------------

## ❌ Mistake 6 --- Expose 9092 publicly

Do not open:

``` text
9092 → 0.0.0.0/0
```

Use the internal Security Group.

------------------------------------------------------------------------

## ❌ Mistake 7 --- Expose 9093 publicly

Never expose the KRaft controller port to the Internet for this lab.

------------------------------------------------------------------------

## ❌ Mistake 8 --- Create a new cluster ID on every machine

Only generate one cluster ID.

------------------------------------------------------------------------

## ❌ Mistake 9 --- Format existing production data

Never blindly execute:

``` bash
rm -rf /home/ec2-user/kafka-data/*
```

on a cluster containing data you care about.

------------------------------------------------------------------------

## ❌ Mistake 10 --- Start brokers before controllers

For this lab, use:

``` text
Controller 1
     ↓
Controller 2
     ↓
Broker 1
     ↓
Broker 2
```

------------------------------------------------------------------------

# 🩺 Troubleshooting

## Problem: Broker cannot connect to controller

Check:

``` bash
nc -zv 172.31.39.10 9093
nc -zv 172.31.39.223 9093
```

Then inspect Security Group rules.

------------------------------------------------------------------------

## Problem: Broker-to-broker communication fails

Check:

``` bash
nc -zv 172.31.39.40 9092
```

from Broker 1.

And:

``` bash
nc -zv 172.31.45.5 9092
```

from Broker 2.

------------------------------------------------------------------------

## Problem: Offset Explorer cannot connect

Check:

``` powershell
Test-NetConnection <BROKER1_PUBLIC_IP> -Port 29092
```

Then verify:

``` bash
sudo ss -lntp | grep 29092
```

Then verify:

``` properties
advertised.listeners=INTERNAL://PRIVATE-IP:9092,EXTERNAL://PUBLIC-IP:29092
```

------------------------------------------------------------------------

## Problem: Offset Explorer connects but then fails

This is usually an `advertised.listeners` problem.

The bootstrap server may be reachable while the broker metadata
advertises an unreachable private IP.

Check:

``` properties
advertised.listeners
```

------------------------------------------------------------------------

## Problem: Cluster ID mismatch

Check:

``` bash
cat /home/ec2-user/kafka-data/meta.properties
```

All nodes must have the same:

``` text
cluster.id
```

------------------------------------------------------------------------

## Problem: Node ID mismatch

Check:

``` bash
cat /home/ec2-user/kafka-data/meta.properties
```

Compare with:

``` properties
node.id=
```

------------------------------------------------------------------------

## Problem: Kafka refuses to format storage

First inspect:

``` bash
cat /home/ec2-user/kafka-data/meta.properties
```

If the directory is already formatted, do not force formatting unless
you intentionally want to rebuild that node.

------------------------------------------------------------------------

# 📊 Verification Checklist

## Infrastructure

-   [ ] Four EC2 instances available.
-   [ ] Amazon Linux 2023 installed.
-   [ ] Java 17+ installed.
-   [ ] Kafka 4.0.2 installed.
-   [ ] `/home/ec2-user/kafka` exists.
-   [ ] `/home/ec2-user/kafka-data` exists.

## Controllers

-   [ ] Controller 1 private IP = `172.31.39.10`
-   [ ] Controller 2 private IP = `172.31.39.223`
-   [ ] Controller 1 node ID = `1`
-   [ ] Controller 2 node ID = `2`
-   [ ] Controller port = `9093`
-   [ ] Controller quorum configured.
-   [ ] Controllers not publicly exposed.

## Brokers

-   [ ] Broker 1 private IP = `172.31.45.5`
-   [ ] Broker 2 private IP = `172.31.39.40`
-   [ ] Broker 1 node ID = `101`
-   [ ] Broker 2 node ID = `102`
-   [ ] Internal listener = `9092`
-   [ ] External listener = `29092`.
-   [ ] Public IPs configured only in `advertised.listeners`.

## KRaft

-   [ ] One cluster ID generated.
-   [ ] Same cluster ID used on all nodes.
-   [ ] Unique node IDs.
-   [ ] `controller.quorum.voters` identical everywhere.
-   [ ] Storage formatted.
-   [ ] Controllers started.
-   [ ] Brokers started.

## Kafka

-   [ ] KRaft quorum healthy.
-   [ ] Both brokers visible.
-   [ ] Topic created.
-   [ ] Replication factor = 2.
-   [ ] ISR contains both brokers.
-   [ ] Producer works.
-   [ ] Consumer works.
-   [ ] Offset Explorer connects.

------------------------------------------------------------------------

# 🎓 What This Lab Teaches

After completing this lab, you should understand:

``` text
                    KAFKA
                      │
        ┌─────────────┼─────────────┐
        │             │             │
      KRaft         Brokers       Clients
        │             │             │
   Controllers    Partitions    Offset Explorer
        │             │             │
     Quorum       Replication   Bootstrap
        │             │             │
   Cluster ID     ISR/Leader    Advertised
                                  Listeners
```

You will also understand the difference between:

``` text
node.id
cluster.id
controller.quorum.voters
listeners
advertised.listeners
inter.broker.listener.name
controller.listener.names
```

------------------------------------------------------------------------

# 🧠 Quick Memory Diagram

``` text
NODE ID
   ↓
Identifies ONE Kafka server

CLUSTER ID
   ↓
Identifies THE Kafka cluster

CONTROLLER QUORUM
   ↓
Manages Kafka metadata

9093
   ↓
KRaft controller communication

9092
   ↓
Internal broker communication

29092
   ↓
External client communication

PRIVATE IP
   ↓
AWS internal traffic

PUBLIC IP
   ↓
External laptop/client traffic

ADVERTISED LISTENERS
   ↓
"What address should Kafka give to clients?"
```

------------------------------------------------------------------------

# 📖 Official Documentation

-   [Apache Kafka 4.0.2
    Downloads](https://kafka.apache.org/community/downloads/)
-   [Kafka 4.0 Quick
    Start](https://kafka.apache.org/40/getting-started/quickstart/)
-   [Kafka 4.0 KRaft
    Documentation](https://kafka.apache.org/40/operations/kraft/)
-   [Kafka 4.0 Listener
    Configuration](https://kafka.apache.org/40/security/listener-configuration/)
-   [Kafka 4.0 Broker
    Configuration](https://kafka.apache.org/40/configuration/broker-configs/)
-   [Apache Kafka
    Documentation](https://kafka.apache.org/documentation/)

------------------------------------------------------------------------

# 🏁 Final Architecture Summary

``` text
                         🌐 INTERNET
                              │
                    ┌─────────┴─────────┐
                    │                   │
              PUBLIC:29092        PUBLIC:29092
                    │                   │
                    ▼                   ▼
             ┌────────────┐      ┌────────────┐
             │  BROKER 1  │      │  BROKER 2  │
             │   ID 101   │      │   ID 102   │
             │            │      │            │
             │ PRIVATE    │      │ PRIVATE    │
             │172.31.45.5 │◄────►│172.31.39.40│
             │   :9092    │      │   :9092    │
             └─────┬──────┘      └──────┬─────┘
                   │                    │
                   └────────┬───────────┘
                            │
                         KRaft
                            │
                ┌───────────┴───────────┐
                │                       │
         ┌──────▼───────┐       ┌──────▼───────┐
         │ CONTROLLER 1 │       │ CONTROLLER 2 │
         │    ID = 1    │       │    ID = 2    │
         │172.31.39.10  │◄─────►│172.31.39.223 │
         │    :9093     │       │    :9093     │
         └──────────────┘       └──────────────┘

                  SAME CLUSTER ID
                        │
                        ▼
                 Kafka 4.0.2
                   KRaft Mode
```

------------------------------------------------------------------------

## ⭐ Lab Golden Rules

> **1. One cluster → one Cluster ID**

> **2. One server → one unique Node ID**

> **3. Controllers use private IPs**

> **4. Brokers use private IPs internally**

> **5. Offset Explorer uses broker public IPs**

> **6. Internal broker traffic → 9092**

> **7. KRaft controller traffic → 9093**

> **8. External client traffic → 29092**

> **9. `advertised.listeners` must be reachable by the client**

> **10. Never expose controller port 9093 publicly**

> **11. Never advertise AWS private IPs to your laptop**

> **12. Never generate separate cluster IDs for individual nodes**

------------------------------------------------------------------------

```{=html}
<p align="center">
```
`<b>`{=html}🚀 Happy Kafka Learning!`</b>`{=html}`<br/>`{=html}
`<sub>`{=html}Kafka 4.0.2 • KRaft • AWS EC2 • 2 Controllers • 2 Brokers
• Offset Explorer`</sub>`{=html}
```{=html}
</p>
```
