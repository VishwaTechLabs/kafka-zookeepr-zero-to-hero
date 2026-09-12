
# 🚀 VishwaTech Labs — 3-Broker Kafka 4.0.2 KRaft Cluster on AWS EC2
### Option-B: 3 Kafka Nodes + 1 Dedicated Management EC2

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.0.2-black?logo=apachekafka)
![KRaft](https://img.shields.io/badge/Mode-KRaft-blue)
![AWS](https://img.shields.io/badge/AWS-EC2-FF9900?logo=amazonaws)
![Amazon Linux](https://img.shields.io/badge/OS-Amazon%20Linux%202023-orange)
![Java](https://img.shields.io/badge/Java-17-red?logo=openjdk)
![Brokers](https://img.shields.io/badge/Brokers-3-success)
![Controllers](https://img.shields.io/badge/KRaft%20Controllers-3-success)
![HA](https://img.shields.io/badge/HA-Lab%20Ready-brightgreen)
![VishwaTech](https://img.shields.io/badge/VishwaTech-Labs-purple)

> **Complete hands-on lab:** build a real 3-broker Apache Kafka 4.0.2 KRaft cluster on AWS EC2, with 3 combined broker/controller nodes and a separate management EC2 for Kafbat UI, Kpow and Conduktor Console.

---

# 🎯 1. What We Are Building

You already have a single-node Kafka KRaft lab. This lab moves to a real **3-node cluster**.

## Option-B architecture

```text
                                  INTERNET
                                     │
                         ┌───────────┴───────────┐
                         │                       │
                     Public IPs              Public IP
                         │                       │
              ┌──────────┴──────────┐            │
              │                     │            │
              ▼                     ▼            ▼
        Kafka-1 :9092        Kafka-2 :9092   Kafka-3 :9092
        External client      External client External client
              │                     │            │
              └─────────────┬───────┴────────────┘
                            │
                    PRIVATE VPC NETWORK
                            │
       ┌────────────────────┼────────────────────┐
       │                    │                    │
       ▼                    ▼                    ▼
 ┌────────────┐       ┌────────────┐       ┌────────────┐
 │  Kafka-1   │◄─────►│  Kafka-2   │◄─────►│  Kafka-3   │
 │ node.id=1  │       │ node.id=2  │       │ node.id=3  │
 │ Broker     │       │ Broker     │       │ Broker     │
 │ Controller │       │ Controller │       │ Controller │
 │ :19092     │       │ :19092     │       │ :19092     │
 │ :9093      │       │ :9093      │       │ :9093      │
 └────────────┘       └────────────┘       └────────────┘
       ▲                    ▲                    ▲
       │                    │                    │
       └──────────── KRaft metadata quorum ─────┘

                     PRIVATE NETWORK
                            │
                            ▼
                 ┌────────────────────┐
                 │ Management EC2     │
                 │                    │
                 │ Kafbat UI :8080    │
                 │ Kpow :3000         │
                 │ Conduktor :8081    │
                 └────────────────────┘
                            │
                     PUBLIC IP :8080
                     PUBLIC IP :3000
                     PUBLIC IP :8081
```

### The key design

Each Kafka EC2 is a **combined server**:

```text
process.roles=broker,controller
```

Kafka documents combined broker/controller mode as suitable for smaller/development environments, while dedicated controllers are preferable for critical production deployments. Three controllers provide a quorum that can tolerate one controller failure. citeturn0search1

For this training lab, combined mode is ideal because it lets you learn broker HA and KRaft quorum concepts with only 3 Kafka EC2 instances.

---

# 🧠 2. Why 3 Kafka EC2 Instances?

A single broker cannot demonstrate real replication.

With 3 brokers:

```text
Topic: orders
Partitions: 3
Replication Factor: 3

Partition 0 → Broker 1 + Broker 2 + Broker 3
Partition 1 → Broker 2 + Broker 3 + Broker 1
Partition 2 → Broker 3 + Broker 1 + Broker 2
```

You can now test:

- broker failure
- leader election
- follower replicas
- ISR
- under-replicated partitions
- consumer rebalancing
- replication
- KRaft controller quorum
- failover
- rolling restart
- partition reassignment
- preferred leader election
- replication recovery

---

# 🏆 3. Why Option-B?

We intentionally separate Kafka from the management layer.

```text
Kafka EC2-1 ─┐
Kafka EC2-2 ─┼── Kafka Cluster
Kafka EC2-3 ─┘

Management EC2
├── Kafbat UI
├── Kpow
└── Conduktor
```

### Benefits

| Area | Benefit |
|---|---|
| Kafka | Dedicated compute |
| GUI tools | Do not consume broker resources |
| Security | Separate management access |
| Scaling | Add more management tools without changing Kafka |
| Troubleshooting | Easier isolation |
| Training | Closest to a realistic platform architecture |

---

# 🖥️ 4. AWS EC2 Count

## Required

### Kafka

```text
3 × EC2
```

### Management

```text
1 × EC2
```

### Total

```text
4 EC2 instances
```

---

# 💻 5. Recommended EC2 Specifications

## Kafka nodes

For learning:

```text
Instance:       t3.medium
vCPU:           2
RAM:            4 GiB
Disk:           30–50 GiB gp3
OS:             Amazon Linux 2023
```

For heavier labs:

```text
t3.large
2 vCPU
8 GiB RAM
50+ GiB gp3
```

## Management EC2

Recommended:

```text
t3.medium
2 vCPU
4 GiB RAM
30 GiB gp3
Amazon Linux 2023
```

If running Kafbat + Kpow + Conduktor simultaneously, **8 GiB is more comfortable**.

---

# 🌐 6. AWS Networking — VERY IMPORTANT

Do not treat the public IP as the Kafka cluster's internal network.

AWS EC2 has:

```text
Private IP
    +
Public IPv4 / Elastic IP
```

Kafka should use the **private IPs for broker-to-broker and controller communication**.

External Kafka clients can use the **public IP listener**.

Kafka's `advertised.listeners` exists specifically for environments where the address clients should use differs from the address Kafka binds to, which is common in IaaS/cloud environments. citeturn0search9

---

# 🔥 7. Listener Design

This is the most important configuration in this lab.

We use **three listener concepts**:

```text
INTERNAL
    ↓
19092
    ↓
Private VPC communication

EXTERNAL
    ↓
9092
    ↓
Public client access

CONTROLLER
    ↓
9093
    ↓
KRaft metadata quorum
```

### Listener architecture

```text
Kafka-1

INTERNAL://10.0.1.11:19092
EXTERNAL://PUBLIC_IP_1:9092
CONTROLLER://10.0.1.11:9093
```

Kafka-2:

```text
INTERNAL://10.0.1.12:19092
EXTERNAL://PUBLIC_IP_2:9092
CONTROLLER://10.0.1.12:9093
```

Kafka-3:

```text
INTERNAL://10.0.1.13:19092
EXTERNAL://PUBLIC_IP_3:9092
CONTROLLER://10.0.1.13:9093
```

### Why?

Broker-to-broker traffic should not unnecessarily traverse the public Internet.

Therefore:

```text
Kafka-1 ──private──► Kafka-2
Kafka-2 ──private──► Kafka-3
Kafka-3 ──private──► Kafka-1
```

External client:

```text
Laptop
   │
Internet
   │
Public IP
   │
:9092
   ▼
Kafka broker
```

---

# 🚨 8. Public Kafka Warning

You specifically asked to expose Kafka publicly.

It can be done for this lab, but it is **not a production security design**.

A public Kafka listener means:

```text
Internet
   ↓
Kafka :9092
```

Without TLS/SASL, anyone who can reach the port may potentially interact with the broker according to Kafka ACL/configuration.

### For the training lab

We use:

```text
EXTERNAL://PUBLIC_IP:9092
```

but restrict the AWS Security Group to:

```text
YOUR_PUBLIC_IP/32
```

whenever possible.

AWS recommends least-permissive Security Group rules and warns that unrestricted `0.0.0.0/0` access increases attack surface. citeturn0search0turn0search7

---

# 🔐 9. AWS Security Group Design

Create:

```text
SG-KAFKA-CLUSTER
```

and:

```text
SG-KAFKA-MANAGEMENT
```

## Kafka SG inbound

| Port | Protocol | Source | Purpose |
|---:|---|---|---|
| 22 | TCP | Your IP /32 | SSH |
| 9092 | TCP | Your IP /32 | Public Kafka clients |
| 19092 | TCP | SG-KAFKA-CLUSTER | Internal broker traffic |
| 9093 | TCP | SG-KAFKA-CLUSTER | KRaft controller quorum |

Do **NOT** expose:

```text
19092 → 0.0.0.0/0
9093  → 0.0.0.0/0
```

## Management SG inbound

| Port | Protocol | Source |
|---:|---|---|
| 22 | TCP | Your IP /32 |
| 8080 | TCP | Your IP /32 |
| 3000 | TCP | Your IP /32 |
| 8081 | TCP | Your IP /32 |

Management EC2 connects to Kafka using:

```text
10.0.1.11:19092
10.0.1.12:19092
10.0.1.13:19092
```

---

# 📌 10. Use Elastic IPs

For a public Kafka lab, use **Elastic IPs** rather than relying on changing ephemeral public IPs.

Example:

```text
Kafka-1
Private: 10.0.1.11
Public/EIP: 18.x.x.11

Kafka-2
Private: 10.0.1.12
Public/EIP: 18.x.x.12

Kafka-3
Private: 10.0.1.13
Public/EIP: 18.x.x.13

Management
Private: 10.0.1.21
Public/EIP: 18.x.x.21
```

### Why?

Kafka publishes its advertised addresses to clients.

If the public IP changes:

```text
Client
  ↓
old public IP
  ↓
❌
```

Elastic IPs prevent this problem.

---

# 🧩 11. Common vs Per-Node Configuration

This is extremely important.

## Common on ALL 3 Kafka nodes

```properties
process.roles=broker,controller

controller.listener.names=CONTROLLER

inter.broker.listener.name=INTERNAL

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

log.dirs=/var/lib/kafka/data

controller.quorum.voters=1@PRIVATE_IP_1:9093,2@PRIVATE_IP_2:9093,3@PRIVATE_IP_3:9093

num.network.threads=3
num.io.threads=8

default.replication.factor=3
min.insync.replicas=2

offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

auto.create.topics.enable=false
```

## Different on each node

```properties
node.id
listeners
advertised.listeners
```

### Example

Kafka-1:

```properties
node.id=1

listeners=INTERNAL://10.0.1.11:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://10.0.1.11:9093

advertised.listeners=INTERNAL://10.0.1.11:19092,EXTERNAL://18.x.x.11:9092
```

Kafka-2:

```properties
node.id=2

listeners=INTERNAL://10.0.1.12:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://10.0.1.12:9093

advertised.listeners=INTERNAL://10.0.1.12:19092,EXTERNAL://18.x.x.12:9092
```

Kafka-3:

```properties
node.id=3

listeners=INTERNAL://10.0.1.13:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://10.0.1.13:9093

advertised.listeners=INTERNAL://10.0.1.13:19092,EXTERNAL://18.x.x.13:9092
```

### Golden rule

```text
COMMON
   ↓
same on all nodes

NODE-SPECIFIC
   ↓
node.id
private IP
public IP
advertised.listeners
```

Kafka requires unique `node.id` values across the cluster, and KRaft controller voter IDs must correspond to those controller nodes. citeturn0search1turn0search9

---

# 🧠 12. KRaft Controller Quorum

Our static quorum is:

```text
controller.quorum.voters=
1@10.0.1.11:9093,
2@10.0.1.12:9093,
3@10.0.1.13:9093
```

This means:

```text
Controller 1
Controller 2
Controller 3
```

form a metadata quorum.

With 3 controllers:

```text
3 controllers
↓
majority = 2
↓
1 controller can fail
↓
quorum remains available
```

Kafka documentation recommends 3 or 5 controllers depending on availability requirements. citeturn0search1

---

# 🧱 13. Why Controller Port 9093 Is NOT Public

KRaft controller traffic is cluster-internal.

```text
Kafka-1 :9093
      ↕
Kafka-2 :9093
      ↕
Kafka-3 :9093
```

Never:

```text
Internet
   ↓
9093
```

Only the Kafka Security Group should access it.

Kafka's listener model separates the controller listener from the inter-broker listener. citeturn0search10

---

# ☕ 14. Java

Use Java 17.

Check:

```bash
java -version
```

Expected:

```text
openjdk version "17..."
```

---

# 📦 15. Kafka Version

This lab uses:

```text
Apache Kafka 4.0.2
```

Download:

```text
https://archive.apache.org/dist/kafka/4.0.2/
```

The official Kafka KRaft documentation covers the `process.roles`, controller quorum and listener model used here. citeturn0search1

---

# 👤 16. User Model

Everything runs under the same EC2 user:

```text
ec2-user
```

We do NOT create:

```text
kafka
zookeeper
conduktor
```

Linux users for this training lab.

Kafka files:

```text
/home/ec2-user/kafka
```

Kafka data:

```text
/var/lib/kafka/data
```

Systemd runs the service as:

```text
User=ec2-user
Group=ec2-user
```

---

# 🚀 17. Installation Prerequisites — ALL THREE NODES

SSH into each node.

Example:

```bash
ssh -i YOUR_KEY.pem ec2-user@PUBLIC_IP
```

Update:

```bash
sudo dnf update -y
```

Install utilities:

```bash
sudo dnf install -y java-17-amazon-corretto wget tar gzip curl nc
```

Verify:

```bash
java -version
```

---

# 🏷️ 18. Hostnames

Kafka-1:

```bash
sudo hostnamectl set-hostname kafka-1
```

Kafka-2:

```bash
sudo hostnamectl set-hostname kafka-2
```

Kafka-3:

```bash
sudo hostnamectl set-hostname kafka-3
```

---

# 🧾 19. `/etc/hosts`

Use private IPs.

On **ALL THREE** nodes:

```bash
sudo tee -a /etc/hosts > /dev/null <<'EOF'
10.0.1.11 kafka-1
10.0.1.12 kafka-2
10.0.1.13 kafka-3
EOF
```

Replace the example private IPs with your actual VPC addresses.

Validate:

```bash
getent hosts kafka-1
getent hosts kafka-2
getent hosts kafka-3
```

---

# 📥 20. Install Kafka

On each node:

```bash
cd /home/ec2-user

wget https://archive.apache.org/dist/kafka/4.0.2/kafka_2.13-4.0.2.tgz

tar -xzf kafka_2.13-4.0.2.tgz

ln -sfn kafka_2.13-4.0.2 kafka

mkdir -p /var/lib/kafka/data
sudo chown -R ec2-user:ec2-user /var/lib/kafka
```

Verify:

```bash
/home/ec2-user/kafka/bin/kafka-storage.sh --help
```

---

# 🔥 21. The Cluster ID

All three nodes MUST use the same Kafka cluster ID.

Generate it **once**:

```bash
/home/ec2-user/kafka/bin/kafka-storage.sh random-uuid
```

Example:

```text
4xVQJf7hQkW2uWnJQW1z3A
```

Save this value.

Example:

```text
KAFKA_CLUSTER_ID=4xVQJf7hQkW2uWnJQW1z3A
```

Do NOT generate a different cluster ID on each broker.

---

# 🧠 22. Why Same Cluster ID?

The cluster ID identifies the Kafka cluster.

```text
Kafka-1 ─┐
Kafka-2 ─┼── SAME CLUSTER ID
Kafka-3 ─┘
```

If each node has a different ID:

```text
Kafka-1 → Cluster A
Kafka-2 → Cluster B
Kafka-3 → Cluster C
```

That is not one cluster.

---

# 🛠️ 23. Configuration Template

Create:

```bash
sudo mkdir -p /etc/kafka
sudo chown ec2-user:ec2-user /etc/kafka
```

The supplied script:

```text
install-kafka-3node-kraft.sh
```

generates the correct node-specific configuration.

---

# 🧰 24. Shell Script

Download:

**`install-kafka-3node-kraft.sh`**

The script:

- installs Java if needed
- downloads Kafka 4.0.2
- creates Kafka directories
- creates KRaft configuration
- configures three listeners
- configures static KRaft voters
- configures replication factor
- creates systemd service
- formats storage
- enables Kafka
- starts Kafka
- validates the broker

Run:

```bash
chmod +x install-kafka-3node-kraft.sh
```

---

# ⚙️ 25. Script Variables

Before running, export:

```bash
export NODE_ID=1

export NODE_PRIVATE_IP=10.0.1.11
export NODE_PUBLIC_IP=18.x.x.11

export KAFKA_CLUSTER_ID="YOUR_CLUSTER_ID"

export CONTROLLER_VOTERS="1@10.0.1.11:9093,2@10.0.1.12:9093,3@10.0.1.13:9093"
```

For Kafka-2:

```bash
export NODE_ID=2
export NODE_PRIVATE_IP=10.0.1.12
export NODE_PUBLIC_IP=18.x.x.12
export KAFKA_CLUSTER_ID="YOUR_CLUSTER_ID"
export CONTROLLER_VOTERS="1@10.0.1.11:9093,2@10.0.1.12:9093,3@10.0.1.13:9093"
```

For Kafka-3:

```bash
export NODE_ID=3
export NODE_PRIVATE_IP=10.0.1.13
export NODE_PUBLIC_IP=18.x.x.13
export KAFKA_CLUSTER_ID="YOUR_CLUSTER_ID"
export CONTROLLER_VOTERS="1@10.0.1.11:9093,2@10.0.1.12:9093,3@10.0.1.13:9093"
```

Run:

```bash
sudo -E ./install-kafka-3node-kraft.sh
```

---

# 🧪 26. Validate Each Broker

On each node:

```bash
sudo systemctl status kafka-kraft
```

Check listeners:

```bash
sudo ss -lntp | grep -E '9092|9093|19092'
```

Expected:

```text
9092
9093
19092
```

---

# 🔎 27. Test Internal Connectivity

From Kafka-1:

```bash
nc -vz 10.0.1.12 19092
nc -vz 10.0.1.13 19092
```

Controller:

```bash
nc -vz 10.0.1.12 9093
nc -vz 10.0.1.13 9093
```

From Kafka-2:

```bash
nc -vz 10.0.1.11 19092
nc -vz 10.0.1.13 19092
```

From Kafka-3:

```bash
nc -vz 10.0.1.11 19092
nc -vz 10.0.1.12 19092
```

---

# 🧠 28. Verify KRaft Quorum

Run from any broker:

```bash
/home/ec2-user/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.0.1.11:19092 \
  describe --status
```

Look for:

```text
LeaderId
HighWatermark
MaxFollowerLag
CurrentVoters
CurrentObservers
```

Expected voter count:

```text
3
```

---

# 📊 29. Verify Brokers

```bash
/home/ec2-user/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server 10.0.1.11:19092
```

You should see:

```text
kafka-1
kafka-2
kafka-3
```

---

# 🏗️ 30. Create a Replicated Topic

Create:

```bash
/home/ec2-user/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.0.1.11:19092 \
  --create \
  --topic vishwatech-orders \
  --partitions 6 \
  --replication-factor 3
```

Verify:

```bash
/home/ec2-user/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.0.1.11:19092 \
  --describe \
  --topic vishwatech-orders
```

Expected:

```text
ReplicationFactor: 3
```

---

# 🧠 31. Leader and Replicas

Example:

```text
Partition 0
Leader: 1
Replicas: 1,2,3
ISR: 1,2,3
```

Meaning:

```text
Leader
   ↓
Broker 1

Followers
   ↓
Broker 2
Broker 3

ISR
   ↓
1,2,3
```

---

# 📦 32. Replication Factor

Replication Factor = number of copies of a partition.

```text
RF=1
1 copy

RF=2
2 copies

RF=3
3 copies
```

Our cluster:

```text
RF=3
```

---

# 🛡️ 33. `min.insync.replicas=2`

We configure:

```properties
min.insync.replicas=2
```

This works with:

```text
RF=3
```

Meaning:

```text
3 replicas
    │
    ├── Broker 1
    ├── Broker 2
    └── Broker 3

Minimum ISR required for writes = 2
```

So the lab demonstrates a practical availability/durability trade-off.

---

# 🧪 34. Produce Messages

```bash
/home/ec2-user/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 10.0.1.11:19092 \
  --topic vishwatech-orders
```

Enter:

```text
order-1001
order-1002
order-1003
order-1004
```

---

# 🧪 35. Consume Messages

```bash
/home/ec2-user/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.0.1.11:19092 \
  --topic vishwatech-orders \
  --from-beginning
```

---

# 💥 36. MOST IMPORTANT LAB — Broker Failure

Before failure:

```text
Broker 1  UP
Broker 2  UP
Broker 3  UP

ISR = 1,2,3
```

Stop Kafka-1:

```bash
sudo systemctl stop kafka-kraft
```

Now inspect:

```bash
/home/ec2-user/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.0.1.12:19092 \
  --describe \
  --topic vishwatech-orders
```

You should observe leader changes.

Possible:

```text
Before:
Leader = 1
ISR = 1,2,3

After:
Leader = 2
ISR = 2,3
```

This is the heart of Kafka HA.

---

# 🔄 37. Restart Broker-1

```bash
sudo systemctl start kafka-kraft
```

Wait:

```bash
sleep 20
```

Check:

```bash
/home/ec2-user/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.0.1.11:19092 \
  --describe \
  --topic vishwatech-orders
```

Eventually:

```text
ISR = 1,2,3
```

---

# 🧪 38. Under-Replicated Partition Lab

Stop one broker:

```bash
sudo systemctl stop kafka-kraft
```

Inspect:

```bash
kafka-topics.sh --describe ...
```

Study:

```text
Replicas
ISR
Leader
```

If:

```text
Replicas = 1,2,3
ISR      = 1,2
```

then:

```text
Broker 3
   ↓
Not currently in ISR
```

This is an **under-replication condition**.

---

# 🧪 39. KRaft Controller Failure

Because all three are combined servers:

```text
Kafka-1 = broker + controller
Kafka-2 = broker + controller
Kafka-3 = broker + controller
```

Stop Kafka-1.

You still have:

```text
Controller 2
Controller 3
```

Majority:

```text
2 / 3
```

Therefore the controller quorum remains available.

---

# 🧪 40. Consumer Group Lab

Create:

```bash
/home/ec2-user/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.0.1.11:19092 \
  --topic vishwatech-orders \
  --group vishwatech-orders-group
```

Inspect:

```bash
/home/ec2-user/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.0.1.11:19092 \
  --describe \
  --group vishwatech-orders-group
```

Study:

```text
CURRENT-OFFSET
LOG-END-OFFSET
LAG
CONSUMER-ID
HOST
CLIENT-ID
```

---

# 🧪 41. Management EC2

Create the fourth EC2:

```text
Name:
vishwatech-kafka-management
```

Install Docker:

```bash
sudo dnf install -y docker
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user
```

Log out and back in.

Verify:

```bash
docker --version
docker compose version
```

---

# 🖥️ 42. Kafbat Configuration

Kafbat should connect to:

```text
10.0.1.11:19092
10.0.1.12:19092
10.0.1.13:19092
```

NOT:

```text
PUBLIC_IP:9092
```

Why?

Because the management EC2 is inside the AWS VPC.

Private communication is:

```text
Management EC2
       │
       ├── 10.0.1.11:19092
       ├── 10.0.1.12:19092
       └── 10.0.1.13:19092
```

---

# 🖥️ 43. Kpow Configuration

Use:

```text
10.0.1.11:19092
```

as bootstrap server.

Kpow can discover the rest of the cluster from Kafka metadata.

---

# 🖥️ 44. Conduktor Configuration

Use:

```text
10.0.1.11:19092
```

or multiple bootstrap servers:

```text
10.0.1.11:19092,
10.0.1.12:19092,
10.0.1.13:19092
```

Recommended:

```text
VishwaTech-Kafka-3Node-KRaft
```

---

# 🌍 45. External Kafka Client

From your laptop:

```text
PUBLIC_IP_1:9092
PUBLIC_IP_2:9092
PUBLIC_IP_3:9092
```

Example:

```bash
kafka-topics.sh \
  --bootstrap-server 18.x.x.11:9092,18.x.x.12:9092,18.x.x.13:9092 \
  --list
```

Kafka will return metadata containing the advertised external addresses.

That is why:

```text
advertised.listeners
```

must be correct.

---

# ⚠️ 46. The Most Common AWS Kafka Mistake

DO NOT configure:

```properties
advertised.listeners=EXTERNAL://0.0.0.0:9092
```

Kafka documentation explicitly notes that `advertised.listeners` must advertise usable client addresses; `0.0.0.0` is not a valid advertised address. citeturn0search3

Correct:

```properties
advertised.listeners=EXTERNAL://18.x.x.11:9092
```

---

# 🔐 47. Production Security Upgrade

This lab deliberately uses:

```text
PLAINTEXT
```

for simplicity.

A production architecture should use:

```text
TLS
+
SASL
+
ACL
+
Authentication
+
Authorization
```

Example future architecture:

```text
Client
  │
  ▼
TLS
  │
SASL
  │
ACL
  │
Kafka
```

---

# 🧪 48. Public Kafka Security Lab — Later

After the basic cluster works, create:

```text
Lab 1  → PLAINTEXT
Lab 2  → TLS
Lab 3  → SASL/PLAIN
Lab 4  → SASL/SCRAM
Lab 5  → ACL
Lab 6  → least privilege
Lab 7  → client certificates
Lab 8  → audit
```

Do NOT jump directly to TLS/SASL before proving the basic networking and replication architecture.

---

# 📈 49. Kafka Cluster Monitoring

Important metrics:

```text
Broker availability
Under-replicated partitions
Offline partitions
ISR shrink/expand
Leader count
Request latency
Produce rate
Fetch rate
Consumer lag
Disk usage
Network throughput
CPU
Heap
Controller health
KRaft metadata quorum
```

---

# 🚨 50. Critical Kafka KPIs

For your CISO / platform engineering knowledge:

| KPI | Meaning |
|---|---|
| Broker Availability | Are brokers healthy? |
| URP | Under-replicated partitions |
| Offline Partitions | Partitions unavailable |
| ISR Count | Replicas currently caught up |
| Consumer Lag | Consumer processing delay |
| Disk Usage | Kafka storage pressure |
| Network Throughput | Broker traffic |
| Request Latency | Kafka response performance |
| Controller Health | KRaft metadata availability |

---

# 🧠 51. Common Configuration Cheat Sheet

## Same on all brokers

```properties
process.roles=broker,controller
controller.listener.names=CONTROLLER
inter.broker.listener.name=INTERNAL

controller.quorum.voters=1@10.0.1.11:9093,2@10.0.1.12:9093,3@10.0.1.13:9093

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

default.replication.factor=3
min.insync.replicas=2

offsets.topic.replication.factor=3

transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

auto.create.topics.enable=false
```

## Different

```text
node.id
NODE_PRIVATE_IP
NODE_PUBLIC_IP
advertised.listeners
```

---

# 🧠 52. What Is Actually Stored on Each Broker?

Kafka topic:

```text
vishwatech-orders
```

with:

```text
6 partitions
RF=3
```

means every partition has three copies distributed across brokers.

Example:

```text
Broker-1
├── P0
├── P1
├── P2
└── ...

Broker-2
├── P0
├── P1
├── P2
└── ...

Broker-3
├── P0
├── P1
├── P2
└── ...
```

The exact leader/replica placement is controlled by Kafka.

---

# 🔥 53. What Happens When Broker-1 Dies?

```text
BEFORE

Broker-1       Broker-2       Broker-3
   │              │              │
 Leader          Follower       Follower
   │              │              │
   └──────────── Replicas ───────┘


BROKER-1 FAILURE

        X
       / \
      /   \
 Broker-2  Broker-3
    │          │
    └── new leader election ──┘
```

Kafka elects another eligible replica.

That is why replication factor matters.

---

# 🧪 54. Lab Roadmap

## Level 1 — Infrastructure

```text
01 EC2 creation
02 Security Groups
03 Private IP
04 Elastic IP
05 Java
06 Kafka installation
```

## Level 2 — KRaft

```text
07 Cluster ID
08 node.id
09 controller quorum
10 listeners
11 advertised.listeners
12 systemd
```

## Level 3 — Kafka

```text
13 Topic
14 Partition
15 Replication
16 Producer
17 Consumer
18 Consumer Group
19 Offset
20 Lag
```

## Level 4 — HA

```text
21 Broker failure
22 Leader election
23 ISR
24 Under-replication
25 Recovery
26 Controller failure
27 Rolling restart
```

## Level 5 — Management

```text
28 Kafbat
29 Kpow
30 Conduktor
31 Schema Registry
32 Kafka Connect
```

## Level 6 — Security

```text
33 TLS
34 SASL
35 SCRAM
36 ACL
37 RBAC
38 Audit
```

## Level 7 — Production

```text
39 Monitoring
40 Prometheus
41 Grafana
42 Alerting
43 Capacity planning
44 Backup/recovery
45 Disaster recovery
46 Multi-AZ
```

---

# 🧹 55. Stop Kafka

```bash
sudo systemctl stop kafka-kraft
```

---

# ▶️ 56. Start Kafka

```bash
sudo systemctl start kafka-kraft
```

---

# 🔄 57. Restart Kafka

```bash
sudo systemctl restart kafka-kraft
```

---

# 📜 58. Logs

```bash
sudo journalctl -u kafka-kraft -f
```

Last 200 lines:

```bash
sudo journalctl -u kafka-kraft -n 200 --no-pager
```

---

# 🔍 59. Service Status

```bash
sudo systemctl status kafka-kraft
```

---

# 🧨 60. Clean Lab Reset

⚠️ This destroys the broker's Kafka data.

Stop:

```bash
sudo systemctl stop kafka-kraft
```

Remove:

```bash
sudo rm -rf /var/lib/kafka/data/*
```

Then format again with the SAME cluster ID:

```bash
/home/ec2-user/kafka/bin/kafka-storage.sh format \
  --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c /home/ec2-user/kafka/config/kraft/server.properties
```

For a real 3-node cluster, follow the script's quorum formatting flow and do not mix standalone formatting with a multi-controller production design.

---

# 🚨 61. Important — Do Not Run Random Commands

KRaft storage is sensitive.

Never randomly execute:

```bash
kafka-storage.sh format
```

on an existing broker.

Formatting removes/invalidates the local metadata state.

Always know whether you are:

```text
NEW CLUSTER
```

or:

```text
EXISTING CLUSTER
```

before formatting.

---

# 📋 62. Final Validation Checklist

## AWS

```text
[ ] 3 Kafka EC2s created
[ ] 1 management EC2 created
[ ] Same VPC
[ ] Same subnet or routable subnets
[ ] Private IPs recorded
[ ] Elastic IPs allocated
[ ] Kafka SG created
[ ] Management SG created
[ ] SSH restricted
[ ] Kafka 9092 restricted
[ ] Internal 19092 SG-only
[ ] Controller 9093 SG-only
[ ] Management UI ports restricted
```

## Kafka

```text
[ ] Java 17
[ ] Kafka 4.0.2
[ ] Same cluster ID
[ ] node.id 1/2/3
[ ] Same controller voters
[ ] Unique private IP
[ ] Unique public IP
[ ] INTERNAL listener
[ ] EXTERNAL listener
[ ] CONTROLLER listener
[ ] advertised.listeners correct
[ ] systemd configured
[ ] Kafka running
```

## KRaft

```text
[ ] 3 controllers
[ ] Metadata quorum healthy
[ ] Leader elected
[ ] All voters visible
```

## Replication

```text
[ ] RF=3 topic
[ ] ISR=3
[ ] Broker failure tested
[ ] Leader election tested
[ ] ISR recovery tested
[ ] Under-replication observed
```

## Management

```text
[ ] Kafbat connected
[ ] Kpow connected
[ ] Conduktor connected
[ ] All use private 19092
[ ] Public UIs restricted
```

---

# 🏁 63. Final VishwaTech Architecture

```text
                         🌍 INTERNET
                              │
                 ┌────────────┴─────────────┐
                 │                          │
          Your Laptop                 Kafka Client
                 │                          │
        ┌────────┴────────┐          Public :9092
        │                 │                 │
        ▼                 ▼                 ▼
   Management EC2     Management EC2    Kafka Brokers
   Public :8080       Public :3000
   Public :8081                           │
        │                                 │
        └──────────────┬──────────────────┘
                       │
                  PRIVATE VPC
                       │
       ┌───────────────┼─────────────────┐
       │               │                 │
       ▼               ▼                 ▼
 ┌──────────┐    ┌──────────┐     ┌──────────┐
 │ Kafka-1  │    │ Kafka-2  │     │ Kafka-3  │
 │ ID=1     │    │ ID=2     │     │ ID=3     │
 │ Broker   │    │ Broker   │     │ Broker   │
 │Controller│    │Controller│     │Controller│
 │ :19092   │    │ :19092   │     │ :19092   │
 │ :9093    │    │ :9093    │     │ :9093    │
 └────┬─────┘    └────┬─────┘     └────┬─────┘
      │               │                │
      └───────────────┼────────────────┘
                      │
                KRaft Quorum
                    3 nodes
                  majority = 2
```

---

# 📚 Official Documentation

- [Apache Kafka 4.0 KRaft](https://kafka.apache.org/40/operations/kraft/)
- [Kafka 4.0 Broker Configuration](https://kafka.apache.org/40/configuration/broker-configs/)
- [Kafka Listener Configuration](https://kafka.apache.org/40/security/listener-configuration/)
- [AWS EC2 Best Practices](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-best-practices.html)
- [AWS Security Groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/changing-security-group.html)

---

# 🎓 What You Have Built

At the end of this lab you will understand:

```text
AWS EC2
   ↓
Linux
   ↓
Java
   ↓
Kafka
   ↓
KRaft
   ↓
3 Broker Cluster
   ↓
Controller Quorum
   ↓
Listeners
   ↓
Advertised Listeners
   ↓
Replication
   ↓
ISR
   ↓
Leader Election
   ↓
Consumer Groups
   ↓
Consumer Lag
   ↓
High Availability
   ↓
Kafbat
   ↓
Kpow
   ↓
Conduktor
   ↓
Kafka Security
```

> **This is the foundation for the next VishwaTech Kafka labs: TLS/SASL, ACL, Schema Registry, Kafka Connect, Prometheus/Grafana, Conduktor Gateway, multi-AZ design and production hardening.**
