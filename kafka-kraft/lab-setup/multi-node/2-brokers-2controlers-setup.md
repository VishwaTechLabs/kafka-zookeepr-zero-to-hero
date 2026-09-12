# 🚀 VishwaTech Kafka — AWS EC2 2-Broker + 2-Dedicated-Controller KRaft Lab

![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.0.2-black?logo=apachekafka)
![KRaft](https://img.shields.io/badge/KRaft-Dedicated%20Controllers-blue)
![AWS](https://img.shields.io/badge/AWS-EC2-orange?logo=amazonaws)
![Amazon Linux](https://img.shields.io/badge/Amazon%20Linux-2023-green)
![Java](https://img.shields.io/badge/Java-17-red?logo=openjdk)
![User](https://img.shields.io/badge/Linux%20User-ec2--user-purple)
![Lab](https://img.shields.io/badge/Lab-VishwaTech-success)

> 🎯 **Goal:** Build a very clear AWS EC2 Kafka KRaft laboratory with **2 dedicated KRaft controllers + 2 Kafka brokers**, using **Amazon Linux 2023** and **only `ec2-user`**.
>
> ⚠️ This is a **learning/lab topology**, not a production HA topology. A 2-controller quorum cannot tolerate one controller failure and still retain majority. Production normally uses 3 or 5 controllers. Apache Kafka documents 3 or 5 controllers as the typical quorum sizes.  
> 📚 [Apache Kafka KRaft documentation](https://kafka.apache.org/40/operations/kraft/)

---

# 🧭 1. What We Are Building

```text
                           AWS VPC
                    ┌───────────────────────┐
                    │                       │
                    │  KRaft Controller     │
                    │  QUORUM               │
                    │                       │
                    │ ┌───────────────┐     │
                    │ │ Controller-1  │     │
                    │ │ node.id=1     │     │
                    │ │ :9093         │     │
                    │ └───────┬───────┘     │
                    │         │              │
                    │         │ metadata     │
                    │         │ quorum       │
                    │ ┌───────▼───────┐     │
                    │ │ Controller-2  │     │
                    │ │ node.id=2     │     │
                    │ │ :9093         │     │
                    │ └───────────────┘     │
                    │                       │
                    │       KAFKA DATA      │
                    │                       │
                    │ ┌───────────────┐     │
                    │ │ Broker-1      │     │
                    │ │ node.id=3     │     │
                    │ │ :9092         │     │
                    │ │ :19092        │     │
                    │ └───────┬───────┘     │
                    │         │ replication │
                    │ ┌───────▼───────┐     │
                    │ │ Broker-2      │     │
                    │ │ node.id=4     │     │
                    │ │ :9092         │     │
                    │ │ :19092        │     │
                    │ └───────────────┘     │
                    │                       │
                    └───────────────────────┘
```

## 🧱 EC2 Inventory

| EC2 | Role | Node ID | Main Port | Public IP? | Private IP |
|---|---|---:|---:|---|---|
| EC2-1 | Controller-1 | 1 | 9093 | Yes for lab SSH only | `<CTRL1_PRIVATE_IP>` |
| EC2-2 | Controller-2 | 2 | 9093 | Yes for lab SSH only | `<CTRL2_PRIVATE_IP>` |
| EC2-3 | Broker-1 | 3 | 9092 / 19092 | Yes for lab clients | `<BROKER1_PRIVATE_IP>` |
| EC2-4 | Broker-2 | 4 | 9092 / 19092 | Yes for lab clients | `<BROKER2_PRIVATE_IP>` |

### Why 4 EC2?

We deliberately separate:

- 🧠 **Controllers** → Kafka metadata/control plane
- 💾 **Brokers** → Kafka data plane

Kafka supports `process.roles=broker`, `controller`, or both. Dedicated controllers are better for understanding production-style separation. Combined mode is simpler for development, while dedicated roles isolate the controller workload.  
📚 [Kafka KRaft roles](https://kafka.apache.org/40/operations/kraft/)

---

# ⭐ 2. IMPORTANT: 2 Controllers vs 3 Controllers

This lab uses:

```text
Controller-1
Controller-2
```

The controller quorum needs a majority.

With 2:

```text
2 controllers
Majority = 2

2/2 = quorum
1/2 = NO quorum
```

Therefore:

```text
Controller-1 DOWN
       ↓
Controller-2 alive
       ↓
1 of 2
       ↓
NO MAJORITY
```

This is intentional for the lab.

### Production recommendation

```text
Controller-1
Controller-2
Controller-3

Majority = 2/3

1 controller can fail
and quorum can continue.
```

Kafka's KRaft documentation describes 3 controllers as tolerating one controller failure and 5 as tolerating two.  
📚 [Kafka KRaft quorum guidance](https://kafka.apache.org/40/operations/kraft/)

---

# 🧠 3. Key Definitions

| Term | Meaning |
|---|---|
| **Broker** | Stores Kafka topic partitions and serves producers/consumers |
| **Controller** | Maintains Kafka cluster metadata and controller quorum |
| **KRaft** | Kafka's metadata quorum architecture replacing ZooKeeper |
| **Node ID** | Unique Kafka server ID |
| **Controller quorum** | Set of KRaft controller voters |
| **Partition** | Ordered log unit inside a topic |
| **Replica** | Copy of a partition |
| **Leader** | Replica handling writes/reads for a partition |
| **Follower** | Replica following the leader |
| **ISR** | In-Sync Replicas |
| **RF** | Replication Factor |
| **Bootstrap server** | Initial broker address used by clients |
| **Listener** | Network endpoint Kafka binds to |
| **Advertised listener** | Address Kafka tells clients to use |
| **Inter-broker listener** | Listener used for broker-to-broker traffic |
| **Controller listener** | Listener used for KRaft controller communication |

Kafka requires unique `node.id` values in KRaft and uses `process.roles` to define whether a server is a broker, controller, or both.  
📚 [Kafka broker configuration](https://kafka.apache.org/40/configuration/broker-configs/)

---

# ☁️ 4. AWS Architecture

For a simple learning environment:

```text
                         Internet
                            │
                    Your Laptop / PC
                            │
                     SSH / Kafka Client
                            │
                    ┌───────▼────────┐
                    │ AWS VPC        │
                    │                │
                    │ Controller-1   │
                    │ Controller-2   │
                    │ Broker-1       │
                    │ Broker-2       │
                    │                │
                    └────────────────┘
```

For a more realistic lab:

```text
AZ-A                         AZ-B

Controller-1                 Controller-2
Broker-1                     Broker-2
```

⚠️ Do not call a 2-controller deployment production HA. The purpose here is to learn dedicated KRaft architecture.

---

# 💻 5. EC2 Instance Recommendation

For the lab:

| Setting | Recommendation |
|---|---|
| OS | Amazon Linux 2023 |
| Architecture | x86_64 |
| Instance | t3.medium |
| vCPU | 2 |
| RAM | 4 GB |
| Disk | 30–50 GB gp3 |
| User | `ec2-user` |
| Java | OpenJDK 17 |
| Kafka | Apache Kafka 4.0.2 |

For a very small lab, controllers may run on smaller instances, but using the same `t3.medium` everywhere keeps the training environment simple.

---

# 🔐 6. AWS Security Group

Create a security group such as:

```text
VishwaTech-Kafka-Lab-SG
```

## Controller inbound

| Port | Source | Purpose |
|---:|---|---|
| 22 | YOUR_PUBLIC_IP/32 | SSH |
| 9093 | Kafka SG / private CIDR | KRaft controller traffic |

## Broker inbound

| Port | Source | Purpose |
|---:|---|---|
| 22 | YOUR_PUBLIC_IP/32 | SSH |
| 9092 | YOUR_PUBLIC_IP/32 | External lab client |
| 19092 | VPC private CIDR / Kafka SG | Internal broker traffic |

### 🚨 Never expose 9093 to the Internet

Do NOT create:

```text
9093 → 0.0.0.0/0
```

Controller traffic is internal cluster traffic.

Similarly, avoid:

```text
9092 → 0.0.0.0/0
```

unless you intentionally need a public Kafka lab endpoint.

AWS security groups should follow least-privilege rules.  
📚 [AWS EC2 security best practices](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-best-practices.html)  
📚 [AWS security groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html)

---

# 📝 7. FIRST — Collect Your IP Addresses

After launching all four EC2 instances, run on each:

```bash
hostname -I
```

and:

```bash
curl -s http://169.254.169.254/latest/meta-data/local-ipv4
```

For public IPv4:

```bash
curl -s http://169.254.169.254/latest/meta-data/public-ipv4
```

Example inventory:

```text
Controller-1
Private IP: 10.0.1.11
Public IP: 54.10.10.11

Controller-2
Private IP: 10.0.2.12
Public IP: 54.10.10.12

Broker-1
Private IP: 10.0.1.21
Public IP: 54.10.10.21

Broker-2
Private IP: 10.0.2.22
Public IP: 54.10.10.22
```

> 🔥 These are examples only. Replace them with your actual AWS IPs.

---

# 🧩 8. VERY IMPORTANT — Public IP vs Private IP

For this lab:

```text
Controller traffic
        ↓
PRIVATE IP

Broker-to-broker
        ↓
PRIVATE IP

External Kafka client
        ↓
PUBLIC IP

SSH
        ↓
PUBLIC IP
```

### Controller

```text
10.0.1.11:9093
10.0.2.12:9093
```

### Broker internal

```text
10.0.1.21:19092
10.0.2.22:19092
```

### Broker external

```text
54.10.10.21:9092
54.10.10.22:9092
```

Kafka's listener model separates what Kafka binds to from what it advertises to clients. This is particularly important in cloud environments.  
📚 [Kafka listener configuration](https://kafka.apache.org/40/security/listener-configuration/)

---

# 🏷️ 9. Hostnames — HIGHLY RECOMMENDED

On every EC2 instance:

```bash
sudo hostnamectl set-hostname <hostname>
```

Example:

### Controller-1

```bash
sudo hostnamectl set-hostname kafka-controller-1
```

### Controller-2

```bash
sudo hostnamectl set-hostname kafka-controller-2
```

### Broker-1

```bash
sudo hostnamectl set-hostname kafka-broker-1
```

### Broker-2

```bash
sudo hostnamectl set-hostname kafka-broker-2
```

Then:

```bash
hostname
```

---

# 🗺️ 10. /etc/hosts

For the lab, private IP based `/etc/hosts` makes troubleshooting easier.

On **ALL FOUR SERVERS**, edit:

```bash
sudo vi /etc/hosts
```

Add:

```text
10.0.1.11 kafka-controller-1
10.0.2.12 kafka-controller-2
10.0.1.21 kafka-broker-1
10.0.2.22 kafka-broker-2
```

Test:

```bash
ping -c 2 kafka-controller-1
ping -c 2 kafka-controller-2
ping -c 2 kafka-broker-1
ping -c 2 kafka-broker-2
```

---

# ☕ 11. Install Java 17 — ALL SERVERS

Run on:

- Controller-1
- Controller-2
- Broker-1
- Broker-2

```bash
sudo dnf update -y
sudo dnf install -y java-17-amazon-corretto wget tar gzip curl nc
```

Verify:

```bash
java -version
```

Expected:

```text
openjdk version "17..."
```

Find Java:

```bash
readlink -f $(which java)
```

---

# 📦 12. Kafka Directory Structure

On **ALL SERVERS**:

```bash
mkdir -p /home/ec2-user/kafka
mkdir -p /home/ec2-user/kafka-data
mkdir -p /home/ec2-user/kafka-logs
```

We intentionally use:

```text
ec2-user
```

for everything.

No:

```text
kafka
zookeeper
confluent
```

Linux users are required.

This lab uses:

```text
ec2-user
```

only.

---

# 📥 13. Download Kafka 4.0.2

On **ALL FOUR SERVERS**:

```bash
cd /home/ec2-user

wget https://downloads.apache.org/kafka/4.0.2/kafka_2.13-4.0.2.tgz
```

Extract:

```bash
tar -xzf kafka_2.13-4.0.2.tgz
```

Rename:

```bash
mv kafka_2.13-4.0.2 kafka
```

Check:

```bash
ls -lah /home/ec2-user/kafka
```

Kafka binaries:

```bash
ls /home/ec2-user/kafka/bin
```

---

# 🧠 14. KRaft Roles

## Controller servers

Controller-1:

```properties
process.roles=controller
```

Controller-2:

```properties
process.roles=controller
```

## Broker servers

Broker-1:

```properties
process.roles=broker
```

Broker-2:

```properties
process.roles=broker
```

This is the major difference from our previous combined architecture.

---

# 🔢 15. Node ID Plan

Use:

```text
Controller-1 → node.id=1
Controller-2 → node.id=2

Broker-1     → node.id=3
Broker-2     → node.id=4
```

Never duplicate node IDs.

```text
1 → Controller-1
2 → Controller-2
3 → Broker-1
4 → Broker-2
```

---

# 🧠 16. Controller Quorum

For this lab:

```text
1@10.0.1.11:9093
2@10.0.2.12:9093
```

Every Kafka server must know the controller quorum.

For Kafka 4.0.2 this README intentionally uses the static KRaft quorum configuration:

```properties
controller.quorum.voters=1@10.0.1.11:9093,2@10.0.2.12:9093
```

Kafka 4.0 documents static quorum configuration using `controller.quorum.voters`; newer Kafka releases also document dynamic KRaft quorum configuration.  
📚 [Kafka 4.0 KRaft](https://kafka.apache.org/40/operations/kraft/)

---

# 🟦 17. CONTROLLER-1 — Configuration

SSH:

```bash
ssh -i your-key.pem ec2-user@<CTRL1_PUBLIC_IP>
```

Create config:

```bash
mkdir -p /home/ec2-user/kafka/config/kraft
```

Edit:

```bash
vi /home/ec2-user/kafka/config/kraft/controller.properties
```

Use:

```properties
process.roles=controller

node.id=1

listeners=CONTROLLER://10.0.1.11:9093

controller.listener.names=CONTROLLER

controller.quorum.voters=1@10.0.1.11:9093,2@10.0.2.12:9093

listener.security.protocol.map=CONTROLLER:PLAINTEXT

log.dirs=/home/ec2-user/kafka-data
```

### Replace

```text
10.0.1.11
```

with your real Controller-1 private IP.

---

# 🟩 18. CONTROLLER-2 — Configuration

SSH:

```bash
ssh -i your-key.pem ec2-user@<CTRL2_PUBLIC_IP>
```

Create:

```bash
mkdir -p /home/ec2-user/kafka/config/kraft
```

Edit:

```bash
vi /home/ec2-user/kafka/config/kraft/controller.properties
```

Use:

```properties
process.roles=controller

node.id=2

listeners=CONTROLLER://10.0.2.12:9093

controller.listener.names=CONTROLLER

controller.quorum.voters=1@10.0.1.11:9093,2@10.0.2.12:9093

listener.security.protocol.map=CONTROLLER:PLAINTEXT

log.dirs=/home/ec2-user/kafka-data
```

Replace the IP with your actual Controller-2 private IP.

---

# 🟥 19. BROKER-1 — Configuration

SSH:

```bash
ssh -i your-key.pem ec2-user@<BROKER1_PUBLIC_IP>
```

Create:

```bash
mkdir -p /home/ec2-user/kafka/config/kraft
```

Edit:

```bash
vi /home/ec2-user/kafka/config/kraft/server.properties
```

Use:

```properties
process.roles=broker

node.id=3

listeners=INTERNAL://10.0.1.21:19092,EXTERNAL://0.0.0.0:9092

advertised.listeners=INTERNAL://10.0.1.21:19092,EXTERNAL://54.10.10.21:9092

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

inter.broker.listener.name=INTERNAL

controller.listener.names=CONTROLLER

controller.quorum.voters=1@10.0.1.11:9093,2@10.0.2.12:9093

log.dirs=/home/ec2-user/kafka-data

num.partitions=2

default.replication.factor=2

min.insync.replicas=1

offsets.topic.replication.factor=2

transaction.state.log.replication.factor=2

transaction.state.log.min.isr=1

auto.create.topics.enable=false
```

### Important

```text
10.0.1.21
```

= Broker-1 PRIVATE IP

```text
54.10.10.21
```

= Broker-1 PUBLIC IP

Replace both.

---

# 🟨 20. BROKER-2 — Configuration

SSH:

```bash
ssh -i your-key.pem ec2-user@<BROKER2_PUBLIC_IP>
```

Edit:

```bash
vi /home/ec2-user/kafka/config/kraft/server.properties
```

Use:

```properties
process.roles=broker

node.id=4

listeners=INTERNAL://10.0.2.22:19092,EXTERNAL://0.0.0.0:9092

advertised.listeners=INTERNAL://10.0.2.22:19092,EXTERNAL://54.10.10.22:9092

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

inter.broker.listener.name=INTERNAL

controller.listener.names=CONTROLLER

controller.quorum.voters=1@10.0.1.11:9093,2@10.0.2.12:9093

log.dirs=/home/ec2-user/kafka-data

num.partitions=2

default.replication.factor=2

min.insync.replicas=1

offsets.topic.replication.factor=2

transaction.state.log.replication.factor=2

transaction.state.log.min.isr=1

auto.create.topics.enable=false
```

Replace:

```text
10.0.2.22
54.10.10.22
```

with your actual IPs.

---

# 🧩 21. WHY BROKER HAS TWO LISTENERS

Broker:

```text
INTERNAL
   ↓
19092
   ↓
private VPC
```

and:

```text
EXTERNAL
   ↓
9092
   ↓
external client
```

Architecture:

```text
             AWS VPC
                │
       ┌────────┴────────┐
       │                 │
 Broker-1            Broker-2
10.0.1.21            10.0.2.22
   :19092               :19092
       │                 │
       └──── replication ┘


Laptop
  │
  │ public IP
  ▼
Broker-1 :9092
Broker-2 :9092
```

The `advertised.listeners` value is critical because Kafka tells clients what address to use.  
📚 [Kafka listener configuration](https://kafka.apache.org/40/security/listener-configuration/)

---

# ⚠️ 22. Why Controllers Don't Need Public Kafka Ports

Controller:

```text
10.0.1.11:9093
```

Controller-2:

```text
10.0.2.12:9093
```

Only the private VPC needs access.

Never:

```text
54.x.x.x:9093
```

for normal lab client access.

---

# 🆔 23. Generate ONE Cluster ID

This is VERY important.

Do this **once only**.

You can generate it on Controller-1:

```bash
cd /home/ec2-user/kafka

bin/kafka-storage.sh random-uuid
```

Example:

```text
MkU3OEVBNTcwNTJENDM2Qk
```

Save it:

```bash
export KAFKA_CLUSTER_ID='YOUR_CLUSTER_ID'
```

Example:

```bash
export KAFKA_CLUSTER_ID='MkU3OEVBNTcwNTJENDM2Qk'
```

### 🚨 Do NOT generate a different cluster ID on each server.

All four Kafka nodes belong to the same cluster.

---

# 🧨 24. Format Controllers

Kafka KRaft storage formatting must be done carefully.

## Controller-1

```bash
cd /home/ec2-user/kafka

bin/kafka-storage.sh format \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --initial-controllers "1@10.0.1.11:9093,2@10.0.2.12:9093" \
  --config config/kraft/controller.properties
```

## Controller-2

Use the **same cluster ID** and same initial controller set:

```bash
cd /home/ec2-user/kafka

export KAFKA_CLUSTER_ID='YOUR_CLUSTER_ID'

bin/kafka-storage.sh format \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --initial-controllers "1@10.0.1.11:9093,2@10.0.2.12:9093" \
  --config config/kraft/controller.properties
```

Kafka documents `--initial-controllers` for bootstrapping a multi-controller KRaft quorum.  
📚 [Kafka multi-controller bootstrap](https://kafka.apache.org/40/operations/kraft/)

---

# 💾 25. Format Brokers

After the controller quorum is bootstrapped, format the brokers.

## Broker-1

```bash
cd /home/ec2-user/kafka

bin/kafka-storage.sh format \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --config config/kraft/server.properties \
  --no-initial-controllers
```

## Broker-2

```bash
cd /home/ec2-user/kafka

export KAFKA_CLUSTER_ID='YOUR_CLUSTER_ID'

bin/kafka-storage.sh format \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --config config/kraft/server.properties \
  --no-initial-controllers
```

### 🚨 CRITICAL

Do not casually run:

```bash
rm -rf /home/ec2-user/kafka-data/*
```

on an existing node.

That destroys the node's Kafka metadata/data.

---

# ⚙️ 26. Systemd — Controller-1

Create:

```bash
sudo vi /etc/systemd/system/kafka-controller.service
```

Paste:

```ini
[Unit]
Description=Apache Kafka KRaft Controller
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
Group=ec2-user

Environment="KAFKA_HEAP_OPTS=-Xms512M -Xmx512M"

ExecStart=/home/ec2-user/kafka/bin/kafka-server-start.sh /home/ec2-user/kafka/config/kraft/controller.properties

ExecStop=/home/ec2-user/kafka/bin/kafka-server-stop.sh

Restart=on-failure
RestartSec=10

LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka-controller
sudo systemctl start kafka-controller
```

Check:

```bash
sudo systemctl status kafka-controller
```

---

# ⚙️ 27. Systemd — Controller-2

Same service:

```bash
sudo vi /etc/systemd/system/kafka-controller.service
```

Paste exactly the same service file.

Because configuration determines which controller it is.

Start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka-controller
sudo systemctl start kafka-controller
```

Check:

```bash
sudo systemctl status kafka-controller
```

---

# ⚙️ 28. Systemd — Broker-1

Create:

```bash
sudo vi /etc/systemd/system/kafka.service
```

```ini
[Unit]
Description=Apache Kafka KRaft Broker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
Group=ec2-user

Environment="KAFKA_HEAP_OPTS=-Xms1G -Xmx1G"

ExecStart=/home/ec2-user/kafka/bin/kafka-server-start.sh /home/ec2-user/kafka/config/kraft/server.properties

ExecStop=/home/ec2-user/kafka/bin/kafka-server-stop.sh

Restart=on-failure
RestartSec=10

LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
```

Start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka
sudo systemctl start kafka
```

Check:

```bash
sudo systemctl status kafka
```

---

# ⚙️ 29. Systemd — Broker-2

Exactly the same service:

```bash
sudo vi /etc/systemd/system/kafka.service
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka
sudo systemctl start kafka
```

Check:

```bash
sudo systemctl status kafka
```

---

# 🔍 30. Check Ports

## Controllers

On each controller:

```bash
sudo ss -lntp | grep 9093
```

Expected:

```text
LISTEN ... 10.0.x.x:9093
```

## Brokers

```bash
sudo ss -lntp | grep -E '9092|19092'
```

Expected:

```text
9092
19092
```

---

# 🧪 31. Test Controller Connectivity

From Controller-1:

```bash
nc -vz 10.0.2.12 9093
```

From Controller-2:

```bash
nc -vz 10.0.1.11 9093
```

Expected:

```text
succeeded
```

---

# 🧪 32. Test Broker Internal Connectivity

From Broker-1:

```bash
nc -vz 10.0.2.22 19092
```

From Broker-2:

```bash
nc -vz 10.0.1.21 19092
```

---

# 🧠 33. Check KRaft Metadata Quorum

From a controller:

```bash
cd /home/ec2-user/kafka

bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.0.1.21:19092 \
  describe --status
```

Depending on Kafka 4.0 command availability/configuration, you can also inspect the controller endpoint:

```bash
bin/kafka-metadata-quorum.sh \
  --bootstrap-controller 10.0.1.11:9093 \
  describe --status
```

Look for:

```text
LeaderId
HighWatermark
MaxFollowerLag
CurrentVoters
```

---

# 📊 34. Check Broker Registration

From Broker-1:

```bash
cd /home/ec2-user/kafka

bin/kafka-broker-api-versions.sh \
  --bootstrap-server 10.0.1.21:19092
```

---

# 🏗️ 35. Create Test Topic

From Broker-1:

```bash
cd /home/ec2-user/kafka

bin/kafka-topics.sh \
  --bootstrap-server 10.0.1.21:19092 \
  --create \
  --topic vishwatech-demo \
  --partitions 4 \
  --replication-factor 2
```

Verify:

```bash
bin/kafka-topics.sh \
  --bootstrap-server 10.0.1.21:19092 \
  --describe \
  --topic vishwatech-demo
```

You should see replicas spread between:

```text
3
4
```

---

# 📦 36. Why Replication Factor = 2?

Because:

```text
Broker-1
Broker-2
```

There are only two brokers.

Therefore:

```text
RF=2
```

means:

```text
Partition 0
 ├── Broker-3 = Leader
 └── Broker-4 = Follower
```

and another partition may reverse leadership.

---

# 📤 37. Producer Test

On Broker-1:

```bash
cd /home/ec2-user/kafka

bin/kafka-console-producer.sh \
  --bootstrap-server 10.0.1.21:19092 \
  --topic vishwatech-demo
```

Enter:

```text
Hello Kafka
Hello VishwaTech
Kafka KRaft Lab
Broker 1
Broker 2
```

---

# 📥 38. Consumer Test

On Broker-2:

```bash
cd /home/ec2-user/kafka

bin/kafka-console-consumer.sh \
  --bootstrap-server 10.0.2.22:19092 \
  --topic vishwatech-demo \
  --from-beginning
```

Messages should appear.

---

# 🌐 39. External Client Test

From your laptop:

```bash
kafka-topics.sh \
  --bootstrap-server <BROKER1_PUBLIC_IP>:9092,<BROKER2_PUBLIC_IP>:9092 \
  --list
```

Example:

```bash
kafka-topics.sh \
  --bootstrap-server 54.10.10.21:9092,54.10.10.22:9092 \
  --list
```

⚠️ This only works if:

1. Broker 9092 is allowed in the security group.
2. `advertised.listeners` contains reachable public addresses.
3. Your local Kafka client is compatible.

---

# 🔥 40. VERY IMPORTANT — advertised.listeners

Suppose:

```properties
advertised.listeners=EXTERNAL://54.10.10.21:9092
```

The client initially connects to:

```text
54.10.10.21:9092
```

Kafka then returns metadata telling the client:

```text
Broker-1 = 54.10.10.21:9092
Broker-2 = 54.10.10.22:9092
```

If you advertise private IPs to an Internet client:

```text
10.0.1.21:19092
```

your laptop cannot reach them.

This is one of the most common Kafka-on-cloud lab mistakes.

---

# 🧪 41. Failure Lab — Kill Broker-1

On Broker-1:

```bash
sudo systemctl stop kafka
```

On Broker-2:

```bash
cd /home/ec2-user/kafka

bin/kafka-topics.sh \
  --bootstrap-server 10.0.2.22:19092 \
  --describe \
  --topic vishwatech-demo
```

Observe:

```text
Leader
ISR
Replicas
```

This teaches:

```text
Broker failure
      ↓
Leader unavailable
      ↓
Follower becomes leader
      ↓
Client continues
```

---

# 🔄 42. Start Broker-1 Again

```bash
sudo systemctl start kafka
```

Check:

```bash
sudo systemctl status kafka
```

Then:

```bash
bin/kafka-topics.sh \
  --bootstrap-server 10.0.2.22:19092 \
  --describe \
  --topic vishwatech-demo
```

Watch ISR recover.

---

# 💥 43. Controller Failure Lab

⚠️ This is an educational demonstration.

Stop Controller-1:

```bash
sudo systemctl stop kafka-controller
```

Because this lab has:

```text
2 controllers
```

you now have:

```text
1 / 2
```

and therefore no majority.

This demonstrates exactly why production uses an odd-sized controller quorum such as:

```text
3
```

rather than:

```text
2
```

---

# 🔁 44. Restore Controller-1

```bash
sudo systemctl start kafka-controller
```

Check:

```bash
sudo systemctl status kafka-controller
```

Then:

```bash
bin/kafka-metadata-quorum.sh \
  --bootstrap-controller 10.0.1.11:9093 \
  describe --status
```

---

# 🧪 45. LAB — Consumer Groups

Create consumer group:

```bash
bin/kafka-console-consumer.sh \
  --bootstrap-server 10.0.1.21:19092 \
  --topic vishwatech-demo \
  --group vishwa-group
```

Check:

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.0.1.21:19092 \
  --describe \
  --group vishwa-group
```

Study:

```text
GROUP
TOPIC
PARTITION
CURRENT-OFFSET
LOG-END-OFFSET
LAG
CONSUMER-ID
HOST
```

---

# 📈 46. LAB — Consumer Lag

Produce many messages:

```bash
for i in {1..100}; do
  echo "message-$i"
done | bin/kafka-console-producer.sh \
  --bootstrap-server 10.0.1.21:19092 \
  --topic vishwatech-demo
```

Then:

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server 10.0.1.21:19092 \
  --describe \
  --group vishwa-group
```

Observe:

```text
LAG
```

This is one of the most important Kafka operational metrics.

---

# 🛠️ 47. Logs

Controller:

```bash
sudo journalctl -u kafka-controller -f
```

Broker:

```bash
sudo journalctl -u kafka -f
```

Last 100 lines:

```bash
sudo journalctl -u kafka -n 100 --no-pager
```

---

# 🔍 48. Kafka Process

```bash
ps -ef | grep kafka
```

Java process:

```bash
jps -lv
```

---

# 🧹 49. Stop Kafka

Broker:

```bash
sudo systemctl stop kafka
```

Controller:

```bash
sudo systemctl stop kafka-controller
```

---

# 🔄 50. Restart Kafka

```bash
sudo systemctl restart kafka
```

Controller:

```bash
sudo systemctl restart kafka-controller
```

---

# 📋 51. Which File Goes Where?

## Controller-1

```text
/home/ec2-user/kafka/config/kraft/controller.properties
```

Important:

```properties
process.roles=controller
node.id=1
listeners=CONTROLLER://CTRL1_PRIVATE_IP:9093
controller.quorum.voters=1@CTRL1_PRIVATE_IP:9093,2@CTRL2_PRIVATE_IP:9093
```

---

## Controller-2

```text
/home/ec2-user/kafka/config/kraft/controller.properties
```

Important:

```properties
process.roles=controller
node.id=2
listeners=CONTROLLER://CTRL2_PRIVATE_IP:9093
controller.quorum.voters=1@CTRL1_PRIVATE_IP:9093,2@CTRL2_PRIVATE_IP:9093
```

---

## Broker-1

```text
/home/ec2-user/kafka/config/kraft/server.properties
```

Important:

```properties
process.roles=broker
node.id=3
listeners=INTERNAL://BROKER1_PRIVATE_IP:19092,EXTERNAL://0.0.0.0:9092
advertised.listeners=INTERNAL://BROKER1_PRIVATE_IP:19092,EXTERNAL://BROKER1_PUBLIC_IP:9092
```

---

## Broker-2

```text
/home/ec2-user/kafka/config/kraft/server.properties
```

Important:

```properties
process.roles=broker
node.id=4
listeners=INTERNAL://BROKER2_PRIVATE_IP:19092,EXTERNAL://0.0.0.0:9092
advertised.listeners=INTERNAL://BROKER2_PRIVATE_IP:19092,EXTERNAL://BROKER2_PUBLIC_IP:9092
```

---

# 🗂️ 52. Final Directory Structure

Every server:

```text
/home/ec2-user/
│
├── kafka/
│   ├── bin/
│   ├── config/
│   │   └── kraft/
│   │       ├── controller.properties
│   │       └── server.properties
│   └── libs/
│
├── kafka-data/
│
└── kafka-logs/
```

---

# 🔐 53. Security — LAB vs REAL WORLD

Current lab:

```text
PLAINTEXT
```

This is intentional so students understand Kafka first.

Production should move toward:

```text
TLS
+
SASL
+
ACL/RBAC
+
Private networking
+
Secrets management
```

Example:

```text
Producer
   │
   │ TLS/SASL
   ▼
Kafka Broker
   │
   │ ACL
   ▼
Topic
```

---

# 🛡️ 54. Production Security Roadmap

After completing this lab, implement:

### Level 1

```text
Private VPC
```

### Level 2

```text
TLS
```

### Level 3

```text
SASL/SCRAM
```

### Level 4

```text
IAM / OAuth
```

### Level 5

```text
Kafka ACL
```

### Level 6

```text
Secrets Manager / Vault
```

### Level 7

```text
Monitoring + SIEM
```

---

# 📊 55. Kafka Monitoring Roadmap

Monitor:

```text
Broker CPU
Memory
Disk
Network
Request latency
Request rate
Under-replicated partitions
Offline partitions
ISR shrink
Consumer lag
Controller health
Metadata quorum
Disk utilization
```

Later integrate:

```text
Prometheus
Grafana
CloudWatch
OpenTelemetry
SIEM
```

---

# 🧰 56. Management GUI Roadmap

After this cluster is working, deploy management tools on a separate management EC2:

```text
                  Management EC2
                       │
        ┌──────────────┼──────────────┐
        │              │              │
     Kafbat           Kpow        Conduktor
      :8080            :3000         :8081
        │              │              │
        └──────────────┼──────────────┘
                       │
                Private VPC
                       │
              ┌────────┴────────┐
              │                 │
           Broker-1          Broker-2
```

Use private broker addresses:

```text
10.0.1.21:19092
10.0.2.22:19092
```

Do not make the management tools use public broker addresses unless there is a specific reason.

---

# 🎓 57. VishwaTech Lab Roadmap

## LEVEL 1 — Foundation

- [x] EC2 provisioning
- [x] Amazon Linux
- [x] Java 17
- [x] Kafka installation
- [x] KRaft
- [x] Dedicated controllers
- [x] Dedicated brokers

## LEVEL 2 — Kafka Core

- [ ] Topics
- [ ] Partitions
- [ ] Replication
- [ ] ISR
- [ ] Leaders
- [ ] Followers
- [ ] Consumer groups
- [ ] Consumer lag

## LEVEL 3 — Failure Engineering

- [ ] Broker failure
- [ ] Broker recovery
- [ ] Controller failure
- [ ] Controller recovery
- [ ] Partition reassignment
- [ ] ISR shrink
- [ ] ISR recovery

## LEVEL 4 — Security

- [ ] TLS
- [ ] SASL
- [ ] ACL
- [ ] Authentication
- [ ] Authorization
- [ ] Secrets

## LEVEL 5 — Enterprise

- [ ] Schema Registry
- [ ] Kafka Connect
- [ ] Kafbat
- [ ] Kpow
- [ ] Conduktor
- [ ] Prometheus
- [ ] Grafana
- [ ] Alerting

## LEVEL 6 — Production

- [ ] 3 controllers
- [ ] 3+ brokers
- [ ] Multi-AZ
- [ ] Private subnets
- [ ] DNS
- [ ] TLS
- [ ] SASL
- [ ] ACL
- [ ] IaC
- [ ] Backup
- [ ] DR
- [ ] Capacity planning

---

# 🧠 58. Common Mistakes

## ❌ Mistake 1 — Different cluster IDs

Wrong:

```text
Controller-1 → Cluster A
Controller-2 → Cluster B
Broker-1 → Cluster C
```

Correct:

```text
ALL → SAME CLUSTER ID
```

---

## ❌ Mistake 2 — Duplicate node IDs

Wrong:

```text
Controller-1 = 1
Controller-2 = 1
```

Correct:

```text
1
2
3
4
```

---

## ❌ Mistake 3 — Public controller listener

Avoid:

```text
9093 → 0.0.0.0/0
```

---

## ❌ Mistake 4 — Wrong advertised IP

Wrong:

```properties
advertised.listeners=EXTERNAL://10.0.1.21:9092
```

for a laptop outside the VPC.

---

## ❌ Mistake 5 — Using public IP for broker replication

Avoid:

```text
Broker-1 public IP
        ↓
Broker-2 public IP
```

Use:

```text
private IP
```

---

## ❌ Mistake 6 — Running Kafka as root

This lab intentionally uses:

```text
ec2-user
```

Kafka service:

```ini
User=ec2-user
Group=ec2-user
```

---

# 🧪 59. Complete Validation Checklist

Run:

```bash
java -version
```

```bash
sudo systemctl status kafka-controller
```

on controllers.

Run:

```bash
sudo systemctl status kafka
```

on brokers.

Then:

```bash
sudo ss -lntp | grep 9093
```

controllers.

Then:

```bash
sudo ss -lntp | grep -E '9092|19092'
```

brokers.

Then:

```bash
bin/kafka-topics.sh \
  --bootstrap-server BROKER_PRIVATE_IP:19092 \
  --list
```

Then create:

```bash
vishwatech-demo
```

Then produce.

Then consume.

Then describe:

```bash
bin/kafka-topics.sh \
  --bootstrap-server BROKER_PRIVATE_IP:19092 \
  --describe \
  --topic vishwatech-demo
```

Finally:

```bash
bin/kafka-metadata-quorum.sh \
  --bootstrap-controller CONTROLLER_PRIVATE_IP:9093 \
  describe --status
```

---

# 🏁 60. Final Architecture

```text
                         VishwaTech
                    Kafka KRaft Lab
                         AWS VPC

       ┌──────────────────────────────────────────┐
       │                                          │
       │       KRAFT CONTROLLER QUORUM            │
       │                                          │
       │   ┌────────────┐       ┌────────────┐   │
       │   │ Controller │       │ Controller │   │
       │   │     1      │◄─────►│     2      │   │
       │   │  node.id=1 │       │  node.id=2 │   │
       │   │   :9093    │       │   :9093    │   │
       │   └────────────┘       └────────────┘   │
       │                                          │
       │                METADATA                  │
       │                   │                      │
       │          ┌────────┴────────┐             │
       │          │                 │             │
       │   ┌──────▼──────┐   ┌──────▼──────┐     │
       │   │   Broker 1  │   │   Broker 2  │     │
       │   │ node.id=3   │   │ node.id=4   │     │
       │   │ :19092 INT  │   │ :19092 INT  │     │
       │   │ :9092 EXT   │   │ :9092 EXT   │     │
       │   └──────┬──────┘   └──────┬──────┘     │
       │          │                 │             │
       │          └──── replication ┘             │
       │                                          │
       └──────────────────────────────────────────┘
                           ▲
                           │
                    Kafka Clients
                           │
                     Public :9092
```

---

# 📚 Official Documentation

- 🟢 [Apache Kafka 4.0 Documentation](https://kafka.apache.org/40/)
- 🟢 [Kafka KRaft](https://kafka.apache.org/40/operations/kraft/)
- 🟢 [Kafka Broker Configuration](https://kafka.apache.org/40/configuration/broker-configs/)
- 🟢 [Kafka Listener Configuration](https://kafka.apache.org/40/security/listener-configuration/)
- 🟠 [AWS EC2 Documentation](https://docs.aws.amazon.com/ec2/)
- 🟠 [AWS Security Groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html)
- 🟠 [AWS VPC Documentation](https://docs.aws.amazon.com/vpc/)
- 🟣 [Kafbat UI](https://github.com/kafbat/kafka-ui)
- 🔵 [Kpow](https://factorhouse.io/kpow/)
- 🟦 [Conduktor](https://www.conduktor.io/)

---

# 🎯 Final Learning Principle

> **Don't just install Kafka. Understand the traffic.**

```text
CLIENT
  │
  │ EXTERNAL :9092
  ▼
BROKER
  │
  │ INTERNAL :19092
  ▼
BROKER
  │
  │ Controller communication
  ▼
KRaft CONTROLLER QUORUM
  │
  ▼
METADATA
```

Once you understand this flow, Kafka on AWS becomes much easier to troubleshoot.

---

## ⭐ VishwaTech Rule

```text
Same Linux user
       ↓
ec2-user
       ↓
Same Kafka distribution
       ↓
Different node.id
       ↓
Same cluster.id
       ↓
Private controller communication
       ↓
Private broker replication
       ↓
Controlled external client access
       ↓
TLS/SASL/ACL later
```

**This is the foundation for the next VishwaTech Kafka labs.**
