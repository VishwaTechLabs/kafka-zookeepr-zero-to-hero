
# 🚀 VishwaTech Labs — Real-World Enterprise Kafka Architecture on AWS
## 3-Broker KRaft Cluster + Multi-AZ + Private Networking + Security + Observability + Management Plane

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.x-black?logo=apachekafka)
![KRaft](https://img.shields.io/badge/KRaft-Enabled-blue)
![AWS](https://img.shields.io/badge/AWS-EC2-FF9900?logo=amazonaws)
![Multi AZ](https://img.shields.io/badge/AWS-Multi--AZ-success)
![TLS](https://img.shields.io/badge/Security-TLS-green)
![SASL](https://img.shields.io/badge/Security-SASL-orange)
![ACL](https://img.shields.io/badge/Authorization-ACL-purple)
![Monitoring](https://img.shields.io/badge/Monitoring-Prometheus%20%2B%20Grafana-red)
![Management](https://img.shields.io/badge/Management-Kafbat%20%7C%20Kpow%20%7C%20Conduktor-informational)
![VishwaTech](https://img.shields.io/badge/VishwaTech-Labs-purple)

> **Purpose:** Understand how Kafka is actually designed and operated in enterprise environments, then build a realistic AWS lab that moves beyond a simple public-IP, single-AZ Kafka installation.

---

# 📚 Table of Contents

1. [What This Lab Is](#-1-what-this-lab-is)
2. [Training Lab vs Real Production](#-2-training-lab-vs-real-production)
3. [Real-World Architecture](#-3-real-world-architecture)
4. [AWS Multi-AZ Design](#-4-aws-multi-az-design)
5. [How Many EC2 Instances](#-5-how-many-ec2-instances)
6. [Broker and Controller Architecture](#-6-broker-and-controller-architecture)
7. [Networking Design](#-7-networking-design)
8. [Public IP vs Private IP](#-8-public-ip-vs-private-ip)
9. [DNS Design](#-9-dns-design)
10. [Security Groups](#-10-security-groups)
11. [Kafka Listener Architecture](#-11-kafka-listener-architecture)
12. [advertised.listeners Explained](#-12-advertisedlisteners-explained)
13. [Common vs Node-Specific Configuration](#-13-common-vs-node-specific-configuration)
14. [KRaft Quorum](#-14-kraft-quorum)
15. [Replication](#-15-replication)
16. [ISR](#-16-isr)
17. [min.insync.replicas](#-17-mininsyncreplicas)
18. [Producer Durability](#-18-producer-durability)
19. [Consumer Groups and Lag](#-19-consumer-groups-and-lag)
20. [Topic and Partition Design](#-20-topic-and-partition-design)
21. [Security Architecture](#-21-security-architecture)
22. [TLS](#-22-tls)
23. [SASL](#-23-sasl)
24. [ACL](#-24-acl)
25. [Enterprise Identity](#-25-enterprise-identity)
26. [Management Plane](#-26-management-plane)
27. [Schema Registry](#-27-schema-registry)
28. [Kafka Connect](#-28-kafka-connect)
29. [Observability](#-29-observability)
30. [Capacity Planning](#-30-capacity-planning)
31. [Failure Scenarios](#-31-failure-scenarios)
32. [Rolling Restart](#-32-rolling-restart)
33. [AZ Failure](#-33-az-failure)
34. [Disaster Recovery](#-34-disaster-recovery)
35. [Backup and Recovery](#-35-backup-and-recovery)
36. [Automation](#-36-automation)
37. [Infrastructure as Code](#-37-infrastructure-as-code)
38. [Production vs Lab Matrix](#-38-production-vs-lab-matrix)
39. [VishwaTech Lab Roadmap](#-39-vishwatech-lab-roadmap)
40. [Final Architecture](#-40-final-architecture)
41. [Official Documentation](#-41-official-documentation)

---

# 🎯 1. What This Lab Is

You have already built:

```text
Level 1
Single-node Kafka

Level 2
Single-node KRaft

Level 3
3-broker KRaft cluster
```

Now we move to the **enterprise architecture mindset**.

The important lesson is:

> A production Kafka platform is not just a set of Kafka brokers.

It is a complete platform consisting of:

```text
Networking
+
Compute
+
Storage
+
Kafka
+
KRaft
+
Replication
+
Security
+
Identity
+
Authorization
+
Schema management
+
Kafka Connect
+
Monitoring
+
Alerting
+
Automation
+
Disaster Recovery
+
Governance
```

---

# 🧠 2. Training Lab vs Real Production

## Your basic training lab

```text
Internet
   │
Public IP
   │
Kafka :9092
   │
┌──┴──┐
│ EC2 │
│Kafka│
└─────┘
```

Excellent for:

- learning Kafka commands
- learning topics
- learning partitions
- learning producers
- learning consumers
- learning KRaft
- learning replication

But it is **not production architecture**.

---

# 🏢 Real enterprise architecture

A more realistic AWS design is:

```text
                         Corporate Users
                               │
                         VPN / ZTNA / SSO
                               │
                               ▼
                       Private AWS Network
                               │
               ┌───────────────┼────────────────┐
               │               │                │
             AZ-A             AZ-B              AZ-C
               │               │                │
          ┌────────┐      ┌────────┐       ┌────────┐
          │Broker 1│      │Broker 3│       │Broker 5│
          │Broker 2│      │Broker 4│       │Broker 6│
          └────────┘      └────────┘       └────────┘
               │               │                │
               └───────────────┼────────────────┘
                               │
                         Kafka Cluster
                               │
             ┌─────────────────┼─────────────────┐
             │                 │                 │
             ▼                 ▼                 ▼
       Schema Registry    Kafka Connect      Monitoring
             │                 │                 │
             ▼                 ▼                 ▼
        Governance          Data Lake       Prometheus
                                             Grafana
```

---

# ⭐ 3. Real-World Architecture

There are two important dimensions:

## Dimension 1 — Kafka cluster

```text
Broker layer
+
Controller layer
+
Storage
+
Networking
```

## Dimension 2 — Kafka platform

```text
Kafka
+
Schema Registry
+
Kafka Connect
+
Monitoring
+
Security
+
Governance
+
Management UI
+
DR
```

That distinction is extremely important for a **Platform Engineer**.

---

# 🌎 4. AWS Multi-AZ Design

Production Kafka should normally be spread across multiple Availability Zones.

AWS recommends using multiple Availability Zones when high availability and fault tolerance are required.

Example:

```text
AWS Region: ap-south-1
│
├── AZ-A
│   ├── Kafka Broker 1
│   └── Kafka Broker 2
│
├── AZ-B
│   ├── Kafka Broker 3
│   └── Kafka Broker 4
│
└── AZ-C
    ├── Kafka Broker 5
    └── Kafka Broker 6
```

Why?

Because:

```text
Single AZ failure
       ↓
Kafka cluster remains available
```

Instead of:

```text
AZ-A
 │
 ├── Broker 1
 ├── Broker 2
 └── Broker 3

AZ-A fails
     ↓
Entire Kafka cluster fails ❌
```

---

# 🖥️ 5. How Many EC2 Instances?

There is no universal number.

Kafka capacity is driven by:

```text
Throughput
+
Partition count
+
Replication
+
Storage
+
Network
+
CPU
+
Memory
+
Availability requirement
```

## Example environments

| Environment | Typical starting point |
|---|---:|
| Developer laptop | 1 |
| Training | 1–3 |
| QA | 3 |
| Small production | 3 |
| Medium production | 6+ |
| Large production | 10+ |
| Very large | Capacity-driven |

---

# ⭐ Recommended VishwaTech Production-Style Lab

For learning:

```text
3 Kafka brokers
+
1 management EC2
```

Total:

```text
4 EC2
```

For a stronger multi-AZ lab:

```text
AZ-A → Kafka-1
AZ-B → Kafka-2
AZ-C → Kafka-3

Management EC2 → AZ-A
```

For a closer production simulation:

```text
AZ-A → Kafka-1 + Management
AZ-B → Kafka-2
AZ-C → Kafka-3
```

---

# 🧩 6. Broker and Controller Architecture

## Training configuration

We use:

```text
process.roles=broker,controller
```

Therefore:

```text
Kafka-1
 ├── Broker
 └── Controller

Kafka-2
 ├── Broker
 └── Controller

Kafka-3
 ├── Broker
 └── Controller
```

This is called **combined mode**.

It is excellent for:

- labs
- development
- learning KRaft
- small environments

---

# 🏭 Production approach

For critical production systems, Kafka can use dedicated controller nodes:

```text
                 KRaft Controller Quorum
                 ┌──────┬──────┬──────┐
                 │ C1   │ C2   │ C3   │
                 └──────┴──────┴──────┘
                         │
                    Metadata
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
         B1             B2             B3
         B4             B5             B6
```

Why separate them?

Because:

```text
Controller
    ↓
Metadata management

Broker
    ↓
Client traffic
    ↓
Partition storage
    ↓
Replication
```

Separating roles reduces resource contention and makes larger deployments easier to operate.

Kafka's KRaft documentation recommends avoiding combined broker/controller mode for critical production deployments.

---

# 🌐 7. Networking Design

This is one of the biggest differences between a lab and production.

## Lab

```text
Public IP
   ↓
Kafka
```

## Production

```text
Application
    │
    ▼
Private DNS
    │
    ▼
Private IP
    │
    ▼
Kafka
```

The Kafka brokers should normally live in private networking.

AWS recommends private subnets for resources that should not be directly accessible from the Internet.

---

# 🔐 8. Public IP vs Private IP

## Public IP

Used for:

```text
Internet-facing services
```

Examples:

```text
Load Balancer
Web application
VPN endpoint
```

## Private IP

Used for:

```text
Application → Kafka
Kafka → Kafka
Management → Kafka
Kafka → Schema Registry
Kafka → Connect
```

Production should normally use:

```text
Private IP
+
Private DNS
+
Security Groups
```

rather than:

```text
Public IP
+
PLAINTEXT
```

---

# 🚨 9. If You MUST Expose Kafka Publicly

For this VishwaTech training lab, you may intentionally expose:

```text
PUBLIC_IP:9092
```

But:

```text
Do NOT use 0.0.0.0/0
```

Prefer:

```text
YOUR_PUBLIC_IP/32
```

or a trusted corporate CIDR.

AWS recommends least-permissive Security Group rules and warns against unrestricted access.

---

# 🌍 10. DNS Design

Production should avoid hard-coding IP addresses everywhere.

Instead:

```text
kafka-1.kafka.internal
kafka-2.kafka.internal
kafka-3.kafka.internal
```

Or:

```text
kafka-bootstrap.kafka.internal
```

Example:

```text
Application
    │
    ▼
kafka-bootstrap.internal.company
    │
    ▼
Kafka
```

Use:

- Route 53 private hosted zones
- corporate DNS
- service discovery
- managed Kafka endpoints

---

# 🛡️ 11. Security Groups

Create separate groups.

```text
SG-KAFKA
SG-KAFKA-MANAGEMENT
SG-KAFKA-CLIENTS
```

## Kafka SG

```text
22
→ administration only

9092
→ trusted Kafka clients only

19092
→ Kafka SG / management SG

9093
→ Kafka controller SG
```

Never expose:

```text
9093 → Internet
19092 → Internet
```

AWS Security Groups act as instance-level firewalls and can reference other Security Groups, which is ideal for private Kafka traffic.

---

# 🔥 12. Kafka Listener Architecture

Use separate listeners.

```text
INTERNAL
    ↓
19092

EXTERNAL
    ↓
9092

CONTROLLER
    ↓
9093
```

Example Kafka-1:

```properties
listeners=INTERNAL://10.0.1.11:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://10.0.1.11:9093

advertised.listeners=INTERNAL://10.0.1.11:19092,EXTERNAL://PUBLIC_IP_1:9092
```

Kafka-2:

```properties
listeners=INTERNAL://10.0.2.11:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://10.0.2.11:9093

advertised.listeners=INTERNAL://10.0.2.11:19092,EXTERNAL://PUBLIC_IP_2:9092
```

Kafka-3:

```properties
listeners=INTERNAL://10.0.3.11:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://10.0.3.11:9093

advertised.listeners=INTERNAL://10.0.3.11:19092,EXTERNAL://PUBLIC_IP_3:9092
```

---

# 🧠 13. `advertised.listeners` Explained

This is one of the most important Kafka concepts.

`listeners` means:

> Where Kafka binds/listens.

`advertised.listeners` means:

> What Kafka tells clients to use.

Example:

```text
Kafka binds:

0.0.0.0:9092
```

But advertises:

```text
18.x.x.x:9092
```

because clients need a reachable address.

In AWS:

```text
listeners
      ↓
local network interface

advertised.listeners
      ↓
client-reachable endpoint
```

Kafka explicitly documents `advertised.listeners` as useful in cloud/IaaS environments where the bind address differs from the client-reachable address.

---

# 🧩 14. Common vs Node-Specific Configuration

This is critical.

## Same on all brokers

```properties
process.roles=broker,controller

controller.listener.names=CONTROLLER

inter.broker.listener.name=INTERNAL

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

controller.quorum.voters=1@10.0.1.11:9093,2@10.0.2.11:9093,3@10.0.3.11:9093

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

private IP

public IP

listeners

advertised.listeners
```

---

# 🧠 15. KRaft Quorum

Three controllers:

```text
C1
C2
C3
```

Majority:

```text
2
```

Therefore:

```text
C1 ❌
C2 ✅
C3 ✅

Quorum survives
```

But:

```text
C1 ❌
C2 ❌
C3 ✅

Quorum lost
```

This is why odd controller counts are common.

---

# 📦 16. Replication

Suppose:

```text
Topic = orders
Partition = P0
Replication Factor = 3
```

Kafka may have:

```text
Broker-1 → Leader
Broker-2 → Replica
Broker-3 → Replica
```

If Broker-1 dies:

```text
Broker-2 or Broker-3
       ↓
new leader
```

Applications can continue if the cluster remains healthy.

---

# 🟢 17. ISR

ISR means:

> In-Sync Replicas

Example:

```text
Replicas:
1,2,3

ISR:
1,2,3
```

Everything is synchronized.

If Broker-3 falls behind:

```text
Replicas:
1,2,3

ISR:
1,2
```

Broker-3 is no longer considered caught up enough to participate in the protected replica set.

---

# 🛡️ 18. `min.insync.replicas`

Production-style setting:

```properties
default.replication.factor=3
min.insync.replicas=2
```

Meaning:

```text
RF = 3

At least 2 ISR replicas
required for protected writes
```

Combine with:

```text
acks=all
```

on producers.

This gives:

```text
Producer
   │
acks=all
   ▼
Kafka
 ├── Replica 1
 ├── Replica 2
 └── Replica 3
```

---

# ✍️ 19. Producer Durability

Important producer configuration:

```properties
acks=all
enable.idempotence=true
```

Conceptually:

```text
Producer
   │
   ├── retries
   ├── idempotence
   └── acks=all
           │
           ▼
        Kafka
```

This is much safer than:

```text
acks=0
```

for important data.

---

# 👥 20. Consumer Groups and Lag

Production systems commonly have:

```text
Application A
   │
Consumer Group A
   │
Partitions
```

Example:

```text
Topic: orders
Partitions: 6

Consumer-1 → P0,P1
Consumer-2 → P2,P3
Consumer-3 → P4,P5
```

If processing falls behind:

```text
Log End Offset = 10000
Current Offset = 8500

Lag = 1500
```

Consumer lag is one of the most important operational Kafka metrics.

---

# 🧱 21. Topic and Partition Design

Do not blindly create:

```text
1000 partitions
```

Partition count affects:

```text
Memory
File descriptors
Controller metadata
Recovery time
Rebalancing
Network
Broker overhead
```

Choose partitions based on:

```text
Expected throughput
+
Consumer parallelism
+
Retention
+
Growth
```

---

# 🔐 22. Security Architecture

Production Kafka should normally look like:

```text
Application
    │
    ▼
TLS
    │
    ▼
Authentication
    │
    ▼
Authorization
    │
    ▼
Kafka
```

Security layers:

```text
Network Security
       ↓
TLS
       ↓
SASL / OAuth / mTLS
       ↓
ACL
       ↓
RBAC / Platform governance
       ↓
Audit
```

---

# 🔒 23. TLS

TLS provides encryption in transit.

Without TLS:

```text
Client ── plaintext ──> Kafka
```

With TLS:

```text
Client
   │
 encrypted
   │
   ▼
 Kafka
```

Use TLS for:

```text
Client → Kafka
Broker → Broker
Management → Kafka
Kafka Connect → Kafka
Schema Registry → Kafka
```

---

# 🔑 24. SASL

SASL provides authentication.

Common mechanisms include:

```text
SASL/SCRAM
SASL/OAUTHBEARER
```

Depending on the enterprise identity architecture.

Concept:

```text
Application
    │
username/password
or token
    │
    ▼
Kafka authentication
    │
    ▼
ACL evaluation
```

---

# 🛂 25. ACL

ACL answers:

> What is this identity allowed to do?

Example:

```text
payment-service

READ  payments
WRITE payments
```

But:

```text
READ hr-data ❌
DELETE topic ❌
ALTER cluster ❌
```

This is least privilege.

---

# 👤 26. Enterprise Identity

In mature companies, Kafka identities are integrated with enterprise identity systems.

Examples:

```text
Okta
Microsoft Entra ID
OIDC
LDAP
mTLS certificates
Cloud IAM
```

Concept:

```text
Developer
   │
   ▼
SSO
   │
   ▼
Identity
   │
   ▼
Kafka Platform
   │
   ▼
RBAC
```

---

# 🖥️ 27. Management Plane

Your Option-B management architecture is good:

```text
                 Management EC2
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
     Kafbat           Kpow        Conduktor
      :8080           :3000          :8081
        │              │              │
        └──────────────┼──────────────┘
                       │
                  Private Kafka
```

The management applications should use:

```text
10.x.x.x:19092
```

rather than public Kafka addresses when they live in the same VPC.

---

# 🧬 28. Schema Registry

Enterprise Kafka commonly uses schemas.

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
Consumer
```

Supported formats commonly include:

```text
Avro
JSON Schema
Protobuf
```

Schema compatibility prevents one application team from accidentally breaking consumers.

---

# 🔌 29. Kafka Connect

Kafka Connect moves data into/out of Kafka.

## Source

```text
Database
   │
   ▼
Kafka Connect
   │
   ▼
Kafka
```

## Sink

```text
Kafka
   │
   ▼
Kafka Connect
   │
   ▼
S3 / Snowflake / Elasticsearch / DB
```

This becomes a major enterprise data integration layer.

---

# 📊 30. Observability

Production Kafka must be observable.

```text
Kafka
 │
 ├── Metrics
 │
 ├── Logs
 │
 ├── Traces where applicable
 │
 └── Events
```

Typical platform:

```text
Kafka
  │
  ▼
Prometheus
  │
  ▼
Grafana
  │
  ▼
Alertmanager
```

---

# 🚨 31. Critical Kafka KPIs

| KPI | Why it matters |
|---|---|
| Broker availability | Broker health |
| Offline partitions | Immediate availability problem |
| Under-replicated partitions | Replication health |
| ISR count | Replica synchronization |
| Consumer lag | Application processing health |
| Request latency | Kafka performance |
| Produce rate | Incoming workload |
| Fetch rate | Consumer workload |
| Disk utilization | Storage capacity |
| Disk I/O | Storage bottleneck |
| Network throughput | Network capacity |
| CPU | Compute pressure |
| JVM heap | Kafka process health |
| KRaft quorum | Metadata/control-plane health |

---

# 📈 32. Capacity Planning

Don't choose broker count only by:

```text
"Kafka recommends 3"
```

Instead calculate:

```text
Peak producer throughput
+
Peak consumer throughput
+
Replication overhead
+
Retention
+
Growth
+
Failure capacity
```

Example:

```text
Incoming:
500 MB/s

RF=3

Approximate replication traffic:
additional copies × workload
```

Then consider:

```text
Network
Disk write
Disk read
CPU
Consumer traffic
```

---

# 💾 33. Storage

Kafka is heavily dependent on storage performance.

Production decisions include:

```text
EBS gp3 / io2
Provisioned IOPS
Throughput
Disk size
Retention
Segment size
Log cleanup
```

Monitor:

```text
Disk %
Disk latency
IOPS
Throughput
```

Never allow Kafka storage to reach:

```text
100%
```

---

# 💥 34. Failure Scenarios

A production platform must be tested.

## Failure 1 — Broker failure

```text
Broker-1 ❌
Broker-2 ✅
Broker-3 ✅
```

Test:

```text
Leader election
ISR
Producer behavior
Consumer behavior
```

---

## Failure 2 — Controller failure

```text
Controller-1 ❌
Controller-2 ✅
Controller-3 ✅
```

Quorum survives.

---

## Failure 3 — AZ failure

```text
AZ-A ❌
```

Kafka should remain available if replicas are properly distributed across AZs.

---

## Failure 4 — Network partition

```text
Broker-1
   X
Broker-2
   │
Broker-3
```

Study:

```text
ISR
Leader
Quorum
Client behavior
```

---

## Failure 5 — Disk pressure

```text
Disk 90%
Disk 95%
Disk 100%
```

Test:

```text
Alerts
Retention
Operational response
```

---

# 🔄 35. Rolling Restart

Never restart every production broker simultaneously.

Bad:

```text
B1 ❌
B2 ❌
B3 ❌
```

Good:

```text
B1 restart
 ↓
healthy
 ↓
B2 restart
 ↓
healthy
 ↓
B3 restart
```

This is:

> Rolling restart.

---

# 🌎 36. AZ Failure Design

Example:

```text
Region
│
├── AZ-A
│    ├── Broker 1
│    └── Broker 2
│
├── AZ-B
│    ├── Broker 3
│    └── Broker 4
│
└── AZ-C
     ├── Broker 5
     └── Broker 6
```

Replication should be distributed so a complete AZ outage does not destroy all replicas of a partition.

For larger production environments, Kafka placement/rack-awareness should be designed carefully so replicas are not unnecessarily concentrated in the same failure domain.

---

# 🆘 37. Disaster Recovery

High availability is not the same as disaster recovery.

## HA

Protects against:

```text
Broker failure
AZ failure
```

## DR

Protects against:

```text
Region failure
Major corruption
Human error
Security incident
```

Example:

```text
AWS Region A
     │
     │ replication
     ▼
AWS Region B
```

Possible technologies/approaches:

```text
MirrorMaker 2
Cluster Linking
Managed replication
Application-level replication
```

---

# 💾 38. Backup and Recovery

Do not assume:

```text
Kafka replication = backup
```

It is not the same thing.

Replication protects:

```text
availability
```

Backup/DR protects:

```text
recovery from catastrophic events
```

For enterprise planning consider:

```text
RPO
RTO
Retention
DR region
Data reconstruction
Configuration backup
Secrets backup
IaC
```

---

# 🤖 39. Automation

Real companies don't SSH manually into 50 brokers and run commands one by one.

They use:

```text
Git
 ↓
CI/CD
 ↓
Terraform
 ↓
Ansible
 ↓
Kafka
```

Or Kubernetes operators/platform automation.

---

# 🏗️ 40. Infrastructure as Code

AWS infrastructure:

```text
Terraform
   │
   ├── VPC
   ├── Subnets
   ├── Route tables
   ├── Security Groups
   ├── EC2
   ├── IAM
   └── EBS
```

Kafka configuration:

```text
Ansible / Terraform / automation
        │
        ▼
Kafka configuration
```

---

# 🔐 41. Secrets Management

Never store production secrets inside:

```text
server.properties
GitHub
README.md
shell scripts
```

Use:

```text
AWS Secrets Manager
HashiCorp Vault
AWS Systems Manager Parameter Store
```

Concept:

```text
Kafka
  │
  ▼
Secrets Manager / Vault
  │
  ▼
Credentials
```

---

# 🛡️ 42. Platform Security Architecture

For your Platform Security background, think in layers:

```text
Layer 1
AWS IAM

Layer 2
VPC

Layer 3
Security Groups

Layer 4
Private Subnets

Layer 5
TLS

Layer 6
Authentication

Layer 7
Authorization / ACL

Layer 8
Secrets Management

Layer 9
Audit

Layer 10
Monitoring / SIEM
```

---

# 🧪 43. Security Test Cases

Once the basic cluster works, test:

```text
01 Unauthorized client
02 Invalid certificate
03 Expired certificate
04 Invalid SASL credentials
05 Valid credentials + unauthorized topic
06 Topic creation authorization
07 Topic deletion authorization
08 Consumer group authorization
09 Admin privilege escalation
10 Secret leakage
11 Public port exposure
12 Security Group misconfiguration
13 Controller port exposure
14 Broker port exposure
15 TLS downgrade attempts
```

This turns your Kafka lab into a **Kafka Security Lab**.

---

# ☁️ 44. Managed Kafka in Real Companies

Not every company manages Kafka themselves.

On AWS, a major option is:

> Amazon MSK

Architecture:

```text
Applications
     │
     ▼
Amazon MSK
     │
     ├── Broker
     ├── Broker
     └── Broker
```

Advantages:

```text
Less infrastructure management
AWS integration
Managed broker lifecycle
Managed networking integration
```

But you still need to understand:

```text
Kafka
Networking
Security
Replication
Monitoring
Capacity
ACL
Applications
```

Managed Kafka does not eliminate Kafka engineering.

---

# ⚖️ 45. Self-Managed vs Managed

| Area | Self-managed EC2 | Managed Kafka |
|---|---|---|
| OS management | You | Provider |
| Kafka upgrades | You | Provider/service |
| Broker lifecycle | You | Provider |
| Networking | You | You + provider |
| Security | You | You + provider |
| Kafka knowledge | Required | Required |
| Flexibility | High | Service-dependent |
| Operations | More work | Less work |
| Cost model | Infrastructure + operations | Service + usage |
| Learning value | Excellent | Excellent |

---

# 🏭 46. What Would a Mature Enterprise Choose?

It depends.

### Small platform team

```text
Managed Kafka
```

### Kafka-heavy company

```text
Self-managed Kafka
```

### Regulated environment

```text
Architecture + security + compliance requirements
```

### Large AWS organization

```text
Amazon MSK
+
Private VPC
+
IAM
+
Monitoring
+
Security controls
```

There is no universal answer.

---

# 🔥 47. Real-World Production Checklist

## Infrastructure

```text
[ ] Multiple AZs
[ ] Capacity-based broker count
[ ] Dedicated EBS
[ ] Encrypted EBS
[ ] Backup strategy
[ ] Patch strategy
```

## Networking

```text
[ ] Private subnets
[ ] Private DNS
[ ] No unnecessary public IPs
[ ] Security Groups
[ ] Network ACLs where appropriate
[ ] VPC Flow Logs
```

## Kafka

```text
[ ] KRaft
[ ] Controller quorum
[ ] Replication factor
[ ] min.insync.replicas
[ ] acks=all
[ ] Idempotence
[ ] Partition strategy
```

## Security

```text
[ ] TLS
[ ] SASL / OAuth / mTLS
[ ] ACL
[ ] RBAC
[ ] Secrets management
[ ] Certificate rotation
[ ] Audit
```

## Operations

```text
[ ] Prometheus
[ ] Grafana
[ ] Alerting
[ ] Central logs
[ ] Consumer lag monitoring
[ ] Disk monitoring
[ ] Incident response
```

## Data platform

```text
[ ] Schema Registry
[ ] Kafka Connect
[ ] Data governance
[ ] Data classification
[ ] Retention
```

## DR

```text
[ ] RPO
[ ] RTO
[ ] Multi-region strategy
[ ] Replication
[ ] DR testing
```

---

# 📊 48. Production vs VishwaTech Lab

| Capability | Current Lab | Production |
|---|---|---|
| Brokers | 3 | Capacity-driven |
| Controllers | Combined | Often dedicated |
| AZ | Could be 1 | 3 AZ preferred |
| Public IP | Yes for lab | Normally no |
| Private DNS | Optional | Yes |
| TLS | Future | Required |
| SASL | Future | Usually |
| ACL | Future | Required |
| Schema Registry | Future | Common |
| Kafka Connect | Future | Common |
| Monitoring | Future | Required |
| DR | Future | Required |
| IaC | Script | Terraform/automation |
| Secrets | Env/config | Vault/Secrets Manager |
| Management UI | Kafbat/Kpow/Conduktor | Controlled/private |
| Kafka service | EC2 | EC2 or managed Kafka |
| Failure testing | Manual | Regular/automated |

---

# 🧭 49. VishwaTech Lab Roadmap

This is the recommended learning sequence.

```text
LEVEL 1
Single Kafka
        ↓
LEVEL 2
KRaft
        ↓
LEVEL 3
3-Broker KRaft
        ↓
LEVEL 4
Replication
        ↓
LEVEL 5
ISR + Leader Election
        ↓
LEVEL 6
Kafbat
        ↓
LEVEL 7
Kpow
        ↓
LEVEL 8
Conduktor
        ↓
LEVEL 9
Multi-AZ
        ↓
LEVEL 10
TLS
        ↓
LEVEL 11
SASL/SCRAM
        ↓
LEVEL 12
ACL
        ↓
LEVEL 13
Schema Registry
        ↓
LEVEL 14
Kafka Connect
        ↓
LEVEL 15
Prometheus
        ↓
LEVEL 16
Grafana
        ↓
LEVEL 17
Alerting
        ↓
LEVEL 18
Conduktor Gateway
        ↓
LEVEL 19
DR / Multi-region
        ↓
LEVEL 20
AWS MSK
```

---

# 🧪 50. Recommended Hands-On Labs

## Cluster

```text
Lab 01 — Create VPC
Lab 02 — Create 3 AZ subnets
Lab 03 — Create Kafka SG
Lab 04 — Create Management SG
Lab 05 — Create 3 Kafka EC2s
Lab 06 — Create Management EC2
Lab 07 — Install Java
Lab 08 — Install Kafka
Lab 09 — Configure KRaft
Lab 10 — Verify quorum
```

## Kafka

```text
Lab 11 — Create topic
Lab 12 — Partitions
Lab 13 — RF=3
Lab 14 — Producer
Lab 15 — Consumer
Lab 16 — Consumer group
Lab 17 — Lag
Lab 18 — ISR
Lab 19 — Leader election
```

## HA

```text
Lab 20 — Kill Broker 1
Lab 21 — Observe leader change
Lab 22 — Observe ISR
Lab 23 — Recover Broker 1
Lab 24 — Kill Broker 2
Lab 25 — Controller failure
Lab 26 — Rolling restart
Lab 27 — AZ simulation
```

## Security

```text
Lab 28 — Generate CA
Lab 29 — Broker TLS
Lab 30 — Client TLS
Lab 31 — SASL/SCRAM
Lab 32 — Kafka ACL
Lab 33 — Least privilege
Lab 34 — Certificate rotation
Lab 35 — Security validation
```

## Data Platform

```text
Lab 36 — Schema Registry
Lab 37 — Avro
Lab 38 — JSON Schema
Lab 39 — Protobuf
Lab 40 — Kafka Connect
Lab 41 — Source connector
Lab 42 — Sink connector
```

## Observability

```text
Lab 43 — Prometheus
Lab 44 — Grafana
Lab 45 — Broker dashboard
Lab 46 — Consumer lag dashboard
Lab 47 — Alerting
```

## Enterprise

```text
Lab 48 — Conduktor
Lab 49 — Conduktor Gateway
Lab 50 — DR
Lab 51 — Multi-region
Lab 52 — Compare self-managed vs MSK
```

---

# 🧠 51. The Most Important Enterprise Mental Model

Do not think:

```text
"Kafka = 3 servers"
```

Think:

```text
Kafka Platform
│
├── Compute
│
├── Storage
│
├── Networking
│
├── KRaft
│
├── Replication
│
├── Security
│
├── Identity
│
├── Authorization
│
├── Governance
│
├── Schema
│
├── Integration
│
├── Monitoring
│
├── Automation
│
└── DR
```

That is how you should answer architecture questions in a Principal/Staff-level interview.

---

# 🏆 52. Final Enterprise Architecture

```text
                             USERS
                               │
                         SSO / VPN / ZTNA
                               │
                               ▼
                       PRIVATE AWS NETWORK
                               │
             ┌─────────────────┼──────────────────┐
             │                 │                  │
           AZ-A               AZ-B               AZ-C
             │                 │                  │
       ┌────────────┐    ┌────────────┐    ┌────────────┐
       │ Kafka B1   │    │ Kafka B3   │    │ Kafka B5   │
       │ Kafka B2   │    │ Kafka B4   │    │ Kafka B6   │
       └──────┬─────┘    └──────┬─────┘    └──────┬─────┘
              │                  │                  │
              └──────────────────┼──────────────────┘
                                 │
                          KAFKA CLUSTER
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
        ▼                        ▼                        ▼
 Schema Registry           Kafka Connect             Monitoring
        │                        │                        │
        ▼                        ▼                        ▼
  Data Governance           Data Platforms        Prometheus/Grafana
        │
        ▼
 Kafbat / Kpow / Conduktor
        │
        ▼
 Security / RBAC / Audit
```

---

# 🎯 53. What Your Current 4-EC2 Lab Represents

Your VishwaTech lab:

```text
EC2-1
Kafka Broker 1
+
KRaft Controller

EC2-2
Kafka Broker 2
+
KRaft Controller

EC2-3
Kafka Broker 3
+
KRaft Controller

EC2-4
Management
├── Kafbat
├── Kpow
└── Conduktor
```

This is a **learning architecture** that teaches the same core concepts used in production.

Then the production evolution is:

```text
3 EC2
   ↓
3 AZ
   ↓
6+ brokers
   ↓
Dedicated controllers
   ↓
Private networking
   ↓
TLS
   ↓
SASL/OAuth
   ↓
ACL/RBAC
   ↓
Schema Registry
   ↓
Kafka Connect
   ↓
Prometheus/Grafana
   ↓
Automation
   ↓
DR
   ↓
Managed Kafka / MSK
```

---

# 📚 54. Official Documentation

### Apache Kafka

- Kafka Documentation: https://kafka.apache.org/documentation/
- KRaft: https://kafka.apache.org/40/operations/kraft/
- Broker Configuration: https://kafka.apache.org/40/configuration/broker-configs/
- Listener Configuration: https://kafka.apache.org/40/security/listener-configuration/
- Security: https://kafka.apache.org/documentation/#security
- Kafka Connect: https://kafka.apache.org/documentation/#connect
- Kafka Replication: https://kafka.apache.org/documentation/#replication

### AWS

- EC2 Best Practices: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-best-practices.html
- EC2 Security Groups: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html
- VPC Security Best Practices: https://docs.aws.amazon.com/vpc/latest/userguide/vpc-security-best-practices.html
- EC2 IP Addressing: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html
- Amazon MSK: https://aws.amazon.com/msk/

### Management

- Kafbat UI: https://github.com/kafbat/kafka-ui
- Kpow: https://docs.factorhouse.io/kpow/
- Conduktor: https://docs.conduktor.io/guide

---

# 🏁 55. Final Takeaway

### Your lab

```text
3 Kafka EC2
+
1 Management EC2
```

### Real production

```text
Multi-AZ
+
Private networking
+
Capacity-driven broker count
+
Replication
+
KRaft quorum
+
TLS
+
Authentication
+
ACL/RBAC
+
Schema Registry
+
Kafka Connect
+
Monitoring
+
Automation
+
DR
```

> **The objective is not merely to run Kafka. The objective is to build a secure, highly available, observable and governable Kafka platform.**

---

# 👨‍💻 VishwaTech Platform Engineering Path

```text
AWS
 ↓
Linux
 ↓
Networking
 ↓
Kafka
 ↓
KRaft
 ↓
High Availability
 ↓
Security
 ↓
IAM / Identity
 ↓
TLS / SASL
 ↓
ACL / RBAC
 ↓
Schema Registry
 ↓
Kafka Connect
 ↓
Observability
 ↓
Automation
 ↓
DR
 ↓
AWS MSK
 ↓
Enterprise Kafka Platform
```

**VishwaTech Labs — Learn it. Break it. Secure it. Automate it. Operate it.**
