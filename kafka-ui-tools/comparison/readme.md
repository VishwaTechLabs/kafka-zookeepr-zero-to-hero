
# 🚀 VishwaTech Labs — Apache Kafka GUI Tools Master Guide

![Kafka](https://img.shields.io/badge/Apache%20Kafka-GUI%20Tools-black?logo=apachekafka)
![Kafbat](https://img.shields.io/badge/Recommended-Kafbat%20UI-2ea44f)
![AWS](https://img.shields.io/badge/AWS-EC2-orange?logo=amazonaws)
![KRaft](https://img.shields.io/badge/Kafka-KRaft-blue)
![ZooKeeper](https://img.shields.io/badge/Kafka-ZooKeeper-purple)
![Linux](https://img.shields.io/badge/Linux-Amazon%20Linux%202023-orange?logo=linux)
![Java](https://img.shields.io/badge/Java-17-red?logo=openjdk)
![Training](https://img.shields.io/badge/VishwaTech-Labs-1f6feb)

> 🎓 **VishwaTech Labs — Practical Kafka GUI Master Guide**
>
> Learn Kafka administration visually using **Kafbat UI, AKHQ, Redpanda Console, Conduktor, Offset Explorer, Kpow, Lenses, Kafdrop, Confluent Control Center and CMAK**.

---

## 📚 Table of Contents

- [1. What is a Kafka GUI?](#1--what-is-a-kafka-gui)
- [2. Why do we need a Kafka GUI?](#2--why-do-we-need-a-kafka-gui)
- [3. CLI vs GUI](#3--cli-vs-gui)
- [4. Kafka GUI architecture](#4--kafka-gui-architecture)
- [5. Tools covered](#5--tools-covered)
- [6. Master comparison](#6--master-comparison)
- [7. Kafbat UI](#7--kafbat-ui)
- [8. AKHQ](#8--akhq)
- [9. Redpanda Console](#9--redpanda-console)
- [10. Conduktor](#10--conduktor)
- [11. Offset Explorer](#11--offset-explorer)
- [12. Kpow](#12--kpow)
- [13. Lenses](#13--lenses)
- [14. Kafdrop](#14--kafdrop)
- [15. Confluent Control Center](#15--confluent-control-center)
- [16. CMAK](#16--cmak)
- [17. GUI feature matrix](#17--gui-feature-matrix)
- [18. GUI selection by use case](#18--gui-selection-by-use-case)
- [19. Recommended VishwaTech learning path](#19--recommended-vishwatech-learning-path)
- [20. Kafbat + AWS EC2 architecture](#20--kafbat--aws-ec2-architecture)
- [21. KRaft GUI architecture](#21--kraft-gui-architecture)
- [22. ZooKeeper GUI architecture](#22--zookeeper-gui-architecture)
- [23. Security considerations](#23--security-considerations)
- [24. What students should learn](#24--what-students-should-learn)
- [25. Practical lab roadmap](#25--practical-lab-roadmap)
- [26. Troubleshooting](#26--troubleshooting)
- [27. Production guidance](#27--production-guidance)
- [28. Official resources](#28--official-resources)
- [29. Final recommendation](#29--final-recommendation)

---

# 1. 🧠 What is a Kafka GUI?

A **Kafka GUI** is a graphical/web/desktop application that connects to a Kafka cluster and allows engineers to **observe, inspect and manage Kafka resources visually**.

Instead of typing:

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

a GUI can show:

```text
┌─────────────────────────────────────────┐
│             KAFKA GUI                   │
├─────────────────────────────────────────┤
│ 🖥 Cluster                              │
│                                         │
│  Brokers                                │
│  Topics                                 │
│  Partitions                             │
│  Messages                               │
│  Consumer Groups                        │
│  Consumer Lag                           │
│  Schemas                                │
│  Connectors                             │
│  ACLs / Security                        │
└─────────────────────────────────────────┘
```

### Important

A GUI does **not replace Kafka**.

It is a management/observability layer that talks to Kafka APIs and related components.

---

# 2. 🎯 Why do we need a Kafka GUI?

Kafka CLI commands are extremely important because they teach the underlying platform.

But GUIs are valuable when you need to:

- 👀 visually inspect topics
- 🔎 search messages
- 📊 understand partitions
- 👥 inspect consumer groups
- 📉 investigate consumer lag
- ⚙️ inspect topic configuration
- 🧩 inspect schemas
- 🔌 inspect Kafka Connect
- 🔐 manage/inspect access control where supported
- 🐛 troubleshoot message-flow problems
- 🏢 manage multiple clusters
- 🎓 teach students visually

### Example

CLI:

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-consumer
```

GUI:

```text
Consumer Groups
       │
       ▼
┌──────────────────────────────────────────┐
│ order-consumer                           │
├──────────┬──────────────┬───────────────┤
│ Topic    │ Partition    │ Lag           │
├──────────┼──────────────┼───────────────┤
│ orders   │ 0            │ 0             │
│ orders   │ 1            │ 12            │
│ orders   │ 2            │ 0             │
└──────────┴──────────────┴───────────────┘
```

The GUI makes the **same Kafka concepts easier to visualize**.

---

# 3. ⚔️ CLI vs GUI

| Capability | Kafka CLI | Kafka GUI |
|---|---:|---:|
| Learn Kafka fundamentals | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Automation | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| CI/CD | ⭐⭐⭐⭐⭐ | ⭐ |
| Scripting | ⭐⭐⭐⭐⭐ | ⭐ |
| Message inspection | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Visual topic management | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| Consumer lag visualization | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Beginner friendly | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Troubleshooting | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Production operations | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Teaching | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

### 🧠 Golden rule

> **Learn Kafka using CLI. Operate and visualize Kafka using GUI.**

---

# 4. 🏗️ Kafka GUI Architecture

A Kafka GUI normally sits **outside or beside** the Kafka cluster.

```text
                         👨‍💻 Engineer
                              │
                              ▼
                        🌐 Web Browser
                              │
                              ▼
                       ┌─────────────┐
                       │  Kafka GUI  │
                       │             │
                       │ Kafbat UI   │
                       │ AKHQ        │
                       │ Console     │
                       └──────┬──────┘
                              │
                         Kafka APIs
                              │
                              ▼
                  ┌─────────────────────┐
                  │    Kafka Cluster    │
                  ├─────────────────────┤
                  │ Broker 1            │
                  │ Broker 2            │
                  │ Broker 3            │
                  └─────────────────────┘
```

The GUI itself is **not the message broker**.

---

# 5. 🧰 Tools Covered

## 🥇 Recommended learning tools

1. **Kafbat UI**
2. **AKHQ**
3. **Redpanda Console**
4. **Conduktor**

## 🏢 Enterprise-oriented tools

5. **Kpow**
6. **Lenses**
7. **Confluent Control Center**

## 🛠 Developer / lightweight tools

8. **Offset Explorer**
9. **Kafdrop**

## 🕰️ Legacy / awareness

10. **CMAK**

---

# 6. 🏆 Master Comparison

| # | Tool | Interface | Open Source | Best For | Difficulty | Enterprise |
|---:|---|---|---|---|---|---|
| 🥇 | **Kafbat UI** | Web | ✅ | Learning + DevOps | ⭐ | ⭐⭐⭐ |
| 🥈 | **AKHQ** | Web | ✅ | Administration | ⭐⭐ | ⭐⭐⭐ |
| 🥉 | **Redpanda Console** | Web | Community/Commercial | Developers + operations | ⭐⭐ | ⭐⭐⭐⭐ |
| 4 | **Conduktor** | Web/Desktop | Commercial | Enterprise Kafka | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| 5 | **Offset Explorer** | Desktop | Commercial | Developers | ⭐⭐ | ⭐⭐⭐ |
| 6 | **Kpow** | Web | Commercial | Kafka operations | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| 7 | **Lenses** | Web | Commercial | DataOps / streaming | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| 8 | **Kafdrop** | Web | ✅ | Lightweight inspection | ⭐ | ⭐ |
| 9 | **Confluent Control Center** | Web | Platform component | Confluent environments | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| 10 | **CMAK** | Web | ✅ | Legacy Kafka management | ⭐⭐ | ⭐⭐ |

> ⚠️ Licensing, packaging and feature availability can change by edition/version. Always check the vendor's current licensing page before using a tool commercially.

---

# 7. 🥇 Kafbat UI

🔗 **Official documentation:**  
https://ui.docs.kafbat.io/

🔗 **GitHub:**  
https://github.com/kafbat/kafka-ui

### ⭐ Recommendation

**BEST OVERALL FOR VISHWATECH LABS**

Kafbat UI is a modern Kafka web interface and is particularly suitable for learning and development labs.

### Core capabilities

```text
Kafbat UI
│
├── 🏢 Clusters
├── 🖥 Brokers
├── 📚 Topics
│   ├── Partitions
│   ├── Messages
│   └── Configuration
├── 👥 Consumer Groups
│   └── Lag
├── 🧬 Schema Registry
├── 🔌 Kafka Connect
└── 🔐 Security / integrations
```

### Why I recommend it

| Requirement | Kafbat |
|---|---:|
| Student friendly | ⭐⭐⭐⭐⭐ |
| Modern interface | ⭐⭐⭐⭐⭐ |
| Topics | ✅ |
| Partitions | ✅ |
| Messages | ✅ |
| Consumer Groups | ✅ |
| Consumer Lag | ✅ |
| Schema Registry | ✅ |
| Kafka Connect | ✅ |
| Multi-cluster | ✅ |
| Docker-friendly | ✅ |
| Open source | ✅ |

### Ideal lab

```text
AWS EC2
   │
   ├── Kafka KRaft
   │     └── :9092
   │
   └── Kafbat UI
         └── :8080

Browser
   │
   └── http://EC2_PUBLIC_IP:8080
```

---

# 8. 🥈 AKHQ

🔗 **Official website:**  
https://akhq.io/

AKHQ is an open-source Kafka GUI focused on Kafka administration and inspection.

### Typical areas

```text
AKHQ
│
├── Clusters
├── Topics
├── Messages
├── Consumer Groups
├── Schema Registry
├── Kafka Connect
└── Administration
```

### Best use

- Kafka administration labs
- Open-source environments
- Multi-cluster exploration
- Message inspection
- Consumer-group investigation

### Kafbat vs AKHQ

| Feature | Kafbat | AKHQ |
|---|---:|---:|
| Open source | ✅ | ✅ |
| Web UI | ✅ | ✅ |
| Topics | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Messages | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Consumer Groups | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Schema Registry | ✅ | ✅ |
| Multi-cluster | ✅ | ✅ |
| Student learning | 🏆 | ⭐⭐⭐⭐ |

---

# 9. 🥉 Redpanda Console

🔗 **Official documentation:**  
https://docs.redpanda.com/current/console/

🔗 **Product page:**  
https://www.redpanda.com/data-streaming/redpanda-console-kafka-ui

Redpanda Console is a web UI that can work with Redpanda and Kafka API-compatible platforms.

Current documentation describes capabilities including broker/topic management, consumer groups and lag, message inspection, Schema Registry, Kafka Connect, access control and debugging/replay capabilities. citeturn0search0turn0search1

### Architecture

```text
Browser
   │
   ▼
Redpanda Console
   │
   ├── Topics
   ├── Messages
   ├── Consumer Groups
   ├── Schemas
   ├── Connectors
   └── Security
   │
   ▼
Apache Kafka
```

### Strong areas

- 🔎 Message inspection
- 📊 Consumer groups
- 📉 Consumer lag
- 🧬 Schema Registry
- 🔌 Kafka Connect
- 🔐 ACL/RBAC capabilities
- 🐛 Troubleshooting
- 🔁 Offset/replay workflows where supported

Redpanda documents message filtering, consumer lag visibility, offset management and replay/debugging features. citeturn0search0turn0search10

---

# 10. 🏢 Conduktor

🔗 **Official website:**  
https://www.conduktor.io/

Conduktor is strongly oriented toward professional and enterprise Kafka operations.

Current product information highlights multi-cluster management, topic/schema/connectors/producers/consumers, message browsing/replay, monitoring, alerts and governance/security capabilities. citeturn0search2

### Think of it as:

```text
             Conduktor
                 │
       ┌─────────┼─────────┐
       │         │         │
   Kafka Dev   Platform   Security
       │         │         │
       └─────────┼─────────┘
                 │
          Multiple Kafka
             clusters
```

### Best for

- Enterprise Kafka
- Many clusters
- Platform engineering
- Governance
- Security
- Developer self-service
- Operations

---

# 11. 🖥️ Offset Explorer

Offset Explorer is a desktop-oriented Kafka client.

```text
┌──────────────────────────────┐
│       Offset Explorer        │
├──────────────────────────────┤
│ Topics                       │
│ Partitions                   │
│ Messages                     │
│ Consumer Groups              │
└───────────────┬──────────────┘
                │
                ▼
             Kafka
```

### Best for

- Developers
- Local development
- Message inspection
- Quick Kafka connectivity testing

### Limitation

It is not the same type of web-based centralized platform as Kafbat, AKHQ or Conduktor.

---

# 12. 🏢 Kpow

Kpow is oriented toward Kafka operations and production environments.

### Think:

```text
Kpow
 │
 ├── Monitoring
 ├── Operations
 ├── Consumer Groups
 ├── Topics
 ├── Security
 ├── Multi-cluster
 └── Governance
```

### Best for

- Production operations
- Multiple Kafka clusters
- Enterprise administration
- Operations teams

---

# 13. 🧠 Lenses

Lenses is more than a simple Kafka browser.

It is oriented toward **streaming/DataOps** use cases.

```text
Kafka
 │
 ▼
Lenses
 │
 ├── Data discovery
 ├── Streaming
 ├── SQL
 ├── Governance
 ├── Data pipelines
 └── Operations
```

### Best for

- Data engineering
- Streaming teams
- DataOps
- Governance
- Enterprise streaming platforms

---

# 14. 🪶 Kafdrop

Kafdrop is a lightweight Kafka web UI.

### Typical view

```text
Kafdrop
│
├── Brokers
├── Topics
│   ├── Partitions
│   └── Messages
└── Consumer Groups
```

### Best for

- Small labs
- Quick inspection
- Development
- Teaching basic concepts

### Not my first choice for your course

For a modern 2026 Kafka curriculum, I would teach **Kafbat before Kafdrop**.

---

# 15. 🏢 Confluent Control Center

If the organization is using Confluent Platform, Control Center becomes relevant.

```text
Confluent Platform
       │
       ├── Kafka
       ├── Schema Registry
       ├── Kafka Connect
       └── Control Center
```

### Best for

- Confluent environments
- Enterprise Kafka
- Centralized monitoring
- Confluent ecosystem

---

# 16. 🕰️ CMAK

CMAK = **Cluster Manager for Apache Kafka**.

It is useful to know historically, but I would **not make it the primary GUI for a new KRaft training environment**.

Why?

Modern Kafka training should focus on current KRaft-era architecture and modern management interfaces.

---

# 17. 📊 GUI Feature Matrix

Legend:

- 🟢 Strong
- 🟡 Available / moderate
- 🔴 Limited/not the main purpose

| Feature | Kafbat | AKHQ | Redpanda Console | Conduktor | Kpow | Lenses | Kafdrop |
|---|---|---|---|---|---|---|---|
| Broker visibility | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟡 |
| Topic management | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟡 |
| Partitions | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟡 |
| Message browser | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟡 |
| Produce messages | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟡 |
| Consumer groups | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟡 |
| Consumer lag | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟡 |
| Schema Registry | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟡 |
| Kafka Connect | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🔴 |
| Multi-cluster | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🟡 |
| RBAC/Governance | 🟡 | 🟡 | 🟢 | 🟢 | 🟢 | 🟢 | 🔴 |
| Enterprise focus | 🟡 | 🟡 | 🟢 | 🟢 | 🟢 | 🟢 | 🔴 |
| Student friendliness | 🟢 | 🟢 | 🟢 | 🟢 | 🟡 | 🟡 | 🟢 |

> Feature depth depends on Kafka version, deployment mode, authentication, edition/license and integrations. Validate exact capabilities against the product documentation before production adoption.

---

# 18. 🎯 GUI Selection by Use Case

| Your Requirement | Best Choice |
|---|---|
| 🎓 Kafka students | **Kafbat UI** |
| 🧪 Personal lab | **Kafbat UI** |
| 🐳 Docker Kafka lab | **Kafbat UI** |
| ☁️ AWS EC2 Kafka | **Kafbat UI** |
| 🏗️ KRaft learning | **Kafbat UI** |
| 🏗️ ZooKeeper learning | **Kafbat UI / AKHQ** |
| 🔎 Message debugging | **Kafbat / Redpanda Console / Conduktor** |
| 👥 Consumer lag | **Kafbat / Redpanda Console / Conduktor** |
| 🏢 Enterprise Kafka | **Conduktor / Kpow / Lenses** |
| 🌐 Multiple clusters | **Conduktor / Kpow / Lenses** |
| 📊 Streaming/DataOps | **Lenses** |
| 🧰 Lightweight GUI | **Kafdrop** |
| 💻 Desktop client | **Offset Explorer** |
| 🔴 Confluent Platform | **Control Center** |

---

# 19. 🎓 Recommended VishwaTech Learning Path

Do NOT make students memorize 10 GUIs.

Use this progression:

```text
                    KAFKA GUI JOURNEY
                           │
                           ▼
                    🥇 Kafbat UI
                           │
                           ▼
                     🥈 AKHQ
                           │
                           ▼
               🥉 Redpanda Console
                           │
                           ▼
                     Conduktor
                           │
                           ▼
                 Enterprise Tools
                  /             \
               Kpow           Lenses
```

### Level 1 — Kafbat

Students learn:

- Cluster
- Broker
- Topic
- Partition
- Message
- Producer
- Consumer
- Consumer group
- Lag

### Level 2 — AKHQ

Students understand:

- Administration
- Topic inspection
- Consumer groups
- Schema Registry
- Connect

### Level 3 — Redpanda Console

Students learn:

- Advanced message exploration
- Filtering
- Debugging
- Consumer management
- Replay concepts
- Connect/schema/security concepts

### Level 4 — Conduktor

Students learn:

- Multi-cluster
- Governance
- Security
- Enterprise operations

### Level 5 — Kpow/Lenses

Students learn:

- Enterprise operations
- DataOps
- Governance
- Large-scale Kafka management

---

# 20. ☁️ Kafbat + AWS EC2 Architecture

For your existing AWS lab:

```text
                         🌐 STUDENT LAPTOP
                                │
                                │ HTTP :8080
                                ▼
                    ┌──────────────────────┐
                    │       AWS EC2        │
                    │                      │
                    │   Kafbat UI :8080    │
                    │          │           │
                    │          ▼           │
                    │   Kafka Broker :9092  │
                    │                      │
                    └──────────────────────┘
```

### Security Group

Recommended lab model:

| Port | Purpose | Source |
|---:|---|---|
| 22 | SSH | Your IP only |
| 9092 | Kafka broker | Only when external client access is required |
| 9093 | KRaft controller | ❌ Do not expose publicly |
| 8080 | Kafbat UI | Your IP only |

### Important

Do **not** expose controller port `9093` to the public internet.

---

# 21. 🟦 KRaft GUI Architecture

Your KRaft lab:

```text
                    Browser
                       │
                       │ :8080
                       ▼
                ┌─────────────┐
                │ Kafbat UI   │
                └──────┬──────┘
                       │
                       │ Kafka API
                       ▼
          ┌─────────────────────────────┐
          │        Kafka JVM            │
          │                             │
          │  ┌───────────────────────┐  │
          │  │ Broker                │  │
          │  │ :9092                 │  │
          │  └───────────────────────┘  │
          │                             │
          │  ┌───────────────────────┐  │
          │  │ KRaft Controller      │  │
          │  │ :9093                 │  │
          │  └───────────────────────┘  │
          └─────────────────────────────┘
```

### Key point

Kafbat talks to the **Kafka broker endpoint**.

The GUI does not normally connect to the KRaft controller port as a browser-facing endpoint.

---

# 22. 🟪 ZooKeeper GUI Architecture

Your ZooKeeper lab:

```text
                    Browser
                       │
                       ▼
                ┌─────────────┐
                │ Kafbat UI   │
                └──────┬──────┘
                       │
                       ▼
                Kafka Broker
                  :9092
                       │
                       ▼
                ZooKeeper
                  :2181
```

The GUI interacts with Kafka; Kafka's architecture determines how metadata coordination works.

---

# 23. 🔐 Security Considerations

A GUI introduces another administrative surface.

Never think:

```text
Kafka = secured
Therefore GUI = secured
```

Instead:

```text
                 SECURITY
                    │
       ┌────────────┼────────────┐
       │            │            │
    Kafka         GUI          Network
       │            │            │
     TLS/SASL      Auth         SG
       │            │            │
      ACLs        RBAC         HTTPS
```

### Production checklist

- 🔐 TLS
- 🔑 SASL authentication
- 👤 Authentication for GUI
- 🛡️ RBAC
- 🌐 Private networking
- 🔥 Security groups/firewalls
- 🔒 HTTPS
- 📝 Audit logging
- 🧑‍💼 Least privilege
- 🚫 Never expose controller ports publicly
- 🚫 Never expose Kafka anonymously to the internet

---

# 24. 👨‍🎓 What Students Should Learn

A student should NOT only learn:

> "Click Create Topic."

They should understand what happens underneath.

### Example

GUI:

```text
Create Topic
Name: orders
Partitions: 3
Replication Factor: 1
```

Behind the GUI conceptually:

```text
Topic
  │
  └── orders
        │
        ├── Partition 0
        ├── Partition 1
        └── Partition 2
```

Then:

```text
Producer
   │
   ▼
Kafka
   │
   ▼
orders
├── P0
├── P1
└── P2
   │
   ▼
Consumer Group
```

The GUI is simply making these concepts visible.

---

# 25. 🧪 Practical Lab Roadmap

## LAB 01 — Connect GUI to Kafka

Goal:

```text
Kafbat → Kafka
```

Verify cluster connectivity.

---

## LAB 02 — Create Topic

Create:

```text
vishwatech-orders
```

Partitions:

```text
3
```

---

## LAB 03 — Inspect Partitions

Observe:

```text
P0
P1
P2
```

---

## LAB 04 — Produce Messages

Example:

```json
{"orderId":1001,"customer":"Vishwa","amount":5000}
```

---

## LAB 05 — Browse Messages

Students inspect:

- key
- value
- timestamp
- partition
- offset

---

## LAB 06 — Consumer Group

Create:

```text
orders-consumer-group
```

---

## LAB 07 — Consumer Lag

Generate messages faster than the consumer.

Observe:

```text
Produced: 1000
Consumed: 800
----------------
Lag:       200
```

---

## LAB 08 — Partition Distribution

Produce multiple messages and inspect partition assignment.

---

## LAB 09 — Topic Configuration

Study:

- retention
- cleanup policy
- segment configuration
- compression
- replication

---

## LAB 10 — Consumer Offset

Understand:

```text
Partition
    │
    ├── Offset 0
    ├── Offset 1
    ├── Offset 2
    ├── Offset 3
    └── ...
```

---

## LAB 11 — Consumer Lag Investigation

Scenario:

```text
Producer
   │
   │ 10,000 msg/sec
   ▼
Kafka
   │
   ▼
Consumer
   │
   │ 5,000 msg/sec
   ▼

Lag ↑
```

Students identify the bottleneck.

---

## LAB 12 — Multiple Consumer Groups

```text
orders
  │
  ├── payment-group
  ├── shipping-group
  └── analytics-group
```

---

## LAB 13 — Schema Registry

Learn:

```text
Producer
   │
   ▼
Schema
   │
   ▼
Kafka
   │
   ▼
Consumer
```

---

## LAB 14 — Kafka Connect

```text
Database
   │
   ▼
Kafka Connect
   │
   ▼
Kafka
```

---

## LAB 15 — Troubleshooting

Students intentionally create:

- consumer lag
- wrong topic
- wrong bootstrap server
- stopped consumer
- unavailable broker
- incorrect permissions

Then investigate using the GUI.

---

# 26. 🛠️ Troubleshooting

## GUI cannot connect

Check:

```bash
ss -lntp | grep 9092
```

Then:

```bash
sudo systemctl status kafka-kraft
```

---

## Kafbat is running but Kafka is unavailable

Check Kafka logs:

```bash
sudo journalctl -u kafka-kraft -n 100 --no-pager
```

---

## EC2 browser cannot access GUI

Check:

```bash
ss -lntp | grep 8080
```

Then verify AWS Security Group.

---

## Kafka GUI connects but topics fail

Check:

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

---

## External client cannot connect

Check Kafka:

```bash
grep -E '^(listeners|advertised.listeners)' \
config/server.properties
```

### Very important

For remote clients, `advertised.listeners` must advertise an address reachable by the client.

A common mistake is:

```text
advertised.listeners=PLAINTEXT://localhost:9092
```

when the client is on another machine.

---

# 27. 🏭 Production Guidance

A single EC2 KRaft node is excellent for:

- learning
- demos
- development
- training
- POCs

But:

```text
❌ NOT HIGH AVAILABILITY
```

Production architecture should normally involve multiple Kafka nodes/controllers according to the selected Kafka architecture and operational requirements.

Example:

```text
                  Load / Clients
                        │
        ┌───────────────┼───────────────┐
        │               │               │
     Broker 1        Broker 2        Broker 3
        │               │               │
        └───────────────┼───────────────┘
                        │
                  Controller
                   Quorum
```

And:

```text
                 Kafka GUI
                    │
        ┌───────────┼───────────┐
        │           │           │
     Kafka-1     Kafka-2     Kafka-3
```

---

# 28. 🔗 Official Resources

## Apache Kafka

🔗 https://kafka.apache.org/

## Kafbat UI

🔗 https://ui.docs.kafbat.io/

🔗 https://github.com/kafbat/kafka-ui

## AKHQ

🔗 https://akhq.io/

## Redpanda Console

🔗 https://www.redpanda.com/data-streaming/redpanda-console-kafka-ui

🔗 https://docs.redpanda.com/current/console/

## Conduktor

🔗 https://www.conduktor.io/

## Kpow

🔗 https://factorhouse.io/kpow/

## Lenses

🔗 https://www.lenses.io/

## Kafdrop

🔗 https://github.com/obsidiandynamics/kafdrop

## Confluent

🔗 https://www.confluent.io/

---

# 29. 🏆 Final Recommendation

For **VishwaTech Labs**, don't install everything at once.

Use this strategy:

```text
                 VISHWATECH KAFKA GUI STACK
                              │
                              ▼
                    🥇 KAFBAT UI
                              │
                     Core learning
                              │
                              ▼
                         🥈 AKHQ
                              │
                     Administration
                              │
                              ▼
                  🥉 REDPANDA CONSOLE
                              │
                     Advanced debugging
                              │
                              ▼
                        CONDUKTOR
                              │
                    Enterprise operations
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
                  KPOW               LENSES
                    │                   │
              Operations             DataOps
```

## ⭐ My ranking for your course

| Rank | Tool | My recommendation |
|---:|---|---|
| 🥇 | **Kafbat UI** | ⭐⭐⭐⭐⭐ — Start here |
| 🥈 | **AKHQ** | ⭐⭐⭐⭐⭐ — Teach next |
| 🥉 | **Redpanda Console** | ⭐⭐⭐⭐⭐ — Advanced |
| 4 | **Conduktor** | ⭐⭐⭐⭐⭐ — Enterprise |
| 5 | **Offset Explorer** | ⭐⭐⭐⭐ — Desktop |
| 6 | **Kpow** | ⭐⭐⭐⭐ — Enterprise ops |
| 7 | **Lenses** | ⭐⭐⭐⭐ — DataOps |
| 8 | **Kafdrop** | ⭐⭐⭐ — Lightweight |
| 9 | **Confluent Control Center** | ⭐⭐⭐⭐ — Confluent environments |
| 10 | **CMAK** | ⭐⭐ — Legacy awareness |

---

# 🎓 VishwaTech Golden Rule

```text
                 KAFKA ENGINEER
                       │
        ┌──────────────┴──────────────┐
        │                             │
       CLI                           GUI
        │                             │
        ▼                             ▼
 Understand Kafka              Operate Kafka
 deeply                         visually
        │                             │
        └──────────────┬──────────────┘
                       │
                       ▼
                REAL KAFKA SKILL
```

### 🏆 Final answer in one line

> **For your AWS EC2 Kafka KRaft + ZooKeeper training, make Kafbat UI your primary GUI, teach AKHQ as the second open-source GUI, Redpanda Console for advanced developer/operations workflows, and Conduktor/Kpow/Lenses for enterprise awareness.**

---

## 🚀 Suggested next VishwaTech lab

The natural next step is:

```text
AWS EC2
   │
   ├── Kafka KRaft :9092
   │
   └── Kafbat UI :8080
             │
             ▼
          Browser
             │
       ┌─────┴─────┐
       │            │
     Topics      Messages
       │            │
       ├── P0       │
       ├── P1       │
       └── P2       │
                    │
             Consumer Groups
                    │
               Consumer Lag
```

Then build **10–20 hands-on Kafbat UI labs** around your existing Kafka KRaft EC2 lab: installation → connection → topics → partitions → producers → consumers → offsets → consumer groups → lag → retention → schemas → Kafka Connect → troubleshooting → security.
