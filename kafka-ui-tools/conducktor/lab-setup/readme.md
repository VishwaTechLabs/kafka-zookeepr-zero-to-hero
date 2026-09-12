
# 🚀 VishwaTech Labs — Conduktor Console + Existing Kafka KRaft

![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.0.2-black?logo=apachekafka)
![KRaft](https://img.shields.io/badge/Mode-KRaft-blue)
![Conduktor](https://img.shields.io/badge/Conduktor-Console-orange)
![Docker](https://img.shields.io/badge/Docker-Required-2496ED?logo=docker)
![AWS](https://img.shields.io/badge/AWS-EC2-FF9900?logo=amazonaws)
![Lab](https://img.shields.io/badge/VishwaTech-Lab-purple)

> **Goal:** Install **Conduktor Console** on the same AWS EC2 instance as the existing single-node Kafka KRaft cluster, without replacing Kafka, Kafbat UI, or Kpow.

---

## 🧠 What is Conduktor?

Conduktor is a Kafka/data platform with two major components:

- **Conduktor Console** → web UI/control plane
- **Conduktor Gateway** → Kafka proxy/governance layer

For this lab we start with **Console**. Gateway will be a separate advanced lab.

Conduktor Console provides centralized visibility and management for Kafka topics, consumer groups, schemas, connectors, security/RBAC, audit and related platform capabilities.

Official docs:
- https://docs.conduktor.io/guide
- https://docs.conduktor.io/guide/get-started
- https://docs.conduktor.io/guide/conduktor-in-production/deploy-artifacts/deploy-console

---

# 🏗️ 1. Existing VishwaTech Kafka Architecture

We are NOT creating another Kafka cluster.

```text
                         AWS EC2
              Amazon Linux 2023 / Docker
        ┌──────────────────────────────────────────┐
        │                                          │
        │   Apache Kafka 4.0.2 — KRaft             │
        │   Broker + Controller                    │
        │                                          │
        │   Kafka Broker     : 9092                │
        │   KRaft Controller : 9093                │
        │                                          │
        │        ▲             ▲                   │
        │        │             │                   │
        │   ┌────┴────┐   ┌────┴─────┐             │
        │   │ Kafbat  │   │   Kpow   │             │
        │   │  :8080  │   │   :3000  │             │
        │   └─────────┘   └──────────┘             │
        │                                          │
        │             NEW                          │
        │        ┌───────────────┐                 │
        │        │ Conduktor     │                 │
        │        │ Console       │                 │
        │        │ :8081         │                 │
        │        └───────┬───────┘                 │
        │                │                         │
        │                ▼                         │
        │        PostgreSQL :5432                  │
        │        Conduktor state DB                │
        │                                          │
        └──────────────────────────────────────────┘

Browser
   │
   ├── :8080 → Kafbat UI
   ├── :3000 → Kpow
   └── :8081 → Conduktor Console
```

### Why port 8081?

Your existing Kafbat UI already uses **8080**.

Therefore:

| Tool | Port |
|---|---:|
| Kafka broker | 9092 |
| KRaft controller | 9093 |
| Kafbat UI | 8080 |
| **Conduktor Console** | **8081** |
| Kpow | 3000 |
| PostgreSQL | 5432 |

Conduktor documents `CDK_LISTENING_PORT` for changing the Console listening port.

---

# ⚠️ 2. Important: Your Existing EC2 Size

Your original lab was built around:

```text
t3.medium
2 vCPU
4 GB RAM
```

Conduktor's current technical requirements list **2 CPU / 3 GB RAM minimum for Console**, while Gateway has a higher minimum. Therefore, running Kafka + Kafbat + Kpow + PostgreSQL + Conduktor together on 4 GB is a **lab stress test**, not an ideal setup.

### Recommended

```text
t3.large
2 vCPU
8 GB RAM
```

For a lightweight learning lab, you can also temporarily stop Kpow while running Conduktor.

Official requirements:
https://docs.conduktor.io/guide/conduktor-in-production/system-requirements

---

# 🔐 3. Conduktor License

Conduktor currently supports a free Community Edition.

The Console deployment uses a license/configuration value. If your account provides a license key, put it in:

```text
CDK_LICENSE=YOUR_LICENSE_KEY
```

For a Free/Community deployment, follow the current Conduktor onboarding flow and omit the enterprise-only license value where instructed.

**Never commit a real license key to GitHub.**

---

# 📁 4. Directory Structure

Create:

```bash
mkdir -p /home/ec2-user/conduktor
cd /home/ec2-user/conduktor
```

Expected:

```text
/home/ec2-user/conduktor/
├── docker-compose.yml
├── .env
└── README.md
```

---

# 🔑 5. Create Environment File

```bash
cd /home/ec2-user/conduktor
nano .env
```

Use:

```dotenv
# -----------------------------
# Conduktor
# -----------------------------
CDK_ORGANIZATION_NAME=VishwaTech
CDK_ADMIN_EMAIL=admin@vishwatech.local
CDK_ADMIN_PASSWORD=ChangeMe_Strong_123!

# Add your Conduktor license if your CE onboarding provides one.
# Do NOT commit this file to Git.
CDK_LICENSE=

# -----------------------------
# Console
# -----------------------------
CDK_LISTENING_PORT=8081

# -----------------------------
# PostgreSQL
# -----------------------------
POSTGRES_DB=conduktor
POSTGRES_USER=conduktor
POSTGRES_PASSWORD=ChangeMe_DB_123!
```

> Change both passwords before using the lab.

---

# 🐘 6. PostgreSQL

Conduktor Console requires PostgreSQL for its persistent state.

Conduktor's current deployment documentation requires PostgreSQL 13+ and provides PostgreSQL 14 examples.

We use PostgreSQL 14 for this lab.

```text
Conduktor
    │
    │ stores users/config/state
    ▼
PostgreSQL
    │
    └── conduktor database
```

---

# 🐳 7. Docker Compose

Create:

```bash
nano docker-compose.yml
```

Paste:

```yaml
services:

  postgresql:
    image: postgres:14
    container_name: conduktor-postgresql
    restart: unless-stopped
    network_mode: host
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_HOST_AUTH_METHOD: scram-sha-256
    volumes:
      - conduktor_pg_data:/var/lib/postgresql/data
    command:
      - postgres
      - -c
      - listen_addresses=127.0.0.1
    healthcheck:
      test:
        [
          "CMD-SHELL",
          "pg_isready -h 127.0.0.1 -U ${POSTGRES_USER} -d ${POSTGRES_DB}"
        ]
      interval: 10s
      timeout: 5s
      retries: 10

  conduktor-console:
    image: conduktor/conduktor-console:latest
    container_name: conduktor-console
    restart: unless-stopped
    network_mode: host
    depends_on:
      postgresql:
        condition: service_healthy
    environment:
      CDK_DATABASE_URL: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:5432/${POSTGRES_DB}

      CDK_ORGANIZATION_NAME: ${CDK_ORGANIZATION_NAME}
      CDK_ADMIN_EMAIL: ${CDK_ADMIN_EMAIL}
      CDK_ADMIN_PASSWORD: ${CDK_ADMIN_PASSWORD}

      CDK_LISTENING_PORT: ${CDK_LISTENING_PORT}

      # If your Community Edition requires a license key,
      # uncomment/add it in .env:
      # CDK_LICENSE: ${CDK_LICENSE}

    volumes:
      - conduktor_data:/var/conduktor

    healthcheck:
      test:
        [
          "CMD-SHELL",
          "curl --fail http://127.0.0.1:${CDK_LISTENING_PORT}/api/health/live"
        ]
      interval: 10s
      start_period: 120s
      timeout: 5s
      retries: 12

volumes:
  conduktor_pg_data:
  conduktor_data:
```

### Why `network_mode: host`?

Your existing Kafka advertises:

```text
PLAINTEXT://localhost:9092
```

A normal Docker bridge container would interpret `localhost` as **itself**, not the EC2 host.

Using host networking allows Conduktor Console to reach:

```text
127.0.0.1:9092
```

directly.

This is specifically convenient for this **single-EC2 training lab**.

For production, use proper DNS/listeners/network segmentation instead.

---

# 🚀 8. Start Conduktor

First check Docker:

```bash
sudo docker --version
sudo docker compose version
```

Then:

```bash
cd /home/ec2-user/conduktor
sudo docker compose pull
sudo docker compose up -d
```

Check:

```bash
sudo docker compose ps
```

Expected:

```text
conduktor-postgresql   Up (healthy)
conduktor-console      Up (healthy)
```

---

# 🔎 9. Check Logs

Console:

```bash
sudo docker logs -f conduktor-console
```

PostgreSQL:

```bash
sudo docker logs -f conduktor-postgresql
```

Exit log view:

```text
CTRL+C
```

---

# ❤️ 10. Health Check

Run:

```bash
curl -s http://127.0.0.1:8081/api/health/live
```

Expected HTTP 200 / successful response.

You can also inspect:

```bash
sudo docker compose ps
```

---

# 🌐 11. AWS Security Group

Because Kafbat already uses 8080, Conduktor uses:

```text
8081
```

Add:

| Type | Port | Source |
|---|---:|---|
| SSH | 22 | YOUR_PUBLIC_IP/32 |
| Custom TCP | 3000 | YOUR_PUBLIC_IP/32 |
| Custom TCP | 8080 | YOUR_PUBLIC_IP/32 |
| **Custom TCP** | **8081** | **YOUR_PUBLIC_IP/32** |
| Kafka | 9092 | only if external clients need it |
| KRaft Controller | 9093 | **DO NOT expose publicly** |
| PostgreSQL | 5432 | **DO NOT expose publicly** |

Do NOT use:

```text
0.0.0.0/0
```

for the admin UIs in this training lab unless you intentionally understand the exposure.

---

# 🖥️ 12. Open Conduktor

Find EC2 public IP:

```bash
curl -s http://169.254.169.254/latest/meta-data/public-ipv4
```

Then:

```text
http://EC2_PUBLIC_IP:8081
```

Login using the admin email/password configured in `.env`.

Example:

```text
Email:
admin@vishwatech.local

Password:
ChangeMe_Strong_123!
```

---

# 🔌 13. Add Existing Kafka Cluster

After login:

```text
Settings
   ↓
Clusters
   ↓
Add a cluster
```

Configure:

```text
Cluster name:
VishwaTech-Kafka-KRaft

Bootstrap servers:
127.0.0.1:9092

Security protocol:
PLAINTEXT
```

Then validate/save.

### Important

Do NOT use:

```text
127.0.0.1:9093
```

for the Kafka bootstrap server.

`9093` is the KRaft controller listener.

Use:

```text
9092
```

for Kafka client traffic.

---

# 🧠 14. Why Conduktor Needs PostgreSQL

This is an important architecture concept.

Kafka stores:

```text
Topics
Partitions
Messages
Consumer offsets
Kafka metadata
```

Conduktor stores its own platform state in PostgreSQL, such as:

```text
Users
Permissions
Configuration
Platform metadata
Application/platform state
```

So:

```text
                 Conduktor Console
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
        Apache Kafka          PostgreSQL
          :9092                :5432
```

They serve different purposes.

---

# 🧪 15. Lab 1 — Verify Kafka Connection

In Conduktor:

```text
Settings
 → Clusters
 → VishwaTech-Kafka-KRaft
```

Verify the cluster is connected.

Expected:

```text
Cluster
   │
   ├── Connected
   ├── Broker
   ├── Topics
   ├── Consumer Groups
   └── Cluster information
```

---

# 🧪 16. Lab 2 — Create Topic

Create:

```text
vishwatech-conduktor-demo
```

Partitions:

```text
3
```

Replication factor:

```text
1
```

### Why RF=1?

Your current Kafka cluster has only:

```text
1 broker
```

Therefore:

```text
RF=3 ❌
RF=2 ❌
RF=1 ✅
```

---

# 🧪 17. Lab 3 — Produce Messages

Produce sample events:

```json
{
  "student_id": 101,
  "name": "Vishwa",
  "course": "Kafka",
  "status": "active"
}
```

Then produce:

```json
{
  "student_id": 102,
  "name": "Rahul",
  "course": "DevOps",
  "status": "active"
}
```

Observe:

```text
Topic
  ↓
Partition
  ↓
Offset
  ↓
Message
```

---

# 🧪 18. Lab 4 — Browse Messages

Open:

```text
Topics
 → vishwatech-conduktor-demo
 → Messages
```

Study:

```text
Partition
Offset
Timestamp
Key
Value
Headers
```

This is one of the most useful Kafka administrator skills.

---

# 🧪 19. Lab 5 — Consumer Group

Create/use:

```text
vishwatech-demo-consumer
```

Study:

```text
Consumer Group
      │
      ├── Consumer
      │
      ├── Partition
      │
      ├── Current Offset
      │
      ├── Log End Offset
      │
      └── Lag
```

---

# 🧪 20. Lab 6 — Consumer Lag

Produce 100 messages.

Then inspect:

```text
Consumer Groups
   ↓
vishwatech-demo-consumer
   ↓
Lag
```

Concept:

```text
Log End Offset = 100
Current Offset = 70

Lag = 30
```

This is an important Kafka monitoring KPI.

---

# 🧪 21. Lab 7 — Partition Distribution

Create:

```text
vishwatech-orders
```

Partitions:

```text
3
```

Produce multiple messages.

Observe how Kafka distributes records across:

```text
Partition 0
Partition 1
Partition 2
```

---

# 🧪 22. Lab 8 — Topic Configuration

Inspect configuration such as:

```text
cleanup.policy
retention.ms
retention.bytes
segment.bytes
min.insync.replicas
```

Understand:

```text
Producer
   ↓
Kafka Topic
   ↓
Retention Policy
   ↓
Segments
   ↓
Cleanup
```

---

# 🧪 23. Lab 9 — Compare Kafka GUIs

You now have:

```text
                 Kafka KRaft
                     │
       ┌─────────────┼──────────────┐
       ▼             ▼              ▼
    Kafbat          Kpow         Conduktor
     :8080          :3000          :8081
```

### Your learning matrix

| Capability | Kafbat | Kpow | Conduktor |
|---|---:|---:|---:|
| Topic management | ✅ | ✅ | ✅ |
| Message browser | ✅ | ✅ | ✅ |
| Consumer groups | ✅ | ✅ | ✅ |
| Lag | ✅ | ✅ | ✅ |
| Broker visibility | ✅ | ✅ | ✅ |
| Enterprise governance | ◐ | ✅ | ✅ |
| RBAC | ◐ | ✅ | ✅ |
| Audit | ◐ | ✅ | ✅ |
| Schema Registry | ✅ | ✅ | ✅ |
| Kafka Connect | ✅ | ✅ | ✅ |
| Platform/self-service | ❌ | ◐ | ✅ |
| Gateway/governance | ❌ | ❌ | ✅ |

---

# 🧪 24. Lab 10 — KRaft Understanding

Your architecture is:

```text
Kafka 4.x
   │
   └── KRaft
       │
       ├── Broker role
       └── Controller role
```

Conduktor connects to:

```text
Kafka client listener
       ↓
     :9092
```

It does NOT need to connect directly to:

```text
KRaft controller :9093
```

---

# 🧪 25. Lab 11 — Schema Registry Roadmap

Next add:

```text
Conduktor
    │
    ├── Kafka
    │
    ├── Schema Registry
    │      ├── Avro
    │      ├── JSON Schema
    │      └── Protobuf
    │
    └── Kafka Connect
```

Then build:

```text
Producer
   ↓
Schema Registry
   ↓
Kafka
   ↓
Consumer
```

---

# 🧪 26. Lab 12 — Kafka Connect Roadmap

Next deploy:

```text
Kafka
   │
   ▼
Kafka Connect
   │
   ├── Source Connector
   │
   └── Sink Connector
```

Example:

```text
PostgreSQL
    ↓
Kafka Connect
    ↓
Kafka
    ↓
Conduktor
```

---

# 🧪 27. Lab 13 — Security/RBAC

Conduktor is especially useful for enterprise Kafka administration.

Future lab:

```text
User
 │
 ▼
Conduktor
 │
 ▼
RBAC
 │
 ├── Admin
 ├── Developer
 ├── Operator
 └── Viewer
```

Study:

```text
Authentication
Authorization
RBAC
ACL
Audit
SSO
OIDC
LDAP
```

---

# 🧪 28. Lab 14 — Conduktor Gateway

Gateway is a separate component.

Architecture:

```text
Kafka Client
     │
     ▼
Conduktor Gateway
     │
     ├── Authentication
     ├── Authorization
     ├── Governance
     ├── Traffic control
     ├── Data masking
     ├── Multi-tenancy
     └── Interceptors
     │
     ▼
Apache Kafka
```

Conduktor describes Gateway as a Kafka-compliant proxy layer for governance, security, traffic control and multi-tenancy.

Do this as the **next advanced Conduktor lab**, not as part of the basic Console installation.

---

# 🔥 29. Recommended VishwaTech Conduktor Roadmap

```text
LEVEL 1
Conduktor Console
      ↓
Connect existing KRaft
      ↓
Topics
      ↓
Messages
      ↓
Consumer Groups
      ↓
Lag

LEVEL 2
Schema Registry
      ↓
Avro
      ↓
JSON Schema
      ↓
Protobuf

LEVEL 3
Kafka Connect
      ↓
Source
      ↓
Sink
      ↓
Connector monitoring

LEVEL 4
Security
      ↓
Authentication
      ↓
RBAC
      ↓
ACL
      ↓
Audit
      ↓
OIDC / LDAP

LEVEL 5
Conduktor Gateway
      ↓
Proxy
      ↓
Virtual Clusters
      ↓
Traffic Control
      ↓
Data Masking
      ↓
Governance

LEVEL 6
Production
      ↓
PostgreSQL HA
      ↓
TLS
      ↓
SSO
      ↓
Monitoring
      ↓
AWS
      ↓
ECS / EKS
```

---

# 🛠️ 30. Troubleshooting

## Problem: Console does not start

```bash
sudo docker compose ps
sudo docker logs conduktor-console --tail 200
```

---

## Problem: PostgreSQL unhealthy

```bash
sudo docker logs conduktor-postgresql --tail 200
```

Check:

```bash
sudo docker exec -it conduktor-postgresql \
  pg_isready -h 127.0.0.1 -U conduktor -d conduktor
```

---

## Problem: Port 8081 already used

```bash
sudo ss -lntp | grep 8081
```

Change:

```dotenv
CDK_LISTENING_PORT=8082
```

Then:

```bash
sudo docker compose down
sudo docker compose up -d
```

---

## Problem: Conduktor cannot connect to Kafka

Check Kafka:

```bash
sudo systemctl status kafka-kraft
```

Check:

```bash
ss -lntp | grep 9092
```

Then:

```bash
curl -s http://127.0.0.1:8081/api/health/live
```

Also verify Kafka advertises:

```text
PLAINTEXT://localhost:9092
```

Because this lab uses host networking, Conduktor can reach it.

---

# 🧹 31. Stop

```bash
cd /home/ec2-user/conduktor
sudo docker compose stop
```

---

# 🗑️ 32. Remove Containers

```bash
sudo docker compose down
```

This does NOT delete named volumes.

---

# 💣 33. Complete Reset

⚠️ This deletes Conduktor PostgreSQL state and Console data.

```bash
sudo docker compose down -v
```

Then:

```bash
sudo docker compose up -d
```

---

# 🔍 34. Final Validation Checklist

```text
[ ] Docker installed
[ ] Docker Compose available
[ ] PostgreSQL running
[ ] PostgreSQL healthy
[ ] Conduktor Console running
[ ] Console health endpoint returns HTTP 200
[ ] AWS SG 8081 restricted to your IP
[ ] Conduktor login works
[ ] Existing Kafka cluster added
[ ] Kafka connection successful
[ ] Topic created
[ ] Messages produced
[ ] Messages browsed
[ ] Consumer group inspected
[ ] Consumer lag observed
[ ] KRaft architecture understood
[ ] Kafbat :8080 still works
[ ] Kpow :3000 still works
```

---

# 🎓 35. VishwaTech Kafka GUI Stack

You now have a very powerful learning environment:

```text
                         VishwaTech Kafka Lab
                                  │
                     ┌────────────┴────────────┐
                     │                         │
                Kafka 4.0.2                 KRaft
                     │
                     │ :9092
                     │
        ┌────────────┼─────────────┐
        │            │             │
        ▼            ▼             ▼
     Kafbat         Kpow       Conduktor
     :8080          :3000         :8081
        │            │             │
        └────────────┴─────────────┘
                     │
                     ▼
              Single Kafka Node
```

### What you learn

```text
Kafka CLI
   +
Kafbat UI
   +
Kpow
   +
Conduktor
   +
KRaft
   +
Consumer Groups
   +
Lag
   +
Schema Registry
   +
Kafka Connect
   +
Security
   +
RBAC
   +
Gateway
```

That becomes a strong **Kafka Administrator + Kafka Developer + Kafka Security + Kafka Platform Engineering** training environment.

---

## 📚 Official Conduktor References

- Conduktor platform guide:
  https://docs.conduktor.io/guide
- Quick start:
  https://docs.conduktor.io/guide/get-started
- Console deployment:
  https://docs.conduktor.io/guide/conduktor-in-production/deploy-artifacts/deploy-console
- Console environment variables:
  https://docs.conduktor.io/guide/conduktor-in-production/deploy-artifacts/deploy-console/environment-variables
- System requirements:
  https://docs.conduktor.io/guide/conduktor-in-production/system-requirements
- Gateway:
  https://docs.conduktor.io/gateway
- Gateway → Kafka:
  https://docs.conduktor.io/guide/conduktor-in-production/deploy-artifacts/deploy-gateway/connect-to-kafka

---

# 🏁 End State

```text
EC2
│
├── Kafka 4.0.2 KRaft
│   ├── Broker :9092
│   └── Controller :9093
│
├── Kafbat UI :8080
│
├── Kpow :3000
│
├── Conduktor Console :8081
│
└── PostgreSQL :5432
       └── Conduktor state
```

> **Next recommended lab:** Conduktor + Schema Registry + Kafka Connect, followed by **Conduktor Gateway with authentication, ACL/RBAC, virtual clusters and data governance**.
