# 🚀 VishwaTechLabs — Conduktor Console with Existing Kafka KRaft Cluster on AWS EC2

<p align="center">

![Conduktor](https://img.shields.io/badge/Conduktor-Console-5B5BD6?style=for-the-badge)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-EC2-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Conduktor%20State-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Amazon Linux](https://img.shields.io/badge/Amazon%20Linux-2023-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)

</p>

<p align="center">
<b>Deploy Conduktor Console on a dedicated EC2 instance and connect it to the existing 2-Broker + 2-Controller Apache Kafka KRaft lab.</b>
</p>

---

> [!IMPORTANT]
> This guide is for the existing VishwaTechLabs Kafka lab.  
> Kafka is already working. We are **not rebuilding Kafka**.  
> We are adding Conduktor Console as a separate administration/observability layer.

---

# 📚 Table of Contents

- [🎯 Objective](#-objective)
- [💡 What Is Conduktor?](#-what-is-conduktor)
- [🆚 Where It Fits](#-where-it-fits)
- [🖥️ Existing Kafka Inventory](#️-existing-kafka-inventory)
- [🏗️ Final Architecture](#️-final-architecture)
- [🌐 Port and Network Design](#-port-and-network-design)
- [🔐 AWS Security Groups](#-aws-security-groups)
- [☁️ Create Dedicated Conduktor EC2](#️-create-dedicated-conduktor-ec2)
- [🔑 SSH to Conduktor EC2](#-ssh-to-conduktor-ec2)
- [🐳 Install Docker and Docker Compose](#-install-docker-and-docker-compose)
- [🔎 Test Kafka Connectivity](#-test-kafka-connectivity)
- [📁 Create Project Directory](#-create-project-directory)
- [🔐 Create Secrets](#-create-secrets)
- [🐘 Why PostgreSQL Is Required](#-why-postgresql-is-required)
- [📝 Create Docker Compose](#-create-docker-compose)
- [▶️ Start Conduktor](#️-start-conduktor)
- [✅ Validate Containers](#-validate-containers)
- [🌍 Open Conduktor Console](#-open-conduktor-console)
- [🔌 Add Existing Kafka Cluster](#-add-existing-kafka-cluster)
- [🧪 Verify Kafka Resources](#-verify-kafka-resources)
- [📊 What You Can Learn](#-what-you-can-learn)
- [🔄 Lifecycle Commands](#-lifecycle-commands)
- [🛠️ Troubleshooting](#️-troubleshooting)
- [🔒 Security Hardening](#-security-hardening)
- [⚠️ Production Notes](#️-production-notes)
- [🎓 Learning Outcomes](#-learning-outcomes)
- [🔗 Official References](#-official-references)

---

# 🎯 Objective

Our existing Kafka cluster contains:

```text
2 x Dedicated KRaft Controllers
2 x Dedicated Kafka Brokers
```

We will add:

```text
1 x Dedicated Conduktor Console EC2
```

Conduktor will connect to Kafka through the **broker private listeners**:

```text
172.31.36.36:9092
172.31.42.7:9092
```

It will **not** use the KRaft controller endpoints as Kafka bootstrap servers.

---

# 💡 What Is Conduktor?

Conduktor Console is a web-based Kafka operations and governance platform.

It can help engineers work with resources such as:

```text
Kafka Clusters
Topics
Partitions
Messages
Consumer Groups
Consumer Lag
Kafka Configuration
Schema Registry
Kafka Connect
Monitoring
RBAC
Audit capabilities
```

Availability of individual capabilities can depend on your Conduktor edition, license, version and configured permissions.

---

# 🆚 Where It Fits

Kafka CLI remains extremely important:

```text
kafka-topics.sh
kafka-console-producer.sh
kafka-console-consumer.sh
kafka-consumer-groups.sh
kafka-configs.sh
```

Conduktor gives us a graphical operational layer:

```text
                    CONDUKTOR CONSOLE
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
        Topics       Consumer Groups      Data
          |                |                |
          v                v                v
      Partitions          Lag            Records
```

This is useful for:

- Kafka administrators
- Platform engineers
- DevOps/SRE engineers
- Data engineers
- Developers
- Security/platform teams
- Students learning Kafka operations

---

# 🖥️ Existing Kafka Inventory

| Server | Role | Node ID | Private IP | Public IP | Kafka Port |
|---|---|---:|---|---|---|
| Controller-1 | KRaft Controller | `1` | `172.31.35.220` | `3.251.97.0` | `9093` |
| Controller-2 | KRaft Controller | `2` | `172.31.34.34` | `108.133.98.249` | `9093` |
| Broker-1 | Kafka Broker | `3` | `172.31.36.36` | `34.253.224.235` | `9092 / 19092` |
| Broker-2 | Kafka Broker | `4` | `172.31.42.7` | `18.203.251.147` | `9092 / 19092` |

Existing external Offset Explorer endpoints:

```text
34.253.224.235:19092
18.203.251.147:19092
```

Existing internal Kafka endpoints:

```text
172.31.36.36:9092
172.31.42.7:9092
```

For Conduktor inside the VPC, use the **internal endpoints**.

---

# 🏗️ Final Architecture

```text
                              YOUR LAPTOP
                                   |
                +------------------+------------------+
                |                                     |
                | Offset Explorer                     | Browser
                |                                     |
                | TCP 19092                           | TCP 8080
                |                                     |
                v                                     v
        +---------------+                    +-------------------+
        | Kafka Brokers |                    | CONDUKTOR CONSOLE |
        | Public Access |                    | Dedicated EC2     |
        +---------------+                    +---------+---------+
                                                      |
                                           Private TCP 9092
                                                      |
                               +----------------------+----------------------+
                               |                                             |
                               v                                             v
                     +------------------+                          +------------------+
                     |     BROKER-1     |<------ TCP 9092 ------->|     BROKER-2     |
                     | node.id = 3      |                          | node.id = 4      |
                     | 172.31.36.36     |                          | 172.31.42.7      |
                     +---------+--------+                          +---------+--------+
                               |                                             |
                               +----------------------+----------------------+
                                                      |
                                                KRaft 9093
                                                      |
                               +----------------------+----------------------+
                               |                                             |
                               v                                             v
                     +------------------+                          +------------------+
                     |   CONTROLLER-1   |<------ TCP 9093 ------->|   CONTROLLER-2   |
                     | node.id = 1      |                          | node.id = 2      |
                     | 172.31.35.220    |                          | 172.31.34.34     |
                     +------------------+                          +------------------+


                 Inside Conduktor EC2 / Docker Network

                  +--------------------------+
                  |  Conduktor Console :8080 |
                  +------------+-------------+
                               |
                               | PostgreSQL :5432
                               | Docker-private network
                               v
                  +--------------------------+
                  | PostgreSQL               |
                  | Conduktor state database |
                  +--------------------------+
```

---

# 🌐 Port and Network Design

## Golden Rule

```text
KRaft Controller ↔ Controller/Brokers
TCP 9093
PRIVATE

Broker ↔ Broker / Internal Clients
TCP 9092
PRIVATE

Conduktor → Kafka
TCP 9092
PRIVATE

Offset Explorer → Kafka
TCP 19092
PUBLIC / RESTRICTED

Laptop Browser → Conduktor
TCP 8080
PUBLIC / RESTRICTED

Conduktor → PostgreSQL
TCP 5432
DOCKER-INTERNAL
```

---

## ❌ Do Not Configure Controllers as Kafka Bootstrap Servers

Do not use:

```text
172.31.35.220:9093
172.31.34.34:9093
```

Those are KRaft controller listeners.

Use:

```text
172.31.36.36:9092
172.31.42.7:9092
```

---

# 🔐 AWS Security Groups

Create or use a dedicated security group for Conduktor.

## Conduktor EC2 inbound

| Port | Source | Purpose |
|---:|---|---|
| `22` | Your laptop public IP `/32` | SSH |
| `8080` | Your laptop public IP `/32` | Conduktor Console |

Recommended:

```text
22   → YOUR_PUBLIC_IP/32
8080 → YOUR_PUBLIC_IP/32
```

Avoid:

```text
8080 → 0.0.0.0/0 ❌
```

unless you deliberately need temporary lab access and understand the risk.

---

## Kafka Broker SG

Allow the Conduktor EC2 security group to reach broker TCP `9092`.

Recommended inbound rule on Kafka broker SG:

```text
Protocol: TCP
Port:     9092
Source:   Conduktor-Security-Group
```

If your lab intentionally uses one shared SG for all nodes, allow the SG to reference itself for internal Kafka traffic.

---

## PostgreSQL

In this Docker Compose lab, PostgreSQL is **not exposed to the internet**.

We do not need:

```text
5432 → Internet ❌
```

Conduktor reaches PostgreSQL through the Docker network.

---

# ☁️ Create Dedicated Conduktor EC2

Recommended lab starting point:

```text
Name:
conduktor

AMI:
Amazon Linux 2023

VPC:
Same VPC as Kafka

Subnet:
Must privately reach Broker-1 and Broker-2

Instance:
Start with at least a reasonably sized lab instance
```

> [!NOTE]
> Conduktor Console is heavier than a tiny Kafka GUI because it includes a server application and requires PostgreSQL. Size CPU/RAM based on the current Conduktor technical requirements and your workload. Do not treat a minimal lab size as a production recommendation.

Disk recommendation for a lab:

```text
20–30 GB gp3 or larger
```

---

# 🔑 SSH to Conduktor EC2

After launching, record:

```text
CONDUKTOR_PUBLIC_IP
CONDUKTOR_PRIVATE_IP
```

From Windows PowerShell:

```powershell
ssh -i .\kaf20266.pem ec2-user@CONDUKTOR_PUBLIC_IP
```

Keep SSH alive:

```powershell
ssh -o ServerAliveInterval=30 -o ServerAliveCountMax=120 -i .\kaf20266.pem ec2-user@CONDUKTOR_PUBLIC_IP
```

---

# 🐳 Install Docker and Docker Compose

Run these commands on the **Conduktor EC2 only**.

Update:

```bash
sudo dnf update -y
```

Install Docker:

```bash
sudo dnf install docker -y
```

Enable and start:

```bash
sudo systemctl enable --now docker
```

Check:

```bash
sudo systemctl status docker
```

Verify:

```bash
docker --version
```

Add `ec2-user` to Docker group:

```bash
sudo usermod -aG docker ec2-user
```

Log out:

```bash
exit
```

SSH back in.

Test:

```bash
docker ps
```

---

## Verify Docker Compose

Try:

```bash
docker compose version
```

If the Compose plugin is not available on your Amazon Linux image, install Docker Compose using Docker's current supported installation instructions for your platform before continuing.

Do not continue until this works:

```bash
docker compose version
```

---

# 🔎 Test Kafka Connectivity

Before installing Conduktor, verify that the new EC2 can reach both Kafka brokers.

Check netcat:

```bash
which nc
```

Then:

```bash
nc -vz 172.31.36.36 9092
```

and:

```bash
nc -vz 172.31.42.7 9092
```

Expected conceptually:

```text
Connection succeeded
```

If either fails, stop here.

Check:

```text
Kafka broker process
Kafka listeners
AWS Security Groups
Private IP addresses
VPC/subnet routing
Network ACLs
Port 9092
```

---

# 📁 Create Project Directory

```bash
mkdir -p ~/conduktor
cd ~/conduktor
```

Check:

```bash
pwd
```

Expected:

```text
/home/ec2-user/conduktor
```

---

# 🔐 Create Secrets

For this lab we need:

```text
PostgreSQL database password
Conduktor administrator email
Conduktor administrator password
Optional Conduktor license
```

Generate strong random passwords, for example:

```bash
openssl rand -base64 24
```

Do not publish real passwords in GitHub.

---

# 🐘 Why PostgreSQL Is Required

Current Conduktor Console requires PostgreSQL to store Console state.

For our lab:

```text
Conduktor Console
       |
       | postgresql://...
       v
PostgreSQL Container
```

PostgreSQL is **not Kafka storage**.

Kafka messages remain in:

```text
Kafka Broker storage
```

PostgreSQL stores Conduktor's own application state.

---

# 📝 Create Docker Compose

Create:

```bash
vim ~/conduktor/docker-compose.yml
```

Use this lab template:

```yaml
services:

  postgresql:
    image: postgres:14
    container_name: conduktor-postgresql
    hostname: postgresql
    restart: unless-stopped

    environment:
      POSTGRES_DB: conduktor
      POSTGRES_USER: conduktor
      POSTGRES_PASSWORD: CHANGE_ME_STRONG_DB_PASSWORD
      POSTGRES_HOST_AUTH_METHOD: scram-sha-256

    volumes:
      - pg_data:/var/lib/postgresql/data

    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U conduktor -d conduktor"]
      interval: 10s
      timeout: 5s
      retries: 10


  conduktor-console:
    image: conduktor/conduktor-console:latest
    container_name: conduktor-console
    restart: unless-stopped

    depends_on:
      postgresql:
        condition: service_healthy

    ports:
      - "8080:8080"

    volumes:
      - conduktor_data:/var/conduktor

    environment:
      CDK_DATABASE_URL: "postgresql://conduktor:CHANGE_ME_STRONG_DB_PASSWORD@postgresql:5432/conduktor"

      CDK_ORGANIZATION_NAME: "VishwaTechLabs"

      CDK_ADMIN_EMAIL: "CHANGE_ME_ADMIN_EMAIL"

      CDK_ADMIN_PASSWORD: "CHANGE_ME_STRONG_ADMIN_PASSWORD"

      # Optional/edition-dependent:
      # CDK_LICENSE: "CHANGE_ME_LICENSE_KEY"

    healthcheck:
      test:
        [
          "CMD-SHELL",
          "curl -f http://localhost:8080/platform/api/modules/health/live || exit 1"
        ]
      interval: 10s
      start_period: 20s
      timeout: 5s
      retries: 10


volumes:
  pg_data:
  conduktor_data:
```

---

# ⚠️ Replace These Values

Replace:

```text
CHANGE_ME_STRONG_DB_PASSWORD
CHANGE_ME_ADMIN_EMAIL
CHANGE_ME_STRONG_ADMIN_PASSWORD
```

If your Conduktor plan requires/provides a license, also configure the exact license according to the current documentation:

```yaml
CDK_LICENSE: "YOUR_LICENSE"
```

Do not invent a license value.

---

# 🔐 Protect Compose File

Because this lab compose file contains credentials:

```bash
chmod 600 ~/conduktor/docker-compose.yml
```

Never commit the real file containing secrets to a public repository.

For a real project, use:

```text
.env
AWS Secrets Manager
SSM Parameter Store
Docker secrets
Kubernetes Secrets
External Secrets
```

depending on architecture.

---

# 🔍 Validate Compose Syntax

Run:

```bash
cd ~/conduktor
docker compose config
```

If there are YAML errors, fix them before starting.

---

# ▶️ Start Conduktor

From:

```bash
cd ~/conduktor
```

Run:

```bash
docker compose up -d
```

This starts:

```text
PostgreSQL
     ↓
Conduktor Console
```

---

# ✅ Validate Containers

Run:

```bash
docker compose ps
```

Also:

```bash
docker ps
```

Expected conceptually:

```text
conduktor-postgresql
conduktor-console
```

Conduktor should eventually become healthy.

---

# 📜 Check Conduktor Logs

```bash
docker logs -f conduktor-console
```

Exit log follow with:

```text
Ctrl+C
```

This does not stop the container.

Last 100 lines:

```bash
docker logs --tail 100 conduktor-console
```

---

# 📜 Check PostgreSQL Logs

```bash
docker logs --tail 100 conduktor-postgresql
```

---

# ❤️ Check Health Endpoint

From the Conduktor EC2:

```bash
curl -f http://localhost:8080/platform/api/modules/health/live
```

If healthy, the command should succeed.

Check port:

```bash
sudo ss -lntp | grep 8080
```

---

# 🌍 Open Conduktor Console

From your laptop browser:

```text
http://CONDUKTOR_PUBLIC_IP:8080
```

Example:

```text
http://54.x.x.x:8080
```

Log in using the admin account configured in:

```text
CDK_ADMIN_EMAIL
CDK_ADMIN_PASSWORD
```

---

# 🔌 Add Existing Kafka Cluster

Current Conduktor documentation recommends adding Kafka clusters through the Console UI.

After login:

```text
Conduktor Console
       ↓
Settings
       ↓
Clusters
       ↓
Add Cluster
```

Use a friendly name:

```text
VishwaTechLabs-Kafka-KRaft
```

Bootstrap servers:

```text
172.31.36.36:9092,172.31.42.7:9092
```

Our lab Kafka security is:

```text
PLAINTEXT
```

So select/configure the equivalent non-TLS/non-SASL Kafka connection for this controlled lab.

Test the connection.

Then save.

---

# ⭐ Why Private Broker IPs?

Because Conduktor runs in the same AWS VPC:

```text
Conduktor
    |
    | AWS Private Network
    v
172.31.36.36:9092
172.31.42.7:9092
```

This is preferable to:

```text
Conduktor
    |
    | Internet
    v
34.253.224.235:19092
18.203.251.147:19092
```

for our architecture.

---

# 🧠 Kafka Metadata Flow

Kafka bootstrap does **not** mean every operation always goes through only one bootstrap address.

Conceptually:

```text
Conduktor
    |
    | bootstrap
    v
Broker-1 / Broker-2
    |
    | Kafka metadata
    v
Discover cluster brokers
    |
    v
Connect to required brokers
```

Therefore the addresses returned through Kafka metadata must be reachable from Conduktor.

Our internal advertised listeners are designed for this:

Broker-1:

```properties
INTERNAL://172.31.36.36:9092
```

Broker-2:

```properties
INTERNAL://172.31.42.7:9092
```

---

# 🧪 Verify Kafka Resources

We already created:

```text
vishwatech-topic
```

From Broker-1, verify with CLI:

```bash
cd /opt/kafka

bin/kafka-topics.sh \
  --bootstrap-server 172.31.36.36:9092 \
  --list
```

Describe:

```bash
bin/kafka-topics.sh \
  --bootstrap-server 172.31.36.36:9092 \
  --describe \
  --topic vishwatech-topic
```

Now find the same topic in Conduktor.

Compare:

```text
Kafka CLI
    vs
Conduktor UI
```

---

# 📤 Producer Test

From Broker-1:

```bash
cd /opt/kafka

bin/kafka-console-producer.sh \
  --bootstrap-server 172.31.36.36:9092 \
  --topic vishwatech-topic
```

Enter:

```text
Hello Conduktor
Hello Kafka KRaft
Welcome to VishwaTechLabs
Conduktor integration is working
```

---

# 📥 Consumer Test

From Broker-2:

```bash
cd /opt/kafka

bin/kafka-console-consumer.sh \
  --bootstrap-server 172.31.42.7:9092 \
  --topic vishwatech-topic \
  --from-beginning
```

Then inspect the topic and consumer-group behavior in Conduktor where supported by your edition and permissions.

---

# 📊 What You Can Learn

With the CLI + Conduktor together, study:

```text
Kafka Cluster
│
├── Brokers
│
├── Topics
│   ├── Partitions
│   ├── Leaders
│   ├── Replicas
│   └── Configuration
│
├── Consumer Groups
│   ├── Members
│   ├── Offsets
│   └── Lag
│
└── Records / Data
```

This is especially useful for learning real-world Kafka troubleshooting.

---

# 🧪 Consumer Lag Lab

Create a consumer group:

```bash
bin/kafka-console-consumer.sh \
  --bootstrap-server 172.31.42.7:9092 \
  --topic vishwatech-topic \
  --group vishwatech-students \
  --from-beginning
```

Produce many records.

Stop or slow the consumer.

Check using Kafka CLI:

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server 172.31.36.36:9092 \
  --describe \
  --group vishwatech-students
```

Study:

```text
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

Then inspect the group in Conduktor.

---

# 🔄 Lifecycle Commands

## Check Everything

```bash
cd ~/conduktor
docker compose ps
```

---

## Stop

```bash
docker compose stop
```

---

## Start

```bash
docker compose start
```

---

## Restart

```bash
docker compose restart
```

---

## View Logs

```bash
docker compose logs
```

---

## Follow Logs

```bash
docker compose logs -f
```

---

## Console Logs Only

```bash
docker compose logs -f conduktor-console
```

---

## PostgreSQL Logs Only

```bash
docker compose logs -f postgresql
```

---

# 🛑 Bring Stack Down

```bash
docker compose down
```

This removes containers/network but keeps named volumes.

Your named volumes remain:

```text
pg_data
conduktor_data
```

---

# 🚨 Delete Everything Including Volumes

Only do this if you intentionally want to destroy Conduktor/PostgreSQL lab state:

```bash
docker compose down -v
```

> [!CAUTION]
> `-v` removes named volumes. Do not run it casually.

---

# 🔄 Upgrade Conduktor

For a lab using `latest`:

```bash
cd ~/conduktor

docker compose pull

docker compose up -d
```

Then:

```bash
docker compose ps
docker compose logs --tail 100 conduktor-console
```

> [!IMPORTANT]
> In production, pin and test specific versions rather than blindly upgrading to `latest`. Review Conduktor release notes and migration guidance before upgrading.

---

# 🛠️ Troubleshooting

## Problem 1 — Conduktor UI Does Not Open

Check:

```bash
docker compose ps
```

Check:

```bash
sudo ss -lntp | grep 8080
```

Check:

```bash
curl -f http://localhost:8080/platform/api/modules/health/live
```

Check AWS SG:

```text
TCP 8080
Source YOUR_PUBLIC_IP/32
```

Check logs:

```bash
docker logs --tail 200 conduktor-console
```

---

## Problem 2 — Conduktor Container Exits

```bash
docker ps -a
```

Then:

```bash
docker logs conduktor-console
```

Common areas:

```text
PostgreSQL connectivity
Invalid database URL
Invalid credentials
License/configuration issue
Memory/resources
Filesystem permissions
```

---

## Problem 3 — PostgreSQL Is Not Healthy

Check:

```bash
docker compose ps
```

Logs:

```bash
docker logs conduktor-postgresql
```

Check the configured:

```text
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
```

and ensure `CDK_DATABASE_URL` uses the same values.

---

## Problem 4 — Conduktor Cannot Connect to Kafka

From Conduktor EC2:

```bash
nc -vz 172.31.36.36 9092
nc -vz 172.31.42.7 9092
```

If these fail:

```text
AWS Security Group
Kafka broker listener
Kafka process
VPC routing
Network ACL
Wrong private IP
```

---

## Problem 5 — Bootstrap Works but Cluster Discovery Fails

This often points to `advertised.listeners`.

Broker-1 must advertise an address Conduktor can reach:

```properties
INTERNAL://172.31.36.36:9092
```

Broker-2:

```properties
INTERNAL://172.31.42.7:9092
```

Conduktor inside AWS must be able to reach both.

---

## Problem 6 — Accidentally Used Controller Port

Wrong:

```text
172.31.35.220:9093 ❌
172.31.34.34:9093  ❌
```

Correct:

```text
172.31.36.36:9092 ✅
172.31.42.7:9092  ✅
```

Remember:

```text
9093 = KRaft controller

9092 = Kafka broker internal client listener
```

---

## Problem 7 — Docker Permission Denied

Run:

```bash
sudo usermod -aG docker ec2-user
```

Logout/login.

Then:

```bash
groups
docker ps
```

---

## Problem 8 — Port 8080 Already Used

Check:

```bash
sudo ss -lntp | grep 8080
```

Check containers:

```bash
docker ps
```

Do not randomly kill processes. Identify the existing service first.

---

## Problem 9 — Forgot Admin Password

For a disposable learning lab, consult the current Conduktor documentation for the supported account recovery/reset procedure for your installed version. Do not destroy PostgreSQL volumes just to reset a password unless you intentionally want to erase the lab.

---

# 🔒 Security Hardening

The current Kafka lab uses:

```text
PLAINTEXT
```

and this simple Conduktor deployment exposes:

```text
HTTP :8080
```

That is for a controlled lab.

For production consider:

```text
HTTPS
DNS
Reverse proxy / load balancer
SSO / OIDC
RBAC
Audit logging
Kafka TLS
Kafka SASL_SSL
SCRAM
mTLS
Kafka ACLs
Private subnets
VPN / corporate network
Secrets Manager
RDS PostgreSQL
Backups
Monitoring
Pinned container versions
Restricted security groups
```

---

# 🔐 Secrets Rule

Never put real credentials into a public README.

Create:

```text
.env.example
```

but keep real:

```text
.env
docker-compose.yml containing secrets
license keys
passwords
certificates
private keys
```

out of Git.

Example `.gitignore`:

```gitignore
.env
*.env
secrets/
certs/
*.key
*.p12
*.jks
```

---

# ⚠️ Production Notes

## PostgreSQL

For this lab:

```text
PostgreSQL = Docker container
```

For production, an externally managed PostgreSQL service such as appropriately supported Amazon RDS/Aurora PostgreSQL may be preferable depending on architecture and current Conduktor support requirements.

---

## Conduktor Persistence

Keep:

```text
conduktor_data
pg_data
```

persistent.

Do not casually run:

```bash
docker compose down -v
```

---

## High Availability

One Conduktor EC2 is perfect for learning, but it is a single point of failure.

Production design may require:

```text
Multiple application instances
Load balancer
External PostgreSQL
Backups
DNS
TLS
Monitoring
```

according to Conduktor's supported architecture.

---

## Kafka Authentication

When the Kafka lab is upgraded from:

```text
PLAINTEXT
```

to:

```text
SSL
SASL_SSL
SCRAM
mTLS
```

update the Conduktor Kafka cluster configuration accordingly.

Current Conduktor versions provide certificate management and Kafka security options through the cluster configuration UI.

---

# 🧠 Conduktor vs Kpow in This Lab

Both tools sit **outside Kafka** and connect as Kafka clients/admin applications.

Conceptually:

```text
                  Kafka KRaft Cluster
                         |
             +-----------+-----------+
             |                       |
             v                       v
           Kpow                  Conduktor
        Kafka UI/ops          Kafka UI/ops
```

You do **not** need both for Kafka itself to work.

Using both in a learning environment is useful because you can compare:

```text
Kafka CLI
Offset Explorer
Kpow
Conduktor
```

and understand how different Kafka administration tools expose the same underlying cluster concepts.

---

# 🎓 Learning Outcomes

After this lab, students should understand:

```text
What is Conduktor Console?

Why is it separate from Kafka?

Why does Conduktor connect to brokers?

What is a Kafka bootstrap server?

Why not use KRaft controller port 9093?

Why use private broker IPs?

What is advertised.listeners?

Why does Conduktor require PostgreSQL?

What does PostgreSQL store?

How does Docker Compose work?

How do containers communicate?

Why is 5432 not exposed publicly?

Why is 8080 restricted?

How do you troubleshoot Conduktor → Kafka?

How do you inspect consumer lag?

How do Kafka CLI and UI tools complement each other?
```

---

# 🧩 Complete Lab Flow

```text
Existing Kafka Cluster
        |
        v
Create Conduktor EC2
        |
        v
Same AWS VPC
        |
        v
Configure Security Groups
        |
        v
Install Docker
        |
        v
Verify Docker Compose
        |
        v
Test Broker-1 :9092
        |
        v
Test Broker-2 :9092
        |
        v
Create ~/conduktor
        |
        v
Create PostgreSQL + Console Compose
        |
        v
docker compose config
        |
        v
docker compose up -d
        |
        v
PostgreSQL Healthy
        |
        v
Conduktor Healthy
        |
        v
Open :8080
        |
        v
Login as Admin
        |
        v
Settings → Clusters
        |
        v
Add Kafka Bootstrap Servers
        |
        v
172.31.36.36:9092
172.31.42.7:9092
        |
        v
Test Connection
        |
        v
Open vishwatech-topic
        |
        v
Inspect Partitions / Records / Consumers / Lag
        |
        v
🎉 CONDUKTOR LAB COMPLETE
```

---

# 📌 Quick Reference

## Controllers

```text
Controller-1
172.31.35.220:9093

Controller-2
172.31.34.34:9093
```

Do not use them as Conduktor Kafka bootstrap endpoints.

---

## Brokers

```text
Broker-1 Internal
172.31.36.36:9092

Broker-2 Internal
172.31.42.7:9092
```

Conduktor bootstrap:

```text
172.31.36.36:9092,172.31.42.7:9092
```

---

## External Kafka

```text
Broker-1
34.253.224.235:19092

Broker-2
18.203.251.147:19092
```

Used by Offset Explorer/laptop, not needed by Conduktor in the same VPC.

---

## Conduktor

```text
Browser:
http://CONDUKTOR_PUBLIC_IP:8080
```

---

## Useful Commands

```bash
cd ~/conduktor

docker compose config

docker compose up -d

docker compose ps

docker compose logs -f conduktor-console

docker compose restart

docker compose down
```

---

# 🔗 Official References

- [Conduktor Documentation](https://docs.conduktor.io/)
- [Conduktor Console Deployment](https://docs.conduktor.io/guide/conduktor-in-production/deploy-artifacts/deploy-console)
- [Apache Kafka](https://kafka.apache.org/)
- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [PostgreSQL](https://www.postgresql.org/)
- [AWS EC2](https://docs.aws.amazon.com/ec2/)
- [AWS Security Groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html)

---

# 👨‍💻 VishwaTechLabs

> **Learn → Build → Observe → Break → Troubleshoot → Fix → Master**

<p align="center">

![Kafka](https://img.shields.io/badge/Kafka-KRaft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Conduktor](https://img.shields.io/badge/Conduktor-Console-5B5BD6?style=for-the-badge)
![Hands On](https://img.shields.io/badge/Learning-Hands--On-brightgreen?style=for-the-badge)
![AWS](https://img.shields.io/badge/Platform-AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)

### 🎉 Kafka CLI + Offset Explorer + Kpow + Conduktor = Excellent Kafka Administration Practice Lab

</p>
