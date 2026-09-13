# 🚀 VishwaTechLabs — Kpow for Existing Apache Kafka KRaft Cluster on AWS EC2

<p align="center">

![Kpow](https://img.shields.io/badge/Kpow-Kafka%20UI-6A5ACD?style=for-the-badge)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-EC2-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Amazon Linux](https://img.shields.io/badge/Amazon%20Linux-2023-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)
![Lab](https://img.shields.io/badge/Lab-2%20Brokers%20%2B%202%20Controllers-success?style=for-the-badge)

</p>

<p align="center">
<b>Install Kpow on a dedicated AWS EC2 instance and connect it securely to the existing VishwaTechLabs Kafka KRaft cluster.</b>
</p>

---

> [!IMPORTANT]
> This guide is written specifically for the existing VishwaTechLabs lab:
>
> - 2 dedicated KRaft controllers
> - 2 dedicated Kafka brokers
> - AWS EC2 / Amazon Linux
> - Kafka internal listener on TCP `9092`
> - Kafka external listener on TCP `19092`
> - KRaft controller listener on TCP `9093`
> - Kpow UI on TCP `3000`

---

# 📚 Table of Contents

1. [🎯 Lab Objective](#-lab-objective)
2. [💡 Why Kpow?](#-why-kpow)
3. [🏗️ Existing Kafka Architecture](#️-existing-kafka-architecture)
4. [🖥️ Existing Cluster Inventory](#️-existing-cluster-inventory)
5. [🆕 Where Kpow Should Be Installed](#-where-kpow-should-be-installed)
6. [🌐 Network Flow](#-network-flow)
7. [🔐 Security Group Design](#-security-group-design)
8. [☁️ Create the Kpow EC2 Instance](#️-create-the-kpow-ec2-instance)
9. [🔑 SSH to Kpow](#-ssh-to-kpow)
10. [🐳 Install Docker](#-install-docker)
11. [🔎 Verify Kafka Connectivity Before Kpow](#-verify-kafka-connectivity-before-kpow)
12. [🔐 Obtain a Kpow License](#-obtain-a-kpow-license)
13. [📁 Create the Kpow Directory](#-create-the-kpow-directory)
14. [⚙️ Create `kpow.env`](#️-create-kpowenv)
15. [📥 Pull the Kpow Image](#-pull-the-kpow-image)
16. [▶️ Start Kpow](#️-start-kpow)
17. [✅ Validate Kpow](#-validate-kpow)
18. [🌍 Open Kpow from Laptop](#-open-kpow-from-laptop)
19. [🧪 Validate the Existing Kafka Topic](#-validate-the-existing-kafka-topic)
20. [🔄 Docker Lifecycle Commands](#-docker-lifecycle-commands)
21. [🛠️ Troubleshooting](#️-troubleshooting)
22. [🔒 Security Improvements](#-security-improvements)
23. [⚠️ Production Considerations](#️-production-considerations)
24. [🎓 Learning Outcomes](#-learning-outcomes)
25. [🔗 Official References](#-official-references)

---

# 🎯 Lab Objective

We already have a working Kafka KRaft cluster.

Now we want to add a graphical Kafka administration and observability tool:

```text
Kpow
```

Instead of running Kpow on a Kafka broker or controller, we will create a **dedicated fifth EC2 instance**.

The final environment becomes:

```text
2 x KRaft Controllers
2 x Kafka Brokers
1 x Kpow Server
```

---

# 💡 Why Kpow?

Kafka command-line tools are extremely important:

```text
kafka-topics.sh
kafka-console-producer.sh
kafka-console-consumer.sh
kafka-consumer-groups.sh
kafka-configs.sh
```

But in enterprise operations, a UI can make administration, troubleshooting and learning much easier.

Kpow can connect to Kafka using Kafka client APIs and provides a web interface for Kafka resources.

Typical activities include:

- 📦 Inspect Kafka topics
- 🧩 Inspect partitions
- 👑 Inspect partition leaders
- 🔁 Inspect replicas
- 👥 Inspect consumer groups
- ⏱️ Analyze consumer lag
- 🔍 Inspect topic data when permitted
- ✉️ Produce records when permitted
- ⚙️ Inspect/configure Kafka resources according to permissions
- 📊 Observe Kafka operational information
- 🧪 Troubleshoot application/Kafka behavior

---

# 🏗️ Existing Kafka Architecture

Our Kafka cluster already looks like this:

```text
                         YOUR LAPTOP
                              |
                              | Offset Explorer
                              | TCP 19092
                              |
             +----------------+----------------+
             |                                 |
             v                                 v
     +---------------+                 +---------------+
     |   BROKER-1    |                 |   BROKER-2    |
     | node.id = 3   |<---- 9092 ----->| node.id = 4   |
     |               |                 |               |
     | 172.31.36.36  |                 | 172.31.42.7   |
     +-------+-------+                 +-------+-------+
             |                                 |
             +----------------+----------------+
                              |
                             9093
                              |
             +----------------+----------------+
             |                                 |
             v                                 v
    +----------------+                +----------------+
    |  CONTROLLER-1  |<---- 9093 ---->|  CONTROLLER-2  |
    |   node.id=1    |                |   node.id=2    |
    | 172.31.35.220  |                | 172.31.34.34   |
    +----------------+                +----------------+
```

---

# 🖥️ Existing Cluster Inventory

| Node | Role | Node ID | Private IP | Public IP | Port |
|---|---|---:|---|---|---|
| Controller-1 | KRaft Controller | `1` | `172.31.35.220` | `3.251.97.0` | `9093` |
| Controller-2 | KRaft Controller | `2` | `172.31.34.34` | `108.133.98.249` | `9093` |
| Broker-1 | Kafka Broker | `3` | `172.31.36.36` | `34.253.224.235` | `9092 / 19092` |
| Broker-2 | Kafka Broker | `4` | `172.31.42.7` | `18.203.251.147` | `9092 / 19092` |

Our Kafka bootstrap addresses for an application running **inside the VPC** are:

```text
172.31.36.36:9092
172.31.42.7:9092
```

Kpow will use these private addresses.

---

# 🆕 Where Kpow Should Be Installed

## Recommended Lab Design

Create one additional EC2 instance:

```text
Name: kpow
OS: Amazon Linux 2023
Instance type: t3.medium
```

For a small learning/dev lab, we will start the Kpow container with a `2G` memory limit.

Factor House currently recommends more resources for production environments, so production sizing must be done separately.

---

## Why a Separate EC2?

Do **not** install Kpow directly on:

```text
Controller-1 ❌
Controller-2 ❌
Broker-1     ❌
Broker-2     ❌
```

Recommended:

```text
Dedicated Kpow EC2 ✅
```

Benefits:

- Separation of responsibilities
- Easier troubleshooting
- Kafka brokers keep their CPU/RAM for Kafka
- KRaft controllers remain dedicated to metadata
- Kpow can be restarted independently
- Cleaner enterprise-style architecture

---

# 🌐 Network Flow

After Kpow is installed:

```text
                            YOUR LAPTOP
                                 |
                 +---------------+---------------+
                 |                               |
                 | Offset Explorer               | Browser
                 | TCP 19092                     | TCP 3000
                 |                               |
                 |                               v
                 |                       +---------------+
                 |                       |     KPOW      |
                 |                       | Dedicated EC2 |
                 |                       +-------+-------+
                 |                               |
                 |                        TCP 9092
                 |                        PRIVATE
                 |                               |
       +---------+-------------------------------+---------+
       |                                                   |
       v                                                   v
+---------------+                                  +---------------+
|   BROKER-1    |                                  |   BROKER-2    |
| node.id = 3   |<----------- TCP 9092 ---------->| node.id = 4   |
|172.31.36.36   |                                  |172.31.42.7    |
+-------+-------+                                  +-------+-------+
        |                                                  |
        +-------------------------+------------------------+
                                  |
                           KRaft TCP 9093
                                  |
                  +---------------+---------------+
                  |                               |
                  v                               v
          +---------------+               +---------------+
          | CONTROLLER-1  |               | CONTROLLER-2  |
          | node.id = 1   |               | node.id = 2   |
          |172.31.35.220  |               |172.31.34.34   |
          +---------------+               +---------------+
```

---

# ⭐ Golden Networking Rule

```text
KRaft Controller Communication
        TCP 9093
        PRIVATE ONLY

Broker Internal Communication
        TCP 9092
        PRIVATE ONLY

Kpow → Kafka
        TCP 9092
        PRIVATE

Offset Explorer → Kafka
        TCP 19092
        EXTERNAL

Browser → Kpow
        TCP 3000
```

Kpow does **not** need the controllers configured as Kafka bootstrap servers.

Do not configure this:

```text
172.31.35.220:9093 ❌
172.31.34.34:9093  ❌
```

Configure:

```text
172.31.36.36:9092,172.31.42.7:9092 ✅
```

---

# 🔐 Security Group Design

This step is extremely important.

## Kpow EC2 Inbound

Recommended:

| Port | Source | Purpose |
|---:|---|---|
| `22` | Your laptop public IP `/32` | SSH |
| `3000` | Your laptop public IP `/32` | Kpow Web UI |

Do not unnecessarily use:

```text
TCP 3000
Source 0.0.0.0/0
```

Prefer:

```text
TCP 3000
Source YOUR_PUBLIC_IP/32
```

---

## Kafka Broker Inbound

Kpow must be able to reach:

```text
Broker-1 172.31.36.36:9092
Broker-2 172.31.42.7:9092
```

The Kafka broker security group therefore needs to allow TCP `9092` from the Kpow instance/security group.

Recommended AWS design:

```text
Type: Custom TCP
Port: 9092
Source: Kpow Security Group
```

If all Kafka/Kpow instances intentionally use the same SG, a same-security-group source rule can be used.

---

## KRaft Port

Keep:

```text
9093
```

private between Kafka nodes.

Kpow does not need direct access to the KRaft controller listener for this setup.

---

# ☁️ Create the Kpow EC2 Instance

AWS Console:

```text
EC2
 ↓
Instances
 ↓
Launch Instances
```

Recommended lab values:

```text
Name:
kpow

AMI:
Amazon Linux 2023

Instance Type:
t3.medium

Key Pair:
Use the same key pair if appropriate for your lab

VPC:
SAME VPC as Kafka

Subnet:
A subnet that can privately reach the Kafka brokers

Security Group:
Kpow SG
```

The most important requirement is:

```text
Kpow PRIVATE IP
        |
        | VPC routing + SG
        v
Kafka Broker PRIVATE IPs
```

---

# 🔑 SSH to Kpow

After the EC2 instance starts, note:

```text
KPOW_PUBLIC_IP
KPOW_PRIVATE_IP
```

From Windows PowerShell:

```powershell
ssh -i .\kaf20266.pem ec2-user@KPOW_PUBLIC_IP
```

To keep the SSH connection alive:

```powershell
ssh -o ServerAliveInterval=30 -o ServerAliveCountMax=120 -i .\kaf20266.pem ec2-user@KPOW_PUBLIC_IP
```

---

# 🐳 Install Docker

Run the following **only on the Kpow EC2 server**.

Update packages:

```bash
sudo dnf update -y
```

Install Docker:

```bash
sudo dnf install docker -y
```

Enable and start Docker:

```bash
sudo systemctl enable --now docker
```

Check service:

```bash
sudo systemctl status docker
```

Press:

```text
q
```

to leave the status screen.

Verify Docker:

```bash
docker --version
```

---

## Add `ec2-user` to Docker Group

```bash
sudo usermod -aG docker ec2-user
```

Check:

```bash
groups ec2-user
```

Now log out:

```bash
exit
```

SSH back into Kpow:

```powershell
ssh -i .\kaf20266.pem ec2-user@KPOW_PUBLIC_IP
```

Test:

```bash
docker ps
```

If this works without `sudo`, Docker permissions are ready.

---

# 🔎 Verify Kafka Connectivity Before Kpow

Do this **before starting Kpow**.

We want:

```text
Kpow EC2
   |
   +----> 172.31.36.36:9092
   |
   +----> 172.31.42.7:9092
```

Check whether `nc` is available:

```bash
which nc
```

If necessary, install a package providing netcat for your Amazon Linux version, then test both brokers.

Broker-1:

```bash
nc -vz 172.31.36.36 9092
```

Broker-2:

```bash
nc -vz 172.31.42.7 9092
```

Both must succeed before continuing.

If they fail, investigate:

```text
AWS Security Group
VPC/Subnet routing
Broker process
Kafka listener
Port 9092
Private IP
```

---

# 🔐 Obtain a Kpow License

Kpow Community Edition requires valid license information.

Official Kpow:

https://factorhouse.io/products/kpow/

Official Docker installation guide:

https://docs.factorhouse.io/kpow/installation/docker

The license information supplied by Factor House is used in the environment file.

It includes values such as:

```text
LICENSE_ID
LICENSE_CODE
LICENSEE
LICENSE_EXPIRY
LICENSE_SIGNATURE
```

> [!CAUTION]
> Do not commit a real license file or license values to a public Git repository.

---

# 📁 Create the Kpow Directory

On the Kpow EC2:

```bash
mkdir -p ~/kpow
```

Enter:

```bash
cd ~/kpow
```

Check:

```bash
pwd
```

Expected:

```text
/home/ec2-user/kpow
```

---

# ⚙️ Create `kpow.env`

Create:

```bash
vim ~/kpow/kpow.env
```

Use:

```properties
########################################################
# VishwaTechLabs
# Kpow Community Edition
# Existing Kafka KRaft Cluster
########################################################


########################################################
# Kpow Environment Name
########################################################

ENVIRONMENT_NAME=VishwaTechLabs-Kafka-KRaft


########################################################
# Existing Kafka Brokers
########################################################

BOOTSTRAP=172.31.36.36:9092,172.31.42.7:9092


########################################################
# Kafka Security
########################################################

SECURITY_PROTOCOL=PLAINTEXT


########################################################
# Kpow License
# Replace with the exact values supplied by Factor House
########################################################

LICENSE_ID=YOUR_LICENSE_ID

LICENSE_CODE=COMMUNITY

LICENSEE=YOUR_LICENSEE

LICENSE_EXPIRY=YOUR_LICENSE_EXPIRY

LICENSE_SIGNATURE=YOUR_LICENSE_SIGNATURE
```

Save.

---

## Protect the Environment File

Because the file contains license information:

```bash
chmod 600 ~/kpow/kpow.env
```

Check:

```bash
ls -l ~/kpow/kpow.env
```

---

## Verify Without Printing Secrets

Instead of running:

```bash
cat kpow.env
```

on shared terminals/screenshots, verify only the non-sensitive Kafka settings:

```bash
grep -E '^(ENVIRONMENT_NAME|BOOTSTRAP|SECURITY_PROTOCOL)=' ~/kpow/kpow.env
```

Expected:

```text
ENVIRONMENT_NAME=VishwaTechLabs-Kafka-KRaft
BOOTSTRAP=172.31.36.36:9092,172.31.42.7:9092
SECURITY_PROTOCOL=PLAINTEXT
```

---

# 📥 Pull the Kpow Image

Community Edition image:

```bash
docker pull factorhouse/kpow-ce:latest
```

Check:

```bash
docker images
```

You should see:

```text
factorhouse/kpow-ce
```

---

# ▶️ Start Kpow

First make sure no old container exists:

```bash
docker ps -a
```

Start Kpow:

```bash
docker run -d \
  --name kpow \
  --restart unless-stopped \
  -p 3000:3000 \
  -m 2G \
  --env-file ~/kpow/kpow.env \
  factorhouse/kpow-ce:latest
```

Explanation:

```text
docker run
   |
   +-- -d
   |     Run in background
   |
   +-- --name kpow
   |     Container name
   |
   +-- --restart unless-stopped
   |     Restart after host/Docker restart unless manually stopped
   |
   +-- -p 3000:3000
   |     EC2 port 3000 → Kpow container port 3000
   |
   +-- -m 2G
   |     Limit container memory to 2 GB for this lab
   |
   +-- --env-file
   |     Load Kafka + license configuration
   |
   +-- factorhouse/kpow-ce:latest
         Community Edition image
```

---

# ✅ Validate Kpow

Check running containers:

```bash
docker ps
```

Expected conceptually:

```text
CONTAINER ID   IMAGE                        PORTS
xxxxxxxxxxxx   factorhouse/kpow-ce:latest   0.0.0.0:3000->3000/tcp
```

---

## Check Container State

```bash
docker inspect kpow --format '{{.State.Status}}'
```

Expected:

```text
running
```

---

## Check Port 3000

```bash
sudo ss -lntp | grep 3000
```

---

## Follow Kpow Logs

```bash
docker logs -f kpow
```

Look for normal startup and successful Kafka discovery.

Possible problems include:

```text
Connection refused
Timeout
Authentication error
License error
Bootstrap connection error
```

Press:

```text
Ctrl+C
```

to stop following logs.

This **does not stop Kpow**.

---

## Last 100 Log Lines

```bash
docker logs --tail 100 kpow
```

---

# 🌍 Open Kpow from Laptop

Get the Kpow EC2 public IP.

Example:

```text
KPOW_PUBLIC_IP=xx.xx.xx.xx
```

Open:

```text
http://KPOW_PUBLIC_IP:3000
```

For example:

```text
http://54.x.x.x:3000
```

You should see the Kpow UI.

The configured environment name should be:

```text
VishwaTechLabs-Kafka-KRaft
```

---

# 🔄 How Kpow Discovers the Cluster

Kpow starts with:

```text
BOOTSTRAP
   |
   +----> 172.31.36.36:9092
   |
   +----> 172.31.42.7:9092
```

Kafka then returns broker metadata.

Therefore, the brokers must continue advertising addresses reachable from the Kpow server.

Our existing internal advertised listeners are:

Broker-1:

```text
INTERNAL://172.31.36.36:9092
```

Broker-2:

```text
INTERNAL://172.31.42.7:9092
```

That is exactly what Kpow inside the same VPC needs.

---

# 🧠 Why We Do Not Use Port 19092 for Kpow

Port `19092` exists for external clients such as Offset Explorer on the laptop.

```text
Laptop
   |
   | Internet
   v
Public-IP:19092
```

Kpow lives inside AWS:

```text
Kpow EC2
   |
   | Private VPC
   v
Private-IP:9092
```

Using the private listener is cleaner and avoids unnecessary internet routing.

---

# 🧠 Why We Do Not Use Port 9093

`9093` belongs to our KRaft controller listener.

```text
Controller-1
172.31.35.220:9093

Controller-2
172.31.34.34:9093
```

Kpow is configured as a Kafka client against broker bootstrap endpoints:

```text
172.31.36.36:9092
172.31.42.7:9092
```

Do not put controller addresses into `BOOTSTRAP`.

---

# 🧪 Validate the Existing Kafka Topic

Our earlier lab created:

```text
vishwatech-topic
```

Before blaming Kpow, confirm Kafka itself still works.

From Broker-1:

```bash
cd /opt/kafka
```

List topics:

```bash
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

Then inspect the corresponding topic/resources in Kpow.

This gives us a very useful comparison:

```text
Kafka CLI
    vs
Kpow UI
```

---

# 📤 Optional Producer Test

On Broker-1:

```bash
cd /opt/kafka

bin/kafka-console-producer.sh \
  --bootstrap-server 172.31.36.36:9092 \
  --topic vishwatech-topic
```

Enter:

```text
Hello from VishwaTechLabs
Testing Kafka with Kpow
Kpow lab is working
```

Press:

```text
Ctrl+C
```

when done.

Where permitted by your Kpow edition/configuration, use Kpow's data inspection features to observe records.

---

# 📥 Optional Consumer Test

On Broker-2:

```bash
cd /opt/kafka

bin/kafka-console-consumer.sh \
  --bootstrap-server 172.31.42.7:9092 \
  --topic vishwatech-topic \
  --from-beginning
```

This confirms the Kafka data path independently of Kpow.

---

# 🔄 Docker Lifecycle Commands

## Show Running Container

```bash
docker ps
```

---

## Show All Containers

```bash
docker ps -a
```

---

## Stop Kpow

```bash
docker stop kpow
```

---

## Start Kpow

```bash
docker start kpow
```

---

## Restart Kpow

```bash
docker restart kpow
```

---

## View Logs

```bash
docker logs kpow
```

---

## Follow Logs

```bash
docker logs -f kpow
```

---

## Remove Container

Stop first:

```bash
docker stop kpow
```

Remove:

```bash
docker rm kpow
```

This does not delete the Docker image.

---

## Remove Image

Only if required:

```bash
docker rmi factorhouse/kpow-ce:latest
```

---

# 🔄 Update Kpow Community Edition

When you intentionally want to move to the current `latest` image:

```bash
docker pull factorhouse/kpow-ce:latest
```

Stop:

```bash
docker stop kpow
```

Remove the old container:

```bash
docker rm kpow
```

Recreate:

```bash
docker run -d \
  --name kpow \
  --restart unless-stopped \
  -p 3000:3000 \
  -m 2G \
  --env-file ~/kpow/kpow.env \
  factorhouse/kpow-ce:latest
```

Verify:

```bash
docker ps
```

Then:

```bash
docker logs --tail 100 kpow
```

> [!TIP]
> For controlled production change management, pin and test a specific image version rather than automatically relying on `latest`.

---

# 🛠️ Troubleshooting

# Problem 1 — Kpow Cannot Reach Broker-1

Test from Kpow:

```bash
nc -vz 172.31.36.36 9092
```

If failed:

```text
Check Broker-1 process
Check port 9092
Check AWS Security Group
Check VPC
Check private IP
Check Kafka listener
```

On Broker-1:

```bash
ss -lntp | grep 9092
```

---

# Problem 2 — Kpow Cannot Reach Broker-2

From Kpow:

```bash
nc -vz 172.31.42.7 9092
```

On Broker-2:

```bash
ss -lntp | grep 9092
```

---

# Problem 3 — Browser Cannot Open Kpow

Check Kpow container:

```bash
docker ps
```

Check logs:

```bash
docker logs --tail 100 kpow
```

Check EC2 port:

```bash
sudo ss -lntp | grep 3000
```

Check AWS Security Group:

```text
TCP 3000
Source = YOUR_PUBLIC_IP/32
```

Check URL:

```text
http://KPOW_PUBLIC_IP:3000
```

---

# Problem 4 — Container Exits Immediately

Check:

```bash
docker ps -a
```

Then:

```bash
docker logs kpow
```

Common areas to inspect:

```text
Invalid/missing license information
Bad environment variable
Kafka bootstrap connectivity
Memory/resource problems
```

---

# Problem 5 — License Error

Verify that all license fields came directly from Factor House:

```text
LICENSE_ID
LICENSE_CODE
LICENSEE
LICENSE_EXPIRY
LICENSE_SIGNATURE
```

Do not guess these values.

---

# Problem 6 — Kafka Works from Broker but Not from Kpow

This usually points to networking rather than Kafka itself.

Check:

```bash
nc -vz 172.31.36.36 9092
nc -vz 172.31.42.7 9092
```

Then check Broker-1:

```properties
advertised.listeners=INTERNAL://172.31.36.36:9092,EXTERNAL://34.253.224.235:19092
```

Broker-2:

```properties
advertised.listeners=INTERNAL://172.31.42.7:9092,EXTERNAL://18.203.251.147:19092
```

Kpow must be able to reach the broker addresses Kafka returns to it.

---

# Problem 7 — Public EC2 IP Changes

Normal auto-assigned EC2 public IPv4 addresses can change after stop/start.

This matters for:

```text
Kpow browser URL
Broker EXTERNAL advertised.listeners
Offset Explorer
```

For a stable lab, consider Elastic IPs or DNS where appropriate.

Kpow-to-Kafka traffic itself should continue using private IPs.

---

# Problem 8 — Docker Permission Denied

Example:

```text
permission denied while trying to connect to Docker daemon
```

Run:

```bash
sudo usermod -aG docker ec2-user
```

Then log out and log back in.

Verify:

```bash
groups
docker ps
```

---

# Problem 9 — Port 3000 Already in Use

Check:

```bash
sudo ss -lntp | grep 3000
```

Check containers:

```bash
docker ps -a
```

If an old Kpow container exists:

```bash
docker stop kpow
docker rm kpow
```

Then recreate it.

---

# 🔒 Security Improvements

Our lab intentionally uses:

```text
Kafka PLAINTEXT
Kpow HTTP
```

That is acceptable only for a controlled learning environment.

A more secure environment should consider:

```text
Kafka TLS
Kafka SASL_SSL
Kafka ACLs
Kpow authentication
Kpow authorization / RBAC
HTTPS
Reverse proxy / load balancer
Private Kpow access
VPN
Secrets management
Restricted security groups
```

---

# 🔐 Do Not Commit Secrets to Git

Do not upload a real:

```text
kpow.env
```

containing license or authentication secrets to a public Git repository.

Create:

```bash
vim .gitignore
```

Add:

```gitignore
kpow.env
*.env
.env
```

A repository should contain an example instead:

```text
kpow.env.example
```

Example:

```properties
ENVIRONMENT_NAME=VishwaTechLabs-Kafka-KRaft
BOOTSTRAP=172.31.36.36:9092,172.31.42.7:9092
SECURITY_PROTOCOL=PLAINTEXT

LICENSE_ID=CHANGE_ME
LICENSE_CODE=COMMUNITY
LICENSEE=CHANGE_ME
LICENSE_EXPIRY=CHANGE_ME
LICENSE_SIGNATURE=CHANGE_ME
```

---

# ⚠️ Production Considerations

## 1. Resource Sizing

Our lab command uses:

```text
-m 2G
```

for a small environment.

Factor House recommends larger resources for production. Size according to:

```text
Number of brokers
Number of topics
Number of partitions
Consumer groups
Traffic
Data inspection workload
Concurrent users
```

---

## 2. Kpow Internal Data

Kpow uses Kafka resources for its operational data/metrics.

Therefore monitor Kafka disk usage and understand Kpow's internal topic behavior before production deployment.

---

## 3. Authentication

Do not expose an unauthenticated administrative UI publicly.

Production should have an authentication/authorization design.

---

## 4. HTTPS

Instead of:

```text
http://KPOW_PUBLIC_IP:3000
```

production should generally use:

```text
HTTPS
DNS
Authentication
Restricted network access
```

---

## 5. Kafka Security

Our current:

```properties
SECURITY_PROTOCOL=PLAINTEXT
```

matches the learning cluster.

If Kafka later moves to:

```text
SSL
SASL_SSL
SCRAM
mTLS
```

Kpow must be updated with the corresponding Kafka client security configuration.

---

# 🎓 Learning Outcomes

After this lab, students should be able to explain:

```text
What is Kpow?

Why does Kpow connect to brokers?

What is a Kafka bootstrap server?

Why doesn't Kpow use KRaft controller port 9093?

Why does Kpow use private IPs inside AWS?

Why does Offset Explorer use external broker listeners?

What is listeners?

What is advertised.listeners?

Why use a separate EC2 for Kpow?

How does Docker expose Kpow port 3000?

How do AWS Security Groups affect Kafka connectivity?

How do you troubleshoot Kpow → Kafka connectivity?
```

---

# 🧪 Real-World Troubleshooting Exercise

Try this only in a controlled lab.

### Exercise 1

Block Kpow access to Broker-2 port `9092`.

Observe:

```text
Kpow behavior
Kafka logs
Kpow logs
```

Restore the rule.

---

### Exercise 2

Stop Broker-2:

```bash
cd /opt/kafka
bin/kafka-server-stop.sh
```

Observe:

```text
Topic leaders
Replicas
ISR
Consumer behavior
Kpow
```

Then restart Broker-2.

---

### Exercise 3

Create a consumer group and produce messages faster than the consumer processes them.

Observe consumer lag in the Kafka CLI and Kpow.

This teaches a very common real-world Kafka operations scenario.

---

# 🧩 Complete End-to-End Flow

```text
Existing Kafka KRaft Cluster Working
               |
               v
Create Dedicated Kpow EC2
               |
               v
Place in Same VPC
               |
               v
Configure Security Groups
               |
               v
Install Docker
               |
               v
Test Kpow → Broker-1:9092
               |
               v
Test Kpow → Broker-2:9092
               |
               v
Obtain Kpow Community License
               |
               v
Create ~/kpow/kpow.env
               |
               v
BOOTSTRAP =
172.31.36.36:9092,
172.31.42.7:9092
               |
               v
Pull factorhouse/kpow-ce:latest
               |
               v
Start Container
               |
               v
Expose TCP 3000
               |
               v
Check docker ps
               |
               v
Check docker logs
               |
               v
Open Browser
               |
               v
http://KPOW_PUBLIC_IP:3000
               |
               v
Inspect Kafka Cluster
               |
               v
Topics / Partitions / Consumer Groups / Data
               |
               v
        🎉 LAB COMPLETE
```

---

# 📌 Quick Reference Card

## Kafka Brokers

```text
Broker-1
Private: 172.31.36.36:9092
Public : 34.253.224.235:19092

Broker-2
Private: 172.31.42.7:9092
Public : 18.203.251.147:19092
```

## KRaft Controllers

```text
Controller-1
172.31.35.220:9093

Controller-2
172.31.34.34:9093
```

## Kpow Bootstrap

```text
172.31.36.36:9092,172.31.42.7:9092
```

## Kpow UI

```text
http://KPOW_PUBLIC_IP:3000
```

## Kpow Container

```bash
docker run -d \
  --name kpow \
  --restart unless-stopped \
  -p 3000:3000 \
  -m 2G \
  --env-file ~/kpow/kpow.env \
  factorhouse/kpow-ce:latest
```

## Logs

```bash
docker logs -f kpow
```

## Restart

```bash
docker restart kpow
```

---

# 🏆 Final Architecture

```text
                    VishwaTechLabs Kafka Lab
                              |
       +----------------------+----------------------+
       |                                             |
       v                                             v
   External                                      Internal
   Clients                                       Services
       |                                             |
       |                                             |
Offset Explorer                                  Kpow EC2
       |                                          :3000
       |                                             |
       | TCP 19092                                   | TCP 9092
       |                                             |
       +----------------+----------------------------+
                        |
              +---------+---------+
              |                   |
              v                   v
          Broker-1            Broker-2
          node.id=3           node.id=4
              |                   |
              +---------+---------+
                        |
                     TCP 9093
                        |
              +---------+---------+
              |                   |
              v                   v
         Controller-1        Controller-2
          node.id=1           node.id=2
```

---

# 🔗 Official References

- 🌐 [Kpow Product](https://factorhouse.io/products/kpow/)
- 📘 [Kpow Documentation](https://docs.factorhouse.io/kpow/)
- 🐳 [Kpow Docker Installation](https://docs.factorhouse.io/kpow/installation/docker)
- ⚙️ [Kpow Environment Variables](https://docs.factorhouse.io/kpow/configuration/config-kpow/environment-variables)
- 🖥️ [Kpow System Requirements](https://docs.factorhouse.io/kpow/faq/system-requirements)
- 📨 [Kpow Kafka Cluster Configuration](https://docs.factorhouse.io/kpow/configuration/kafka-cluster)
- 🐳 [Docker Documentation](https://docs.docker.com/)
- ☁️ [AWS EC2 Documentation](https://docs.aws.amazon.com/ec2/)
- 🔐 [AWS EC2 Security Groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html)
- 📨 [Apache Kafka](https://kafka.apache.org/)

---

# 👨‍💻 VishwaTechLabs

> **Learn → Build → Break → Observe → Troubleshoot → Fix → Master**

<p align="center">

![Kafka Admin](https://img.shields.io/badge/Kafka-Administration-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Kpow](https://img.shields.io/badge/Kpow-Observability-6A5ACD?style=for-the-badge)
![Hands On](https://img.shields.io/badge/Learning-Hands--On-brightgreen?style=for-the-badge)
![AWS](https://img.shields.io/badge/Platform-AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)

### 🎉 Existing Kafka KRaft Cluster + Kpow = Complete Kafka Administration Lab

</p>
