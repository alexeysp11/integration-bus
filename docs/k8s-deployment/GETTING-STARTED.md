# 🧑‍🎓 Deploying integration-bus to Kubernetes From Scratch (for Those Who Have Never Used Kubernetes)

[English](GETTING-STARTED.md) | [Русский](GETTING-STARTED.ru.md)

This guide is written in maximum detail — step by step, explaining what you are doing and why, what to download,
and where to put it. The technical breakdown of the architecture and the issues found is in
[`README.md`](README.md) next to this file; this document is purely "do step one, do step two".

All commands are run in a terminal (the black window where you type text commands). This is normal — Kubernetes
is not operated with a mouse; the entire DevOps world runs through a terminal. You will need to copy commands
from this guide and paste them into the terminal, pressing Enter after each one.

---

## 0. What's Actually Going On (in Plain Terms)

- **Docker** — a program that can run "containers": lightweight, isolated mini-computers, each running one
  program (a database, a web server, etc.). You most likely already have this set up — this whole project has
  so far been run via `docker compose up -d`.
- **Kubernetes** ("K8s") — a conductor program that automatically manages many containers: it restarts crashed
  ones and distributes them across servers by itself. At real companies, Kubernetes runs on a cluster of dozens
  of servers. For learning/local purposes, we don't need anything close to that.
- **Kind** ("Kubernetes IN Docker") — a utility that creates a **fake but fully functional** Kubernetes cluster
  right on your computer, using an ordinary Docker container instead of a real server. This is our "mini
  Kubernetes".
- **Helm** — an installer program for Kubernetes. Instead of manually creating 40+ separate objects (databases,
  queues, services), you tell Helm with one command "install this set of instructions", and it creates everything
  in the right order by itself. Think of it as `apt install`, or a program installer with "Next → Next → Finish",
  but for Kubernetes.
- **kubectl** — the utility you "talk" to Kubernetes with: asking its status, viewing logs, and so on.

The overall sequence: **Docker Desktop** (the platform) → **kind** (creates the mini-cluster) →
**kubectl** (talks to the cluster) → **Helm** (installs the whole project into the cluster with one command).

---

## 1. What to Download and Install (Once)

### 1.1. Docker Desktop

If you have already run this project via `docker compose up -d`, you already have Docker Desktop — skip to §1.2.

If not:
1. Open in your browser: `https://www.docker.com/products/docker-desktop/`
2. Click **Download for Windows**.
3. Run the downloaded installer, `Docker Desktop Installer.exe`, clicking **Ok** / **Next** through every screen
   (on the step with the **"Use WSL 2 instead of Hyper-V"** checkbox — leave it checked, this is important).
4. If prompted to restart your computer — restart it.
5. After rebooting, launch **Docker Desktop** from the Start menu. Wait until the bottom-left corner of the
   window shows **"Engine running"**, and the whale icon in the system tray (near the clock) stops animating.

**How to verify everything is fine:** open a terminal (see §2) and type:
```
docker --version
```
If a line like `Docker version 29...` is printed back — you're good.

### 1.2. kind

This is not a program with an installer, just a single file, `kind.exe`, that you simply place into the right
folder.

1. Create (if it doesn't already exist) the folder `C:\Users\<YourUsername>\bin`.
2. Open in your browser: `https://kind.sigs.k8s.io/dl/v0.25.0/kind-windows-amd64`
   — your browser will download the file. Rename the downloaded file to `kind.exe` (if it downloaded without an
   extension or under a different name) and move it into `C:\Users\<YourUsername>\bin`.
3. Windows needs to know about this folder (via the so-called PATH variable), otherwise typing `kind` in a
   terminal will produce a "command not found" error:
   - Press **Win**, type **"Edit environment variables for your account"**, and open it.
   - In the top list ("User variables"), find the **Path** entry, click **Edit**.
   - Click **New**, type `C:\Users\<YourUsername>\bin`, click **OK** in every window.
   - **Close and reopen your terminal** — otherwise the change won't take effect.

**How to verify:**
```
kind --version
```
`kind version 0.25.0` is expected.

### 1.3. Helm

Similar — a single exe file, except it's inside a zip archive.

1. Open in your browser: `https://get.helm.sh/helm-v3.16.4-windows-amd64.zip`
2. Extract the downloaded archive (right-click → **Extract All**).
3. Inside the extracted folder, find the `helm.exe` file (it's in the `windows-amd64` subfolder).
4. Move that `helm.exe` into the same `C:\Users\<YourUsername>\bin` folder as `kind.exe` (PATH is already
   configured from the previous step — no need to do it again).

**How to verify:**
```
helm version
```
A line containing `v3.16.4` is expected.

### 1.4. kubectl

Already installed alongside Docker Desktop — no separate download needed.

**How to verify:**
```
kubectl version --client
```

---

## 2. How to Open a Terminal in the Right Folder

1. Open Windows Explorer and navigate to the project folder.
2. Click in Explorer's address bar (where the path is shown), clear it, and type `git-bash` → Enter — a Git Bash
   terminal will open right in that folder.
   *(If Git Bash isn't installed — plain **PowerShell** will do: type `powershell` in the address bar → Enter.
   The commands below are written in Git Bash/Linux style; they work in PowerShell with almost no changes, with
   differences called out separately.)*

Enter all commands from the following steps into this same window.

---

## 3. Create the Mini Kubernetes Cluster

The project folder already contains a ready-made cluster config file. Run:

```bash
kind create cluster --config deploy/k8s/kind/kind-cluster.yaml
```

What happens: kind downloads a special "container-pretending-to-be-a-server" image (once — on first run this can
take a couple of minutes) and starts it. At the end you will see:
```
Set kubectl context to "kind-integration-bus"
```
This means `kubectl` is now "looking at" your new mini-cluster.

**How to verify:**
```bash
kubectl get nodes
```
One line with status `Ready` should appear.

> ⚠️ If you already have this same project's docker-compose running somewhere (`docker compose up -d`), your
> computer will have to pull double the usual 18 containers at once, and on a weak machine everything will start
> badly lagging and crashing. It is recommended to stop it first: `docker compose down` (database data is not
> deleted in the process — this is safe, you can bring everything back later with `docker compose up -d`).

---

## 4. Build the Application Images

An "image" is a packaged program ready to run in a container. We have 6 of our own .NET services plus one custom
Kafka Connect image — they need to be built:

```bash
docker compose build
```

This can take several minutes (all of the .NET code is being compiled). Wait until the terminal becomes free
again (a command prompt appears).

---

## 5. Load the Built Images Into the Mini-Cluster

The images built in step 4 live only in Docker on your computer — the kind mini-cluster needs them explicitly
"handed over" (our own images have no server on the internet the cluster could download them from):

```bash
for img in \
  integration-bus-integration-bus-gateway-api \
  integration-bus-integration-bus-processing-api \
  integration-bus-integration-bus-saga-orchestrator-service \
  integration-bus-integration-bus-account-balance-service \
  integration-bus-integration-bus-compliance-service \
  integration-bus-integration-bus-core-ledger-service \
  integration-bus-integration-bus-kafka-connect \
; do
  kind load docker-image "$img:latest" --name integration-bus
done
```

*(In PowerShell, a `for` loop is written differently — it is easiest to copy and run the 7 lines individually,
replacing `$img` with each image's specific name, e.g.:
`kind load docker-image integration-bus-integration-bus-gateway-api:latest --name integration-bus`.)*

The remaining images (Postgres, Kafka, Redis, etc.) do not need loading — they have a real address on the
internet, and the cluster will download them itself in the next step.

---

## 6. Install the Entire Project With One Command

First, create a "separate room" inside the cluster for our application (called a namespace):

```bash
kubectl create namespace integration-bus
```

And now — the main command, which deploys the **entire** project at once (18 programs: 6 services, a database, a
message queue, a cache, an analytics database, dashboards, and so on):

```bash
helm install integration-bus deploy/k8s/charts/integration-bus -n integration-bus --timeout 10m
```

This command runs for **several minutes** — don't worry if the terminal "hangs" for 3-5 minutes, that's normal:
Kubernetes is downloading the remaining images in parallel and waiting for every part of the system to start up
in the correct order. At the end you should see:
```
STATUS: deployed
```
If you get a timeout error instead — don't worry, repeating the exact same command, replacing `install` with
`upgrade`, almost always helps:
```bash
helm upgrade integration-bus deploy/k8s/charts/integration-bus -n integration-bus --timeout 10m
```

---

## 7. Check That Everything Came Up

```bash
kubectl get pods -n integration-bus
```

You will see a list of about 20 lines. Each has a `READY` column (e.g. `1/1` means everything is fine) and a
`STATUS` column (`Running` — working, `Completed` — normal for one-shot setup tasks, which are supposed to
finish).

To watch the list update in real time (instead of re-typing the command every time), add `-w`:
```bash
kubectl get pods -n integration-bus -w
```
(To exit this mode, press `Ctrl+C`.)

**This is normal:** during the first minute or two, some of the 6 .NET services may restart 1-2 times (the
`RESTARTS` column will not be `0`) — this is expected behavior on the very first startup (a service starts
slightly faster than the internal message queues get created, and Kubernetes restarts it by itself within a few
seconds). If, after 2-3 minutes, every line shows `Running` and `RESTARTS` has stopped growing — everything is
fine.

**If something seems wrong** — look at the details of a specific pod (its name is the first column):
```bash
kubectl describe pod <pod-name> -n integration-bus
```
Near the bottom there will be an `Events` section explaining the problem.

---

## 8. Open Everything in Your Browser

If the cluster was created exactly per these instructions (via `deploy/k8s/kind/kind-cluster.yaml`), every
service is already available directly in your browser on your computer at these addresses:

| What it is | Address |
|---|---|
| API health check (should show `Healthy`) | http://localhost:15038/health |
| Grafana (dashboards, login/password: `admin`/`admin`) | http://localhost:13000 |
| Jaeger (request tracing) | http://localhost:26686 |
| Prometheus (metrics) | http://localhost:19090 |
| Kafka UI (what's happening in the message queue) | http://localhost:18080 |
| Redis Commander | http://localhost:18082 |
| Metabase (analytics dashboards) | http://localhost:13001 |

Just open any of these addresses like an ordinary website.

---

## 9. Verify the Business Logic Actually Works

The simplest approach for someone with no `curl` experience is to download and use **Postman** (a free program
with an easy-to-use interface for sending requests): `https://www.postman.com/downloads/`. Or, if you're already
comfortable with the terminal, you can just copy the commands from
[`docs/business-logic/validation-guide.md`](../business-logic/validation-guide.md), only replacing the port
numbers with the ones from the table above (the usual ones plus `10000`).

Briefly, via the terminal:
```bash
curl -X POST http://localhost:15038/api/v1/accounts/seed -H "Content-Type: application/json" -d "{\"count\": 5, \"currency\": 1}"
```
A response with code `202` should come back — meaning the request was accepted and is being processed inside the
cluster.

---

## 10. How to Shut Everything Down When You're Done

You can shut things down step by step (from smallest to largest):

```bash
# Remove only the project itself, leaving the mini-cluster running
helm uninstall integration-bus -n integration-bus
kubectl delete namespace integration-bus

# Fully tear down the mini-cluster (frees up the most computer resources)
kind delete cluster --name integration-bus
```

If you stopped the regular docker-compose stack in step 3 and want to go back to it, run:
```bash
docker compose up -d
```

---

## 11. If Something Went Wrong — a Short FAQ

- **`kind: command not found` / `'kind' is not recognized as an internal or external command`** — you forgot to
  close and reopen your terminal after changing the PATH variable (§1.2). Close the terminal window completely
  and open it again.
- **A pod hangs forever in `Pending` status** — this usually means there isn't enough disk space for data
  (`PersistentVolumeClaim`). Check: `kubectl get pvc -n integration-bus` — all should be in `Bound` status.
- **A pod is stuck in `ImagePullBackOff`** — your computer can't download the image from the internet. Check
  your internet connection and retry `kubectl get pods -n integration-bus` after a minute — it usually resolves
  itself.
- **One of the 6 .NET services restarts more than 3-4 times in a row and doesn't settle down** — check its logs:
  `kubectl logs <pod-name> -n integration-bus --previous` and read the last lines containing `ERROR`/`Exception`.
- **The computer is lagging badly, the fan is roaring** — this is expected: almost 20 programs are running at
  once. Close unnecessary applications, give it a couple of minutes to stabilize, and avoid also keeping this
  same project's regular docker-compose stack running at the same time if possible (see the warning in §3).
