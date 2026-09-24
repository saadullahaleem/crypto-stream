# Kubernetes, Docker Desktop and Windows

Docker Desktop 4.87 on Windows 11 Home, with its Kubernetes (kind type, node `desktop-control-plane`).

## 1. A full C: drive stops Docker

**What happened:** an image pull failed with `input/output error`. Later, the Kubernetes API and `docker version`
stopped answering.

**Cause:** Docker keeps all images, containers and the Kubernetes node in one disk file,
`docker_data.vhdx`, on C:. It had grown to 66 GB, and C: had less than 2 GB free.

**Fix:** Docker Desktop, Settings, Resources, Advanced, Disk image location: `G:\Docker`. After the copy, Docker
Desktop did not start its VM again, so we ran `docker desktop restart`.

**Note:** the disk file does not shrink when you delete images. The free space stays inside the file, and Docker
uses it first.

## 2. The kind-type cluster does not see local images

**What happened:** `ErrImageNeverPull` for `crypto-worker:dev`.

**Cause:** the cluster node is a container with its own image store (containerd), separate from `docker images`.

**Fix:** import each image after each build:
`docker save crypto-worker:dev | docker exec -i desktop-control-plane ctr -n k8s.io images import -`

## 3. Pods are not in `docker ps` or in `kubectl get pods`

- `kubectl get pods` shows the `default` namespace. Our pods are in `crypto`: use `-n crypto` or `-A`.
- `docker ps` is empty. Docker Desktop hides the node container, and the pods run inside it, on containerd.
  To see them: `docker exec desktop-control-plane crictl ps`.

## 4. Smaller points

- Docker Desktop serves `LoadBalancer` services on `localhost`, so Grafana is at `localhost:3000` without port-forward.
- A change to pod annotations changes the pod template, so the pods restart. Adding scrape annotations restarted
  Redpanda and RisingWave.
- Jobs cannot be changed after they are created. With `ttlSecondsAfterFinished`, a finished Job is deleted, and
  `kubectl apply` runs it again. To run one again now: delete it, then apply.
- During a rollout, the old pod can still get requests for a few seconds. A 404 from a new route can come from the old pod.
- A Deployment with no labels on its metadata matches `-l 'app notin (...)'`, so that selector restarted too much.
  We select by name instead.

## 5. Windows tools

- **Git Bash changes paths.** `/config/connect.yaml` in a `kubectl exec` command became
  `C:/Program Files/Git/config/connect.yaml`. Fix: `MSYS_NO_PATHCONV=1` before the command.
- **Git Bash and Windows Python do not share `/tmp`.** Use a Windows path.
- **Line endings.** Git here changes files to CRLF on checkout, and Python on Windows writes CRLF in text mode.
  A shell script inside a YAML block with CRLF breaks `sh`. Fix: `.gitattributes` with `* text=auto eol=lf`.
- **Long heredocs failed** in the Bash tool with `unexpected EOF while looking for matching`. Writing the script
  to a file and running the file worked.
- **Renaming the project folder failed** while PyCharm had the project open.
