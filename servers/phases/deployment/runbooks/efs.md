# EFS → Filestore

**Moves by:** Storage Transfer Service with a POSIX agent, or `rsync` from a
host that mounts both.
**Cutover class:** repeatable incremental — re-runs move only what changed, so
the final pass after writes stop is short if the tree is not enormous. Not
continuous: that final pass is what makes the copy consistent.

Every command below is yours to run, with your own credentials.

## Substitutions

| Placeholder | What it is |
|---|---|
| `<TARGET_ARGS>` | The targeting arguments exactly as the tool printed them — `address="…"`, plus `directory=`/`identifier=` when it showed them. Two root modules can declare one address, and the bare address is then refused |
| `<MOUNT_PATH>` | Where the EFS file system is mounted on the agent host, e.g. `/mnt/efs` |
| `<POOL>` | The transfer agent pool name |
| `<STAGING_BUCKET>` | A Cloud Storage bucket used as the intermediate |
| `<GCP_PROJECT>` | The project holding the Filestore instance |
| `<FILESTORE_INSTANCE>` | The Filestore instance name |
| `<FILESTORE_SHARE>` | The file share name on that instance |
| `<FILESTORE_MOUNT>` | Where that share is mounted on the copy host, e.g. `/mnt/filestore` |

## 1. Prepare the source

- A Linux host — in the source VPC, with the EFS file system mounted at
  `<MOUNT_PATH>` — to run the transfer agent. It needs read on everything being
  copied, which for a multi-tenant EFS usually means running as root.
- Note the **access points** in use. EFS access points impose a root directory
  and a POSIX identity per client; Filestore has no equivalent, so what each
  access point enforced becomes a directory convention plus pod security
  context on the GKE side. Enumerate them before copying — this is the part
  that is a translation rather than a copy.
- Note the performance mode. Bursting maps to Filestore Basic; Provisioned
  Throughput or Max I/O maps to Enterprise, or to Parallelstore if the
  workload is genuinely parallel-throughput bound.

## 2. Prepare the target

The landing zone Terraform has to be applied first. Check the tier decision it
made against the note above; a Basic instance under a workload that needed
Enterprise is a performance incident, not a capacity one.

The GKE side needs the Filestore CSI driver, and the StorageClass that
`storage-translation` produced (`filestore.csi.storage.gke.io`). A PVC bound to
the wrong class silently provisions a Persistent Disk instead of mounting the
share.

## 3. Run the copy

```bash
gcloud transfer agents install --pool=<POOL> \
    --mount-directories=<MOUNT_PATH> --project=<GCP_PROJECT>

gcloud transfer jobs create posix://<MOUNT_PATH> gs://<STAGING_BUCKET> \
    --source-agent-pool=<POOL> --project=<GCP_PROJECT>
```

Then land the staged tree on the share, from a GKE pod or a Compute Engine VM
that mounts `<FILESTORE_INSTANCE>:/<FILESTORE_SHARE>`:

```bash
gcloud storage rsync -r gs://<STAGING_BUCKET> <FILESTORE_MOUNT>
```

For a small tree on a host that can reach both sides, plain
`rsync -aHAX --delete` between the two mounts is simpler and preserves more
metadata than the bucket hop. Choose deliberately: the bucket route scales and
loses POSIX detail, the direct route preserves it and does not scale.

## 4. Validation gates

- File and directory counts match (`find <MOUNT_PATH> | wc -l` on both sides).
- Total bytes match.
- Ownership, mode bits and symlinks survived on a sample — especially if you
  went through the bucket.
- Hard links and sparse files, if the workload uses them, are intact. If they
  are not, the bucket route is the reason.
- A pod on GKE can mount the share and read what it expects.

## 5. Cutover

1. Stop the writers on the EKS side.
2. Run one final incremental pass.
3. Scale up the GKE workload with the Filestore-backed PVC.
4. Verify from inside the pod, not from the copy host.

## 6. After cutover

Keep the EFS file system for the soak, then delete it, then report:

```
mark_data_service_migrated(<TARGET_ARGS>, target="<FILESTORE_INSTANCE>")
```

## Rollback

Scale the GKE workload down and the EKS one back up. Files written on
Filestore after cutover do not exist on EFS unless you copy them back — a
reverse `rsync` is the whole rollback, and it is worth rehearsing once.

## Known limitations

- EFS access points have no Filestore equivalent. Their root-directory and
  POSIX-identity enforcement becomes a convention you have to reimplement.
- Filestore Basic is not EFS Bursting and Enterprise is not Max I/O; the tiers
  are close analogues, not equivalents.
- The Cloud Storage hop does not preserve hard links, sparse files, or extended
  attributes.
- Filestore minimum capacities are large; a small EFS file system may cost more
  on the target than it did on the source. That is a decision to surface, not a
  problem to solve here.
- Copying while the source is live gives a fuzzy snapshot. The final pass after
  writes stop is what makes it consistent.
