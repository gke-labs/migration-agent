# FSx → Parallelstore, NetApp Volumes, or Filestore

**Moves by:** depends entirely on the flavour.
**Cutover class:** decide the target first; there is no single procedure.

**Read this before offering a copy.** FSx is four products behind one name and
they do not share a target:

| Source | Usual target | Shape of the move |
|---|---|---|
| FSx for Lustre | Parallelstore | Copy. Scratch file systems are often rebuilt from the source of truth instead. |
| FSx for NetApp ONTAP | Google Cloud NetApp Volumes | Same vendor both sides — a NetApp conversation, usually SnapMirror, not a file copy. |
| FSx for Windows File Server | NetApp Volumes (SMB) | Copy plus an identity problem: Active Directory, SMB shares and ACLs. |
| FSx for OpenZFS | no direct equivalent | Re-platform. Decide the target before anything else. |

So the first step is not a command. **Name the flavour**, then decide whether
the answer is a copy, a vendor-native replication, or keeping it in AWS.

If the target is not obvious after that conversation, keeping it in AWS for
now is a legitimate answer and usually the right one:

```
annotate_data_dependency(<TARGET_ARGS>, disposition="keep-in-aws", note="...")
```

It costs cross-cloud connectivity, a credential path for a pod that used to
use IRSA, and egress on every call. Say so when recording it.

Every command below is yours to run, with your own credentials.

## Substitutions

| Placeholder | What it is |
|---|---|
| `<TARGET_ARGS>` | The targeting arguments exactly as the tool printed them — `address="…"`, plus `directory=`/`identifier=` when it showed them. Two root modules can declare one address, and the bare address is then refused |
| `<MOUNT_PATH>` | Where the FSx file system is mounted on the copy host |
| `<POOL>` | The transfer agent pool name |
| `<STAGING_BUCKET>` | A Cloud Storage bucket used as the intermediate |
| `<GCP_PROJECT>` | The project holding the target |
| `<TARGET_MOUNT>` | Where the GCP-side file system is mounted on that host |
| `<TARGET_NAME>` | The target instance or volume name, for the report |

## Lustre → Parallelstore

Ask first whether the data needs to move at all. A scratch file system whose
contents are derived — checkpoints, intermediate results, a hydrated copy of an
S3 bucket — is cheaper to rebuild on the target than to copy across clouds.

If it does need to move, it is a POSIX-to-POSIX copy through the same route as
EFS:

```bash
gcloud transfer agents install --pool=<POOL> \
    --mount-directories=<MOUNT_PATH> --project=<GCP_PROJECT>

gcloud transfer jobs create posix://<MOUNT_PATH> gs://<STAGING_BUCKET> \
    --source-agent-pool=<POOL> --project=<GCP_PROJECT>
```

then land it on the Parallelstore mount with
`gcloud storage rsync -r gs://<STAGING_BUCKET> <TARGET_MOUNT>`.

Parallelstore's own Cloud Storage import is the faster path when the data is
already in a bucket — check that before staging through one by hand.

## ONTAP → NetApp Volumes

Do not treat this as a file copy. Both ends are ONTAP, so the vendor's own
replication (SnapMirror) preserves snapshots, efficiency settings and export
policies that a copy would flatten. That is a conversation with NetApp and
with whoever owns the storage, and it belongs in the migration plan rather
than in a shell.

What this step needs from you is the decision and the schedule, recorded with
`mark_data_service_migrating(<TARGET_ARGS>, note="...")` while it
runs.

## Windows File Server → NetApp Volumes (SMB)

The bytes are the easy half. Budget for:

- **Active Directory.** The target has to join a domain the GKE-side workload
  can authenticate against. Managed Microsoft AD on GCP, or connectivity back
  to the existing forest.
- **ACLs.** NTFS ACLs are carried by a copy tool that understands them
  (`robocopy /COPYALL` from a Windows host that mounts both). A POSIX copy
  loses them.
- **Share definitions**, which are configuration on the target rather than
  data in the copy.

## OpenZFS → decide first

There is no managed OpenZFS on GCP. The realistic answers are Filestore or
NetApp Volumes with the feature set re-expressed, self-managed ZFS on Compute
Engine, or keeping it in AWS. Whichever it is, it is a decision recorded before
any copy starts.

If the eventual route is a ZFS-level send over the internet, tune the TCP
buffers on both hosts before measuring throughput — the default Linux settings
bottleneck a WAN transfer badly enough to mis-size the cutover window.

## Validation gates

Whatever the route:

- File and directory counts match.
- Total bytes match.
- Permissions survived — POSIX mode bits, or NTFS ACLs for the Windows case.
- A workload pod on GKE can mount the target and read what it expects.

## After cutover

Keep the source for the soak, then decommission, then report:

```
mark_data_service_migrated(<TARGET_ARGS>, target="<TARGET_NAME>")
```

## Rollback

Per route, because the routes do not share one:

- **Lustre → Parallelstore.** Point the workload back at the source file
  system. Files written on Parallelstore since cutover exist only there; copy
  them back before scaling the source's consumers up, or accept losing them.
- **ONTAP → NetApp Volumes.** SnapMirror is resumable and reversible, and that
  is most of the reason to use it — but the reverse direction has to be
  configured deliberately, before the cutover, not improvised after it.
- **Windows File Server.** The bytes roll back like the Lustre case; the
  identity side does not. If the target joined a different domain, the
  rollback includes putting share permissions back.
- **OpenZFS.** Whatever the chosen target was, this is the route with no
  managed path in either direction. Rehearse the reverse copy once before the
  cutover, or do not schedule one.

In every case the source stays readable through the soak. Deleting it is the
step that ends the rollback, and it belongs after the soak rather than in the
window.

## Known limitations

- The four flavours share nothing but the name. A procedure written for one is
  wrong for the others.
- OpenZFS has no GCP equivalent; that row is a re-platform.
- The Cloud Storage hop loses hard links, sparse files and extended
  attributes.
- A live source gives a fuzzy copy. The final pass after writes stop is what
  makes it consistent.
