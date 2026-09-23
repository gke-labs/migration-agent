# AWS Secrets Manager → Secret Manager

**Moves by:** by hand, one secret at a time.
**Cutover class:** point-in-time copy — and usually the value **changes** as
part of the move, which is what makes this different from every other runbook
in the set.

**This is the one that fails quietly.** A cache can start cold and a queue can
drain, but an application whose connection string did not come across starts,
fails to connect, and has nothing to rebuild the value from. In a typical
estate more than half the data-dependency entries are secrets holding the
passwords for the other half.

Every command below is yours to run, with your own credentials. This runbook in
particular: the command prints a plaintext secret to a pipe, and running it
inside an agent process would put customer credentials through the transcript.

## Substitutions

| Placeholder | What it is |
|---|---|
| `<TARGET_ARGS>` | The targeting arguments exactly as the tool printed them — `address="…"`, plus `directory=`/`identifier=` when it showed them. Two root modules can declare one address, and the bare address is then refused |
| `<SECRET_NAME>` | The AWS secret id |
| `<GCP_SECRET>` | The Secret Manager secret name, usually the same |
| `<GCP_PROJECT>` | The project the landing zone created |
| `<KSA_NAMESPACE>`, `<KSA_NAME>` | The Kubernetes service account that reads it |
| `<PATH_TO_NEW_VALUE>` | A tightly-permissioned local file holding the new value |
| `<GSA_EMAIL>` | The Google service account bound to that KSA |

## 1. Decide what the value should be

Not what it **is**. Three kinds of secret, and only one of them is a copy:

- **A credential for something that also moved.** A database password, a Redis
  auth string, a connection URL. The target has a different host, a different
  port and usually a different password, so the new value is written from the
  migrated resource, not copied from AWS. Copying it verbatim produces a secret
  that exists, passes every check here, and fails at first connect.
- **A credential for something that stayed in AWS.** Copy it, and expect to
  keep both copies in step until the source is retired.
- **A credential for the source AWS environment itself** — deploy keys, IRSA
  bootstrap material. Do not migrate it. Retire it after cutover.

Sort the list into those three before touching anything.

## 2. Prepare the target

The landing zone Terraform has to be applied first. The Secret Manager API has
to be enabled in `<GCP_PROJECT>`, and you need `secretmanager.admin` on it.

## 3. Copy or write the value

For a genuine copy:

```bash
# printf %s, not a bare pipe: `--output text` terminates with a newline and
# --data-file=- stores stdin byte for byte, so a plain pipe stores "hunter2\n".
# That is exactly the quiet failure this runbook opens by warning about — the
# secret exists, every check below passes, and the application cannot connect.
printf %s "$(aws secretsmanager get-secret-value --secret-id <SECRET_NAME> \
    --query SecretString --output text)" \
  | gcloud secrets create <GCP_SECRET> --data-file=- --project=<GCP_PROJECT>
```

For a value that changed, write the new one instead — from a file, or from
whatever produced it — rather than piping the old one:

```bash
gcloud secrets versions add <GCP_SECRET> --data-file=<PATH_TO_NEW_VALUE> \
    --project=<GCP_PROJECT>
```

Neither command should end up in shell history with the value inline. Use a
file with tight permissions, delete it afterwards.

## 4. Wire the access path

The IRSA role that let the pod read Secrets Manager has no counterpart on GKE
unless Workload Identity is wired up. The translation phase generates those
bindings; this step verifies them rather than creating them.

```bash
gcloud secrets add-iam-policy-binding <GCP_SECRET> \
    --member="serviceAccount:<GSA_EMAIL>" \
    --role="roles/secretmanager.secretAccessor" \
    --project=<GCP_PROJECT>
```

And on the cluster side, the workload reads the secret through the Secret
Manager CSI provider or the External Secrets Operator pointed at GCP — not
through the AWS SDK it used before. That is an application change, and it is
the half people forget until the pod crash-loops.

## 5. Validation gates

- Every secret in scope exists in Secret Manager with a current version.
- `gcloud secrets versions access latest --secret=<GCP_SECRET>` returns what
  you expect, run by a human, not by an agent.
- The KSA `<KSA_NAMESPACE>/<KSA_NAME>` can read it — test from inside a pod,
  which is the only test that exercises the whole path.
- No secret in the list is still pointing at a resource that moved.

## 6. Cutover

Secrets move with the workload that reads them, not on their own schedule.
Copy last — immediately before the workload starts against the target — so the
value cannot drift after the copy.

## 7. After cutover

Keep the AWS secret for the rollback window, then delete it, then report:

```
mark_data_service_migrated(<TARGET_ARGS>, target="<GCP_SECRET>")
```

Retire the source-environment credentials from the third category above at the
same time. They are the ones nobody remembers to revoke.

## Rollback

Repoint the workload at the AWS secret. Because the value may have changed on
the GCP side, note which version the target holds before rolling back — the
rollback is to the old credential as well as to the old store.

## Known limitations

- There is no bulk tool, deliberately. Each value needs the decision in step 1.
- Rotation configuration does not migrate. A secret with an AWS rotation Lambda
  behind it needs a new rotation mechanism on the target.
- Versioning semantics differ: AWS staging labels (`AWSCURRENT`,
  `AWSPREVIOUS`) have no direct Secret Manager equivalent.
- `--output text` appends a newline; the command above strips it. A secret
  stored as `hunter2\n` passes every validation gate in step 5.
- Binary secrets (`SecretBinary`) need `--data-file` with the raw bytes, not
  the `SecretString` path above.
- Secrets referenced by workloads the scan could not attribute are still
  secrets. An unattributed entry is not an unused one.
