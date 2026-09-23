# SSM Parameter Store → Secret Manager (or ConfigMaps)

**Moves by:** by hand, one parameter at a time.
**Cutover class:** point-in-time copy, and the values often change with the
resources they describe.

Parameter Store holds two different things behind one API, and they do not
have the same target:

- **`SecureString` parameters** — passwords, tokens, keys. These go to Secret
  Manager, and everything in `secretsmanager.md` applies to them.
- **`String` and `StringList` parameters** — endpoints, feature flags, bucket
  names, tuning values. These are configuration, not secrets. On GKE they
  belong in a ConfigMap, or in the workload's own Helm values, where they are
  reviewable in the GitOps repository instead of being fetched at runtime from
  a store that costs an API call.

Sorting the list into those two is the first step and the one that decides
everything else.

Every command below is yours to run, with your own credentials. The
`SecureString` half prints plaintext.

## Substitutions

| Placeholder | What it is |
|---|---|
| `<TARGET_ARGS>` | The targeting arguments exactly as the tool printed them — `address="…"`, plus `directory=`/`identifier=` when it showed them. Two root modules can declare one address, and the bare address is then refused |
| `<PARAM_PATH>` | The path prefix to inventory, e.g. `/prod/orders/` |
| `<PARAM_NAME>` | One full parameter name from that inventory, e.g. `/prod/orders/db/password` |
| `<GCP_SECRET>` | The Secret Manager secret name for a SecureString |
| `<GCP_PROJECT>` | The project the landing zone created |
| `<NAMESPACE>` | The Kubernetes namespace for a ConfigMap |
| `<CONFIGMAP>` | The ConfigMap name |

## 1. Inventory and sort

```bash
aws ssm get-parameters-by-path --path <PARAM_PATH> --recursive \
    --query 'Parameters[].{name:Name,type:Type}' --output table
```

Split the result by `Type`. Anything `SecureString` is a secret; anything else
is configuration until someone shows otherwise. A `String` parameter holding a
password is a finding, not a copy — fix the classification on the way across
rather than carrying it over.

## 2. SecureString → Secret Manager

The same three-way decision as `secretsmanager.md`: a credential for something
that moved gets a **new** value, a credential for something that stayed is
copied, and a credential for the AWS environment itself is retired rather than
migrated.

```bash
# printf %s, not a bare pipe: `--output text` terminates with a newline and
# --data-file=- stores stdin byte for byte, so a plain pipe stores "hunter2\n".
# Every check below still passes and the application fails to authenticate.
printf %s "$(aws ssm get-parameter --name <PARAM_NAME> --with-decryption \
    --query Parameter.Value --output text)" \
  | gcloud secrets create <GCP_SECRET> --data-file=- --project=<GCP_PROJECT>
```

One parameter per run, from the step-1 inventory — `<PARAM_NAME>` is a full
name, not the prefix. Each one needs its own decision from the three above, so
there is no bulk form to reach for.

Then grant the workload's Google service account
`roles/secretmanager.secretAccessor`, and read it through the Secret Manager
CSI provider or the External Secrets Operator — not through the AWS SDK the
application used before.

## 3. String / StringList → configuration

```bash
# One KEY=VALUE line per parameter, keyed on the leaf of the path.
# The Type filter is load-bearing: without it every SecureString under the
# prefix lands in the ConfigMap — as KMS ciphertext, since --with-decryption
# is deliberately absent here — and step 4's gate is what would catch it,
# after the fact.
aws ssm get-parameters-by-path --path <PARAM_PATH> --recursive \
    --query 'Parameters[?Type!=`SecureString`].{name:Name,value:Value}' \
    --output json \
  | jq -r '.[] | "\(.name | split("/") | last)=\(.value)"' > params.env
```

Read `params.env` before going further. Two things break it: a leaf name that
repeats under two paths silently keeps only the last one, and a value
containing a newline is not expressible in this format — both are signals to
split that parameter out by hand rather than to keep going.

Turn it into a ConfigMap in the workload's own manifests, in the GitOps
repository, reviewed like any other change:

```bash
kubectl create configmap <CONFIGMAP> --namespace=<NAMESPACE> \
    --from-env-file=params.env --dry-run=client -o yaml > configmap.yaml
```

`--from-env-file` gives one ConfigMap key per parameter, which is what the
workload reads. `--from-file` would give a single key holding the whole
inventory — a JSON blob the application would have to parse itself.

Do not apply it by hand into a cluster the GitOps repository owns — commit it,
let the pipeline apply it. A hand-applied ConfigMap is reverted by the next
sync and the outage arrives hours later, disconnected from its cause.

**The values usually change.** An endpoint parameter pointing at an RDS
hostname has to point at the Cloud SQL instance; a bucket-name parameter has
to name the GCS bucket. Copying them verbatim is how a workload starts
successfully and talks to AWS forever.

## 4. Validation gates

- Every parameter in scope is accounted for as a secret, as configuration, or
  as deliberately retired.
- No `SecureString` value ended up in a ConfigMap.
- No parameter still names a resource that moved.
- The workload reads both halves from inside a pod — the only test that
  exercises the real path.

## 5. Cutover

These move with the workload that reads them. Copy last, immediately before
the workload starts against the target.

## 6. After cutover

Keep the AWS parameters for the rollback window, then delete them, then
report:

```
mark_data_service_migrated(<TARGET_ARGS>, target="<GCP_SECRET>")
```

Where a path split across both targets, say so in the report:
`note="SecureStrings to Secret Manager, 14 String params to the orders
ConfigMap"`. The next reader cannot reconstruct that from the record alone.

## Rollback

Repoint the workload at Parameter Store. As with secrets, note which values
changed on the way across — the rollback is to the old values as well as to
the old store.

## Known limitations

- No bulk tool, and the split in step 1 is a judgement call per parameter.
- Parameter hierarchies (`/prod/orders/db/host`) have no equivalent in Secret
  Manager, which has a flat namespace. Encode the path in the secret name.
- Parameter Store versions and labels do not carry across.
- Parameter policies (expiry, no-change notification) have no Secret Manager
  counterpart and do not migrate. Size is not the obstacle people expect it to
  be: Advanced-tier parameters top out at 8 KB and a Secret Manager payload
  holds far more, so a large Advanced parameter is an ordinary copy.
- `--output text` appends a newline; the copy commands above strip it. A
  secret stored with a trailing newline passes every check in step 4 and fails
  at first connect.
- `StringList` is a comma-separated string on the AWS side. Whatever consumes
  it has to keep splitting it, or the format changes with the store.
