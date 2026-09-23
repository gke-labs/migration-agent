# Deployment Step 2 — Data Migration

**DAG state:** `STATE_DEPLOYMENT_DATA_MIGRATION` · **Expected tool call:** `complete_data_migration`

**This is a waiting state, not a task to finish now.** The data services graded
`migrate` at the review have to actually move, and a database migration takes
days or weeks. The workspace sits here while that happens. That is the normal,
expected condition — not a stall, and not something to apologise for or push
the user through.

Nothing here moves anything. The server holds no AWS credentials and never
observes a copy happening, so every move runs with the operator's own
credentials and completion is something they tell you.

## What to do when you arrive

1. Call `list_data_migrations()`. It refreshes the worklist in the ledger and
   returns what is outstanding, who is waiting on each service, and what has
   already moved.
2. Say where things stand, once, plainly. Name the outstanding services and the
   workloads waiting on each — that is what makes it concrete and tells the
   operator who to talk to. Those workloads are not waiting figuratively: an
   application team whose component uses a service in that list is held at
   its own ship gate until this step reports the service settled, so a report
   made here is what lets somebody else's pull request open.
3. **Offer help with the moves that have a procedure behind them.** The listing
   prints an `offer help:` line for each one, carrying the sentence to use and
   the call to make. Nothing else gets an offer — a service without that line
   has no runbook, and offering help you then cannot give is worse than not
   offering.

   One line per service, together, phrased as the listing phrases it:

   > Would you like help migrating **orders-db** (RDS, postgres) to Cloud SQL?
   > I can prepare the runbook for it, adapted to what discovery recorded.

   Ask once. If they say no, or say nothing about it, do not raise it again —
   note it and move on. Being held at a ship gate is a reason to offer, not a
   reason to press: the team on the other side is waiting on the move, not on
   the runbook.

   **A `runbook already adapted:` line above the offer changes what to say.**
   Somebody worked through that procedure with an operator in an earlier
   session, and it carries what only they knew — the endpoint, the agreed
   window, the resolved target names. Offer to *open* it, not to prepare one:

   > There is already a runbook for **orders-db** at
   > `platform/deployment/runbooks/…`, written in an earlier session. Shall I
   > walk you through it, or has something changed that needs it redone?
4. Then **hand control back and leave them alone.** Beyond that one offer, do
   not propose a plan they did not ask for, do not ask them to confirm
   anything, and do not re-list on every turn. They may be here to do something
   else entirely.

## What to do afterwards

The user drives, in their own words. They will not say
`aws_db_instance.orders`; they will say "the orders database is done" or "I
copied the api image by hand". Your job is to map that onto the entries
`list_data_migrations()` printed and call the right tool.

**Resolving what they mean.** Every outstanding item in the listing carries the
exact arguments to report it. A data service gets an `address:` line and a
`report it:` line with the call spelled out; that call is runnable as printed,
and it deliberately carries no `target=` — add one only when the user says
where the service landed, and pass their words verbatim.

An image is listed differently, because the list is uncapped: each copy is
printed as its full source reference on its own line, and ONE `report with:`
line covers them all, carrying `refs=["<ref above>"]`. That placeholder is the
one thing on either half you are meant to substitute — copy the reference from
the line above it, exactly as printed. Everything below about never
constructing an argument applies to it too.

- Match on what they said against the service name, the identifier, the
  workload waiting on it, or the image reference. "The orders database" against
  `rds orders-db`; "the payments bucket" against `s3 payments-exports`.
- **Use the address exactly as the listing printed it. Never construct one.**
  `aws_db_instance.orders` is not derivable from "the orders database", and a
  guessed address either refuses or, worse, matches a different entry.
- If two entries could be what they mean, ask which. Two root modules can
  declare one address — a dev and a prod database with the same name — and
  recording the wrong one says a database has moved when it has not. The
  listing prints the directory when that is the case, and the reporting tools
  refuse an ambiguous address rather than picking.
- If nothing in the listing matches, say so and re-run `list_data_migrations()`
  rather than inventing an argument.

**Then act on it.** Five things are likely, and none is more correct than the
others:

- **A data service is done** — "the orders database is live in Cloud SQL".
  `mark_data_service_migrated(address=..., target=..., note=...)`. Put whatever
  they said about where it landed into `target`, verbatim; if they did not say,
  leave it out rather than guessing.
- **A data service is under way** — "the DMS job is running, about half done".
  `mark_data_service_migrating(address=..., note=...)`. This does **not**
  satisfy anything; a component must not ship against a database that is still
  copying. Record it because the next person needs to know where things stand.
  If the service is **not** graded `migrate`, nothing waits on it — so the step
  can close while the move is still running, and closing ends the ability to
  report it as landed. The tools say so wherever they offer the close; relay
  that sentence rather than dropping it.
- **A note was wrong** — "ignore what I said about Tuesday". Re-call the same
  tool with `note=""`, which clears it. Omitting `note` keeps what is there, so
  there is no way to remove one by leaving it out; `""` is the only way. The
  note reaches the runbook a platform engineer reads before shipping a
  component, so a wrong one left in place is read as current.
- **An image copy is done** — "I pushed the api image myself".
  `mark_replication_complete(refs=["<the full source ref>"])`. If they quote a
  digest, pass it in `digests={...}`; if the runbook carried an
  `<AR_DESTINATION>` placeholder, the tool will ask for `destinations={...}`.
- **They accept the offer, or ask for help doing a migration.** The best use of
  the state. Four steps, in order:

  1. `get_data_migration_runbook(address=...)` — the address from the listing,
     never constructed. It returns the procedure for that service **as a
     template**, with the facts the ledger holds about this particular
     database, bucket or file system above it.

     An RDS whose Terraform builds the engine from a variable is refused,
     because the four RDS procedures differ too much to guess between. Ask the
     operator which engine it runs and re-call with `engine="postgres"` (or
     `mysql`, `mariadb`, `sqlserver`). Passing it is the only way to answer:
     nothing in the system records an engine after discovery, so re-calling
     without the argument gets the same refusal.
  2. **Read it, then adapt it.** That is the work, and it is why the tool hands
     you a template rather than a document: substitute the estate's own names,
     sizes, engine version and region; drop the steps that do not apply (a
     single-shard cluster does not need the shard-merge escalation, a bucket
     with no lifecycle rules needs no lifecycle discussion); and keep every
     validation gate, rollback and limitation. Ask the operator for what only
     they hold — the source endpoint, the maintenance window, the target names
     the applied Terraform created. Leave a placeholder in place and say it is
     open rather than filling it with something plausible.
  3. **Show them the adapted procedure**, and say plainly which parts are still
     open.
  4. `save_data_migration_runbook(address=..., content=...)` — writes it to the
     ledger so the person who runs it in three weeks has it. It refuses while
     any `<PLACEHOLDER>` is left, which is the point at which you go back to
     the operator rather than inventing a value.

     It also refuses to overwrite a runbook an earlier session already saved.
     That refusal is not an obstacle to route around: read the existing file
     first, and pass `replace=True` only once the operator has said what
     changed — then say so in the conversation, because the document you are
     replacing may be the one they were working from.

  Do not improvise commands the runbook does not give you, never invent a
  hostname, instance name or credential, and print the commands for the user to
  run — see the rule below. A service the tool has no runbook for gets a plain
  "I do not have a procedure for this", never the nearest-looking one: the
  refusal says which kind of gap it is and what the alternative is.

They may equally do none of those, and that is fine. Answer whatever they
actually ask.

**Giving up on something is an answer too**, and both kinds have one:

- A data service that is not going to move:
  `annotate_data_dependency(address=..., disposition="keep-in-aws", note=...)`.
- An image that is not going to be copied:
  `abandon_image_replication(refs=[...], reason="...")`. The reason is
  required. Say what it costs: the workload keeps pulling from ECR, so the
  cluster needs ECR pull credentials and pays cross-cloud egress.

## Leaving

`complete_data_migration()` closes the step, and **refuses while anything is
still owed**, with no exceptions. That is deliberate: a terminal state carries
no instructions, so a workspace that moved on with databases outstanding would
have nothing left to tell the next session they exist. Parking here is what
keeps the work discoverable weeks later, to whoever turns up.

It refuses only over what it **gates** — services graded `migrate` and
unconfirmed image copies. A move reported in progress against a service graded
anything else does not hold the step open, so the close proceeds over a running
cutover, and after it `mark_data_service_migrated` is no longer callable: that
record can never be completed. This is not a bug to work around, but it is a
cost, and the tools name it in the same breath as the close. Never answer "can
we finish?" from this file alone when a move is in flight — the tool output
carries the caveat and this paragraph is why it is there.

If a service is not going to move after all — the move needs an engine change,
a licence, or a downtime window nobody will approve — that is a legitimate
answer and this is where people usually reach it. Offer it rather than waiting
to be asked:

```
annotate_data_dependency(address=..., disposition="keep-in-aws", note=...)
```

It stops being owed and stops holding any component up. Say what it costs:
cross-cloud connectivity, a credential path for a pod that used to get one from
IRSA, and egress on every call.

**If that exit itself refuses**, read the ERROR: it names which case this is,
because the repairs differ. `keep-in-aws` is replayed over the scan's own
output, so it needs that object.

- **could not be read** — a transport failure. Try again.
- **not readable**, or **does not describe the current section** — the object is
  there and wrong. Replace it with the copy the scan wrote.
- **does not exist** — the object has been removed; the scan writes it in the
  same call that writes the data services, so its absence is not a state the
  system produces. Restoring it means the object's previous generation, if the
  ledger bucket has object versioning enabled. If it does not, the only
  mechanism this server offers is an admin `join_ledger --reconfigure`: it
  resets the platform walk to the start state, so the whole platform sequence
  is walked again and the scan rewrites this object. Nothing in the ledger is
  discarded, but every step from onboarding onward is redone — say that before
  anyone reaches for it. Do not suggest deleting anything, and do not offer to
  re-run the scan on its own: nothing transitions back to the state it needs.

In every one of those cases `complete_data_migration()` refuses as well, naming
the same object. **The step does not close over a damaged ledger**, and you
should not look for a way to make it: reporting a migration complete tells every
application team the platform side is finished, and that claim cannot rest on an
artifact nobody can vouch for. Say plainly that the ledger needs repair.

**Reporting is never blocked by any of this.** `mark_data_service_migrated` does
not read the baseline. A service that cannot be EXCUSED can still be REPORTED if
it does move, and once every owed service has moved — and every image copy is
confirmed or abandoned — the step closes cleanly with no caveat and no baseline
needed. Never tell the user the workspace cannot be closed.

## Rules

- **Only report what the user states.** A migration you did not watch happen is
  not one you can assert. `target` and `note` are theirs, recorded verbatim;
  never fill them in from what seems likely.
- **`migrating` is not `migrated`.** If they say "it's running", that is
  `mark_data_service_migrating`.
- **Do not chase.** Waiting weeks is the design. Reporting status unprompted on
  every turn is noise, pressing for completion is not your call, and the offer
  in step 3 is made once rather than every time they come back.
- **Never run `gcloud`, `aws`, `skopeo` or any other cloud CLI yourself.** The
  runbooks this step hands out carry more runnable AWS-to-GCP commands than
  anything else in this repository — the image replication runbook and
  `replication._login_command` are the other source;
  some of them move customer data, and two — `secretsmanager.md` and the
  `SecureString` half of `ssm.md` — print a secret in plaintext. Every one is the USER's to run, with their own credentials: print
  it, explain it, let them run it. Running one would put customer data through
  this process and this transcript. The provisioning step's instructions carry
  the same rule for the same reason.
- **The landing zone has to be applied first.** The translation phase ends at a
  Pull Request and nothing in this agent applies it. If the operator has not
  merged it, the targets do not exist yet — say so rather than walking them
  into a procedure that cannot work.
- **An entry with no attributed consumer is not an unused entry.** The scan
  under-detects on purpose and the review may have left it unattributed. Say
  "nobody the scan could attribute", never "nothing uses it".
- Self-service image copies can still be reported from here with
  `mark_replication_complete` — they are not stuck behind the databases.
- If a tool returns an `ERROR`, report it verbatim and stop. The state stays
  here, so the step is retryable.
