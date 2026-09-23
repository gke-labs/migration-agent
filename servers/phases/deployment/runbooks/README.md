# Data migration runbooks

One procedure per file, for the data services graded `migrate` at the data
review. `STATE_DEPLOYMENT_DATA_MIGRATION` offers the matching one per
outstanding service; `get_data_migration_runbook` returns it together with what
the ledger knows about that service, and the agent adapts it before the
operator sees it.

These are templates, not documents. A rendered copy — placeholders replaced
with the estate's own names, sizes and regions — lands in the ledger under
`platform/deployment/runbooks/`, one per data service, written by
`save_data_migration_runbook`.

## The contract

- **Every command is the operator's to run.** Nothing here is run by the
  server or by the agent (DESIGN.md §10, and the rule at the top of
  `knowledge/data-migration.md`). Some of these commands move customer data,
  and two — `secretsmanager.md` and the `SecureString` half of `ssm.md` —
  print a secret in plaintext; running them from an agent process would put
  that data through the transcript.
- **Placeholders are `<UPPER_SNAKE>`.** `runbooks.unresolved()` finds them and
  `save_data_migration_runbook` refuses a rendered runbook that still carries
  one — the same discipline `mark_replication_complete` keeps about
  `<AR_DESTINATION>`. A stand-in in a document an operator runs from is a
  command that fails at best and hits the wrong resource at worst.
- **Five sections are required**, and `runbooks_test` enforces them: the
  opening (what it moves, and the cutover class), Substitutions, Validation
  gates, Rollback, and Known limitations. A runbook without a rollback is a
  one-way door dressed up as a procedure.
- **The middle is shaped by the move.** Most files run prepare-the-source,
  prepare-the-target, run-the-copy, cutover — and a reader who has run one
  will recognise the next. Some cannot: `fsx.md` is organised by flavour
  because FSx is four products behind one name and a single "prepare the
  source" would be wrong for three of them; `secretsmanager.md` and `ssm.md`
  have nothing to prepare on the source and lead with the decision about what
  each value should become; `memorydb.md` and `elasticache-persistent.md` put
  the shard question before step one, because it decides whether the rest of
  the file applies at all; and `rds-mariadb.md` opens on whether to move to a
  different engine at all. Six of the eleven depart from the plain
  four-step middle in one way or another — if yours does too, that is a normal
  shape, not a deviation to apologise for.
- **A limitation that has bitten someone goes in the file**, not in a comment.
  The lists at the end of the RDS runbooks are the reason those two are worth
  handing over at all.

## Adding one

1. Write `<service>.md` (or `<service>-<variant>.md` where the engine decides
   the procedure, as the four `rds-*` files do) following the shape above.
2. Add it to `runbooks._BY_SERVICE`, or to `_RDS_ENGINES` for an engine
   variant.
3. If the service currently sits in `runbooks.NO_RUNBOOK_REASON`, remove it
   from there in the same change — an entry in both is a service the offer
   promises and the tool then declines.
4. Say in `knowledge/data-migration.md` that the procedure moved here, if that
   document still carries commands for it.

A service with no runbook is a supported state, not a hole to paper over.
`escalate` grades have no procedure by design, and the refusal names the
reason. What is not acceptable is handing over the nearest-looking file:
a `pglogical` procedure for an Oracle instance is worse than "I do not have
one for this".
