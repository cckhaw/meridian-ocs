# Working on Meridian OCS

Standing rules for anyone (human or automated) changing this repository.

## Keep the README and the docs current

The README is the front door: what is built, what is not, how to run it. It must never describe a
state the code is not in. With **every** pull request that adds, changes or removes a capability:

1. Update the README status tables and move items between the status tables and the
   **What is not built yet** list; add anything the change leaves undone. Update the *Last updated* line.
2. Update the relevant document in `docs/architecture/` (behaviour, verification, and its numbered
   gap list). Fix gap items elsewhere that the change makes stale.
3. Update the repository layout, build/test/run and testing sections if they changed, and add a new
   document to the README's documentation table.
4. Keep claims honest: say what was verified and how, and what was not (for example "never installed in
   a cluster"). Do not state numbers that were not measured.

## Conventions

- Delivery is one module per pull request, on the designated branch, with CI green before merge.
- Run `make fmt vet build test` (with `OCS_REQUIRE_REDIS=1` and a local `redis-server`) before pushing;
  `deploy/helm/test.sh` when the chart, rules or dashboard change; `dev/smoke.sh` against
  `docker compose up` when the stack changes.
- No silent security defaults: an authentication mode must be chosen explicitly; `off` is for development.
- Prove guarantees with a test that fails when the guarantee is broken (mutation check), not only with a
  test that passes.
- Generated files (`pkg/gen`, `deploy/helm/meridian-ocs/values.schema.json`,
  `deploy/helm/meridian-ocs/files/dashboard.json`) are regenerated with their tools, never edited by hand.
