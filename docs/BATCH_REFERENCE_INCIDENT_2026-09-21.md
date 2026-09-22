# Batch/reference and video incident — 2026-09-21

Status: activated on 2026-09-21 following explicit user authorization. FlowKit API
and TVC restarted after drain; NOVA commit `872fada` pushed and deployed through
Blue-Green (green8081 healthy, blue stopped). Public health/ready/Studio return200.
No paid model approved or wallet edits. Architecture feature remains separate.

## Evidence

Batch `509faad1`: 12 images, 4 completed, 8 failed. Local batch record and durable
media-owner map agree: items1–4 have both references on the same owner; items5–12
have face on owner1 and outfit on owner2. All8 failures are HTTP400
`MEDIA_PROFILE_MISMATCH`. Three application attempts repeated the same invalid
pair. This is a reference-routing defect, not evidence of a prompt, IP or
concurrency failure.

Tenant322 jobs `1900d47`, `24cd281`, `95d8765`, `5a9078d`, `399bd19` (all with
`nova_job_` prefix): each has a Flow operation/media handle and persisted
`upstream_timeout`. Flow polling elapsed540–552 seconds, retry_count0. NOVA total
elapsed560–698 seconds includes submit/queue/poll overhead. A read-only media
lookup for each handle still returned no image/video URL at investigation time.
This does not prove that Google never accepted/executed work. No blind replay,
proxy rotation or increased timeout is justified solely by these durations.

Tenant420 `nova_job_0455d12…`: low-priority/permission guard rejected submission;
no hop handle saved. Public error mapping flattened the safe code to generic
validation then job `generation_failed`.

Tenant420 `nova_job_1c896e4…`: NOVA logs show FlowKit HTTP400 converted to502,
then five video-create attempts over108 seconds. Logs discarded upstream detail,
so the exact original400 cause cannot be established. Multi-reference upload
had the same missing binding design in NOVA, but this specific job is not claimed
as a confirmed reference mismatch.

## Fixes

- Optional `reference_media_id` on upload binds the new image to that known
  reference's owner. Unknown anchor/conflicting explicit owner fails before
  upload; anchored uploads cannot move to another account.
- Batch preparation uploads all face/outfit references on one owner before
  any generation. Legacy unbound references are reuploaded during an explicit
  retry; completed outputs remain intact. Missing reference stops generation.
- Per-batch locks stop duplicate item dispatch; snapshot files replace atomically.
- Image POST wrapper sends once; item retry allowed only for explicit
  `FLOW_REQUEST_NOT_SUBMITTED` + retryable=true. Timeout/ambiguous output/download
  failure does not generate again. Accepted media ID is saved before download.
- Manual retry rejects completed/in-flight/unsafe items with409, displayed by UI.
- NOVA image/video upload groups send the first media ID as anchor on later uploads.
- NOVA preserves `low_priority_only`, `upstream_rejected`,
  `upstream_submission_unknown` through HTTP and durable job error mapping.
  Generic upstream400 stays400. Raw account/provider details remain private.
- NOVA's outer Flow media create loop does not replay generic failures. Adapter
  alone retries explicit not-submitted evidence; retryable=false wins over
  proxy/reload text. Generation transport/response loss is unknown, not an
  availability signal authorizing another generate.

The540s Flow/600s NOVA poll budgets and low-priority-only model policy are unchanged.
No new claim about IP reputation or safe render concurrency is made.

## Verification

- FlowKit full unit suite:477 passed, one existing Starlette/AnyIO deprecation.
  Tests run with isolated database/account paths, not production state.
- New regression simulates the12-image batch and verifies all12 complete with
  references from one owner; mock generation only, no Google calls.
- Missing uploads, duplicate item calls, unknown owner, conflicting owner,
  rejected/timeout generation and failed artifact download regressions pass.
- NOVA `go test ./internal/provider/flowkit ./internal/api -count=1`: pass.
- Targeted `go test -race`: pass; an initial race in a new test counter was
  corrected to atomic, no runtime race was reported by the final run.
- `go vet` for both NOVA packages, Python compile and changed-file diff checks pass.
- Historical video records remain unchanged. Original batch images1–4 verified
  unchanged by media UUID and SHA256 after recovery. All4 current reference
  entities resolve to one owner. Bulk retry regression:8 tests pass.

## Activation order

1. Integrate FlowKit changes with existing uncommitted reliability work intact.
2. Release FlowKit API plus Batch/TVC worker together; upload API must understand
   the anchor before NOVA begins sending it.
3. Release NOVA through its Blue-Green deployment script.
4. Only after activation, explicitly retry the8 proven pre-dispatch batch failures.
   Do not regenerate the5 ambiguous video timeouts as automatic recovery.

Activation completed. One `/api/batch/retry` request with `all_failed:true`
queued only items5–12. Recovery produced5 new images (6,8,10,11,12), all with
completed automatic low-priority videos. Batch reached9/12 images. Items5/7
returned ambiguous `Failed to fetch` without media handles and are blocked from
blind replay. Item9 exhausted safe `FLOW_BUSY_NOT_SUBMITTED` attempts; a second
bulk request selected only9, completed successfully, including its low-priority video. Final batch:10/12
images and10 completed videos. The5 historical tenant322
video timeouts were not regenerated.

Follow-up source fix clears stale image error text when retrying/succeeding;
regression covers recovery from previous mismatch (8 tests pass). TVC had no
generation worker threads after item9 completed; stopped TVC, saved recovery
snapshot, cleared only COMPLETED image stale error fields, then started TVC
with the metadata fix. Both services active. Failed5/7 retain diagnostic errors.
Deployment evidence: `scratch/deployments/reference-binding-20260921-163320/`.
