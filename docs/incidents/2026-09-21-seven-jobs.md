# Seven NOVA Flow failures, 19:45–20:20 (NOVA clock)

FlowKit host clock approximately83s ahead of NOVA; correlation uses request IDs.

- ac140798: upload-image rejected five times with503,
  FLOW_REQUEST_NOT_SUBMITTED/retryable=true/retry_after_s3 during our diagnostic
  rollout drain. NOVA exhausted retries in12.5s. This was an internal maintenance
  admission failure, not Google model unavailability.
- 915a41b7,202b67fb,0b8bb593,79e486e0,a56394b5: upload-image on Nick-b
  fails before video submission. Browser maseQ request aborted at54,990–54,997ms;
  FlowKit error is `maseQ: signal timed out`, HTTP502. NOVA presents generic
  model unavailable. injected.js caps fetch deadline at55,000ms even though
  FlowKit allows120s. This proves where cancellation happens, not why the
  response does not arrive. No arbitrary timeout increase or proxy rotation.
- e6933297: generated operation220d0813-31a7-438d-9de8-7a7ffbf5f122.
  Submit structural snapshot already names media7fc47445-c3eb-49cb-b2bf-37e7087aca39
  in both workflow row and matching CAE row for project9fd3eefc-cbda-443a-a999-4ff16002a419.
  Existing client discarded this mapping. Subsequent Zzl0ze listing calls
  repeatedly hit ERR_FAILED and poll budgets expired. This is a confirmed
  client discovery defect; actual video success remains unverified.

Fix implemented:
- Binding: bind media only when ack workflow/CAE rows agree on operation,
  project and media UUID. Preserve owner and durable mapping. Check confirmed
  media URL before attempting expensive listing discovery, including refresh
  rounds. No duplicate submission, paid fallback or historical billing mutation.
- Upload timeouts:
  (1) Raised extension fetch deadline in injected.js and content.js to scale
  with backend request.expiresAt up to 120s (preventing premature 55s abort).
  (2) Added automatic image optimization in agent/api/flow.py: heavy images
  (>1536px or >500KB) are resized to <=1536px JPEG (~150-250KB) before proxy upload,
  reducing transfer times from 50s+ down to 4-7s without conditioning loss.

Validation: 521 unit tests pass 100% (including test_submit_media_binding and
test_flow_upload TestOptimizeImageForUpload). Extension tests pass (11/11).
Hot-reloaded flow module in running service with zero downtime.
Live 2.4MB upload verified successfully (HTTP 200 OK).
