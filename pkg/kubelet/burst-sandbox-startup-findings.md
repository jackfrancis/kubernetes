# Burst sandbox startup: findings, rejected approaches, and remaining attribution gaps

**Problem statement:** "burst starting sandboxes can take longer time than expected (like over 17s)."

**Status:** Investigation checkpoint, 2026-11-17. No claim that the original problem is fixed.

This report consolidates supplied incident evidence, deterministic Kubernetes reproductions, retry experiments, and a separate volume-read optimization. It is intended for independent technical review, including review by other models. The companion [evidence digest](./burst-sandbox-startup-evidence.json) contains aggregate measurements and source-file SHA-256 fingerprints.

Private chat text, infrastructure identifiers, credentials, raw incident logs, and local filesystem paths are excluded. The original cloud reproduction was not executed during this investigation. No report content has been sent to an external model.

## 1. Executive findings

1. **A real startup failure mechanism was reproduced:** a node can observe an assigned Pod before the NodeAuthorizer graph has processed the relationship needed to authorize its PVC/PV reads. A transient denial can then encounter kubelet processing suppression and ordinary Pod-worker error backoff. Binding success is not an authorizer-graph synchronization barrier.
2. **The reported 17s clock needs clarification.** In five supplied 100-Pod, runc-based minimal-repro cohorts, 428/500 collector-observed creation-to-Ready intervals exceed 17s. None of their journal-derived `RunPodSandbox` start-to-return intervals exceed 17s; the maximum is 13.920s. These are different measurements, not contradictory results.
3. **The five observed startup denials do not establish the cause of the whole burst tail.** There are 234 observations over 17s in three cohorts with no recorded startup relationship denials. Readiness-observer overhead is also included in the collector's clock. Broad critical-path attribution remains unfinished.
4. **Every tested accelerated-retry policy remains NO-GO under the unchanged healthy-work protection criterion.** Some give large isolated recovery improvements, but controlled counterexamples repeatedly delay later healthy Pods. Equal total GET counts do not guarantee equal latency: moving useful work earlier can consume capacity needed by later arrivals.
5. **A separate, narrowly eligible read reduction is promising and has passed substantial functional qualification.** It removes one startup PVC read for new, writable, expandable filesystem volumes with exactly one RWO or RWOP access mode. Real CSI cleanup and actual ext4 growth passed the tested cases.
6. **That optimization does not apply to either reported incident configuration.** The supplied report identifies the original workload as raw-block RWOP; its successful minimal repro uses a read-only built-in hostPath volume. Both are excluded from the read-reduction candidate. It is an adjacent optimization, not an established fix for the original symptom.

**Recommendation:** Pause further retry heuristics and additional CSI-backend qualification. First resolve the latency clock and attribute the original burst's critical path, preferably using the existing evidence before collecting more. Review the standalone read reduction separately, without retry-policy changes.

## 2. What the incident evidence actually measures

### 2.1 Distinct clocks

| Measurement | Start and end | What it does not establish |
|---|---|---|
| Supplied `pod_created_to_ready_ms` | Client timestamp immediately before Pod POST to the collector's observation of Ready | Not API-server creation timestamp to the exact Ready transition; not sandbox RPC duration |
| Supplied `sandbox_ms` | First matching node-journal `RunPodSandbox for ...` entry to first matching `returns sandbox id ...`, joined by Pod UID | Not the whole creation-to-sandbox path; not necessarily the complete kubelet-side RPC interval |
| Supplied denial/retry interval | Failed startup volume lookup to the later observed successful retry | Not continuous authorizer staleness throughout that interval |
| Harness healthy/late `startup_ms` | Pod submission to first successful `SyncPod` | Not real sandbox startup or Pod readiness; runtime and mounts are simulated |
| Harness transient `recovery_ms` | Simulated authorization-window expiry to first successful `SyncPod` | Not time since first denial; `recovery_ms = startup_ms - authorized_ms` |
| CSI qualification observations | Mount, failure, cleanup, restart and expansion milestones | Functional evidence, not an uninstrumented latency comparison |

The original collector waits for **all burst Pod-create requests to complete before beginning readiness observation**, then observes Pods through a bounded thread pool. Its readiness results therefore include possible observation scheduling/polling delays. They should not be relabeled as exact Ready-transition latencies.

For sandbox intervals, the collector selects the first matching start and first matching successful return per UID. This report verified that extraction logic and recomputed summary aggregates; it did not independently revalidate every raw journal pair.

Do not subtract cohort p95s to infer a missing stage's p95. Attribution needs a per-Pod join, consistent clock definitions, and explicit handling of overlapping work and cross-host timestamp uncertainty.

### 2.2 Retrospective aggregation of the supplied minimal repro

All rows below use **runc**, including pool B, whose nodes had also been used for Kata work. These are not measurements of Kata VM startup. Pool labels are anonymized. Pool/run differences are not controlled causal comparisons.

Values are seconds, rounded to three decimals. Percentiles use nearest rank. All metric arrays contain a finite value for every Pod.

| Cohort | Pods | Recorded startup denials | Observed Ready p95 | Observed Ready max | Observed Ready >17s | Journal sandbox p95 | Journal sandbox max |
|---|---:|---:|---:|---:|---:|---:|---:|
| A0: 10-Pod comparison | 10 | 0 | 1.769 | 1.769 | 0 | 0.772 | 0.772 |
| A1: 100-Pod burst | 100 | 0 | 18.765 | 18.818 | 66 | 5.303 | 6.240 |
| A2: 100-Pod burst | 100 | 0 | 18.137 | 18.163 | 75 | 4.888 | 5.119 |
| A3: 100-Pod burst | 100 | 3 | 25.430 | 25.784 | 96 | 13.218 | 13.920 |
| B1: 100-Pod burst | 100 | 2 | 27.500 | 28.536 | 98 | 6.980 | 7.189 |
| B2: 100-Pod burst | 100 | 0 | 21.071 | 21.176 | 93 | 6.725 | 7.143 |

Across the five 100-Pod cohorts:

- **428/500** collector-observed creation-to-Ready intervals exceed 17s.
- **0/500** recorded journal sandbox intervals exceed 17s.
- **234** observations over 17s are in the zero-startup-denial cohorts A1, A2 and B2.
- All six stored burst-observation records state that no workload deletion occurred during observation.

This is new analysis of preserved data for this report, **not a new cluster experiment**. It does not show that the original application, other runtime configurations, or every incident lacked sandbox calls over 17s. Nor does it prove that all 428 Pods actually transitioned to Ready after 17s rather than being observed later.

### 2.3 Authorization findings in the supplied report

The supplied report documents five genuine pre-sandbox startup denials across those 500 Pods: **four PVC GET 403s and one PV GET 403**. Its audit/journal correlation places successful binding first, the denials before initial sandbox creation, and Pod deletion another 29.9-38.5s later.

It includes:

- A denial on the same API-server replica that acknowledged binding.
- A cross-replica case where a PVC GET succeeds but the subsequent PV GET is denied on another replica.
- Successful retry intervals of approximately **11.43-16.20s**.
- Kubelet startup logs consistent with failed volume processing blocking the initial runtime path.

The report also describes **876 rolling, non-burst trials** with concurrency at most ten, zero startup relationship denials, and **129 post-DELETE** relationship 403s. These include 68 real CSI raw-block/RWOP trials. Those cleanup-time errors must not be counted as startup races. These rolling-control totals are the supplied report's findings, not new executions or a new full audit performed for this consolidation.

**Interpretation:** the evidence supports an authorization/backoff contribution for the affected Pods. It does not quantify that contribution across the entire burst, and the retry gap is not a measurement of how long the graph continuously lacked the relationship. The separate Kata cold-start issue has not been shown to share this cause.

## 3. Deterministic reproduction and preserved protections

### 3.1 Kubelet unit reproduction

The first new test injected a transient PVC or PV Forbidden response, then allowed access. Cases covered initial Pod reprocessing versus processing without that initial reprocess entry.

The requested regression assertion was deliberately stronger than unchanged behavior: **recover on a later populator pass without requiring another Pod sync**. The initial-sync cases failed deterministically on the original implementation.

Two mechanisms were separated:

1. Populator bookkeeping can treat a failed initial pass as processed and suppress further lookups until reprocessing.
2. Publishing a volume-processing error ends the volume wait and lets the Pod worker apply its normal error backoff.

Historical inspection found that failed-refresh suppression protects against repeated work, including deleting-PVC failures. Simply retrying every failed pass would remove intentional protection. "Retry on the next pass" was a proposed behavior, not an established upstream promise.

Guards subsequently covered request counts, ordinary refresh errors, deletion/protection, termination, UID replacement, cancellation, retry bounds, error aggregation and fallback scheduling.

### 3.2 Real NodeAuthorizer integration

The integration fixture runs **two real loopback kube-apiserver instances sharing isolated etcd**, with production NodeAuthorizer graph handlers and real node-identity HTTP requests.

Only selected bound-Pod graph notifications are gated. This establishes that:

- Binding has been acknowledged.
- The assigned node's watch can deliver the Pod.
- Informer storage can have advanced while graph-handler processing is still held.
- PVC/PV authorization can still reject the node.
- Releasing graph processing permits the legitimate relationship.
- Unrelated nodes remain forbidden.

Same-replica, cross-replica PVC-success/PV-denial, multiple denied passes, exhaustion and ordinary-fallback cases were exercised. The final set recorded **50 leaf-case passes across five race-enabled repetitions**. The exact final accelerated-recovery cases failed under the unchanged-behavior overlay.

This proves the controlled ordering mechanism and the candidate's behavior under it. It does **not** measure the incidence or duration of spontaneous production graph lag, establish a new synchronous-consistency contract, or involve physical mounts.

### 3.3 Experimental bounded-retry contract

The approved retry experiment was deliberately narrow:

- Initial PVC/PV **Forbidden** failures only.
- At most **four additional processing passes within three seconds**.
- Initial 100ms delay, exponential spacing and 0.1 jitter.
- Normal resync does not renew the episode.
- Lifecycle/UID checks, success, exhaustion or admission refusal close acceleration.
- Normal fallback remains; node permissions remain fail-closed.

Later changes preserved first-denial fallback timing, prioritized ordinary work, allowed at most one optional Pod pass per iteration, and used non-blocking admission against the existing shared limiter.

The attempt bound is a **Pod-pass** bound, not a four-HTTP-request bound. One pass may issue multiple lookups and trigger additional ordinary verification work.

## 4. Retry experiment ladder: what failed and why

### 4.1 Measurement rules

The load harness uses real HTTP/client-go limiting, volume management, `SyncPod`, and worker retry scheduling. Authorization lag, mounts, runtime/status and the outer worker driver are simulated.

Most comparisons contain **three untraced repetitions per cell**, on a shared macOS/arm64 host with Go 1.27.4 and `GOMAXPROCS=4`. Values below are medians of per-run nearest-rank p95s, not pooled p95s. Healthy latency starts at submission; transient recovery starts at simulated authorization availability.

Incomplete groups remain **censored**. A p95 computed only from successful members would falsely reward a policy that failed to start its slowest Pods. Permanent-denial workloads use a fixed 20s observation window; other runs complete or stop at 30s.

QPS/burst and API-delay settings are harness inputs, not measured properties of the original cluster. Primary timing excludes race detection. Correctness checks use it.

### 4.2 Chronology and decisive counterexamples

| Step | Change or question | Key finding and decision |
|---|---|---|
| Generic backoff sweep [E02] | Shorten the normal Pod-sync error backoff | 90 samples over 30 cases. Isolated recovery improves, but persistent failures and slow APIs produce healthy-arrival regressions. No generally safe shorter interval established. |
| Bounded initial retry [E03-E04] | Retry only startup PVC/PV Forbidden failures | After correcting request-context deadline/value propagation: 36 matched samples. Burst recovery p95 **13.032 -> 17.271s**; constrained-client late healthy **7.502 -> 9.301s**; slow-API late healthy **7.501 -> 9.601s**. **NO-GO.** Earlier timing magnitudes are superseded. |
| Anchored fallback [E05] | Credit elapsed retry time against the original failure's normal backoff | 54 matched samples. Burst recovery returns near baseline: **13.495s native / 13.691s anchored**, versus 17.415s pre-anchor in that comparison. Healthy penalties remain about 24% and 28%. **NO-GO.** |
| Ordinary-work priority [E06] | Process ordinary/first-time work before optional retries in each snapshot | Original one-second-arrival cases reach near parity. Moving healthy arrival to three seconds exposes **5.402 -> 7.206s**, **+33.4%**, and 288 -> 320 GETs. A snapshot cannot prioritize work that has not arrived yet. **NO-GO.** |
| One-Pod quantum [E07] | At most one eligible optional Pod pass per iteration | Critical late-healthy p95 improves but remains **5.403 -> 6.002s**, **+11.1%**; GETs 288 -> 300. Light recovery becomes slower than priority-only. **NO-GO.** |
| Non-blocking per-GET admission [E08] | Use shared-limiter `TryAccept` rather than queueing optional requests | Refused requests send no HTTP and retain the original error/timing. Critical p95 remains **5.403 -> 6.003s**. GETs fall to 296-297, but admitted retries still spend capacity. **NO-GO.** |
| Reserved headroom [E09] | Yield to ordinary waiters; retain 25% or 50% of burst capacity | 135 samples. Three-second arrival looks protected; four-second arrival exposes **4.503 -> 5.404s** at 25% reserve and **4.503 -> 4.816s** at 50%, about **+20% / +7%**. **NO-GO.** |
| Token/call-site tracing [E10] | Account for the entire cost induced by optional GETs | 71 samples; 8,818 traced GETs and 38,529 events reconciled. In the 25% reserve trace, **14 marked GETs trigger four ordinary verification GETs before arrival**; no transient Pod finishes before arrival. Successful PVs are fetched again at fallback. Diagnostic explanation, not a qualifying policy. |
| Whole-Pod preflight [E11] | Estimate all used filesystem-PVC lookup/verification costs before admitting a pass | 83 samples. At four-second arrival, baseline **4.502s**, whole-Pod +25% **5.103s**, +50% **4.802s**. All three issue **288 total GETs**. Useful completed work moved earlier still delays healthy arrivals. **NO-GO.** |
| Sticky contention veto [E12] | Once an episode observes actual ordinary token-reservation delay, permanently stop its acceleration | 75 samples. Protects already-busy cases, but an initially uncontented-to-burst transition gives **4.503 -> 5.104s**, **+13.34%**, and 172 -> 184 GETs. All 12 extra GETs occur before healthy arrival and before the first ordinary wait queues. No veto can undo that expenditure. **NO-GO.** |

These are separate matched experiments. A baseline from one row should not be substituted into another row's percentage calculation.

### 4.3 What the ladder taught us

- **Delayed error publication can add another delay before normal backoff even starts.** Anchoring fixed that specific amplification. The full normal jitter is sampled before subtracting elapsed time; fresh or untagged errors must prevent inappropriate shortening.
- **Startup timing metadata must not leak into termination/unmount retries.** A guard caught this during the anchored-fallback work, and it was corrected.
- **Priority, quantum and non-blocking admission are not future-arrival isolation.** They can bound current work without preserving the tokens a later healthy burst would otherwise receive.
- **Marked requests are not the complete cost.** Successful partial progress can trigger ordinary verification reads; an eventual refusal may still prevent the Pod from starting. Four verification reads in the trace moved earlier; they were not four extra reads over the entire observation window.
- **Total request counts alone are insufficient.** Whole-Pod preflight removed observed successful-PV rereads and could finish useful work, yet equal total GET counts still produced worse late-healthy latency.
- **A reactive contention signal is necessarily late in the tested transition.** Its native and observer-only controls reproduced the penalty, so the counterexample does not depend on assuming that the observer adapter is timing-neutral.
- **There is a real benefit/cost trade-off.** Several candidates completed all 32 recoverable Pods within 20s while baseline completed 29-30. That benefit must be presented alongside, not in place of, the healthy-latency regression.

The acceptance criterion has been no known repeatable healthy-work regression in the tested matrix, not an agreed numerical non-inferiority margin or a theorem about every workload. These counterexamples reject the tested policies; they do not prove that every possible retry design is impossible.

## 5. Read reduction: a separate investigation

### 5.1 Why a repeated PVC GET is not automatically redundant

Initial fresh PVC/PV reads validate binding, deletion/protection, claim identity and ownership. Blind reuse across retry episodes was rejected: the PVC can be deleted/recreated or its relationship can change.

Another PVC GET initializes unknown claim-size state during volume verification. Expansion-capable mounts later perform a fresh PVC read as well. This suggested a narrower optimization, but resize ordering mattered:

| Approach | Reproduced issue | Disposition |
|---|---|---|
| Immutable earlier PVC/PV capacity snapshot [E13] | PVC status advances from 1Gi to 2Gi before verification. Reusing the 1Gi snapshot causes an unnecessary legacy `NodeExpand` RPC. Requiring initial PV/PVC size equality still fails when the PV update arrives later. Both legacy interleavings reproduce 5/5. | Rejected before load measurement. This demonstrated extra work, not data corruption. |
| Simply omit the early capacity-only read [E14] | A new-volume Forbidden/NotFound failure moves from **before `SetUp`** to **after `SetUp`**; the volume is attached/uncertain rather than untouched. Both failures reproduce 5/5. | Rejected under the then-approved unchanged-failure-ordering gate. This was a phase probe, not a complete implementation. |
| Broad deferred initialization [E15] | Another node advances global RWX PVC capacity between verification and mount. Deferral initializes local state to the newer capacity and suppresses a required local expansion. Native calls local expansion once; candidate zero, in both modern and legacy paths, 5/5. | Rejected; narrowed to exactly one RWO or RWOP access mode. |

The user explicitly approved **qualifying later failures**, provided runtime startup stays blocked and uncertain-volume cleanup remains correct. That is an intentional behavior change, not semantics-neutral deduplication. Already-initialized-size paths can have late failures today; the experiment did not establish a universal existing pre-setup authorization barrier.

### 5.2 Final narrowed candidate

Eligible cases are new, unknown-size, writable, expansion-capable **filesystem** volumes, with:

- Exactly one PV access mode: `ReadWriteOnce` or `ReadWriteOncePod`.
- Valid nonempty PV/PVC identities.
- No reconstructed/device-reconstructed state, existing mount, or mount elsewhere.
- The required actual-state updater support.

Other paths retain the original verification behavior, including **raw block, read-only, non-expandable, RWX, missing/multiple access modes, reconstruction and already-shared mounts**.

The prototype:

1. Atomically marks a newly attached volume as awaiting mount-time claim-size initialization.
2. Prevents an older populator observation from initializing that deferred entry.
3. Uses the mount's existing fresh PVC read, validating identity before initialization.
4. Preserves already initialized/resized size; successful resize clears deferral.
5. Keeps required-read failures explicit, blocks mounted/runtime success, and retains uncertain-state cleanup.

It changes three production-source copies: actual-state cache, operation-executor updater interface declaration, and operation generator. All are **experiment overlays**, not a standalone production patch. It neither caches PVCs across retries nor grants additional permissions.

### 5.3 Corrected factorial measurements

Four controls separated original behavior, read reduction alone, bounded retry alone, and their combination. After the RWX flaw was found, the broad timing run was stopped. Its **73 complete samples** and one interrupted raw log were retained separately; they do not qualify the narrowed candidate.

The corrected candidate has **48 untraced samples, three per cell**, plus 12 race-enabled workload-fixture samples. Its repeated safety runs contain **385 candidate and 270 native PASS records**, including parent tests; these are not 655 distinct tests.

| Eligible healthy 100-Pod workload | Native | Read reduction alone |
|---|---:|---:|
| Total GETs | 400 | 300 |
| Startup p95 | 6.003s | 4.203s |

This is a 25% GET reduction and approximately 30% startup-p95 reduction in this harness. It must not be credited to bounded retry.

| Mixed workload / metric | Native | Read reduction alone | Combined retry + read reduction |
|---|---:|---:|---:|
| Mixed read-only late arrivals: late-healthy p95 | 6.603s | 6.303s | 7.802s: **+18.16% vs native; +23.79% vs reduction alone** |
| Mixed read-only four-second arrival: late-healthy p95 | 6.902s | 6.302s | 7.802s: **+13.03% vs native; +23.80% vs reduction alone** |

In the light transient-PV workload, recovery p95 improves from **12.911s native to 1.384s combined**, with all five transient Pods completing on the original sync.

The eligible path goes from **PVC + PV + verification PVC + mount PVC: four GETs, to three**. Non-expandable volumes retain three; read-only expandable volumes retain four.

The light-recovery benefit is real within the simulated workload. It does not convert the combined policy into a healthy-work-safe design. Homogeneous eligible workloads can hide the retry penalty because healthy Pods receive the read optimization too; mixed eligibility exposes it.

## 6. Real CSI and physical filesystem qualification

Both qualification stages compare **native versus narrowed read reduction, with bounded startup retry disabled in both**. Identical task-only HTTP transport instrumentation classifies reads and injects scoped 403/404 failures after initial binding lookups. Gates are bounded/cancellable; credentials, headers and bodies are not logged. The transport tests passed 25 repeated race-enabled PASS records.

These are functional tests. Injected failures are not real authorizer-graph lag, and instrumented timings are not performance evidence.

### 6.1 CSI hostpath [E16]

A two-node local kind cluster used real kubelets, runtime, Linux bind mounts and CSI hostpath driver v1.19.0. Control-plane version was v1.38.0-beta.0; the tested worker kubelets were built from the investigation baseline plus the frozen overlays.

**42 total matched native/candidate cases passed**, covering:

- Required read placement, late Forbidden/NotFound, runtime gating and real unpublish/unstage/mount cleanup.
- Failed-read recovery and failed/running/terminating-PVC kubelet restarts.
- Container identity/data preservation.
- RWO sharing without prematurely unstaging the remaining Pod.
- RWOP second-Pod exclusion.
- Online and startup-interleaved resize RPC sequencing.

An initial raw-manifest fixture omitted `--check-volume-lifecycle=true`, which the repository's hostpath e2e preparation normally adds. Native cleanup then lost driver state before unpublish and left a real bind mount. The **16 passing / one failing** initial cases were retained. The owned cluster was recreated and both controls rerun with the standard lifecycle guard; cleanup assertions were not relaxed.

**Limit:** this driver's expansion RPC validates path/capacity but does not physically grow a filesystem. That motivated the next, separate gate.

### 6.2 TopoLVM with actual ext4 growth [E17]

Pinned TopoLVM **0.42.1**, chart **17.3.1**, ran against a real Linux LVM/ext4 stack on a verified, exclusively owned 4GiB file-backed loop device in a temporary dedicated Linux ARM64 VM. Source inspection established that controller expansion enlarges the LV and node expansion invokes filesystem resize.

**36/36 final cases passed:**

- Four steady mounts.
- Eight failure/recovery cases.
- Twenty-four growth cases: online/startup-interleaved x RWO/RWOP x native/candidate x three repetitions.

Every growth case demonstrated:

- Logical block device: **1,073,741,824 -> 2,147,483,648 bytes**.
- Actual ext4 statfs capacity: **1,020,702,720 -> 2,077,073,408 bytes**.
- Controller enlargement while filesystem capacity remained unchanged behind the kubelet read gate; releasing the gate led to real node-side growth.
- A **1,207,959,552-byte file (1152MiB total)** written and verified, larger than the former filesystem capacity.
- Marker checksum, inode, mode, UID/GID and length preserved.
- Large-file checksum and filesystem size preserved through kubelet restart.
- Unchanged container identities and zero container restarts.
- Exactly **one successful node expansion per growth case in each control**.
- Verified unmount, PVC/PV deletion and LV removal.

Two fixture failures were preserved rather than hidden:

1. Nested lvmd placement could not create the required device nodes before the first native PVC bound. The documented VM-host daemon/socket arrangement resolved that environment problem.
2. After native growth succeeded, a buffered 1152MiB writer exceeded a 64MiB memory-cgroup limit. Both controls were rerun with the same 2GiB growth-workload limit and aligned direct I/O plus fsync; integrity and size thresholds were retained.

**Limit:** one driver, ext4 and a loop-backed Linux storage stack are not all-driver, XFS, production-appliance or RWX certification. The intentional later-failure behavior still needs maintainer review.

At experiment completion, the owned clusters, helper services, storage and temporary VM had been removed; the original VM/default connection were restored. This report required no new cluster, build or storage operations.

## 7. Applicability and current decisions

| Question | Current answer |
|---|---|
| Can NodeAuthorizer relationship lag occur after binding/watch visibility? | **Yes**, controlled production-handler reproduction; supplied incident evidence is consistent with it. |
| Can a brief denial cause a much longer pre-runtime delay? | **Yes**, the processing/error/backoff path can amplify it. Actual continuous authorization-staleness duration remains unmeasured. |
| Have we explained the original whole-burst >17s problem? | **No.** Clock definition and per-Pod stage attribution are incomplete. |
| Is bounded retry ready to merge? | **No.** Repeated healthy-latency counterexamples remain. |
| Is standalone read reduction worth review? | **Yes, within its narrowed scope.** Functional and physical-growth gates passed; design/maintenance review remains. |
| Does read reduction directly optimize original raw-block RWOP? | **No.** Block volumes are excluded. |
| Does read reduction optimize the read-only hostPath minimal repro? | **No.** Read-only/non-expandable paths are excluded. |
| Can the synthetic 30% healthy improvement be quoted as an incident fix? | **No.** Different eligibility, workload and clock. |

The original supplied report references v1.37.1 behavior. Our repository baseline is [Kubernetes commit 4518cd0c14fa4014eb4bbeac16c89ec7944baf8f](https://github.com/kubernetes/kubernetes/commit/4518cd0c14fa4014eb4bbeac16c89ec7944baf8f), described locally as v1.38.0-beta.0-429. Release/platform differences are another reason not to extrapolate numerical results directly.

All repository changes remain uncommitted on `test/kubelet-transient-volume-authz-repro`. There are no commits, pushes or PRs from this investigation. The worktree contains the bounded-retry/anchored-fallback/priority/quantum/non-blocking prototype and tests. Later admission policies and read reduction live in overlays. **The dirty branch is not a merge-ready read-reduction patch.**

## 8. Evidence quality and limits

- **Corrected measurements supersede flawed ones.** The original bounded harness lost request deadline/value propagation. The fix and matched rerun retained all three adverse conclusions; original timing magnitudes are historical only.
- **Tracing perturbs timing.** Full tracing shifted a light-PV median about 285ms despite identical request totals, near the existing 300ms volume-wait polling interval. Primary latency uses untraced runs.
- **Functional limiter conformance is not universal timing equivalence.** The ordinary-wait observer passed charging, refill, deadline and cancellation checks, but neutrality was not established in a sensitive busy cell.
- **Small sample sets are counterexample evidence, not capacity certification.** Three runs per cell on shared hardware do not establish universal equivalence, confidence bounds, or a safe global retry threshold.
- **Adversarial arrival phases are deliberate.** One-, three- and four-second arrivals and the uncontented-to-burst transition test protection boundaries; they are not estimates of production workload frequency.
- **No oracle is available to production.** A 403 does not identify a transient versus permanent denial. Harness labels must not enter policy decisions.
- **The initial safety set missed RWX.** The later counterexample invalidated broad deferral; only the separately rerun narrowed implementation is qualified by the reported corrected results.
- **Do not add all sample counts into a grand total.** Some controls are reused, revisions differ, interrupted records are excluded, and PASS-record counts can include repeated parent/leaf records.
- **Passing storage tests is not causal attribution.** They strengthen confidence in one optimization's behavior, not its relevance to the original burst symptom.
- **Report review is not code review.** Implementation approval requires examination of the actual patch and focused tests, not only this narrative.

## 9. Recommended next steps

### Priority 1: Resolve the clock and reuse the existing evidence

Agree on whether the target is creation-to-sandbox-ready, kubelet/CRI RPC duration, creation-to-container-running, or creation-to-Ready. Treat 17s as the reported symptom/example until an actual SLO and burst envelope are agreed.

First use existing audit records, Pod snapshots and node journals to construct a per-Pod timeline:

1. Client submission and API creation acceptance.
2. Scheduling and binding acknowledgement.
3. Kubelet receipt, worker queueing and sync attempts.
4. PVC/PV request starts, limiter waits, responses and failure/retry intervals.
5. Attach/setup/volume-wait completion.
6. Kubelet-side and runtime-side sandbox request start/return.
7. Container start, Ready transition and collector observation.

Mark missing milestones as unknown, not zero. Check clock alignment and compare the Ready condition's transition timestamp with collector observation. Analyze denied and denial-free Pods separately, including the zero-denial cohorts. Do not infer stage tails by subtracting aggregate percentiles.

**Exit condition:** identify which intervals actually cross the target threshold, how much is observer delay, and which critical-path stage dominates the slow Pods. The five-denial mechanism should earn its priority from that attribution rather than from being the first reproducible anomaly.

### Priority 2: Collect only missing causal evidence, on a faithful workload

If existing records are insufficient, design a small matched reproduction of the **original raw-block/RWOP semantics** and the **read-only hostPath minimal control**. Keep runtime comparisons separate; do not silently substitute writable expandable filesystems.

Preserve and record kubelet/API versions, client QPS/burst, node resources, CNI/runtime versions, image warmth, storage mode, burst size and submit spread. Compare burst/non-burst behavior and denied/denial-free Pods. Use low-overhead instrumentation with matched untraced controls.

No new cloud operation is implied by this report. Choose and authorize the environment separately.

### Priority 3: Choose an intervention from the measured stage

| If attribution shows... | Then prioritize... |
|---|---|
| Pre-sandbox error/backoff dominates the affected tail | A narrowly scoped recovery experiment, with first-error timing, explicit bounds and the existing healthy-arrival counterexamples retained |
| Denial-free Pods wait mainly before runtime | The measured worker, client-limiter, API or volume-processing bottleneck; avoid assuming the authorization race explains it |
| Time is primarily inside sandbox creation | Runtime/CNI/node-resource investigation, rather than a PVC-read optimization |
| The collector adds a large observation tail | Correct measurement/reporting before claiming a Kubernetes startup fix |

Change one causal lever at a time. Do not broaden node authorization, raise client limits, or shorten generic backoff merely to make a benchmark pass.

### Priority 4: Review the read reduction as its own change

Seek focused SIG Storage/design review of the later-failure semantics, claim-size provenance, RWO/RWOP eligibility, reconstruction, sharing and resize interleavings. A standalone patch should contain the read reduction and directly relevant tests, **not** the retry-policy ladder.

Further driver qualification can follow a concrete reviewer concern. Another backend is not currently the highest-value way to explain the original >17s symptom.

### Explicit policy decision, if retry work resumes

Keep the current healthy-work criterion unless deliberately changed. If a recovery-versus-healthy-latency trade-off is acceptable, define the affected workload envelope, allowed regression, recovery/completion benefit and evaluation method before tuning another policy.

The current results do not authorize silently exchanging healthy latency for faster recovery.

## 10. Independent-review prompt

Use the same report and prompt for each reviewer independently. Request an initial assessment before sharing other reviewers' answers. Disagreement should produce discriminating measurements, not a vote among model preferences.

> Review this investigation against the original problem: burst sandbox startup can exceed the expected time, for example 17s. Do not assume the proposed read reduction is the right fix or that the reported delay is time inside a CRI RPC.
>
> Separate direct observation, controlled reproduction, inference and unknowns. In particular:
>
> 1. What has actually been established about the original symptom, and what clock/applicability gaps prevent a fix claim?
> 2. What is the smallest next analysis or experiment that distinguishes authorization/backoff amplification from denial-free pre-runtime delay, runtime/CNI delay and readiness-observer overhead?
> 3. Which retry counterexamples generalize, and which depend on this harness? Identify unsupported conclusions, including any claim of universal impossibility or equivalence.
> 4. Does the narrowed read-reduction design have a missed ordering, identity, reconstruction, sharing or resize risk? Distinguish demonstrated regressions from hypothetical risks and say what the physical tests do not cover.
> 5. Rank at most three next steps. For each, specify the competing hypotheses, control, measurable outcome, failure condition and observer-effect check.
> 6. Give separate recommendations for the original latency investigation, the bounded-retry prototype and standalone read reduction. State whether a production decision needs an explicit latency/recovery trade-off.
>
> Do not recommend broader node permissions or a generic backoff reduction without addressing the supplied counterexamples. Do not treat successful synthetic `SyncPod` latency as real sandbox or Ready latency. State what additional evidence would change your conclusion.

## 11. Evidence index and reproduction boundaries

Source identifiers used above map to the following preserved artifacts. **Private originals are not bundled.** The [digest](./burst-sandbox-startup-evidence.json) provides selected non-identifying data and exact SHA-256 hashes, including hashes of each incident cohort summary and observation record. It is not necessary to share raw incident files to obtain a first independent opinion.

A [sanitized derivative of the original repro bundle](./burst-sandbox-startup-repro.md) is also included alongside this report. It retains all 2,876 supplied files with consistent aliases, unchanged measurements and its own checksum manifest. The digest's original-input fingerprints remain distinct from the sanitized derivative's fingerprints.

| ID | Preserved source / role |
|---|---|
| E01 | Supplied incident report, collector source and six cohort summaries/observation records; sanitized aggregate data in the digest |
| E02 | `volume-retry-results.json`: generic backoff sweep |
| E03 | `bounded-volume-retry-evaluation.json`: bounded contract and real-authorizer integration; original timings superseded |
| E04 | `bounded-volume-retry-corrected-evaluation.json`: corrected matched timing |
| E05 | `bounded-volume-retry-anchored-evaluation.json`: fallback-origin experiment |
| E06 | `bounded-volume-retry-priority-evaluation.json`: ordinary priority and arrival-phase counterexample |
| E07 | `bounded-volume-retry-quantum-evaluation.json`: one-Pod quantum |
| E08 | `bounded-volume-retry-capacity-evaluation.json`: non-blocking admission |
| E09 | `bounded-volume-retry-headroom-evaluation.json`: reserve/pending-work policies |
| E10 | `bounded-volume-retry-token-trace-evaluation.json`: request/token attribution and observer controls |
| E11 | `bounded-volume-retry-pod-preflight-evaluation.json`: whole-Pod completion-cost estimate |
| E12 | `bounded-volume-retry-contention-evaluation.json`: sticky epoch veto and transition workload |
| E13 | `bounded-volume-retry-claim-size-evaluation.json`: rejected stale-snapshot handoff |
| E14 | `bounded-volume-retry-fresh-size-phase-evaluation.json`: failure-ordering probe |
| E15 | `bounded-volume-retry-deferred-size-evaluation.json`: corrected narrowed candidate and factorial measurements |
| E16 | `csi-volume-read-reuse-evaluation.json`: real hostpath CSI qualification and initial fixture failure |
| E17 | `physical-csi-read-reuse-evaluation.json`: real ext4 growth, integrity and cleanup |

Useful public source anchors at the investigation baseline:

- [Populator processing and binding/resize checks](https://github.com/kubernetes/kubernetes/blob/4518cd0c14fa4014eb4bbeac16c89ec7944baf8f/pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go).
- [Pod-worker scheduling/backoff](https://github.com/kubernetes/kubernetes/blob/4518cd0c14fa4014eb4bbeac16c89ec7944baf8f/pkg/kubelet/pod_workers.go).
- [Volume verification, mount and expansion operations](https://github.com/kubernetes/kubernetes/blob/4518cd0c14fa4014eb4bbeac16c89ec7944baf8f/pkg/volume/util/operationexecutor/operation_generator.go).
- [Actual-state claim-size cache](https://github.com/kubernetes/kubernetes/blob/4518cd0c14fa4014eb4bbeac16c89ec7944baf8f/pkg/kubelet/volumemanager/cache/actual_state_of_world.go).
- [NodeExpander and RWX handling](https://github.com/kubernetes/kubernetes/blob/4518cd0c14fa4014eb4bbeac16c89ec7944baf8f/pkg/volume/util/operationexecutor/node_expander.go).
- [CSI e2e driver preparation](https://github.com/kubernetes/kubernetes/blob/4518cd0c14fa4014eb4bbeac16c89ec7944baf8f/test/e2e/storage/drivers/csi.go).

Experimental tests, which are not part of that public baseline, include `TestFindAndAddNewPods_TransientVolumeAuthorizationError`, `TestNodeAuthorizerBindingGraphLag`, `TestNodeAuthorizerVolumeStartupRetry`, and `BenchmarkVolumeAuthorizationRetry`.

The private evaluation artifacts preserve exact historical commands, input fingerprints, validation results and revision-specific overlays. Commands should not be copied blindly: temporary clusters/devices no longer exist, and some raw timing logs were removed only after structured evidence and exact fingerprints were retained. No completed experiment or historical cleanup script was rerun for this report.

This investigation and report were AI-assisted. Any upstream submission remains the human author's responsibility. Follow [CONTRIBUTING.md](https://github.com/kubernetes/kubernetes/blob/4518cd0c14fa4014eb4bbeac16c89ec7944baf8f/CONTRIBUTING.md) and disclose how AI was used.
