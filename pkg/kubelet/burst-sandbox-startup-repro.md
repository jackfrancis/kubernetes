# Sanitized original burst-startup repro

[Download the archive](./burst-sandbox-startup-repro.tar.gz).

This is a pseudonymized derivative of the complete supplied original repro bundle,
not a new reproduction or the later retry/read-reduction experiments. All **2,876
supplied files** are retained: the original incident write-up, the unchanged Python
collector, and 2,874 JSON evidence files. Two metadata files were added:
`SANITIZATION.json` and `SHA256SUMS`.

The archive is **1,181,839 bytes** and expands into a single
`burst-sandbox-startup-repro/` directory. Its SHA-256 is:

```text
c85de277fce5395b345a00ed1e3e538304d90a2384bd99722c170d36f638c503
```

See the [investigation report](./burst-sandbox-startup-findings.md) and
[aggregate evidence digest](./burst-sandbox-startup-evidence.json) for the later
analysis and experiments.

## What was sanitized

- Tenant/subscription and object UUIDs, incident-specific infrastructure/resource names, node and
  API-server identities, runtime identifiers, provisioner identity and the
  captured kubeconfig context use consistent synthetic aliases.
- Non-loopback IPv4 addresses use documentation ranges. Zero/loopback addresses
  remain unchanged.
- Identifiers are replaced consistently in JSON values, embedded messages,
  object keys, source references and the incident write-up. The private
  original-to-alias dictionary is **not included**.
- Archive ownership names, user/group IDs, filesystem timestamps and extended
  metadata are removed. Only ordinary files and directories are included.

Generic ownership labels, workload indices, node-identity principal prefixes,
public image digests, versions, driver/runtime names, region, machine sizes and
storage modes are retained. The collector is **byte-identical to the original**;
its ownership and cleanup behavior was not edited.

**This is pseudonymization, not a guarantee of anonymity.** Exact dates,
measurements and technical configuration remain useful for correlation. Review
applicable data-sharing requirements before external publication.

## Fidelity and limits

- All **28,561 numeric JSON values** are unchanged.
- All **30,207 detected timestamp occurrences** in the supplied files are
  unchanged.
- All **1,386 Pod/binding/Ready correlations** and **2,772 sandbox source-excerpt
  joins** were checked after sanitization and again from the packaged archive.
- Five startup relationship denials and 129 rolling cleanup relationship denials
  remain distinct.
- In the five 100-Pod runc cohorts, 428 collector-observed creation-to-Ready
  intervals exceed 17s, while no recorded journal sandbox interval does.
- The supplied bundle contains **selected journal excerpts**, not complete
  original journal captures. Source filename/line references can therefore point
  into captures that are not included.
- Full historical GET audit records for the original application cases were not
  retained in the supplied bundle. Sanitization does not fill that evidence gap.
- IP equality is preserved, but aliases **do not preserve routable addresses or
  CIDR topology**. Original prefix-length strings remain. Do not infer subnet
  membership from the alias addresses or reuse them as network configuration.

Automated checks found no remaining values from the sensitive-identifier inventory
or embedded credential/contact signatures. The original private bundle was left unchanged.
No cloud command, cluster workload or collector execution was performed for this
export.

## Inspect and verify

From the repository root, extract into a fresh temporary directory:

```bash
archive=pkg/kubelet/burst-sandbox-startup-repro.tar.gz
shasum -a 256 "$archive"
unpack=$(mktemp -d)
tar -xzf "$archive" -C "$unpack"
cd "$unpack/burst-sandbox-startup-repro"
shasum -a 256 -c SHA256SUMS
```

On systems that provide `sha256sum` instead, use `sha256sum -c SHA256SUMS`.
The manifest covers all 2,876 retained files plus `SANITIZATION.json`.
The archive checksum covers the manifest itself.

Useful entry points after extraction:

- `aks-pv-pvc-node-authorizer.md`: supplied incident narrative and historical
  reproduction procedure.
- `native_repro.py`: original collector/reproducer, unchanged.
- `evidence/original-startup-cases.json`: original application cases, kept
  separate from the standalone reproduction.
- `evidence/burst-20261113T1326/burst-qualification.json`: burst classification
  and audit correlation.
- Each cohort's `native-summary.json`, `native-source-excerpts.json`, and
  per-Pod binding/Ready snapshots.
- `SANITIZATION.json`: transformation scope, limitations, validation counts and
  cohort statistics.

The historical `evidence/validation.json` receipt retains its original
validation claims, but its attached-file checksum table was recomputed and
explicitly annotated for this derivative. The aggregate digest alongside this
guide records **original input hashes**; changed files in this archive will not
match those hashes. Use this archive's `SHA256SUMS` for derivative integrity.

## Do not execute historical commands blindly

The retained procedure includes privileged diagnostics and cloud
provisioning/deletion commands. Its aliases are not real cloud configuration.
Reading or extracting this archive requires no cluster access. Before running a
reproduction, configure and authorize a disposable, isolated environment and
review the commands. Do not apply the historical snapshots to an existing
cluster.
