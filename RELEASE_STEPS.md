# SDP-META v0.1.0 — High-Level Release Steps

Exact order of operations. Release plan: [#312](https://github.com/databrickslabs/dlt-meta/issues/312).

## Before release day (gates)

| # | Step | Owner |
|---|------|-------|
| 1 | Backward-compat test (`run_backward_compat_tests.py`) green, both phases | Eng |
| 2 | `release` GitHub environment: required reviewers, deployment restricted to `main` / `v*` tags, and "Allow administrators to bypass" UNCHECKED — currently NONE of this is configured, and steps 5–7 depend on it | Repo admin |
| 3 | Actor allowlist in `release.yml` lists all release DRIs | Eng |
| 4 | SBOM scanner App scoped to this repo; `databricks/gh-action-scan` resolves | DRI + IT |

## Release day

**1. PR `v0.1.0` branch → `main`, merge.**
Refresh the branch first — it must contain the hardened `release.yml`, real
`requirements-build.txt` hashes, and the 0.1.0 versions in both `setup.py` files.
Merging to `main` auto-triggers the `gh_pages` workflow (docs are already built
with `baseUrl: /sdp-meta/`).

**2. Rename the GitHub repo: `dlt-meta` → `sdp-meta`.**
Settings → General. Do this immediately after merge — the docs site and PyPI
publishers depend on the new name. Clone URLs redirect; OIDC and Pages URLs do not.

**3. GitHub Pages.**
The rename moves the site to `https://databrickslabs.github.io/sdp-meta/`
automatically. If the post-merge `gh_pages` run finished before the rename,
re-run it once (Actions → gh_pages → Run workflow) and spot-check the site.

**4. Update PyPI Trusted Publishers (both projects → repo `sdp-meta`).**
On `dlt-meta` (existing project): delete old entry, add
`databrickslabs / sdp-meta / release.yml / release`.
For `databricks-labs-sdp-meta` (new): add the same as a *pending publisher*
under the owning PyPI account.

**5. Dry run.**
Actions → release → Run workflow (`dry-run = true`): authorize + build + scan +
provenance must be green; publish stays skipped.

**6. Delete the `v0.1.0` branch (recommended, not strictly required):**
`git push origin :refs/heads/v0.1.0`
Why: step 7 creates a *tag* named `v0.1.0`. Git permits a branch and tag with
the same name, but short refs become ambiguous: `git push origin v0.1.0` errors
with "matches more than one", `git checkout v0.1.0` warns and picks the branch,
and GitHub `tree/v0.1.0` URLs stop being deterministic. Step 7 uses the
fully-qualified `refs/tags/v0.1.0`, which works either way — deleting the
branch (fully merged in step 1, nothing lost) just removes the foot-gun.

**7. Tag and publish.**
`git tag v0.1.0 && git push origin refs/tags/v0.1.0` → pipeline runs → approve
the `release` environment when it pauses → publishes `databricks-labs-sdp-meta`
then `dlt-meta`, cuts the GitHub Release, signs with Sigstore.

**8. Verify.**
`pip install databricks-labs-sdp-meta==0.1.0` · `pip install dlt-meta==0.1.0`
(pulls both) · `databricks labs install sdp-meta` · GitHub Release + `.sigstore`
bundles present · PyPI pages render (banner, links, deprecation notice).

## After release day

9. Update `docs.databricks.com/ldp/developer/dlt-meta` with new repo URL + package name.
10. Announcement (LinkedIn, Databricks community).
11. Open `v0.1.1` / `v0.2.0` milestones; carry-over issues per #312.
