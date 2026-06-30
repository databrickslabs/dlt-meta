# Deploying the SDP-META App from Windows

`scripts/deploy_app.sh` does not run on Windows `cmd.exe` or PowerShell — it
uses POSIX constructs and `rsync`. The repo ships a PowerShell port,
`scripts/deploy_app.ps1`, which performs the same staging + sync + deploy flow
using only Windows-built-in tooling (`robocopy`) and the `databricks` CLI.

For context (what the script does on top of `databricks sync` + `databricks
apps deploy`, how `start.sh` boots inside the container, and the directory
layout it produces), see the **Deploy with the deploy script** section of
[README.md](./README.md#3-deploy-with-the-deploy-script-recommended). This
file covers only the Windows-specific bits.

---

## Quick start

```powershell
# Run from the sdp-meta repo root
cd C:\path\to\sdp-meta

.\scripts\deploy_app.ps1 -DatabricksProfile <DATABRICKS_CLI_PROFILE> `
                         -App <YOUR_APP_NAME> `
                         -Path /Workspace/Users/email/<workspace-folder>
```

`Get-Help .\scripts\deploy_app.ps1 -Full` prints the full argument reference.

Same flow with env vars:

```powershell
$env:DATABRICKS_PROFILE = '<DATABRICKS_CLI_PROFILE>'
$env:WORKSPACE_PATH     = '/Workspace/Users/email/<workspace-folder>'
.\scripts\deploy_app.ps1
```

---

## Requirements & reference

| Aspect | Notes |
|---|---|
| OS / shell | Windows 10/11, PowerShell 5.1+ (default) or 7+. No Git Bash / WSL / Python install needed. |
| External tools | `databricks` CLI on `PATH`. `robocopy` ships with Windows. |
| Profile flag | `-DatabricksProfile` is canonical (named to avoid colliding with PowerShell's built-in `$PROFILE` automatic variable). `-Profile` is accepted as an alias for parity with the bash script. |
| Env-var fallback | `$env:DATABRICKS_PROFILE`, `$env:WORKSPACE_PATH`, `$env:APP_NAME`, `$env:DEPLOY_MODE`. Legacy `$env:PROFILE` is also accepted for parity with the bash script. **Do not** set `$PROFILE = ...` — that assigns the local PS automatic variable, not an env var. |
| Execution policy | If Windows blocks the script on first run: `powershell -ExecutionPolicy Bypass -File .\scripts\deploy_app.ps1 ...` |
| Line endings | The script normalizes CRLF → LF on every staged text file before sync. See [Troubleshooting](#troubleshooting) below if the app still crashes on first deploy. |

---

## Troubleshooting

### App crashes on first deploy with `bad interpreter: /bin/bash\r` or `\r: command not found`

Windows git's default `core.autocrlf=true` checks out every text file with
CRLF line endings. The Linux App container then can't execute
`databricks_app/start.sh` because the shebang line is `#!/bin/bash\r`.

`scripts/deploy_app.ps1` strips `\r` bytes from every staged text file before
sync, so a *normal* `.\scripts\deploy_app.ps1` run should never hit this. You
*can* hit it if:

- You run `databricks sync` manually instead of using the script.
- You cloned the repo before pulling the `.gitattributes` that pins LF for
  shell scripts, and your local working tree still has CRLF copies that some
  other tool (your editor, an IDE save) re-committed.

One-time fix using the repo's `.gitattributes` policy:

```powershell
git add --renormalize .
git status                  # review the file list git wants to re-encode
git commit -m "Normalize line endings to LF per .gitattributes"
```

After that commit, every file in your working tree is LF and stays LF on
future pulls.

### `Execution of scripts is disabled on this system`

Windows PowerShell's default execution policy blocks unsigned scripts.
Bypass for a single invocation without changing the system-wide policy:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\deploy_app.ps1 `
    -DatabricksProfile <profile> -App <app-name> -Path <ws-path>
```

Or, for the current PowerShell session only:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

### `robocopy` returns a non-zero exit code but the script reports success

`robocopy` uses exit codes 0–7 to signal *informational* outcomes (files
copied, files mismatched, etc.) and 8+ for real failures. The deploy script
checks `$LASTEXITCODE -ge 8` deliberately. If you see a non-zero exit code in
the log but no `[FAIL]` line, that's expected.

### "Could not find a deployed app URL in the CLI output"

The CLI succeeded but the script couldn't parse the app URL from its output.
Check the URL manually in the Databricks workspace: **Compute → Apps →
select `<your-app-name>`**. This usually means the CLI output format changed —
file an issue with the raw output of `databricks apps deploy --help` and the
relevant log lines.
