# Running daily

The job is idempotent: re-running it never re-downloads unchanged files, so a
daily trigger (or an extra manual run) is safe. Exit code is 0 on success and 1
if any dataset failed (failed ones are retried automatically on the next run).

Install once (see the main README's Quick start), so the scheduler only has to call
the venv's python. Replace `/path/to/` and `C:\path\to\` below with where the project lives.

## Linux / macOS (cron)
```cron
# every day at 02:00
0 2 * * * cd /path/to/cms-hospital-pipeline && mkdir -p logs && .venv/bin/python -m cms_pipeline >> logs/cron.log 2>&1
```

## Windows (Task Scheduler)
Run from PowerShell. `cmd /c` is there so the output can be appended to a log file,
which Task Scheduler cannot do by itself.
```powershell
$root    = "C:\path\to\cms-hospital-pipeline"
New-Item -ItemType Directory -Force "$root\logs" | Out-Null
$action  = New-ScheduledTaskAction -Execute "cmd.exe" -WorkingDirectory $root `
           -Argument '/c .venv\Scripts\python.exe -m cms_pipeline >> logs\task.log 2>&1'
$trigger = New-ScheduledTaskTrigger -Daily -At 2am
Register-ScheduledTask -TaskName "CmsHospitalPipeline" -Action $action -Trigger $trigger
```
The equivalent one-liner without PowerShell:
```bat
schtasks /Create /SC DAILY /ST 02:00 /TN CmsHospitalPipeline /TR "cmd /c cd /d C:\path\to\cms-hospital-pipeline && (if not exist logs mkdir logs) && .venv\Scripts\python.exe -m cms_pipeline >> logs\task.log 2>&1"
```

If a previous run is still going, the new one exits with code 3 without touching
anything. The lock is an OS file lock on `state/run.lock`, released by the OS when
the holding process exits, so a crashed or killed run never blocks the next one.

State lives in `state/manifest.json` (per-file last-seen `modified`) and
`state/runs.jsonl` (one line per run: start/finish time, counts, failures).
