# Uroboros

Uroboros is the backend orchestrator, scheduler, and HTTP API listener for the BellBeast monitoring stack. It runs as a local .NET service on port `8888`, polls plant-facing data sources, imports laboratory data, synchronizes selected files through Google Drive, persists runtime data to SQLite, and exposes operational endpoints consumed by BellBeast and related tools.

## System Role

Uroboros is the backend layer in the current three-part system:

- `Uroboros` = backend orchestrator / scheduler / API listener
- `BellBeast` = frontend dashboard / operator interface
- `Wayfarer` = Playwright automation worker for WebPM2

In the current architecture, BellBeast calls Uroboros over HTTP for plant dashboards and summary views. Uroboros owns task scheduling, data acquisition, local persistence, and admin control endpoints.

## Purpose

Uroboros centralizes backend work that should not live in the frontend:

- poll plant systems on fixed intervals
- normalize and persist current values into SQLite
- serve HTTP APIs for dashboard modules
- maintain runtime task settings and health state
- import daily laboratory data into local storage
- upload and download selected runtime databases through Google Drive
- expose admin controls for pause, resume, force-run, task config, and cancellation

## Architecture Summary

The application is a single .NET console executable with three long-running runtime loops:

1. `Scheduler`
   Accepts `IEngineTask` jobs, enforces concurrency, timeout, and run-policy rules, and tracks running tasks.

2. `WebListener`
   Hosts an `HttpListener` on `http://+:8888/` and exposes health, admin, and API endpoints.

3. `TriggerLoop`
   Computes the next due time for each registered task, reads live task configuration from SQLite, and enqueues work on schedule while preserving phase across pauses and config changes.

Core execution flow:

1. `Program.Main()` builds runtime services and task registry.
2. Task defaults are loaded into `engine_admin.db`.
3. `Scheduler.RunLoopAsync()` starts with `maxConcurrency: 20`.
4. `WebListener.RunAsync()` starts the HTTP interface on port `8888`.
5. `TriggerLoop.RunAsync()` continuously evaluates due jobs and enqueues enabled tasks.
6. Task handlers fetch plant data, write SQLite state, and APIs read that state back for BellBeast.

## Repository Layout

Top level:

- `Uroboros.slnx` - solution container
- `README.md` - repository documentation
- `Uroboros/` - application source

Main source files:

- `Uroboros/Program.cs` - entry point, scheduler, HTTP listener, task registration, and many task implementations
- `Uroboros/TriggerLoopcs.cs` - periodic trigger loop with phase-preserving scheduling
- `Uroboros/AdminConfig.cs` - runtime task settings store and config service
- `Uroboros/TaskHealth.cs` - in-memory task success/failure health tracking
- `Uroboros/TpsHandlers.cs` - `/api/tps/summary`
- `Uroboros/DpsHandlers.cs` - `/api/dps/summary`
- `Uroboros/RwsHandlers.cs` - `/api/rws/summary`
- `Uroboros/ChemHandlers.cs` - `/api/chem/summary`
- `Uroboros/EVENTHandlers.cs` - `/api/event/summary`
- `Uroboros/ClDetectorHandlers.cs` - `/api/cldetector/summary`
- `Uroboros/LabSummaryModule.cs` - `/api/lab/summary`
- `Uroboros/OnlineLabHandlers.cs` - `/api/online_lab`
- `Uroboros/Listener_AQ.cs` - Aquadat verify/process APIs
- `Uroboros/Listener_CHEM.cs` - chemistry report query/export endpoints
- `Uroboros/AquadatFast.cs` - Aquadat ingestion, mapping, export, and SQLite writing
- `Uroboros/AquadatRemarkHelper.cs` - Aquadat remark and GraphQL-related helpers
- `Uroboros/PTC.cs` - PTC fetch/parse and SQLite persistence helpers
- `Uroboros/SqliteUpperLowerProvider.cs` - PTC series reader from SQLite
- `Uroboros/DailyLabImporter.cs` - daily lab import pipeline
- `Uroboros/GoogleDrive.cs` - database upload/download helpers and Drive auth

## Runtime and Hosting

### Runtime type

- .NET console application
- plain `HttpListener` server, not ASP.NET Core

### SDK and target framework

- SDK pinned in `Uroboros/global.json`
  - `10.0.102`
- target framework in `Uroboros/Uroboros.csproj`
  - `net10.0`

### Listener port

Uroboros listens on:

- `http://+:8888/`

This matches the intended deployment model documented in code comments:

- BellBeast web UI on another port, typically `5082`
- Uroboros backend listener on `8888`

### Startup behavior

At startup:

- the stage gate is initialized as paused
- task defaults are seeded into `engine_admin.db`
- scheduler, web listener, and trigger loop are started
- tasks remain subject to gate and runtime config state

## Main Modules and Responsibilities

### Scheduler and task model

Defined primarily in `Program.cs`:

- `TaskSpec`
- `IEngineTask`
- `Scheduler`
- `StageGate`
- `TaskRegistry`

Capabilities:

- task priorities
- run policies:
  - `Queue`
  - `DropIfRunning`
  - `CoalesceIfRunning`
  - `SkipIfRunning`
- per-task timeout support
- cancellation of individual running tasks
- global cancellation of all running tasks
- task health tracking
- phase-preserving rescheduling

### Runtime task configuration

`AdminConfig.cs` stores task settings in:

- `engine_admin.db`

Table:

- `task_settings`

Fields:

- `name`
- `enabled`
- `interval_ms`
- `timeout_override_ms`
- `updated_at_unixms`

This configuration controls:

- whether a task is enabled
- runtime interval overrides
- timeout overrides
- force-run hints through `updated_at_unixms`

### HTTP API layer

`WebListener` in `Program.cs` routes requests to feature handlers or admin services.

It also:

- writes JSON responses
- applies permissive CORS headers
- catches top-level handler exceptions and returns `500`

### Data acquisition and subsystem handlers

Subsystem-specific polling and read APIs are split across dedicated files:

- TPS
- DPS
- RWS
- CHEM
- EVENT
- CL Detector
- OnlineLab
- LAB
- PTC
- Aquadat

### File sync and backup tasks

`GoogleDrive.cs` implements:

- Google Drive authentication
- upload/update by name in a target folder
- download by name from a target folder
- snapshot-based SQLite upload flow
- atomic database replace with retry

### Daily lab import

`DailyLabImporter.cs` handles:

- reading lab configuration from `config_`
- locating Excel source files from `config.ini`
- parsing mapped structures
- writing imported values into SQLite table `lab_import_daily`

## Registered Background Tasks

The current task registry in `Program.cs` includes:

- `tps.refresh`
- `dps.refresh`
- `rws1.refresh`
- `rws2.refresh`
- `chem1.refresh`
- `chem2.refresh`
- `branch.refresh`
- `rcv38.refresh`
- `ptc.query.once`
- `onlinelab.query`
- `Aquadat.refresh`
- `AquadatFWS.refresh`
- `DB_upload.refresh`
- `DB_download.refresh`
- `MDB_upload.refresh`
- `MDB_download.refresh`
- `LAB.import.daily`

Default intervals from the current catalog:

- most dashboard refresh tasks: `5000 ms`
- PTC: `30000 ms`
- Aquadat: `30000 ms`
- DB and MDB sync tasks: `30000 ms`
- lab import task is currently also seeded at `30000 ms` on `master`

The trigger loop enforces a hard minimum interval of:

- `250 ms`

Certain tasks also have higher minimums in `TaskConfigService.MinIntervalMsByTask`, including:

- `DB_upload.refresh`
- `MDB_upload.refresh`
- `Aquadat.refresh`
- `AquadatFWS.refresh`
- `ptc.query.once`

## API Endpoints

### Health and task inspection

- `GET /health`
- `GET /tasks`
- `GET /tasks/running`
- `POST /tasks/enqueue`
- `POST /tasks/cancel/{guid}`

### Admin control

- `GET /admin/tasks/config`
- `POST /admin/tasks/config`
- `GET /admin/tasks/status`
- `POST /admin/pause`
- `POST /admin/resume`
- `POST /admin/cancelall`
- `POST /admin/tasks/forcerun`

### Dashboard and subsystem APIs

- `POST /api/verify`
- `POST /api/process`
- `GET /api/lookup/products`
- `GET /api/lookup/companies`
- `POST /api/chem_report/export`
- `POST /api/chem_report`
- `GET /api/ptc/keys`
- `GET /api/ptc/series?key=...`
- `POST /api/online_lab`
- `GET /api/dps/summary`
- `GET /api/tps/summary`
- `GET /api/rws/summary`
- `GET /api/chem/summary`
- `GET /api/event/summary`
- `GET /api/cldetector/summary`
- `POST /api/lab/summary`

### Cross-system use

BellBeast consumes these APIs for live dashboard cards and detail views, especially:

- `/api/tps/summary`
- `/api/dps/summary`
- `/api/rws/summary`
- `/api/chem/summary`
- `/api/event/summary`
- `/api/cldetector/summary`
- `/api/lab/summary`
- `/api/online_lab`
- `/api/ptc/*`

## Configuration

### Present in source control

- `Uroboros/global.json` - SDK pinning
- `Uroboros/Uroboros.csproj` - package references and target framework

### Not present as ASP.NET-style app config

This repository does not currently rely on `appsettings.json` on `master`. Configuration is mostly implicit in code and external runtime files.

### Runtime file assumptions

The application expects several local files and directories relative to `AppContext.BaseDirectory` or sibling runtime paths, including:

- `data.db`
- `data_ghost.db`
- `data.db.bak`
- `engine_admin.db`
- `config_/aqtable.db`
- `config_/LAB_structure.json`
- `config_/lab_mapping.json`
- `config_/config.ini`
- `credentials.json`
- `token.json` directory used by `FileDataStore`

### Hardcoded external endpoints

The current `master` branch includes direct calls to plant and service URLs such as:

- internal `allch.cgi` sources for RWS, CHEM, and other plant systems
- Aquadat API endpoints at `aquadat.mwa.co.th`
- Aquadat GraphQL endpoint
- PTC realtime endpoints on internal IP addresses

These values are currently code-configured rather than environment-configured.

## Database and Storage

### Primary SQLite files

- `data.db`
  - primary local runtime database for current values and time-series data used by subsystem APIs

- `engine_admin.db`
  - runtime scheduler/task settings database

- `data_ghost.db`
  - snapshot/transfer name used for Google Drive upload and remote synchronization

- `data.db.bak`
  - local backup created during database replacement workflows

### Additional storage

- `config_/aqtable.db`
  - Aquadat metadata lookup source

- `lab_import_daily` table
  - populated by the daily lab import pipeline

### Access pattern

- writer tasks update SQLite state
- HTTP handlers open read-only SQLite connections to serve dashboard responses
- DB upload/download tasks snapshot and replace runtime databases with lock-aware flows

## External Integrations

### BellBeast

BellBeast acts as the operator-facing frontend and calls Uroboros over HTTP. Uroboros is the backend data source and control plane for BellBeast dashboard modules.

### Wayfarer

Wayfarer is not hosted inside this repository, but Uroboros is part of the same operational ecosystem. Uroboros currently focuses on backend polling, task orchestration, and local data APIs; Wayfarer is the automation worker for WebPM2.

### Google Drive

Used for:

- database upload
- database download
- synchronization/update workflows

Dependencies:

- `credentials.json`
- Drive OAuth token store in `token.json`

### Aquadat

Used for:

- data retrieval from the Aquadat service
- metadata mapping through `aqtable.db`
- remark-related workflows

### OnlineLab

Handled through:

- `/api/online_lab`

and supporting query tasks.

### Daily laboratory Excel imports

`DailyLabImporter.cs` depends on external lab Excel sources and JSON mapping files in `config_`.

### PTC

PTC integration reads remote realtime content and persists upper/lower series into SQLite for later API access.

## Logging and Error Handling

Logging is currently implemented through a simple console logger:

- `ConsoleLogger`

Behavior:

- task starts, success, cancellation, and failures are logged
- HTTP handler failures are caught and returned as generic `500` responses
- subsystem handlers usually log warning messages before returning structured error payloads

Current limitations:

- no structured log sink
- no log level routing
- no persistent log retention
- no correlation IDs

## Setup Instructions

### Prerequisites

- Windows environment is strongly implied
- .NET SDK `10.0.102`
- access to required network endpoints
- access to required local runtime files
- Google Drive credentials if sync tasks are used

### Clone

```powershell
git clone https://github.com/peerapatbps/Uroboros.git
cd Uroboros
```

### Restore

```powershell
dotnet restore .\Uroboros\Uroboros.csproj
```

### Build

```powershell
dotnet build .\Uroboros\Uroboros.csproj
```

### Run

```powershell
dotnet run --project .\Uroboros\Uroboros.csproj
```

Once running, the listener binds to:

- `http://localhost:8888/`

## Operational Workflow

Typical runtime workflow:

1. Start Uroboros.
2. Scheduler, listener, and trigger loop initialize.
3. Trigger loop reads `engine_admin.db` and schedules enabled tasks.
4. Polling tasks fetch external plant data and write SQLite state.
5. BellBeast requests summaries from Uroboros APIs on port `8888`.
6. Admin users can inspect and modify task behavior via `/admin/*`.
7. File sync tasks optionally upload/download database snapshots via Google Drive.
8. LAB import tasks update imported laboratory values for BellBeast LAB views.

## Build and Validation

This repository does not currently include a dedicated test project on `master`.

Recommended validation for documentation changes:

- `dotnet build .\Uroboros\Uroboros.csproj`
- optional manual HTTP smoke tests against port `8888`

## Deployment Assumptions

The current code assumes:

- Windows-compatible runtime
- local file system write access in the application base directory
- SQLite database files colocated with the executable
- plant network reachability to internal endpoints
- Google Drive credentials on disk for sync tasks
- long-running process model rather than ephemeral container execution

Operationally, this is closer to a plant-side service executable than a cloud-native stateless API.

## Known Limitations

- configuration is still heavily code-based and path-based
- external URLs and folder IDs are hardcoded in multiple modules
- no centralized configuration abstraction
- no built-in authentication or authorization on admin endpoints
- `HttpListener` is simpler than ASP.NET Core but less flexible for modern hosting scenarios
- no automated test suite is present on `master`
- runtime behavior depends on external files that are not fully documented by machine-readable config
- some task names and comments still reflect legacy naming patterns

## Future Development Notes

Recommended next improvements:

- move external URLs, folder IDs, and file locations into explicit configuration
- add startup validation for required runtime files and directories
- add authenticated admin endpoints before wider deployment
- extract task implementations from `Program.cs` into dedicated files
- add integration smoke tests for critical APIs
- document SQLite schema and runtime file contracts in more detail
- standardize naming conventions across tasks, APIs, and frontend callers
- consider a gradual migration from `HttpListener` to ASP.NET Core if hosting requirements grow

## Production Audit Notes

For architecture review, LL documentation, OPA reporting, and technical audit, the key points are:

- Uroboros is the operational backend and scheduler, not just a simple API
- port `8888` is a critical interface for BellBeast
- runtime state is split between in-memory health/scheduler state and local SQLite files
- plant polling, lab import, and cloud sync all converge in this single process
- failure in Uroboros can impact multiple frontend dashboards and backend data refresh workflows at once
