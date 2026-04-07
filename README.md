# Uroboros

Uroboros is a .NET backend engine and HTTP listener project for industrial data collection, scheduling, processing, and local SQLite-based storage. It is designed to run as a backend service that coordinates polling tasks, HTTP endpoints, task control, and local data persistence for monitoring and operations support workflows.

## Overview

This project provides a backend runtime for process and utility systems. The application focuses on:

- Scheduled task execution and concurrency control
- HTTP listener endpoints for health checks, task inspection, and task control
- Integration with plant-side data sources and internal handlers
- Local SQLite-based persistence for current values, series data, and task-related storage
- Support for polling, ingest, transformation, and backend orchestration workflows

## Main Features

- .NET backend service architecture
- Built-in scheduler with task policies, timeout handling, and cancellation support
- HTTP listener for backend control and status endpoints
- SQLite-based local storage and data persistence
- Support for multiple plant and subsystem refresh tasks
- Suitable for internal data collection, processing, and operations support use cases

## Project Structure

    Uroboros/
    ├─ Uroboros/                 # Main backend source files
    ├─ Program.cs                # Scheduler, HTTP listener, routing, and task registration
    ├─ Uroboros.csproj           # Project file
    ├─ Uroboros.slnx             # Solution file
    └─ README.md

Typical source files inside the project include handlers and modules such as:

    Uroboros/
    ├─ AdminConfig.cs
    ├─ AquadatFast.cs
    ├─ AquadatRemarkHelper.cs
    ├─ ChemHandlers.cs
    ├─ DpsHandlers.cs
    ├─ EVENTHandlers.cs
    ├─ GoogleDrive.cs
    ├─ HtmlHookModule.cs
    ├─ Listener_AQ.cs
    ├─ Listener_CHEM.cs
    ├─ MdbReaderSafe.cs
    ├─ OnlineLabHandlers.cs
    ├─ PTC.cs
    ├─ RwsHandlers.cs
    ├─ SqliteUpperLowerProvider.cs
    ├─ TaskHealth.cs
    ├─ TpsHandlers.cs
    └─ TriggerLoopcs.cs

## Technology Stack

- .NET / C#
- HTTP Listener
- SQLite
- Microsoft.Data.Sqlite
- System.Data.OleDb
- Google Drive API
- Newtonsoft.Json

## NuGet Packages

This project currently uses the following main packages:

### Top-level packages

- **Google.Apis.Drive.v3** (`1.73.0.4045`)  
  Google Drive client library used for Drive integration and related backend workflows.

- **Microsoft.Data.Sqlite** (`10.0.2`)  
  Lightweight ADO.NET provider for SQLite, used for local database access and persistence.

- **System.Data.OleDb** (`11.0.0-preview.2.26159.112`)  
  Provides OLE DB data access support for .NET applications.

### Transitive packages

These packages are installed automatically as dependencies of the main packages:

- **Google.Apis** (`1.73.0`)  
  Core runtime library for Google service requests and API access.

- **Google.Apis.Auth** (`1.73.0`)  
  Authentication support for Google API access.

- **Google.Apis.Core** (`1.73.0`)  
  Core infrastructure for Google API HTTP and service operations.

- **Microsoft.Data.Sqlite.Core** (`10.0.2`)  
  Core SQLite provider library used internally by `Microsoft.Data.Sqlite`.

- **Newtonsoft.Json** (`13.0.4`)  
  JSON serialization and deserialization library used in the backend.

- **SQLitePCLRaw.bundle_e_sqlite3** (`2.1.11`)  
  Bundles native SQLite components needed for common SQLite usage scenarios.

- **SQLitePCLRaw.core** (`2.1.11`)  
  Core low-level API for SQLite interop.

- **SQLitePCLRaw.lib.e_sqlite3** (`2.1.11`)  
  Native SQLite library wrapper used by SQLitePCLRaw.

- **SQLitePCLRaw.provider.e_sqlite3** (`2.1.11`)  
  Provider implementation for loading and using the bundled SQLite native library.

- **System.CodeDom** (`7.0.0`)  
  Provides types for representing and generating source code structures.

- **System.Configuration.ConfigurationManager** (`11.0.0-preview.1.26104.118`)  
  Supports legacy-style configuration access patterns in .NET applications.

- **System.Diagnostics.EventLog** (`11.0.0-preview.1.26104.118`)  
  Provides access to Windows Event Log APIs.

- **System.Diagnostics.PerformanceCounter** (`11.0.0-preview.1.26104.118`)  
  Provides access to Windows performance counters.

- **System.Management** (`7.0.2`)  
  Provides access to Windows Management Instrumentation (WMI) functionality.

- **System.Security.Cryptography.ProtectedData** (`11.0.0-preview.1.26104.118`)  
  Provides access to Windows Data Protection API (DPAPI).

## Getting Started

### 1. Clone the repository

    git clone https://github.com/peerapatbps/Uroboros.git
    cd Uroboros

### 2. Restore dependencies

    dotnet restore

### 3. Run the project

    dotnet run --project Uroboros/Uroboros.csproj

Or open the solution in Visual Studio and run it from there.

## Development Notes

- The backend includes a scheduler model with task priority, run policy, timeout handling, and cancellation support.
- The built-in HTTP listener exposes backend control and status routes such as `/health`, `/tasks`, `/tasks/running`, `/admin/tasks/status`, `/admin/pause`, `/admin/resume`, and `/admin/tasks/forcerun`.
- The project contains multiple refresh and query tasks for subsystems such as RWS, CHEM, TPS, DPS, Branch, RCV, PTC, OnlineLab, and Aquadat.
- Local runtime artifacts such as `.vs/`, `bin/`, and `obj/` should not be committed to Git.
- Local tokens, credentials, database files, and machine-specific runtime data should be excluded from source control.

## Recommended .gitignore

    .vs/
    bin/
    obj/
    *.user
    *.rsuser
    *.suo
    *.db
    *.db-shm
    *.db-wal
    *.bak
    *.log
    appsettings.Development.json
    appsettings.Local.json
    .env
    .env.*
    token.json
    credentials.json
    *.tmp
    *.zip
    Properties/PublishProfiles/*.user
    Properties/PublishProfiles/*.pubxml.user

## Current Status

This repository is under active development. Features, handlers, task coverage, and runtime configuration may continue to evolve as the backend grows.

## Author

Developed by Peerapat S.

## License

This project is currently intended for private or internal use unless otherwise specified.