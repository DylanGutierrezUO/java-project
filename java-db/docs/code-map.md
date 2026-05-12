# Code Map

This document gives a high-level explanation of the major files in the project and how they interact.

## Application Entry and Startup

### `JavaDbApplication.java`
The main Spring Boot entry point.

Its job is to start the web application and initialize the Spring context.

### `AppStartupInitializer.java`
Runs once during application startup.

Its job is to:

1. ensure the processed dataset exists
2. rebuild the dataset from `data.csv` if needed
3. warm the backend cache before the app begins serving requests

This file is important for deployment because it guarantees the app never starts without data.

## Web/API Layer

### `GradesDistributionController.java`
The REST controller for the dashboard API.

It exposes endpoints used by the frontend, including:

- grouped chart/distribution data
- dropdown option data

It is the bridge between the browser and the backend service layer.

### `CoursesDataService.java`
The main backend service used by the live dashboard.

Its responsibilities include:

- reading and decoding stored course rows
- caching those rows in memory
- deriving department values from course IDs
- filtering rows by current department/course selection
- grouping rows by department, course, or instructor
- computing the selected metric
- enforcing record thresholds
- returning frontend-friendly response objects

This file contains most of the live dashboard’s data-shaping logic.

## Storage Engine Layer

### `Database.java`
Represents the database as a whole.

It manages:

- opening the data directory
- loading or discovering tables from disk
- recovering table metadata
- saving catalog information on close

It is responsible for database-level recovery and table management.

### `Table.java`
Represents a single table in the storage engine.

It is one of the most important classes in the system.

It manages:

- row insertion
- row updates through tail records
- row deletion
- page directory tracking
- index rebuilding
- page recovery from disk
- metadata persistence

In this project, the main table used by the live application is `Courses`.

### `Query.java`
Provides a query interface over a table.

It supports operations such as:

- insert
- update
- delete
- select
- versioned select
- range-based sum

It is the main read/write interface used when interacting with table data.

### `Index.java`
Provides indexing support for columns, especially the primary key.

It helps locate matching records more efficiently and supports range queries.

### `Record.java`
Represents a logical record returned by queries.

It stores:

- RID
- key
- projected column values

It is a lightweight container used by query results.

## Page and Buffer Management

### `Page.java`
Represents a single physical page of integer values.

It supports:

- reading values by slot
- writing values by slot
- appending values
- JSON serialization/deserialization for persistence

### `PageID.java`
Represents the identity of a physical page.

It encodes:

- table name
- column index
- page number
- whether the page is base or tail

### `PageBuffer.java`
Acts as the buffer/cache for pages.

It manages:

- reading and writing pages through cached frames
- marking frames dirty
- flushing pages back to disk
- basic LRU-style eviction behavior

This is part of the storage engine’s memory-management layer.

## Configuration

### `Config.java`
Holds configuration constants for the storage engine.

Examples include:

- data directory name
- page size
- buffer pool size
- metadata column layout
- RID ranges
- file naming conventions

This file centralizes the core tuning values and structural assumptions used by the engine.

## Dataset Bootstrap

### `ImportRunner.java`
A one-time command-line runner for importing `src/main/resources/data.csv` into the database.

It is useful for manual dataset creation or forced re-imports.

The deployed application does not rely on this file directly for web requests, but it does rely on the same bootstrap path during startup.

## Transaction/Concurrency Support

### `Transaction.java`
Groups a set of operations into a transaction and provides commit/abort behavior.

### `LockManager.java`
Provides shared/exclusive locking with a no-wait policy.

### `TransactionWorker.java`
Runs batches of transaction tasks with retries and backoff.

These files are part of the broader database project, even though the deployed dashboard is currently read-only and does not actively use transactional writes in the live UI.

## Frontend Files

### `src/main/resources/static/index.html`
The main dashboard page structure.

It defines:

- the layout
- controls
- summary cards
- chart container
- results table

### `src/main/resources/static/js/app.js`
The main frontend behavior file.

It is responsible for:

- fetching dropdown options from the backend
- fetching grouped distribution data
- tracking current dashboard state
- updating the chart and results
- implementing the hierarchical department → course → instructor interaction model

### `src/main/resources/static/css/styles.css`
The main stylesheet for the dashboard.

It defines:

- the visual theme
- layout rules
- chart presentation
- responsive behavior
- summary card styling
- typography and spacing

## Data File

### `src/main/resources/data.csv`
The raw input dataset used to build the on-disk `Courses` table.

This is the source data used when the application rebuilds the processed database at startup.

## Legacy/Archived Utilities

Some earlier runner files were used during development to test JSON export and grouped aggregation behavior before the live Spring Boot dashboard replaced them.

These were useful while proving the database and API concepts, but they are not part of the current deployed request path.

## How the Main Pieces Fit Together

At a high level, the application works like this:

1. `JavaDbApplication` starts Spring Boot
2. `AppStartupInitializer` ensures the dataset exists and warms the cache
3. the browser loads `index.html`, `app.js`, and `styles.css`
4. the frontend calls `GradesDistributionController`
5. `GradesDistributionController` delegates to `CoursesDataService`
6. `CoursesDataService` reads cached data shaped from the custom storage engine
7. the frontend renders the returned results