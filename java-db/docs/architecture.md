# Architecture Overview

## Purpose

This project is a full-stack academic data visualization system built to demonstrate:

- custom database/storage logic in Java
- backend API development with Spring Boot
- frontend interaction and visualization with vanilla JavaScript
- end-to-end integration between a Java backend and a JavaScript frontend

The deployed application allows users to explore University of Oregon grade distribution data from 2013–2018 by drilling down through:

1. departments
2. courses within a department
3. instructors for a selected course

Users can compare:

- A Percentage
- D/F Percentage

## High-Level Architecture

The project is organized into four main layers:

### 1. Data bootstrap layer
This layer ensures the dataset exists before the application serves requests.

- The raw CSV lives in `src/main/resources/data.csv`
- On startup, the app checks whether the processed on-disk database already exists
- If the processed data is missing, the app rebuilds it from the CSV
- This allows the deployed application to work without committing the generated `data/` directory

### 2. Storage engine layer
This is the custom Java database portion of the project.

It is responsible for:

- storing rows in pages
- managing base and tail records
- tracking record locations through a page directory
- supporting inserts, updates, deletes, selects, and aggregation-related reads
- recovering stored table data from disk

This layer is the core backend systems component of the project.

### 3. API/service layer
This layer exposes the stored data to the frontend in a format that is useful for visualization.

It is responsible for:

- loading and caching course rows from the database
- deriving departments from course IDs
- grouping results by department, course, or instructor
- applying record thresholds to avoid misleading small-sample results
- returning JSON responses for chart rendering and dropdown population

### 4. Frontend layer
This is the browser-based dashboard served by Spring Boot.

It is responsible for:

- rendering the UI
- requesting available departments/courses from the backend
- requesting grouped distribution data
- updating the bar chart and summary cards
- handling the hierarchical drill-down behavior

## Request Flow

The live application follows this general flow:

1. The browser loads the dashboard page
2. The frontend requests available dropdown options from the backend
3. The user selects a department, course, and metric
4. The frontend sends a request to the backend
5. The backend reads cached course data
6. The backend groups and filters the data based on the current selection
7. The backend returns a JSON response
8. The frontend renders the chart, summary, and results table

## Startup Flow

The startup process is intentionally ordered so the application never runs without data.

### Step 1: application startup
Spring Boot launches the application entry point.

### Step 2: dataset initialization
A startup initializer checks whether the processed `Courses` data already exists.

- If it exists, it is reused
- If it does not exist, it is rebuilt from `data.csv`

### Step 3: cache warmup
After the dataset exists on disk, the backend service loads and caches the rows needed by the API layer.

This improves dashboard responsiveness after startup.

### Step 4: request serving
Once initialization is complete, the app begins serving frontend and API requests.

## Frontend Interaction Model

The dashboard uses a guided hierarchy instead of generic filter boxes.

### Department = All
The graph compares departments.

### Department selected, Course = All
The graph compares courses within the selected department.

### Course selected
The graph compares instructors for that specific course.

This structure was chosen to make the UI more intuitive and to avoid confusing overlap between grouping and filtering concepts.

## Metric Logic

The app currently supports two user-facing metrics:

### A Percentage
Shows the average A percentage for each group.

### D/F Percentage
Shows the combined average of D and F percentages for each group.

This was chosen to better reflect how a user might interpret course difficulty, since D grades can matter nearly as much as F grades in many academic contexts.

## Record Threshold Logic

To avoid misleading results, the application uses minimum record thresholds before ranking groups.

Examples:

- departments can appear with minimal thresholding
- courses require more than one matching record
- instructors in broad views require a higher threshold
- instructors for a specific selected course are allowed with a lower threshold so the chart does not go empty unnecessarily

This helps the dashboard avoid overemphasizing tiny sample sizes.

## Deployment Model

The application is deployed as a Spring Boot service.

The generated on-disk database is not committed to the repository. Instead:

- the raw CSV is committed
- the application rebuilds the processed dataset at startup if needed
- the backend then warms its in-memory cache

This deployment model keeps the repository cleaner while still ensuring the app always has data available.

## Why This Design Was Chosen

This design was chosen because it demonstrates both systems-level and full-stack skills in one project.

It shows:

- custom storage engine design
- data recovery and bootstrap behavior
- backend API design
- frontend interaction design
- live deployment of a Java web application