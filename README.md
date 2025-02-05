# 🏎️  F1 Insights: Real-Time Replay & Historical Analytics 🏁

## Project Overview

A real-time Formula 1 telemetry system and historical data analysis system that captures, processes, and visualizes car data during race sessions, enabling both live replay and historical analysis.

The project involves two main components:

- **Real-Time Race Replay** - Replay and analyze race events in real-time, diving deep into how and why outcomes occurred
- **Historical Analysis** - Post-race analysis of weather impacts, tire choices, and race/pit strategies

### Real-Time Race Replay

![Realtime](https://s7.gifyu.com/images/SJl9L.gif) 

The system reconstructs race sessions in real-time, providing insights through:

- Driver performance metrics (speed, throttle, brake data)
- DRS usage analysis
- Gear selection patterns
- Position changes and overtaking maneuvers

### Historical Analysis & Strategic Insights

The system enables comprehensive post-race analysis focusing on:

- Weather impact on tire degradation and pit timing
- Pit strategy effectiveness across conditions
- Tire compound performance analysis

## Architecture & Technology Stack

![High Level Architecture](./images/High_level_architecture_diagram_image_1.jpeg)

### Real-time Pipeline

- **Confluent Kafka** on AWS EC2 - Handles high-throughput telemetry data streams
- **SingleStore** - Operational database for real-time ingestion and analytics
- **AWS EC2** (t3.micro) - Hosts Flask API for data processing

![Streaming](./images/realtime_streaming_pipeline_image_1.jpeg)

### Historical Analysis Pipeline

- **DBT** - Data transformation and modeling
- **Snowflake** - Data warehousing
- **Apache Airflow** - ETL orchestration with WAP pattern
- **Grafana** - Interactive dashboards

## Data Model

```mermaid
erDiagram
    MEETINGS ||--o{ SESSIONS : contains
    MEETINGS ||--o{ DRIVERS : participates-in
    MEETINGS ||--o{ RACE_CONTROL : records
    MEETINGS ||--o{ WEATHER : tracks
    
    SESSIONS ||--o{ LAPS : includes
    SESSIONS ||--o{ CAR_DATA : records
    SESSIONS ||--o{ INTERVALS : tracks
    SESSIONS ||--o{ POSITION : monitors
    SESSIONS ||--o{ PIT : records
    SESSIONS ||--o{ STINTS : tracks
    SESSIONS ||--o{ TEAM_RADIO : captures
    SESSIONS ||--o{ LOCATION : tracks
```

## Key Features

### Data Ingestion

![Ingestion](./images/Airflow_image.jpeg)

- **Idempotent Processing** - Ensures data consistency through merge keys
- **Resilient API Handling** - Retries and fallbacks for reliable data collection
- **Efficient Resource Usage** - Compute optimization for non-race days
- **Safe Data Loading** - Staging tables for atomic updates

### Data Transformation

![Transformations](./images/data_transformations.jpeg)

- **Staging Models** - Initial data cleaning and type conversion
- **Intermediate Models** - Complex calculations and aggregations
- **Mart Models** - Business-ready views for dashboards

### Data Validation

![Validation](./images/validation_1.jpeg)

Comprehensive testing suite including:
- Range validations
- Relationship checks
- Custom domain-specific tests

## Dashboards

### LAP Analysis
![LAP Analysis](https://s7.gifyu.com/images/SJdyo.gif)

### Telemetry Analysis
![Telemetry](https://s7.gifyu.com/images/SJdJ4.gif)

### Tire Strategy Analysis
![Tire Strategy](https://s7.gifyu.com/images/SJdGt.gif)

## Technical Decisions

### Grafana Setup
- EC2 deployment for 1s refresh rates (vs. 5s cloud limitation)
- Dual setup: EC2 for real-time, Cloud for historical analysis

### Database Choices
- SingleStore for real-time operations (ms latency, free tier)
- Snowflake for historical analysis (scalable compute/storage)
