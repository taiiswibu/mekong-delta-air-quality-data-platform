# Architecture

```mermaid
flowchart LR
    WAQI[WAQI REST API] --> KP[Python Kafka Producer]
    KP --> K[Apache Kafka / KRaft]
    K --> KC[Python Kafka Consumer]
    KC --> BAPI[Snowflake Bronze API_RAW]

    CSV[Historical CSV] --> AF[Apache Airflow]
    AF --> BHIST[Snowflake Bronze HISTORICAL_CSV]

    BAPI --> DBT[dbt transformations]
    BHIST --> DBT
    DBT --> SILVER[Silver Star Schema]
    SILVER --> GX[Great Expectations + SQL DQ Gate]
    GX -->|pass| GOLD[Gold Analytics Marts]
    GX -->|fail| STOP[Fail Airflow run]

    AF --> DBT
    AF --> GX
    AF --> OBS[CONTROL observability tables]
    GOLD --> ST[Streamlit Analytics]
    OBS --> ST
```

## Responsibility boundaries

- **Kafka**: streaming transport and consumer offsets; it does not orchestrate warehouse transformations.
- **Airflow**: workflow orchestration, retries, scheduling and failure visibility.
- **Snowflake**: cloud warehouse and Medallion storage layers.
- **dbt**: SQL transformation graph, dimensional modeling and schema tests.
- **Great Expectations**: reusable data-quality expectations; critical failures block Gold.
- **Streamlit**: portfolio analytics and operational monitoring UI.
- **Docker Compose**: reproducible local infrastructure for Kafka and Airflow.

## Data model

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_AQI : date_key
    DIM_TIME ||--o{ FACT_AQI : time_key
    DIM_CITY ||--o{ FACT_AQI : city_key
    DIM_STATION ||--o{ FACT_AQI : station_key
    DIM_CITY ||--o{ DIM_STATION : city_key
```
