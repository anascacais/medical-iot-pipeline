# Schema Defense

## Hot Storage Design (Bigtable)

This component of the infrastructure is designed to support a real-time dashboard that can display various data, such as rolling metrics (e.g., mean, max, etc., for the physiological modalities), time-range plots of the modalities, and system health indicators (e.g., data loss or corrupted samples).

This is achieved via 2 Bigtable tables: `stream_data`, which stores _clean_ physiological data, and `health_check`, which stores bad-health events (e.g., invalid/impossible sample timestamps or data outside the physiological range).

Despite the _overhead of an additional table access_ at the level of the Processing Layer and at the Dashboard API [Google Cloud](https://arc.net/l/quote/xtdadmpq), this is justified by the need for a different row key design, as addressed below.

### Row Key Design

Row key design is heavily influenced by how the data is written and, especially, read.

Table `stream_data`: `sensor_id#reversed_timestamp` (where `reversed_timestamp` corresponds to the timestamp of the sample, `ts_smp`)

- Using the `sensor_id` (which in this case is also a proxy for patient ID) as the first part of the row key allows to efficiently query the table for in-patient queries (which I assume would be most common, contrarily to cross-patient queries), since they are all "bundled together" in the same part of the table.
- The reversed timestamp, in this case corresponding to the _timestamp of the sample received_, causes rows to be ordered from most recent to least recent, so more recent data is earlier in the table [Google Cloud](https://arc.net/l/quote/dzqgzaui).
- Using the `sensor_id` as the first part of the row key also helps avoid _hotspotting_: if the `reversed_timestamp` were being used as the first part of the row key, with multiple sensors streaming at the same time we would have sequential writes being pushed onto a single node (i.e., same "part" of the table), causing a _hotspot_.

Table `health_check`: `sensor_id#reversed_timestamp` (where `reversed_timestamp` corresponds to the timestamp of the ingestion, `ts_ing`)

- Same logic as above applies.
- On the Dashboard API, when looking for system health checks for a specific time range, if the same row key was used as in table `stream_data`, then invalid timestamps (e.g., invalid formats or timestamps in the "future") would not be returned (they would be outside the query time range).

### Bigtable Schema

Table `stream_data`:

| **Column Family** | **Column** |      **Logical type**       |               **Description**                |
| :---------------: | :--------: | :-------------------------: | :------------------------------------------: |
|      vitals       |     hr     |            FLOAT            |                 Heart rate.                  |
|      vitals       |    temp    |            FLOAT            |              Body temperature.               |
|      vitals       |    SpO2    |            FLOAT            | Peripheral oxygen saturation, in percentage. |
|      vitals       |  battery   |            FLOAT            |        Battery level, in percentage.         |
|       meta        |   ts_ing   | TIMESTAMP (epoch ms, int64) |         Timestamp of ingestion time.         |

Table `health_check`:

| **Column Family** | **Column** | **Logical type**            | **Description**                                                       |
| ----------------- | ---------- | --------------------------- | --------------------------------------------------------------------- |
| vitals            | hr         | FLOAT                       | Heart rate.                                                           |
| vitals            | temp       | FLOAT                       | Body temperature.                                                     |
| vitals            | SpO2       | FLOAT                       | Peripheral oxygen saturation, in percentage.                          |
| vitals            | battery    | FLOAT                       | Battery level, in percentage.                                         |
| meta              | ts_smp     | TIMESTAMP (epoch ms, int64) | Timestamp of sample.                                                  |
| flag              | HR_INV     | INT64                       | Flag for heart rate outside of physiological range.                   |
| flag              | HR_NAN     | INT64                       | Flag for missing heart rate value.                                    |
| flag              | TEMP_INV   | INT64                       | Flag for body temperature outside of physiological range.             |
| flag              | TEMP_NAN   | INT64                       | Flag for missing body temperature value.                              |
| flag              | SPO2_INV   | INT64                       | Flag for peripheral oxygen saturation outside of physiological range. |
| flag              | SPO2_NAN   | INT64                       | Flag for missing peripheral oxygen saturation value.                  |
| flag              | BAT_INV    | INT64                       | Flag for invalid battery level.                                       |
| flag              | BAT_NAN    | INT64                       | Flag for missing battery level.                                       |
| flag              | TS_INV     | INT64                       | Flag for invalid (format) timestamp.                                  |
| flag              | TS_IMP     | INT64                       | Flag for impossible timestamp.                                        |

> **COMMENT:** I am not including `sensor_id` and `ts_*` as table columns because they are already present in the corresponding row keys and this would avoid redundancy and added storage -- I don't know if, downstream, it would make it more efficient to already have it as a table column instead of decoding it from the row key, but my intuition says no.

### Garbage Collection Rules

The same garbage collection rule is applied to all column families from both tables: a max of 1 version of each cell is kept. This assumes that there would only be multiple versions of the same cell if the same payload was sent more than once (for `stream_data`) or if the ingestion system received multiple samples simultaneously, which I assume to be impossible (for `health_check`).

## Analytics & Warehousing (BigQuery)

This component of the infrastructure is designed to support:

1. Raw data storage, in compliance with regulatory requirements (under the [Data Act](https://arc.net/l/quote/bukvrfhl) raw data must be retained).
2. Offline, downstream ML pipelines for further analysis and model development.

### BigQuery Schema

| **Field Name** |   **Type**   | **Mode** |                                        **Description**                                        |
| :------------: | :----------: | :------: | :-------------------------------------------------------------------------------------------: |
|     ts_smp     |  TIMESTAMP   | NULLABLE |                                     Timestamp of sample.                                      |
|     ts_ing     |  TIMESTAMP   | REQUIRED |                                 Timestamp of ingestion time.                                  |
|   sensor_id    |    STRING    | REQUIRED |                               Sensor ID, proxy for patient ID.                                |
|    modality    |    STRING    | REQUIRED |                    Type of vital measure (e.g., HR, Temp, SpO2, battery).                     |
|     value      |   FLOAT64    | NULLABLE |                                    Value of vital measure.                                    |
| flag_type_code | INT64 (enum) | REQUIRED | Code for quality flags: {0: NULL (no flag), 1: INV_VALUE, 2: NAN_VALUE, 3: INV_TS, 4: IMP_TS} |

> **COMMENT:** I opted to store physiological data in a long (tidy) format (i.e., one row per timestamp and sensor modality, rather than one row per timestamp) because this structure aligns more naturally with my analytical workflow. However, I was unable to locate a clear reference supporting this choice in the literature. In the long-term, it can also prove to be beneficial if other modalities are introduced (or some are removed) since it won't require schema changes.

> **COMMENT:** Considering the table format, I haven't made a decision on whether is makes sense to "store missing" values for the modalities, but for data completeness (and while I reason about it), I am also storing it as a data row (even if that implies redundant storage).

This assumes that `sensor_id` is a proxy for patient ID. Alternatively, it could be interesting to have 3 additional tables with (mostly) static data that is not queried as often as `physio_data` but could be helpful for documentation purposes: `sensors` (with keys e.g., `sensor_id`, `sampling_frequency`, `firmware`, `units`); `patients` (with keys e.g., `patient_id`, `sex`, `birth_year`); `patient_sensors` (with keys `patient_id`, `sensor_id`, `start_date`, `end_date`.

### Partitioning & Clustering

- **Partitioning:** Data will be partitioned via `ts_smp`, which allows for efficient pruning of historical data when querying specific time ranges. This assumes that most queries will be related to the time when samples were recorded (and not when they were received by the system).
- **Clustering:** Data will be clustered by `modality`, then `flag_type_code`. Clustering by `modality` assumes frequent queries for ML pipelines when computing modality-specific features, as well as data drift monitoring; `flag_type_code` next enables quick identification of valid versus invalid measurements. Alternatively, we could also cluster last by `ts_ing`, if we expect frequent queries related to system health checks.
