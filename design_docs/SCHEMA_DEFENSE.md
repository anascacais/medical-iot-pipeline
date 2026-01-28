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
|      vitals       |    SpO2    |            INT64            | Peripheral oxygen saturation, in percentage. |
|      vitals       |  battery   |            INT64            |        Battery level, in percentage.         |
|       meta        |   ts_ing   | TIMESTAMP (epoch ms, int64) |         Timestamp of ingestion time.         |

Table `health_check`:

| **Column Family** | **Column** | **Logical type**            | **Description**                                                       |
| ----------------- | ---------- | --------------------------- | --------------------------------------------------------------------- |
| vitals            | hr         | FLOAT                       | Heart rate.                                                           |
| vitals            | temp       | FLOAT                       | Body temperature.                                                     |
| vitals            | SpO2       | INT64                       | Peripheral oxygen saturation, in percentage.                          |
| meta              | ts_smp     | TIMESTAMP (epoch ms, int64) | Timestamp of sample.                                                  |
| flag              | HR_INV     | INT64                       | Flag for heart rate outside of physiological range.                   |
| flag              | HR_NAN     | INT64                       | Flag for missing heart rate value.                                    |
| flag              | TEMP_INV   | INT64                       | Flag for body temperature outside of physiological range.             |
| flag              | TEMP_NAN   | INT64                       | Flag for missing body temperature value.                              |
| flag              | SPO2_INV   | INT64                       | Flag for peripheral oxygen saturation outside of physiological range. |
| flag              | SPO2_NAN   | INT64                       | Flag for missing peripheral oxygen saturation value.                  |
| flag              | TS_INV     | INT64                       | Flag for invalid (format) timestamp.                                  |
| flag              | TS_IMP     | INT64                       | Flag for impossible timestamp.                                        |

_**COMMENT:** I am not including `sensor_id` and `ts_\*` as table columns because they are already present in the corresponding row keys and this would avoid redundancy and added storage -- I don't know if, downstream, it would make it more efficient to already have it as a table column instead of decoding it from the row key, but my intuition says no.\_
