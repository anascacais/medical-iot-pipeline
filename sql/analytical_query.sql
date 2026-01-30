
WITH condition AS ( -- add column with when alert condition is met
  SELECT
    *,
    modality = 'temp'
    AND flag_type_code = 0 -- only consider valid measurements
    AND value > 40 AS is_high_temp 
  FROM `{{PROJECT}}.{{DATASET}}.{{TABLE}}`
),

flagged AS (  -- marks where a new blocks starts (i.e., same response to condition)
  SELECT
    *,
    CASE
      WHEN is_high_temp != LAG(is_high_temp) OVER (PARTITION BY sensor_id ORDER BY ts_smp)
      THEN 1
      ELSE 0
    END AS new_block
  FROM condition WHERE modality = 'temp'
),

blocks AS ( -- walks down the rows (ordered by ts_smp) and increments a counter (block_id) every time a new flagged block starts (basically counting blocks) - only for high_temp flags
  SELECT
    *,
    SUM(new_block) OVER (PARTITION BY sensor_id ORDER BY ts_smp) AS block_id
  FROM flagged WHERE is_high_temp
), 

counts AS ( -- count size of blocks with same block_id; compute start and finish of periods
  SELECT
    block_id, sensor_id,
    COUNT(*) AS block_size,
    MIN(ts_smp) AS start_ts,
    MAX(ts_smp) AS end_ts
  FROM blocks
  WHERE is_high_temp
  GROUP BY sensor_id, block_id 
)

SELECT 
    sensor_id, start_ts, end_ts
    FROM counts 
    WHERE block_size >= 3;