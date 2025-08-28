INSERT INTO etl_watermark (key, last_ts)
VALUES (%s, %s)
ON CONFLICT (key) DO UPDATE
SET last_ts = EXCLUDED.last_ts;