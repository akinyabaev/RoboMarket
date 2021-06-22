-- Linear
WITH
  cessions_1 AS (
  SELECT
    UserId,
    time,
    source,
    medium,
    ROW_NUMBER()OVER(PARTITION BY UserId ORDER BY time DESC) AS rn
  FROM
    `cosmic-heaven-317207.123.sessions` 
)

SELECT
  UserId,
  time,
  source,
  medium,
  ROUND(1/MAX(rn)over(partition by UserId), 2) as Registrations
FROM
  cessions_1
ORDER BY
  1,2