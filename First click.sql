-- First click
SELECT
  UserId,
  time,
  source,
  medium,
  case when rank()over(partition by UserId order by time) = 1
  then 1
  end as Registrations

FROM
  `cosmic-heaven-317207.123.sessions`
  order by 1, 2