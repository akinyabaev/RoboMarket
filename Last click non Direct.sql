-- Last click non Direct
SELECT
  UserId,
  time,
  source,
  medium,
  case when rank()over(partition by UserId, source <> 'direct' order by time desc) = 1
  and source <> 'direct'
  then 1
  end as Registrations

FROM
  `cosmic-heaven-317207.123.sessions`
  order by 1, 2