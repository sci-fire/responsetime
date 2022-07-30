library(sqldf)

#load in SCIF incident data
load("data/scif_incidents.Rda")

#create a table of engine paged times
scif_fire_paged_times <- sqldf("
  select distinct *
  from scif_incidents
  where code like '%PAGED%' and (unit like 'E3%' or unit like 'E12%')
  group by incident_id
  order by date,time
")

#create a table of engine en route times
scif_fire_enroute_times <- sqldf("
  select *
  from scif_incidents
  where code like '%ENRT%' and (unit like 'E3%' or unit like 'E12%')
  group by incident_id
  order by date,time
")

#create a table of engine arrived times
scif_fire_arrived_times <- sqldf("
  select *
  from scif_incidents
  where (code like '%ARRVD%' or code like '%STAGE%') and (unit like 'E3%' or unit like 'E12%')
  group by incident_id
")

#merge the incident and rlog data; response time is first unit arrived minus first unit paged
scif_fire_response_times <- sqldf("
  select scif_incidents.incident_id,
  scif_incidents.call_id,
  scif_incidents.nature,
  scif_incidents.city,
  scif_incidents.date,
  strftime('%Y', scif_incidents.date) as year,
  strftime('%m', scif_incidents.date) as month,
  strftime('%H:%M:%S', scif_fire_paged_times.time) as paged,
  strftime('%H:%M:%S', scif_fire_enroute_times.time) as enroute,
  strftime('%H:%M:%S', scif_fire_arrived_times.time) as arrived,
  strftime('%M:%S',JULIANDAY(strftime('%H:%M:%S',scif_fire_arrived_times.time)) - JULIANDAY(strftime('%H:%M:%S', scif_fire_paged_times.time))) as response_time
  from scif_incidents
  left join scif_fire_paged_times on scif_incidents.call_id = scif_fire_paged_times.call_id
  left join scif_fire_enroute_times on scif_incidents.call_id = scif_fire_enroute_times.call_id
  left join scif_fire_arrived_times on scif_incidents.call_id = scif_fire_arrived_times.call_id
  where scif_fire_paged_times.time is not null and
  scif_fire_enroute_times.time is not null and
  scif_fire_arrived_times.time
  group by scif_incidents.incident_id
  order by scif_incidents.date
")

#calculate average and max response times structure pages for 2019 & 2022
response_times_fire_structure <- sqldf("
  select year,
  avg(response_time) as avg_response_time,
  max(response_Time) as max_response_time
  from scif_fire_response_times
  where nature like '%structure%' and year in ('2019', '2022')
  group by year
")

#calculate average and max response times vehicle pages for 2019 & 2022
response_times_fire_vehicle <- sqldf("
  select year,
  avg(response_time) as avg_response_time,
  max(response_Time) as max_response_time
  from scif_fire_response_times
  where nature like '%vehicle%' and year in ('2019', '2022')
  group by year
")
