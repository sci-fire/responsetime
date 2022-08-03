
#save the table for analytics
save(incidents, file = "data/incidents.Rda")

#load in the previous radio log data
load("data/rlog.Rda")

#bring in the updated radio log data
rlog_tmp <- read_excel("input/rlog.xlsx")
rlog_tmp$DateTime <- parse_date_time(rlog_tmp$`Time & Date`, "%H:%M:%S %m/%d/%y")
rlog_tmp$DateTime <- as.character(rlog_tmp$DateTime)
rlog_tmp$date <- substr(rlog_tmp$DateTime, 1, 10)
rlog_tmp$time <- substr(rlog_tmp$DateTime, 12, 19)
rlog_tmp <- sqldf("
  select Call as call_id,
  Unit as unit,
  Code as code,
  `Geo X Coordinate of Unit` as gps_x,
  `Geo Y Coordinate of Unit` as gps_y,
  date,
  time from rlog_tmp
")

#stack the previous radio logs with new radio logs
rlog_merged <- sqldf("
  select * from rlog
  union
  select * from rlog_tmp
")

#check for and remove duplicate radio logs
rlog <- sqldf("\
  select distinct *
  from rlog_merged
  order by call_id, unit, code
")

#save the rlog table for analytics
save(rlog, file = "data/rlog.Rda")

#create a SCIF incident table with incidents and rlog data
scif_incidents <- sqldf("
  select incidents.incident_id,
  incidents.call_id,
  incidents.nature,
  incidents.agency,
  incidents.area,
  incidents.city,
  rlog.unit,
  rlog.code,
  rlog.date,
  rlog.time
  from incidents
  left join rlog
  on incidents.call_id = rlog.call_id
  where agency = 'SCIF'
  order by incidents.incident_id, incidents.call_id, rlog.time
")

#save out the table for further analytics
save(scif_incidents, file = "data/scif_incidents.Rda")
