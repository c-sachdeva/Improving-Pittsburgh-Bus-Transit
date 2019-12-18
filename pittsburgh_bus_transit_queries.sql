# PITTSBURGH BUS TRANSIT DATA MANAGEMENT PROJECT
# BY: RAMI ARISS, CHIRAG SACHDEVA, RHEA UPADHYAY

# This SQL script demonstrates queries that can be run using the pittsburgh_bus_transit database associating Pittsburgh traffic count, bus route, and bus stop performance from numerous Port Authority, SNAP Census, and Carnegie Mellon derived data sources.

SHOW TABLES;
DROP TABLE performanceby_busroute;
TRUNCATE TABLE neighborhood;
SELECT * FROM ridershipby_busroute;
SELECT * FROM performanceby_busroute;
SELECT * FROM neighborhood n
    INNER JOIN population p ON n.neighborhood_name = p.neighborhood_name
    INNER JOIN transportation t ON n.neighborhood_name = t.neighborhood_name
    INNER JOIN education_income ei on n.neighborhood_name = ei.neighborhood_name
;

#Top 5 neighborhoods from where most people take Public Transportation in 2010
SELECT n.neighborhood_name, `Pop. 2010` * `Commute to Work: Public Transportation (2010)` as `People taking Public Transportation` FROM neighborhood n
    INNER JOIN population p ON n.neighborhood_name = p.neighborhood_name
    INNER JOIN transportation t ON n.neighborhood_name = t.neighborhood_name
    INNER JOIN education_income ei on n.neighborhood_name = ei.neighborhood_name
ORDER BY `People taking Public Transportation`DESC 
LIMIT 5
;

# Top 5 BusStops with most users per month (2019)
SELECT busstop_name, fy_avg_total FROM busstop bs
    INNER JOIN busstop_sensors bs_s ON bs.busstop_id = bs_s.busstop_id
    INNER JOIN busstop_busroute bs_br on bs_br.busstop_id = bs.busstop_id
    INNER JOIN busstop_usageby_busroute bub on bs_br.busstop_busroute = bub.busstop_busroute
    INNER JOIN traffic_count t ON t.sensor_id = bs_s.sensor_id
ORDER BY fy_rank
LIMIT 5
;

#Top 5 BusStops with most users getting ON per month (2019)
SELECT busstop_name, fy_avg_on FROM busstop bs
    INNER JOIN busstop_sensors bs_s ON bs.busstop_id = bs_s.busstop_id
    INNER JOIN busstop_busroute bs_br on bs_br.busstop_id = bs.busstop_id
    INNER JOIN busstop_usageby_busroute bub on bs_br.busstop_busroute = bub.busstop_busroute
    INNER JOIN traffic_count t ON t.sensor_id = bs_s.sensor_id
ORDER BY fy_avg_on DESC
LIMIT 5
;

#Morning and Evening taffic for the Top 5 most used Bus Stops with respect to the most used Bus Stops during those times.
SELECT MAX(6a+7a+8a+9a) as `Max Morning commute`, MAX(4p+5p+6p+7p) as `Max Evening commute` FROM busstop bs
    INNER JOIN busstop_sensors bs_s ON bs.busstop_id = bs_s.busstop_id
    INNER JOIN neighborhood_busstop nh_bs ON bs.busstop_id = nh_bs.busstop_id
    INNER JOIN busstop_busroute bs_br on bs_br.busstop_id = bs.busstop_id
    INNER JOIN busstop_usageby_busroute bub on bs_br.busstop_busroute = bub.busstop_busroute
    INNER JOIN traffic_count t ON t.sensor_id = bs_s.sensor_id
;

#Percent of max traffic count for morning and evening commute for top 5 bus stops and rank 6 and 11 East Liberty bus stops by bus usage 
SELECT neighborhood_name, busstop_name, if(6a+7a+8a+9a<0, "No data", (6a+7a+8a+9a)/3279.00) as `Morning commute`, if(4p+5p+6p+7p<0, "No data", (4p+5p+6p+7p)/4747.50) as `Evening commute` FROM busstop bs
    INNER JOIN busstop_sensors bs_s ON bs.busstop_id = bs_s.busstop_id
    INNER JOIN neighborhood_busstop nh_bs ON bs.busstop_id = nh_bs.busstop_id
    INNER JOIN busstop_busroute bs_br on bs_br.busstop_id = bs.busstop_id
    INNER JOIN busstop_usageby_busroute bub on bs_br.busstop_busroute = bub.busstop_busroute
    INNER JOIN traffic_count t ON t.sensor_id = bs_s.sensor_id
where busstop_name in ("SMITHFIELD ST AT SIXTH AVE", "LIBERTY AVE AT 10TH ST","STANWIX ST AT FORBES AVE","7TH ST AT PENN AVE","LIBERTY AVE AT WOOD ST")
group by  busstop_name;
;


