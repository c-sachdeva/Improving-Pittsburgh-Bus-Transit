# PITTSBURGH BUS TRANSIT DATA MANAGEMENT PROJECT
# BY: RAMI ARISS, CHIRAG SACHDEVA, RHEA UPADHYAY

# This SQL script will create an entire database associating Pittsburgh traffic count, bus route, and bus stop performance from numerous Port Authority, SNAP Census, and Carnegie Mellon derived data sources.

# NOTE: Find and replace root directory "/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project" with local root directory before running script.

# WARNING: SCRIPT WILL DROP ENTIRE DATABASE TO START CLEAN
DROP DATABASE IF EXISTS pittsburgh_bus_transit;
CREATE DATABASE pittsburgh_bus_transit;
SET GLOBAL local_infile = 'ON';
USE pittsburgh_bus_transit;

# Create the neighborhood table with GEOMETRY datatypes for location (POINT) and shape (MULTIPOLYGON)
CREATE TABLE IF NOT EXISTS neighborhood
(
  neighborhood_name VARCHAR(150) NOT NULL,
  latitude DECIMAL(10,8),
  longitude DECIMAL(11,8),
  neighborhood_area FLOAT,
  neighborhood_perimeter FLOAT,
  neighborhood_location geometry,
  neighborhood_shape TEXT,
  PRIMARY KEY (neighborhood_name)
);

# Populate the neighborhood table with only the desired data from SNAP file.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Pittsburgh SNAP Data 2010/Neighborhoods_with_SNAP_Data.csv'
    INTO TABLE neighborhood
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@OBJECTID, @Neighborhood_2010_FID_BlockG, @Neighborhood_2010_STATEFP10, @Neighborhood_2010_COUNTYFP10, @Neighborhood_2010_TRACTCE10, @Neighborhood_2010_BLKGRPCE10, @Neighborhood_2010_GEOID10, @Neighborhood_2010_NAMELSAD10, @Neighborhood_2010_MTFCC10, @Neighborhood_2010_FUNCSTAT10, @Neighborhood_2010_ALAND10, @Neighborhood_2010_AWATER10, @Neighborhood_2010_INTPTLAT10, @Neighborhood_2010_INTPTLON10, @Neighborhood_2010_Shape_Leng, @Neighborhood_2010_FID_Neighb, @Neighborhood_2010_AREA, @Neighborhood_2010_PERIMETER, @Neighborhood_2010_NEIGHBOR_, @Neighborhood_2010_NEIGHBOR_I, @Neighborhood_2010_HOOD, @Neighborhood_2010_HOOD_NO, @Neighborhood_2010_ACRES, @Neighborhood_2010_SQMILES, @Neighborhood_2010_DPWDIV, @Neighborhood_2010_UNIQUE_ID, @Neighborhood_2010_SECTORS, @Neighborhood_2010_Shape_Le_1, @Neighborhood_2010_Shape_Ar_1, @Neighborhood_2010_Page_Number, @SNAP_All_csv_Neighborhood, @SNAP_All_csv_Sector__, @Pop__1940, @Pop__1950, @Pop__1960, @Pop__1970, @Pop__1980, @Pop__1990, @Pop__2000, @Pop__2010, @F__Pop__Change__60_70, @F__Pop__Change__70_80, @F__Pop__Change__80_90, @F__Pop__Change__90_00, @F__Pop__Change__00_10, @Pop__as___of_City_total__2010_, @Land_Area__sq__mi_, @SNAP_All_csv_Land_Area__acres_, @Persons___sq__mi__2010_, @Persons___sq__mi__2000_, @SNAP_All_csv_Persons___acre__20, @SNAP_All_csv_Persons___acre__21, @SNAP_All_csv___African_American, @SNAP_All_csv___Asian__2010_, @SNAP_All_csv___Other__2010_, @SNAP_All_csv___White__2010_, @SNAP_All_csv___2__Races__2010_, @SNAP_All_csv___Hispanic__of_any, @F__Pop__Age___5__2010_, @F__Pop__Age_5_19__2010_, @F__Pop__Age_20_34__2010_, @F__Pop__Age_35_59__2010_, @F__Pop__Age_60_74__2010_, @F__Pop__Age___75__2010_, @SNAP_All_csv_Total___Units__200, @SNAP_All_csv_Total___Units__201, @SNAP_All_csv___Occupied_Units__, @SNAP_All_csv___Vacant_Units__20, @SNAP_All_csv___Occupied_Units_1, @SNAP_All_csv___Owner_Occupied_U, @SNAP_All_csv___Renter_Occupied_, @Est__Avg__Yrs__of_Residence__20, @SNAP_All_csv___Living_in_Househ, @SNAP_All_csv___Living_in_Group_, @SNAP_All_csv___Units_Built_00_0, @SNAP_All_csv___Units_Built_90_9, @SNAP_All_csv___Units_Built_80_8, @SNAP_All_csv___Units_Built_60_7, @SNAP_All_csv___Units_Built_40_5, @SNAP_All_csv___Units_Built_befo, @SNAP_All_csv_Median_Home_Value_, @Med__Val____00_in__10_Dollars_, @SNAP_All_csv_Median_Home__Value, @SNAP_All_csv___Change_Real_Valu, @SNAP_All_csv_Median_Sale_Price_, @SNAP_All_csv___Sales_Counted__2, @SNAP_All_csv_Foreclosures__2008, @SNAP_All_csv_Foreclosures__2010, @SNAP_All_csv___of_all_Housing_U, @Total_Age_16__N_hood_Residents_, @SNAP_All_csv_Resident_Jobs__Con, @SNAP_All_csv_Resident_Jobs__Man, @SNAP_All_csv_Resident_Jobs__Ret, @SNAP_All_csv_Resident_Jobs__Tra, @SNAP_All_csv_Resident_Jobs__Inf, @SNAP_All_csv_Resident_Jobs__Fin, @SNAP_All_csv_Resident_Jobs__Pro, @SNAP_All_csv_Resident_Jobs__Edu, @SNAP_All_csv_Resident_Jobs__Art, @SNAP_All_csv_Resident_Jobs__Pub, @SNAP_All_csv_Resident_Jobs__Oth, @Total___Jobs_Located_in_N_hood_, @SNAP_All_csv_Jobs_in__Hood__Con, @SNAP_All_csv_Jobs_in__Hood__Man, @SNAP_All_csv_Jobs_in__Hood__Ret, @SNAP_All_csv_Jobs_in__Hood__Tra, @SNAP_All_csv_Jobs_in__Hood__Inf, @SNAP_All_csv_Jobs_in__Hood__Fin, @SNAP_All_csv_Jobs_in__Hood__Pro, @SNAP_All_csv_Jobs_in__Hood__Edu, @SNAP_All_csv_Jobs_in__Hood__Art, @SNAP_All_csv_Jobs_in__Hood__Pub, @SNAP_All_csv_Jobs_in__Hood__Oth, @SNAP_All_csv_Total_Pop__25_and_, @Edu__Attainment__Less_than_High, @Edu__Attainment__High_School_Gr, @Edu__Attainment__Assoc__Prof__D, @Edu__Attainment__Bachelor_s_Deg, @Edu__Attainment__Postgraduate_D, @SNAP_All_csv_1999_Median_Income, @SNAP_All_csv_2009_Median_Income, @SNAP_All_csv_1999_Median_Inco_1, @F2009_Med__Income___13_Dollars_, @Est__Pop__for_which_Poverty_Cal, @Est__Pop__Under_Poverty__2010_, @Est__Percent_Under_Poverty__201, @SNAP_All_csv__Part_1__Major_Cri, @SNAP_All_csv__Part_2_Reports__2, @SNAP_All_csv__Other_Police_Repo, @SNAP_All_csv_Part_1_Crime_per_1, @SNAP_All_csv_Part_2_Crime_per_1, @SNAP_All_csv__Murder__2010_, @SNAP_All_csv__Rape__2010_, @SNAP_All_csv__Robbery__2010_, @F_Agr__Assault__2010_, @SNAP_All_csv__Burglary__2010_, @SNAP_All_csv__Auto_Theft__2010_, @SNAP_All_csv__Drug_Violations__, @SNAP_All_csv_Land_Area__acres1, @Approx__Total___Parcels__2010_, @Approx__Total___Taxable_Parcels, @Approx____of_Structures__2010_, @Approx____Unoccupied_Parcels__2, @SNAP_All_csv___Good___Excellent, @SNAP_All_csv___Average_Conditio, @SNAP_All_csv___Poor___Derelict_, @F__Residential_Bldg__Permits__2, @F__Residential_Bldg__Permits__3, @F__Commercial_Bldg__Permits__20, @F__Commercial_Bldg__Permits__21, @SNAP_All_csv___Code_Violations_, @F__of_all_Bldgs__w__Code_Violat, @SNAP_All_csv___Condemned_Struct, @F__of_all_Bldgs__Condemned__201, @SNAP_All_csv___Demolitions__201, @F__Tax_Delinquent_Prop___2__yrs, @F__of_Taxable_Prop__Delinquent_, @SNAP_All_csv_Landslide_Prone___, @SNAP_All_csv_Undermined____land, @SNAP_All_csv_Flood_Plain____lan, @SNAP_All_csv___Street_Trees, @SNAP_All_csv_Park_Space__acres_, @SNAP_All_csv_Park_Space____of_l, @Park_Space__acres_1000_pers__, @SNAP_All_csv_Greenway____of_lan, @SNAP_All_csv_Woodland____of_lan, @SNAP_All_csv_Cemetery____of_lan, @SNAP_All_csv_Residential, @SNAP_All_csv_Mixed_Use___Commer, @SNAP_All_csv_Mixed_Use___Indust, @Institutional___Edu____Med_, @SNAP_All_csv_Open_Space, @SNAP_All_csv_Hillside, @SNAP_All_csv_Special_Land_Use, @SNAP_All_csv_Miles_of_Major_Roa, @SNAP_All_csv_Total_Street_Miles, @Street_Density__st__mi_area_sq_, @SNAP_All_csv___Sets_of_Steps, @SNAP_All_csv___Step_Treads, @Res__Permit_Parking_Area_s_, @Total_Working_Pop___Age_16____2, @SNAP_All_csv_Commute_to_Work__D, @SNAP_All_csv_Commute_to_Work__C, @SNAP_All_csv_Commute_to_Work__P, @SNAP_All_csv_Commute_to_Work__T, @SNAP_All_csv_Commute_to_Work__M, @SNAP_All_csv_Commute_to_Work__B, @SNAP_All_csv_Commute_to_Work__W, @SNAP_All_csv_Commute_to_Work__O, @SNAP_All_csv_Work_at_Home__2010)
    SET neighborhood_name = @Neighborhood_2010_HOOD,
        latitude = @Neighborhood_2010_INTPTLAT10,
        longitude = @Neighborhood_2010_INTPTLON10,
        neighborhood_area = @Neighborhood_2010_AREA,
        neighborhood_perimeter = @Neighborhood_2010_PERIMETER,
        neighborhood_location = POINT(@Neighborhood_2010_INTPTLAT10, @Neighborhood_2010_INTPTLON10)
;

# Create a temporary neighborhood_temp table to load shape (MULTIPOLYGON) and update the desired neighborhood table
CREATE TABLE IF NOT EXISTS neighborhood_temp LIKE neighborhood;

# Populate the neighborhood_temp table with only the desired fields from the CSV shape file.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Python/neighborhoods_shp.csv'
    INTO TABLE neighborhood_temp
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@WKT, @Neighbor19)
    SET neighborhood_name = @Neighbor19,
        neighborhood_shape = @WKT
;

# Update the neighbor table with the shape GEOMETRY from neighbor_temp on neighborhood_name PRIMARY KEY
UPDATE neighborhood n
INNER JOIN neighborhood_temp nt on n.neighborhood_name = nt.neighborhood_name
SET n.neighborhood_shape = nt.neighborhood_shape;

# DROP the temporary neighbor table
DROP TABLE neighborhood_temp;

# Create the population table to store desired data from the SNAP Population and Density.
CREATE TABLE IF NOT EXISTS population
(
  neighborhood_name VARCHAR(150) NOT NULL,
  `Pop. 1940` INT NULL,
  `Pop. 1950` INT NULL,
  `Pop. 1960` INT NULL,
  `Pop. 1970` INT NULL,
  `Pop. 1980` INT NULL,
  `Pop. 1990` INT NULL,
  `Pop. 2000` INT NULL,
  `Pop. 2010` INT NULL,
  `% Pop. Change, 60-70` DECIMAL(5,2) NULL,
  `% Pop. Change, 70-80` DECIMAL(5,2) NULL,
  `% Pop. Change, 80-90` DECIMAL(5,2) NULL,
  `% Pop. Change, 90-00` DECIMAL(5,2) NULL,
  `% Pop. Change, 00-10` DECIMAL(5,2) NULL,
  `Pop. as % of City total (2010)` DECIMAL(5,2) NULL,
  `Land Area (sq. mi)` DECIMAL(10,2) NULL,
  `Land Area (acres)` DECIMAL(10,2) NULL,
  `Persons / sq. mi (2010)` DECIMAL(10,2) NULL,
  `Persons / sq. mi (2000)` DECIMAL(10,2) NULL,
  `Persons / acre (2010)` DECIMAL(10,2) NULL,
  `Persons / acre (2000)` DECIMAL(10,2) NULL,
  `% African American (2010)` DECIMAL(5,2) NULL,
  `% Asian (2010)` DECIMAL(5,2) NULL,
  `% Other (2010)` DECIMAL(5,2) NULL,
  `% White (2010)` DECIMAL(5,2) NULL,
  `% 2+ Races (2010)` DECIMAL(5,2) NULL,
  `% Hispanic (of any race) (2010)` DECIMAL(5,2) NULL,
  `% Pop. Age < 5 (2010)` DECIMAL(5,2) NULL,
  `% Pop. Age 5-19 (2010)` DECIMAL(5,2) NULL,
  `% Pop. Age 20-34 (2010)` DECIMAL(5,2) NULL,
  `% Pop. Age 35-59 (2010)` DECIMAL(5,2) NULL,
  `% Pop. Age 60-74 (2010)` DECIMAL(5,2) NULL,
  `% Pop. Age > 75 (2010)` DECIMAL(5,2) NULL,
  CONSTRAINT FOREIGN KEY (neighborhood_name) REFERENCES neighborhood(neighborhood_name)
);

# Populate the population table with the desired fields.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Pittsburgh SNAP Data 2010/opendata-pghsnap-neighborhood-census-data_populationanddensity.csv'
    INTO TABLE population
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@`Neighborhood`,@`Sector #`,`Pop. 1940`,`Pop. 1950`,`Pop. 1960`,`Pop. 1970`,`Pop. 1980`,`Pop. 1990`,`Pop. 2000`,`Pop. 2010`,`% Pop. Change, 60-70`,`% Pop. Change, 70-80`,`% Pop. Change, 80-90`,`% Pop. Change, 90-00`,`% Pop. Change, 00-10`,`Pop. as % of City total (2010)`,`Land Area (sq. mi)`,`Land Area (acres)`,`Persons / sq. mi (2010)`,`Persons / sq. mi (2000)`,`Persons / acre (2010)`,`Persons / acre (2000)`,`% African American (2010)`,`% Asian (2010)`,`% Other (2010)`,`% White (2010)`,`% 2+ Races (2010)`,`% Hispanic (of any race) (2010)`,`% Pop. Age < 5 (2010)`,`% Pop. Age 5-19 (2010)`,`% Pop. Age 20-34 (2010)`,`% Pop. Age 35-59 (2010)`,`% Pop. Age 60-74 (2010)`,`% Pop. Age > 75 (2010)`)
    SET neighborhood_name = @Neighborhood
;

# Create the education_income table to store desired data from the SNAP Education and Income.
CREATE TABLE IF NOT EXISTS education_income
(
  neighborhood_name VARCHAR(150) NOT NULL,
  `Edu. Attainment: Less than High School (2010)` DECIMAL(5,2) NULL,
  `Edu. Attainment: High School Graduate (2010)` DECIMAL(5,2) NULL,
  `Edu. Attainment: Assoc./Prof. Degree (2010)` DECIMAL(5,2) NULL,
  `Edu. Attainment: Bachelor's Degree (2010)` DECIMAL(5,2) NULL,
  `Edu. Attainment: Postgraduate Degree (2010)` DECIMAL(5,2) NULL,
  `1999 Median Income ('99 Dollars)` DECIMAL(12,2) NULL,
  `2009 Median Income ('09 Dollars)` DECIMAL(12,2) NULL,
  `1999 Median Income ('11 Dollars)` DECIMAL(12,2) NULL,
  `2009 Med. Income ('13 Dollars)` DECIMAL(12,2) NULL,
  `Est. Pop. for which Poverty Calc. (2010)` INT NULL,
  `Est. Pop. Under Poverty (2010)` INT NULL,
  `Est. Percent Under Poverty (2010)` DECIMAL(5,2) NULL,
  CONSTRAINT FOREIGN KEY (neighborhood_name) REFERENCES neighborhood(neighborhood_name)
);

# Populate the education_income table from desired data
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Pittsburgh SNAP Data 2010/opendata-pghsnap-neighborhood-census-data_employment_educationandincome.csv'
    INTO TABLE education_income
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@`Neighborhood`,@`Sector #`,@`Population (2010)`,@`Total Pop, 25 and older (2010)`,`Edu. Attainment: Less than High School (2010)`,`Edu. Attainment: High School Graduate (2010)`,`Edu. Attainment: Assoc./Prof. Degree (2010)`,`Edu. Attainment: Bachelor's Degree (2010)`,`Edu. Attainment: Postgraduate Degree (2010)`,`1999 Median Income ('99 Dollars)`,`2009 Median Income ('09 Dollars)`,`1999 Median Income ('11 Dollars)`,`2009 Med. Income ('13 Dollars)`,`Est. Pop. for which Poverty Calc. (2010)`,`Est. Pop. Under Poverty (2010)`,`Est. Percent Under Poverty (2010)`)
    SET neighborhood_name = @Neighborhood
;

# Create the transportation table to store desired data from the SNAP Transportation.
CREATE TABLE IF NOT EXISTS transportation
(
  neighborhood_name VARCHAR(150) NOT NULL,
  `Miles of Major Roads` DECIMAL(10,2) NULL,
  `Total Street Miles` DECIMAL(10,2) NULL,
  `Street Density (st. mi/area sq. mi)` DECIMAL(10,2) NULL,
  `# Sets of Steps` INT NULL,
  `# Step Treads` INT NULL,
  `Res. Permit Parking Area(s)` VARCHAR(45) NULL,
  `Total Working Pop. (Age 16+) (2010)` INT NULL,
  `Commute to Work: Drive Alone (2010)` DECIMAL(5,2) NULL,
  `Commute to Work: Carpool/Vanpool (2010)` DECIMAL(5,2) NULL,
  `Commute to Work: Public Transportation (2010)` DECIMAL(5,2) NULL,
  `Commute to Work: Taxi (2010)` DECIMAL(5,2) NULL,
  `Commute to Work: Motorcycle (2010)` DECIMAL(5,2) NULL,
  `Commute to Work: Bicycle (2010)` DECIMAL(5,2) NULL,
  `Commute to Work: Walk (2010)` DECIMAL(5,2) NULL,
  `Commute to Work: Other (2010)`DECIMAL(5,2) NULL,
  `Work at Home (2010)` DECIMAL(5,2) NULL,
  CONSTRAINT FOREIGN KEY (neighborhood_name) REFERENCES neighborhood(neighborhood_name)
);

# Populate the transportation table with desired data.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Pittsburgh SNAP Data 2010/opendata-pghsnap-neighborhood-census-data_employment_transportation.csv'
    INTO TABLE transportation
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@`Neighborhood`,@`Sector #`,@`Population (2010)`,`Miles of Major Roads`,`Total Street Miles`,`Street Density (st. mi/area sq. mi)`,`# Sets of Steps`,`# Step Treads`,`Res. Permit Parking Area(s)`,`Total Working Pop. (Age 16+) (2010)`,`Commute to Work: Drive Alone (2010)`,`Commute to Work: Carpool/Vanpool (2010)`,`Commute to Work: Public Transportation (2010)`,`Commute to Work: Taxi (2010)`,`Commute to Work: Motorcycle (2010)`,`Commute to Work: Bicycle (2010)`,`Commute to Work: Walk (2010)`,`Commute to Work: Other (2010)`,`Work at Home (2010)`)
    SET neighborhood_name = @Neighborhood
;

# Create the sensors table storing sensor information and location from traffic_counts data.
CREATE TABLE IF NOT EXISTS sensors
(
  sensor_id VARCHAR(12) NOT NULL,
  latitude DECIMAL(10,8),
  longitude DECIMAL(11,8),
  sensor_location geometry,
  PRIMARY KEY (sensor_id)
);

# Populate the sensors table with desired data.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Traffic Counts /8edd8a76-8607-4ed3-960f-dcae914fd937.csv'
    INTO TABLE sensors
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@sensor_ID, @Longitude, @Latitude,@1a,@2a,@3a,@4a,@5a,@6a,@7a,@8a,@9a,@10a,@11a,@12p,@1p,@2p,@3p,@4p,@5p,@6p,@7p,@8p,@9p,@10p,@11p,@12a)
    SET sensor_id = @sensor_ID,
        latitude = @Latitude,
        longitude = @Longitude,
        sensor_location = POINT(@Latitude, @Longitude)
;

# Create the traffic_count table
CREATE TABLE IF NOT EXISTS traffic_count
(
  sensor_id VARCHAR(12) NOT NULL,
  1a DECIMAL(10, 2) NULL,
  2a DECIMAL(10, 2) NULL,
  3a DECIMAL(10, 2) NULL,
  4a DECIMAL(10, 2) NULL,
  5a DECIMAL(10, 2) NULL,
  6a DECIMAL(10, 2) NULL,
  7a DECIMAL(10, 2) NULL,
  8a DECIMAL(10, 2) NULL,
  9a DECIMAL(10, 2) NULL,
  10a DECIMAL(10, 2) NULL,
  11a DECIMAL(10, 2) NULL,
  12p DECIMAL(10, 2) NULL,
  1p DECIMAL(10, 2) NULL,
  2p DECIMAL(10, 2) NULL,
  3p DECIMAL(10, 2) NULL,
  4p DECIMAL(10, 2) NULL,
  5p DECIMAL(10, 2) NULL,
  6p DECIMAL(10, 2) NULL,
  7p DECIMAL(10, 2) NULL,
  8p DECIMAL(10, 2) NULL,
  9p DECIMAL(10, 2) NULL,
  10p DECIMAL(10, 2) NULL,
  11p DECIMAL(10, 2) NULL,
  12a DECIMAL(10, 2) NULL,
  CONSTRAINT FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

# Load data into traffic_count table
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Traffic Counts /8edd8a76-8607-4ed3-960f-dcae914fd937.csv'
    INTO TABLE traffic_count
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@sensor_ID, @Longitude, @Latitude,1a,2a,3a,4a,5a,6a,7a,8a,9a,10a,11a,12p,1p,2p,3p,4p,5p,6p,7p,8p,9p,10p,11p,12a)
    SET sensor_id = @sensor_ID
;

# Create neighborhood_sensors table to spatially associate sensors to neighborhoods.
CREATE TABLE IF NOT EXISTS neighborhood_sensors
(
  neighborhood_name VARCHAR(150) NOT NULL,
  sensor_id VARCHAR(12) NOT NULL,
  CONSTRAINT FOREIGN KEY (neighborhood_name) REFERENCES neighborhood(neighborhood_name),
  CONSTRAINT FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

# Load data into measures table from nearest neighbor by euclidian distance results completed in python.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Python/traffic_count_sensors_to_neighborhoods_clean.csv'
    INTO TABLE neighborhood_sensors
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@node, @polygon)
    SET sensor_id = @node,
        neighborhood_name = @polygon
;

# Create busstop table for all busstop metadata.
CREATE TABLE IF NOT EXISTS busstop
(
    busstop_id VARCHAR(10) NOT NULL,
    busstop_name VARCHAR(150) NOT NULL,
    busstop_type VARCHAR(45),
    current_stop VARCHAR(10),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    busstop_location geometry,
    PRIMARY KEY(busstop_id)
);

# Load data into busstop table with desired data from busstopusagebyroute. Use "IGNORE" to populate just first instance of the stop as each row is a stop and route
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Port Authority Transit Stop Usage/busstopusagebyroute_clean.csv'
    IGNORE
    INTO TABLE busstop
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@STOP_ID, @ROUTE, @STOP_ROUTE, @STOP_NAME, @CLEVER_ID, @LATITUDE, @LONGITUDE, @ALL_ROUTES, @SHELTER, @STOP_TYPE, @CURRENT_STOP, @FY19_AVG_ON, @FY19_AVG_OFF, @FY19_AVG_TOTAL, @FY19_RANK, @1806_ON, @1806_OFF, @1809_ON, @1809_OFF, @1811_ON, @1811_OFF, @1903_ON, @1903_OFF, @1906_ON, @1906_OFF)
    SET busstop_id = @STOP_ID,
        busstop_name = @STOP_NAME,
        busstop_type = @STOP_TYPE,
        current_stop = @CURRENT_STOP,
        latitude = @LATITUDE,
        longitude = @LONGITUDE,
        busstop_location = POINT(@LATITUDE, @LONGITUDE)
;

# Create busroute table for all busroute metadata.
CREATE TABLE IF NOT EXISTS busroute
(
    busroute_id VARCHAR(10) NOT NULL,
    busroute_name VARCHAR(150) NOT NULL,
    current_garage VARCHAR(45),
    mode VARCHAR(10),
    PRIMARY KEY(busroute_id)
);

# Load data into busroute table with desired data from monthly average ridership by route. Use "IGNORE" to populate just first instance of the route.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Port Authority Monthly Average Ridership by Route/12bb84ed-397e-435c-8d1b-8ce543108698.csv'
    IGNORE
    INTO TABLE busroute
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@route, @ridership_route_code, @route_full_name, @current_garage, @mode, @month_start, @year_month, @day_type, @avg_riders, @day_count)
    SET busroute_id = @route,
        busroute_name = @route_full_name,
        current_garage = @current_garage,
        mode = @mode
;

# Create neighborhood_busstop table to associate busstops with the neighborhoods they are in. These were determined using geopandas CONTAINS query in python.
CREATE TABLE IF NOT EXISTS neighborhood_busstop
(
    busstop_id VARCHAR(10) NOT NULL,
    neighborhood_name VARCHAR(150) NOT NULL,
    CONSTRAINT FOREIGN KEY (busstop_id) REFERENCES busstop(busstop_id),
    CONSTRAINT FOREIGN KEY (neighborhood_name) REFERENCES neighborhood(neighborhood_name)
);

# Load data into neighborhood_busstop table from geopandas CONTAINS query results completed in python.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Python/bus_stops_to_neighborhoods_clean.csv'
    INTO TABLE neighborhood_busstop
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@node, @polygon)
    SET busstop_id = @node,
        neighborhood_name = @polygon
;

# Create busstop_sensors table to associate busstops with the nearest traffic sensor determined by euclidian distances using geopandas distance calculations in python.
CREATE TABLE IF NOT EXISTS busstop_sensors
(
    busstop_id VARCHAR(10) NOT NULL,
    sensor_id VARCHAR(12) NOT NULL,
    connection TEXT,
    CONSTRAINT FOREIGN KEY (busstop_id) REFERENCES busstop(busstop_id),
    CONSTRAINT FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

# Load data into neighborhood_busstop table from geopandas CONTAINS query results completed in python.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Python/bus_stops_to_traffic_count_sensors_clean.csv'
    INTO TABLE busstop_sensors
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@STOP_ID, @sensor_ID, @geometry)
    SET busstop_id = @STOP_ID,
        sensor_id = @sensor_ID,
        connection = @geometry
;

# Create busstop_busroute table to associate bus stops with the bus routes that pass through them.
CREATE TABLE IF NOT EXISTS busstop_busroute
(
    busstop_busroute VARCHAR(20) NOT NULL,
    busstop_id VARCHAR(10) NOT NULL,
    busroute_id VARCHAR(10) NOT NULL,
    PRIMARY KEY (busstop_busroute),
    CONSTRAINT FOREIGN KEY (busstop_id) REFERENCES busstop(busstop_id),
    CONSTRAINT FOREIGN KEY (busroute_id) REFERENCES busroute(busroute_id)
);

# Load data into busstop_busroute table from desired data in busstopusagebyroute.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Port Authority Transit Stop Usage/busstopusagebyroute_clean.csv'
#     IGNORE
    INTO TABLE busstop_busroute
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@STOP_ID, @ROUTE, @STOP_ROUTE, @STOP_NAME, @CLEVER_ID, @LATITUDE, @LONGITUDE, @ALL_ROUTES, @SHELTER, @STOP_TYPE, @CURRENT_STOP, @FY19_AVG_ON, @FY19_AVG_OFF, @FY19_AVG_TOTAL, @FY19_RANK, @1806_ON, @1806_OFF, @1809_ON, @1809_OFF, @1811_ON, @1811_OFF, @1903_ON, @1903_OFF, @1906_ON, @1906_OFF)
    SET busstop_busroute = @STOP_ROUTE,
        busstop_id = @STOP_ID,
        busroute_id = @ROUTE
;

# Create busstop_usageby_busroute table to store the bus stop usage (ridership) by bus route. We use a combined busstop_busroute key.
CREATE TABLE IF NOT EXISTS busstop_usageby_busroute
(
    busstop_busroute VARCHAR(20) NOT NULL,
    fiscal_year INT NOT NULL,
    fy_avg_on INT NULL,
    fy_avg_off INT NULL,
    fy_avg_total INT NULL,
    fy_rank INT NULL,
    CONSTRAINT FOREIGN KEY (busstop_busroute) REFERENCES busstop_busroute(busstop_busroute)
);

# Load data into busstop_busroute table from desired data in busstopusagebyroute.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Port Authority Transit Stop Usage/busstopusagebyroute_clean.csv'
#     IGNORE
    INTO TABLE busstop_usageby_busroute
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@STOP_ID, @ROUTE, @STOP_ROUTE, @STOP_NAME, @CLEVER_ID, @LATITUDE, @LONGITUDE, @ALL_ROUTES, @SHELTER, @STOP_TYPE, @CURRENT_STOP, @FY19_AVG_ON, @FY19_AVG_OFF, @FY19_AVG_TOTAL, @FY19_RANK, @1806_ON, @1806_OFF, @1809_ON, @1809_OFF, @1811_ON, @1811_OFF, @1903_ON, @1903_OFF, @1906_ON, @1906_OFF)
    SET busstop_busroute = @STOP_ROUTE,
        fiscal_year = '2019',
        fy_avg_on = @FY19_AVG_ON,
        fy_avg_off = @FY19_AVG_OFF,
        fy_avg_total = @FY19_AVG_TOTAL,
        fy_rank = @FY19_RANK
;

# Create ridershipby_busroute table for monthly average ridership by bus routes. NOTE the conjugate primary key of busroute_id (FK) and month_start.
CREATE TABLE IF NOT EXISTS ridershipby_busroute
(
    busroute_id VARCHAR(10) NOT NULL,
    month_start DATE NOT NULL,
    day_type VARCHAR(7) NOT NULL,
    avg_riders INT NULL,
    day_count INT NULL,
    PRIMARY KEY(busroute_id, month_start, day_type),
    CONSTRAINT FOREIGN KEY(busroute_id) REFERENCES busroute(busroute_id)
);

# Load data into busroute table with desired data from monthly average ridership by route. Use "IGNORE" to populate just first instance of the route.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Port Authority Monthly Average Ridership by Route/12bb84ed-397e-435c-8d1b-8ce543108698.csv'
#     IGNORE
    INTO TABLE ridershipby_busroute
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@route, @ridership_route_code, @route_full_name, @current_garage, @mode, month_start, @year_month, day_type, avg_riders, day_count)
    SET busroute_id = @route
;

# Create performanceby_busroute table for monthly on time performance by bus routes. NOTE the conjugate primary key of busroute_id (FK) and month_start.
CREATE TABLE IF NOT EXISTS performanceby_busroute
(
    busroute_id VARCHAR(10) NOT NULL,
    month_start DATE NOT NULL,
    day_type VARCHAR(7) NOT NULL,
    on_time_percent DECIMAL(5,2) NULL,
    PRIMARY KEY(busroute_id, month_start, day_type),
    CONSTRAINT FOREIGN KEY(busroute_id) REFERENCES busroute(busroute_id)
);

# Load data into busroute table with desired data from monthly average ridership by route. Use "IGNORE" to populate just first instance of the route.
LOAD DATA
    LOCAL INFILE '/Users/ramiariss/Google Drive/CMU/1 - 2019 Fall/12741 - Data Management/data_management_project/Data/Port Authority Monthly On Time Performance by Route/00eb9600-69b5-4f11-b20a-8c8ddd8cfe7a.csv'
#     IGNORE
    INTO TABLE performanceby_busroute
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@route, @ridership_route_code, @route_full_name, @current_garage, @mode, month_start, @year_month, day_type, @on_time_percent, @data_source)
    SET busroute_id = @route,
        on_time_percent = @on_time_percent*100
;
