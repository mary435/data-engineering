--Q1: What is the count for fhv vehicle records for year 2019
CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-zoom.dezoomcamp.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ny-taxi-bucket-zoom/data/fhv/fhv_tripdata_2019-*.csv.gz']
);


SELECT count(*) FROM `ny-taxi-zoom.dezoomcamp.fhv_tripdata`;

--Q2: Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables EXTERNAL AND BQ
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `ny-taxi-zoom.dezoomcamp.fhv_tripdata`;


CREATE OR REPLACE TABLE `ny-taxi-zoom.dezoomcamp.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `ny-taxi-zoom.dezoomcamp.fhv_tripdata`;


SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `ny-taxi-zoom.dezoomcamp.fhv_nonpartitioned_tripdata`;

--Q3: How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT COUNT(1) 
FROM `ny-taxi-zoom.dezoomcamp.fhv_nonpartitioned_tripdata`
WHERE PUlocationID IS NULL 
AND DOlocationID IS NULL;

--Q4: What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number


--OP2: Partition by pickup_datetime Cluster on affiliated_base_number
CREATE OR REPLACE TABLE ny-taxi-zoom.dezoomcamp.fhv_part_date_clus_aff_tripdata
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM ny-taxi-zoom.dezoomcamp.fhv_nonpartitioned_tripdata;

--Q5
-- Query scan: This query will process 23.05 MB when run
SELECT DISTINCT(affiliated_base_number)
FROM ny-taxi-zoom.dezoomcamp.fhv_part_date_clus_aff_tripdata
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
ORDER BY affiliated_base_number;

-- Query scan: This query will process 647.87 MB when run.
SELECT DISTINCT(affiliated_base_number)
FROM ny-taxi-zoom.dezoomcamp.fhv_nonpartitioned_tripdata
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
ORDER BY affiliated_base_number;



