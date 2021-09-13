-- create a database
create database cosmeticshop;

-- create a table
use cosmeticshop;
create table cosmeticshopfunnel
(
    event_time TIMESTAMP,
    event_type CHAR(18),
    product_id CHAR(10),
    category_id CHAR(20),
    category_code TINYTEXT,
    brand TINYTEXT,
    price FLOAT,
    user_id TINYTEXT,
    user_session TINYTEXT
    );

-- create a pipeline to ingest the data in AWS S3
CREATE or REPLACE PIPELINE cosmeticshoppipe
AS LOAD DATA S3 's3://studiotutorials/eCommerce/*'
CONFIG '{"region": "us-east-1"}'
INTO TABLE `cosmeticshopfunnel`
FIELDS TERMINATED BY ',' ENCLOSED BY '"';

-- start the pipeline
START PIPELINE cosmeticshoppipe;

-- see how many events have been ingested
select count(*) from cosmeticshopfunnel;

-- see the data that has been ingested
select * from cosmeticshopfunnel limit 100;

-- create a holiday reference table to store all holiday dates
CREATE REFERENCE TABLE holidays
(
    holiday TINYTEXT,
    date_of_holiday DATE PRIMARY KEY
    );

-- insert holiday dates
INSERT INTO holidays VALUES
("New Year's Day", "2020-1-1"),
("Martin Luther King Jr. Day", "2020-02-20"),
("Memorial Day", "2020-05-25"),
("Independence Day", "2020-07-04"),
("Labor Day", "2020-09-07"),
("Veterans Day", "2019-11-11"),
("Thanksgiving", "2019-11-28"),
("Christmas Day", "2019-12-25");

select * from holidays;

-- find out which holiday has the most activity
select holiday, count(holiday) from cosmeticshopfunnel
    join (select holiday, DATE_SUB(date_of_holiday, INTERVAL 3 DAY) as beforedate, DATE_ADD(date_of_holiday, INTERVAL 3 DAY) as afterdate from holidays)
    on event_time > beforedate and event_time < afterdate
    group by holiday
    order by count(holiday) desc;

-- find out which is the top brand purchased during each of the holidays
select holiday, brand, count(brand) from cosmeticshopfunnel
    join (select holiday, DATE_SUB(date_of_holiday, INTERVAL 3 DAY) as beforedate, DATE_ADD(date_of_holiday, INTERVAL 3 DAY) as afterdate from holidays)
    on event_time > beforedate and event_time < afterdate
    where event_type = "purchase" and brand != ""
    group by holiday
    order by count(holiday) desc;

-- find out if customers are sensitive to the average price of the brands
select brand, avg(price), count(event_type) from cosmeticshopfunnel
where brand != ""
group by brand
order by count(event_type) desc;

-- find out which brands have been removed from cart the most
select brand, count(brand) count from cosmeticshopfunnel
where event_type = "remove_from_cart" and brand != ""
group by brand order by count desc;

-- find out which brands have been purchased the most
select brand, count(brand) as c from cosmeticshopfunnel
where event_type = "purchase"
group by brand
order by c desc;

-- find out which categories have been purchased the most
select distinct category_code, count(category_code) as c from cosmeticshopfunnel
where event_type = "purchase" group by category_code order by c desc;

-- find out which brands have been purchased the most
select brand, count(brand) as c from cosmeticshopfunnel
where event_type = "purchase" group by brand order by c desc;

-- find out which product_id has been the most removed from cart
select product_id, count(product_id) as c from cosmeticshopfunnel
where event_type = "remove_from_cart" group by product_id order by c desc;


DELETE FROM cosmeticshopfunnel;
ALTER PIPELINE cosmeticshoppipe SET OFFSETS EARLIEST;
START PIPELINE cosmeticshoppipe;
STOP PIPELINE cosmeticshoppipe;
DROP PIPELINE cosmeticshoppipe;
DROP TABLE holidays;
DROP TABLE cosmeticshopfunnel;
