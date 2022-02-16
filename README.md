# SuperStore-Big-Data-Analysis

# Hive Query:
show databases;--->displays the existing db
describe database db name;----->displays the location of the db
use db_name;--->to use the db

ctrl+l--->clear the hive screen



create table winequality(fixed_acidity float,volatile_acid float,citric_acid float,res_sugar int,chlorides float,free_so2 int,density float,ph float,sulphates float, quality int,taste string,phrange string, alcohol int) row format delimited fields terminated by ',' stored as textfile;
create table onlinesales(order_id int,order_priority string,order_quantity int,sales float,discount float,shipmode string,profit float,unitprice float,shipping_cost float,customer_name string,customersegment string,productname string,productcontainer string) row format delimited fields terminated by ',' stored as textfile;
create table ecommerce(sm string,seg string,country string,city string, state string,pc int,region string,cat string, ssc string,sales float,quant int, disc float,profit float) row format delimited fields terminated by ',' stored as textfile;
create table weather(summary string,p_type string,temp float,a_temp float,humid float,wind_speed float,wind_bar int,visu float,l_cover int,pressure float,d_summary string)row format delimited fields terminated by ',' stored as textfile;



2.loading data from local file:
load data local inpath '/home/ponny/Desktop/SampleSuperstore.csv' overwrite into table ecommerce;
load data local inpath '/home/ponny/Desktop/Superstore3.csv' overwrite into table onlinesales;


//load data local inpath '/user/ponny/Desktop/Superstore3.csv' into table ecommerce;
select * from ecommerce;




Joins:
INNER JOIN:
Select e.pc,e.city,e.ssc
FROM ecommerce e JOIN Onlinesales o
ON(e.pc = o.order_id);

Left Outer Join:
Select e.quant, e.ssc, e.cat
FROM ecommerce e
LEFT OUTER JOIN onlinesales o 
ON (e.quant=o.order_quantity);

Right outer Join:
Select e. quant, e-state, e. ssc
From ecommerec e
RIGHT OUTER JOIN onlinesales o
ON (e.quant = o.order_quantity);

full outer join:
Select e.pe, e.city, e.cat, e.seg
FROM ecommerce e
FULL OUTER JOIN Onlinesales o
ON (e.pc=o.order_id);


select * from ecommerce;
select * from onlinesales;
select city,quan from ecommerce where sales>=500.0;
select avg(profit),ssc from ecommerce;
select count(city) from ecommerce where cat="Furniture";
select sum(quant) from ecommerce where ssc="Phones";
select distinct(sm) from ecommerce limit 15;
select distinct(ssc) from ecommerce limit 10;
select count(*) from ecommerce where profit>100;
select ssc,avg(sales) from ecommerce group by ssc having avg(sales)>500.0;
select region,max(quant) from ecommerce group by region having sum(sales)>500.0;
select * from ecommerce  where quant<3 or sales>200.0; 
select * from ecommerce  where quant>4 and sales<250.0 limit 10;
select * from ecommerce  where quant between 4 and 6 limit 10; 



# UDF User Defined Function:

export UDF
show functions;
add jar /home/hive.jar; 
create temporary function my_function as 'org.samples.hive.training.hiveUDF';
select distinct(my_function(quant,ssc)) from ecommerce limit 15; 


Predefined Functions:
select lower(quant) from ecommerce limit 10;
