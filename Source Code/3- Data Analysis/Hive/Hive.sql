# Create the table 
create table KCcrime (Report_No INT, Reported_year INT, Reported_month INT, Reported_day INT, Reported_hour INT, Reported_minute INT, From_year INT, From_month INT, From_day INT, From_hour INT, From_minute INT, Offense INT, IBRS STRING, Description STRING, Beat INT, Address STRING, City STRING, Zip_Code INT, Rep_Dist STRING, Area STRING, DVFlag STRING, Invl_No INT, Involvement STRING, Race STRING, Sex STRING, Age INT, Firearm_Used_Flag String) row format delimited fields terminated by "," tblproperties("skip.header.line.count"="1");

# Load the dataset
load data local inpath "/home/cloudera/Desktop/KCcrime2010To2018.csv" into table KCcrime;

# Display the number of crimes
select description, count(*) from kccrime group by description;

# show how many people are involving according to their gender in 206 and 2018
select Reported_year, Sex, count(*) from kccrime where Sex RLIKE 'M|F|U' and Reported_year RLIKE '2018|2016' group by Reported_year, Sex;

# Print the number of arrested people in the Plaza Area in 2018
select Reported_year, Sex, Involvement, count(*), Zip_Code from kccrime where Sex RLIKE 'M|F|U' and Reported_year = '2018' and Involvement = 'ARR' and Zip_Code = '64112' group by Reported_year, Sex, Involvement, Zip_Code;

# in order to know the number of victims in the year of 2016, we used the following query.
select Reported_year, Sex, Involvement, count(*) from kccrime where Sex RLIKE 'M|F|U' and Reported_year = '2016' and Involvement = 'VIC' group by Reported_year, Sex, Involvement;

# show the race of people who were suspect
select Race, count(*), Involvement from kccrime where Race RLIKE 'B|I|W' and Involvement = 'SUS' group by Race, Involvement;

select Sex, count(*) from kccrime  group by Sex;

