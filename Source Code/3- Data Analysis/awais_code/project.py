from pyspark.sql import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
import pdb
import calendar
import datetime
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit,col

def findDay(date):
    ref = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    born = datetime.datetime.strptime(date, '%d %m %Y').weekday()
    return ref[int(born)]





sc = SparkContext('local')
# spark = SparkSession(sc)
sql = SQLContext(sc)

sample_udf = udf(lambda x: findDay(x), StringType())

# df = sql.read.csv("KCcrime2010To2018.csv", inferSchema = True, header = True)
df = sql.read.csv("joined_date - Copy.csv", inferSchema = True, header = True)

df.createTempView(name='kc_crime')

pdb.set_trace()

#getting the number of crimes in 2010 count by month
df2 = sql.sql("select Reported_month, count(1) from kc_crime where Reported_year = '2010' group by Reported_month order by Reported_month")

#getting the maximum row of year 2010
df2 = sql.sql("select y.mnth,y.num from (select Reported_month as mnth, count(1) as num from kc_crime where Reported_year = '2010' group by Reported_month order by num DESC) as y limit 1")

#getting the maximum from each year
df2 = sql.sql("select y.yr,y.mnth,y.num from (select Reported_year as yr,Reported_month as mnth, count(1) as num from kc_crime group by Reported_year, Reported_month) as y")
df2.createTempView(name='temp')
df3 = sql.sql("select yr,mnth,num from temp where num = (select max(num) from temp as f where f.yr = temp.yr) group by yr,mnth,num order by yr")

sql.sql('drop table if exists temp')

#maximum crime by category in 2010
df2 = sql.sql("select y.dec,y.num from (select Description as dec, count(1) as num from kc_crime where Reported_year = '2010' group by Description order by num DESC) as y limit 1")

#maximum crime by category each year by category
df2 = sql.sql("select y.yr,y.dec,y.num from (select Reported_year as yr,Description as dec, count(1) as num from kc_crime group by Reported_year, Description) as y")
df2.createTempView(name='temp2')
df3 = sql.sql("select yr,dec,num from temp2 where num = (select max(num) from temp2 as f where f.yr = temp2.yr) group by yr,dec,num order by yr")


#count of specific crime in each year ordered by year
df2 = sql.sql("select y.yr,y.dec,y.num from (select Reported_year as yr,Description as dec, count(1) as num from kc_crime where Description = 'Burglary - Residence' group by Reported_year, Description) as y")


#joined date column
df3 = df.withColumn('joined_date',sf.concat(sf.col('Reported_day'),sf.lit(' '), sf.col('Reported_month'),sf.lit(' '),sf.col('Reported_year')))
df4 = df3.withColumn("week_day",sample_udf(col("joined_date")))

#maximum crime day of the week
df2 = sql.sql("select y.yr,y.wk,y.num from (select Reported_year as yr,week_day as wk, count(1) as num from kc_crime group by Reported_year, week_day) as y")
df2.createTempView(name='temp')
df3 = sql.sql("select yr,wk,num from temp where num = (select max(num) from temp as f where f.yr = temp.yr) group by yr,wk,num order by yr")



#maximum crime of each day of the year
df2 = sql.sql("select y.yr,y.wk,y.dec,y.num from (select Reported_year as yr,week_day as wk, Description as dec, count(1) as num from kc_crime group by Reported_year, week_day,dec) as y")
df2.createTempView(name='temp')
df3 = sql.sql("select yr,wk,dec,num from temp where num = (select max(num) from temp as f where f.yr = temp.yr and f.wk = temp.wk) group by yr,wk,dec,num order by yr")




###########################################################################
df2 = sql.sql("select y.Reported_year,y.Reported_month,MAX(y.num) from (select Reported_year,Reported_month,COUNT(*) as num From kc_crime group by Reported_year,Reported_month) y")

df2 = sql.sql("select y.yr,y.dsc,AVG(y.num) from (select Reported_year as yr,Description as dsc,COUNT(*) as num From kc_crime group by Reported_year,Description) y group by y.yr,y.dsc")

df4.toPandas().to_csv('joined_date.csv',index = False)

pdb.set_trace()