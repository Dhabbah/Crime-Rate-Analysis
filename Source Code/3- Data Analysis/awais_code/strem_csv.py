import sys
import os
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pdb
from pyspark.sql import Row
import calendar
import numpy as np

# os.environ["SPARK_HOME"] = "D:\\spark-2.3.1-bin-hadoop2.7\\"
# os.environ["HADOOP_HOME"]="D:\\Drive\\winutils"

sc = SparkContext(appName="PysparkStreaming")
ssc = StreamingContext(sc, 20)   #Streaming will execute in each 3 seconds
sql = SQLContext(sc)

"""
This is use for create streaming of text from txt files that creating dynamically 
from files.py code. This spark streaming will execute in each 3 seconds and It'll
 show number of words count from each files dynamically
"""
ref = ['Report_No', 'Reported_year', 'Reported_month', 'Reported_day', 'Reported_hour', 'Reported_minute', 'From_year', 'From_month', 'From_day', 'From_hour', 'From_minute', 'Offense','IBRS', 'Description', 'Beat', 'Address', 'City', 'Zip_Code', 'Rep_Dist', 'Area', 'DVFlag', 'Invl_No', 'Involvement', 'Race', 'Sex', 'Age', 'Firearm_Used_Flag', 'joined_date', 'week_day']


def f(x):
    print(x)

def savetheresult( rdd ):
    if not rdd.isEmpty():
        try:
            try:
                week_ref = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sudany']
                df = sql.createDataFrame(rdd,ref)
                df.createTempView(name='kc_crime')
                df = sql.sql("select * from kc_crime where Report_No!='Report_No'")
                sql.sql('drop table if exists kc_crime')
                df.createTempView(name='kc_crime')
            except:
                print('Error in first')
                pdb.set_trace()


            # df.toPandas().to_csv('testing_dfs\\testing_dataframe.csv',index=False)
            try:
                crime_count = df.count()
                df_p = df.toPandas()
                file_year = list(np.array(df.select('Reported_year').collect()))
                file_name = list(np.array(df.select('Description').collect()))
                file_year = file_year[0][0]
                file_name = file_name[0][0]
            except:
                print('Error in Second')
                pdb.set_trace()

            try:
                df2 = sql.sql("select Reported_month, count(1) from kc_crime group by Reported_month order by Reported_month")
                df3 = sql.sql("select Sex, count(1) from kc_crime group by Sex")
                df4 = sql.sql("select Involvement,count(1) from kc_crime group by Involvement")

                month_count = list(np.array(df2.select('count(1)').collect()))
                month_num = list(np.array(df2.select('Reported_month').collect()))

                sx_list = list(np.array(df3.select('Sex').collect()))
                sx_count = list(np.array(df3.select('count(1)').collect()))

                invlv_ref = list(np.array(df4.select('Involvement').collect()))
                invlv_count = list(np.array(df4.select('count(1)').collect()))
            except:
                print('Error in third')
                pdb.set_trace()

            f = open('testing_dfs\\%s_%s.txt' %(file_year,file_name),'w+' )

            f.write('Total Crimes in the Year %s with categroy %s is %s \n' %(file_year,file_name,str(crime_count)))
            for i in range(3):
                f.write('\n')
            f.write('Crimes Count by Months\n')

            for each in month_num:
                try:
                    month_index = int(each[0])
                    mnth = calendar.month_name[month_index]
                    f.write('Count of Crime in %s is: %s \n' %(mnth,str(month_count[month_index-1][0])))
                except:
                    print('Error in Months')
                    pdb.set_trace()

            for i in range(3):
                f.write('\n')


            for i in range(len(sx_list)):
                try:
                    sx_ref = {'M':'Males','F':'Females','U':'Undefined'}
                    sx = sx_ref[sx_list[i][0]]
                    f.write('Count of Crime involving %s is: %s \n' %(sx,str(sx_count[i][0])))
                except:
                    print('Error in Sex')
                    pdb.set_trace()

            for i in range(3):
                f.write('\n')


            for i in range(len(invlv_ref)):
                try:
                    invlv_ref_list = {'VIC': 'Victims', 'SUS': 'Suspects', 'ARR': 'Arrests'}
                    involvement = invlv_ref_list[invlv_ref[i][0]]
                    f.write('Count of %s: %s \n' %(involvement,invlv_count[i][0]))
                except:
                    print('Error in involvement')
                    pdb.set_trace()
            f.close()
            sql.sql('drop table if exists kc_crime')
            # return df
        except:
            pass

    # return None


def main():
    # sc = SparkContext(appName="PysparkStreaming")
    # ssc = StreamingContext(sc, 3)   #Streaming will execute in each 3 seconds
    # pdb.set_trace()
    lines = ssc.textFileStream('csv')  #'log/ mean directory name
    # lines = ssc.socketTextStream("localhost", 9999)
    # counts = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    counts = lines.map(lambda line: Row(line))
    counts = lines.map( lambda x: x.split(","))

    # character_counts = lines.flatMap(lambda line: line).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    # count_a = lines.flatMap(lambda x: x).map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b )
    # print(lines)
    # print('Type fo counts: ',type(counts))
    # try:
    #     counts.foreachRDD(f)
    # except:
    #     pass
    counts.foreachRDD(savetheresult)
    # try:
    #     if df is not None:
    #         pass
    # except:
    #     pass

    counts.pprint()

    # count_a.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
