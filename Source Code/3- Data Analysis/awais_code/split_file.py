from random import randint
import time

"""
This is use for create 30 file one by one in each 5 seconds interval. 
These files will store content dynamically from 'lorem.txt' using below code
"""

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

import pandas as pd
import numpy as np

sc = SparkContext('local')
# spark = SparkSession(sc)
sql = SQLContext(sc)

def main():
    a = 1
    # df = pd.read_csv('C:\\Big_Data_Hadoop\\second_increment\\joined_date.csv')
    df = sql.read.csv("joined_date - Copy.csv", inferSchema = True, header = True)
    df.createTempView(name='kc_crime')
    year_list = np.array(df.select('Reported_year').collect())
    crime_list = np.array(df.select('Description').collect())
    # crime_list = df['Description'].unique().tolist()
    count = 0
    for year in year_list:
        year = year.item()
        for crime in crime_list:
            try:
                crime = crime.item()

                print('creating file %s_%s.csv' %(str(year),str(crime)))
                query = "select * from kc_crime where Reported_year = '%s' and Description = '%s'" %(str(year),str(crime))
                temp_df = sql.sql(query)
                print('length of temp_df: ',temp_df.count())
                temp_df.toPandas().to_csv('csv\\%s_%s.csv' %(str(year),str(crime)),index=False)
                count+=1
                if count == 5:
                    break
            except:
                pass
        if count == 5:
            break
        time.sleep(5)
    # with open('lorem.txt', 'r') as file:  # reading content from 'lorem.txt' file
    #     lines = file.readlines()
    #     while a <= 30:
    #         totalline = len(lines)
    #         linenumber = randint(0, totalline - 10)
    #         with open('log/log{}.txt'.format(a), 'w') as writefile:
    #             writefile.write(' '.join(line for line in lines[linenumber:totalline]))
    #         print('creating file log{}.txt'.format(a))
    #         a += 1
    #         time.sleep(5)


if __name__ == '__main__':
    main()
