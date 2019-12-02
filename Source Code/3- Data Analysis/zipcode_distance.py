import sqlalchemy
import math
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta


db = sqlalchemy.create_engine('sqlite:///zipcodes.db')
db.echo = False

def build_metadata():
    metadata = sqlalchemy.MetaData(db)
    metadata.bind.echo = False
    metadata.bind.text_factory = str

    return metadata

def select_zipcode(zipcode):
    fields = (
        'zip',
        'city',
        'state',
        'lat',
        'long',
        'timezone',
        'dst',
        )
    metadata = build_metadata()
    zipcodes_table = sqlalchemy.Table('zipcodes', metadata, autoload=True)
    result = zipcodes_table.select(zipcodes_table.c.zip == zipcode)
    try:
        return dict(zip(fields,result.execute().fetchone()))
    except (TypeError, sqlalchemy.exc.OperationalError):
        return False


def distance(zipcode1, zipcode2):
    z1 = select_zipcode(zipcode1)
    z2 = select_zipcode(zipcode2)
    if not(z1) or not(z2):
        return (99999999)
    return haversine(z1['lat'], z1['long'], z2['lat'], z2['long'])

def long_lat(zipcode1):
    z1 = select_zipcode(zipcode1)
    if not(z1):
        return ["NA", "NA"]
    return [z1['lat'], z1['long']]

def haversine(lat1, long1, lat2, long2):
    radius = 3963.1676 #Radius of earth in miles
    lat1, long1, lat2, long2 = map(math.radians, [lat1, long1, lat2, long2])
    dlat = lat2 - lat1
    dlong = long2 - long1

    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlong/2) * math.sin(dlong/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d


def span(today, prev):
    m1, d1, y1 = [int(x) for x in (prev).split('/')]
    m2, d2, y2 = today.month,today.day,today.year
    prev1 = date(y1, m1, d1)
    today1 = date(y2, m2, d2)

    if today1<prev1:
        return True
    return False



import os
import ast
os.environ["SPARK_HOME"] = "D:\spark-2.4.4-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\winutils"
from operator import add
import pdb
vertex=1

from pyspark import SparkContext


from pyspark.sql.functions import udf

def temporary():
    days = 0
    person=[1750, 802, 302, 301, 402, 401, 2001, 201, 1198, 2655, 799, 101, 610, 1701, 2661, 2662, 1770, 3071, 3067, 2530, 3074, 3009]
    months = 6
    crime_person=0
    crime_prop=0
    years = 1
    isSafe=0
    Safety=0
    time=input("How many months worth of data do you want to see?: ")
    months=int(time)
    data = pd.read_csv("data_work.csv")
    for te, de in zip(data["Offense"].unique(), data["Description"].unique()):
        print(te," ", de)
    type=input("WHICH TYPE OF CRIME? enter number or x for all types: ")
    if type!='x':
        type=int(type)
    since = date.today() - relativedelta(days=days, months=months, years=years)
    threshold = int(input("Please enter the Radius: "))
    locality = input("Please enter the Location Zipcode: ")
    data = pd.read_csv("data_work.csv")
    records=0
    for x, y, crime in zip(data["Zip Code"], data["Reported_Date"], data["Offense"]):
        try:
            duration = span(since, y)
            dist = distance(int(locality), int(x))
            match=True if type=='x' else (type==int(crime))
            if (dist < threshold and duration and match):
                if int(crime) in person:
                    crime_person+=1
                    print("personal: ", crime_person)
                else:
                    crime_prop+=1
                    print("property: ", crime_prop)
                print("Distance in between " + str(locality) + " %.2f at date: " % dist, y)
                records+=1
                if records==1000:
                    break
        except IndexError:
            print("arguements are 2 US Zip Codes\nzipcode_distance zipcode1 zipcode2")
    print("Total of {} records",records)
    print("SAFETY LEVE HERE is: ", 100-((crime_person/records)*70)-((crime_prop/records)*30))


if __name__ == "__main__":


    sc = SparkContext.getOrCreate()
    import sys
    import datetime

    duration = udf(temporary())

