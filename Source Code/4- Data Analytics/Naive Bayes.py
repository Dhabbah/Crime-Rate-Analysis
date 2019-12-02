from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import col
import numpy as np
import os

os.environ["SPARK_HOME"] = "C:/spark-2.4.4-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "C:/winutils"

# Creating spark session
spark = SparkSession.builder.appName("Crime Analytics").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Loading the dataset
KCDPFinal = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load("C:/KCcrimeForAnalytics.csv").withColumnRenamed("Firearm_Used_Flag", "label")
KCDPFinal

# Create vector assembler for feature columns
VAssembler = VectorAssembler(inputCols=KCDPFinal.columns[1:19], outputCol="features")
KCDPFinal = VAssembler.transform(KCDPFinal)

# Split the crime dataset into training and testing data sets
trainingData, testingData = KCDPFinal.select("label", "features").randomSplit([0.7, 0.3])

# Using the training set for the model traning
from pyspark.ml.classification import NaiveBayes
NaiveBayesModel = NaiveBayes()
model = NaiveBayesModel.fit(trainingData)

# Generate prediction from test dataset
CrimepredKC = model.transform(testingData)

# Evuluate the accuracy of the model
evaluator = MulticlassClassificationEvaluator()
accuracy = evaluator.evaluate(CrimepredKC)

# Show model accuracy
print("Accuracy:", accuracy)