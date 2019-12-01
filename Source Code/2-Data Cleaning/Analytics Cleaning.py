import pandas as pd
# load the datasets using pandas
KCDPFinal = pd.read_csv("../1-Datasets/KCPD_Crime_Data/KCcrime2010To2018.csv")

from sklearn import preprocessing

#Performing Lable Encoding
label_encoder = preprocessing.LabelEncoder()
KCDPFinal['Firearm_Used_Flag'] = label_encoder.fit_transform(KCDPFinal['Firearm_Used_Flag'])
KCDPFinal['Involvement'] = label_encoder.fit_transform(KCDPFinal['Involvement'])
KCDPFinal['Sex'] = label_encoder.fit_transform(KCDPFinal['Sex'])
KCDPFinal['Sex'] = label_encoder.fit_transform(KCDPFinal['Sex'])
KCDPFinal['Race'] = label_encoder.fit_transform(KCDPFinal['Race'])
KCDPFinal['Area'] = label_encoder.fit_transform(KCDPFinal['Area'])
KCDPFinal['DVFlag'] = label_encoder.fit_transform(KCDPFinal['DVFlag'])
KCDPFinal['DVFlag'] = label_encoder.fit_transform(KCDPFinal['DVFlag'])

KCDPFinal['Reported_hour'] = KCDPFinal['Reported_hour'].astype(str).astype(int)
KCDPFinal['Reported_minute'] = KCDPFinal['Reported_minute'].astype(str).astype(int)
KCDPFinal['From_hour'] = KCDPFinal['From_hour'].astype(str).astype(int)
KCDPFinal['From_minute'] = KCDPFinal['From_minute'].astype(str).astype(int)
KCDPFinal['Zip_Code'] = KCDPFinal['Zip_Code'].astype('int64')
KCDPFinal['Age'] = KCDPFinal['Age'].astype('int64')


KCDPFinal = KCDPFinal[['Report_No','Reported_year', 'Reported_month', 'Reported_day', 'Reported_hour', 'Reported_minute', 'From_year', 'From_month', 'From_day', 'From_hour', 'From_minute', 'Offense', 'Zip_Code','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]

KCDPFinal.to_csv (r'../1-Datasets/KCPD_Crime_Data/KCcrimeForAnalytics.csv', index = None, header=True) 

