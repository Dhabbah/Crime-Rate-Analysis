import pandas as pd
# load the datasets using pandas
KCPDDataset2010 = pd.read_csv("../1-Datasets/KCPD_Crime_Data/KCPD_Crime_Data_2010.csv")
KCPDDataset2011 = pd.read_csv("../1-Datasets/KCPD_Crime_Data/KCPD_Crime_Data_2011.csv")
KCPDDataset2012 = pd.read_csv("../1-Datasets/KCPD_Crime_Data/KCPD_Crime_Data_2012.csv")
KCPDDataset2013 = pd.read_csv("../1-Datasets/KCPD_Crime_Data/KCPD_Crime_Data_2013.csv")
KCPDDataset2014 = pd.read_csv("../1-Datasets/KCPD_Crime_Data/KCPD_Crime_Data_2014.csv")
KCPDDataset2015 = pd.read_csv("../1-Datasets/KCPD_Crime_Data/KCPD_Crime_Data_2015.csv")
KCPDDataset2016 = pd.read_csv("../1-Datasets/KCPD_Crime_Data/KCPD_Crime_Data_2016.csv")
KCPDDataset2017 = pd.read_csv("../1-Datasets/KCPD_Crime_Data/KCPD_Crime_Data_2017.csv")
KCPDDataset2018 = pd.read_csv("../1-Datasets/KCPD_Crime_Data/KCPD_Crime_Data_2018.csv")

# clean features and remove unwanted, rename the features in all datasets so that we can merge them together into one dataset
KCPDDataset2010.rename(columns = {'Reported Time':'Reported_Time', 'From Time':'From_Time', 'Zip Code':'Zip_Code', 'Firearm Used Flag  ':'Firearm_Used_Flag'}, inplace = True) 
DropKCPD2010 = KCPDDataset2010.drop(['Location 1'], axis=1)
KCPD2010 = DropKCPD2010[['Report_No','Reported_Date', 'Reported_Time','From_Date','From_Time','Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]

KCPDDataset2011.rename(columns = {'Reported Time':'Reported_Time', 'From Time':'From_Time', 'Zip Code':'Zip_Code', 'Firearm Used Flag  ':'Firearm_Used_Flag'}, inplace = True) 
DropKCPD2011 = KCPDDataset2011.drop(['Location 1'], axis=1)
KCPD2011 = DropKCPD2011[['Report_No','Reported_Date', 'Reported_Time','From_Date','From_Time','Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]

KCPDDataset2012.rename(columns = {'Reported Time':'Reported_Time', 'From Time':'From_Time', 'Zip Code':'Zip_Code', 'Firearm Used Flag  ':'Firearm_Used_Flag'}, inplace = True) 
DropKCPD2012 = KCPDDataset2012.drop(['Location 1'], axis=1)
KCPD2012 = DropKCPD2012[['Report_No','Reported_Date', 'Reported_Time','From_Date','From_Time','Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]

KCPDDataset2013.rename(columns = {'Reported Time':'Reported_Time', 'From Time':'From_Time', 'Zip Code':'Zip_Code', 'Firearm Used Flag  ':'Firearm_Used_Flag'}, inplace = True)
new = KCPDDataset2013["Location 1"].str.split("\n", n = 2, expand = True) 
KCPDDataset2013['Address'] = new[0]
KCPDDataset2013['City'] = 'KANSAS CITY'
KCPD2013 = KCPDDataset2013[['Report_No','Reported_Date', 'Reported_Time','From_Date','From_Time', 'Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]
KCPD2013 = KCPDDataset2013.drop(['Location 1'], axis=1)
KCPD2013.drop(KCPD2013.loc[KCPD2013['Sex'] == '*'].index, inplace=True)


KCPDDataset2014.rename(columns = {'Reported Time':'Reported_Time', 'From Time':'From_Time', 'Zip Code':'Zip_Code', 'Firearm Used Flag  ':'Firearm_Used_Flag'}, inplace = True) 
DropKCPD2014 = KCPDDataset2014.drop(['Location 1'], axis=1)
KCPD2014 = DropKCPD2014[['Report_No','Reported_Date', 'Reported_Time','From_Date','From_Time', 'Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]

KCPDDataset2015.rename(columns = {'Reported Time':'Reported_Time', 'From Time':'From_Time', 'Zip Code':'Zip_Code', 'Firearm Used Flag':'Firearm_Used_Flag'}, inplace = True) 
DropKCPD2015 = KCPDDataset2015.drop(['Latitude', 'Longitude', 'Location 1'], axis=1)
KCPD2015 = DropKCPD2015[['Report_No','Reported_Date', 'Reported_Time','From_Date','From_Time', 'Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]


KCPDDataset2016.rename(columns = {'Reported Time':'Reported_Time', 'From Time':'From_Time', 'Zip Code':'Zip_Code', 'Firearm Used Flag':'Firearm_Used_Flag'}, inplace = True) 
DropKCPD2016 = KCPDDataset2016.drop(['Latitude', 'Longitude','Location 1'], axis=1)
KCPD2016 = DropKCPD2016[['Report_No','Reported_Date', 'Reported_Time','From_Date','From_Time','Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]

KCPDDataset2017.rename(columns = {'Reported Time':'Reported_Time', 'From Time':'From_Time', 'Zip Code':'Zip_Code', 'Firearm Used Flag':'Firearm_Used_Flag'}, inplace = True) 
DropKCPD2017 = KCPDDataset2017.drop(['Location'], axis=1)
KCPD2017 = DropKCPD2017[['Report_No','Reported_Date', 'Reported_Time','From_Date','From_Time','Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]
KCPD2017.drop(KCPD2017.loc[KCPD2017['Sex'] == '*'].index, inplace=True)

KCPDDataset2018.rename(columns = {'Reported Time':'Reported_Time', 'From Time':'From_Time', 'Zip Code':'Zip_Code', 'Firearm Used Flag':'Firearm_Used_Flag'}, inplace = True) 
DropKCPD2018 = KCPDDataset2018.drop(['Location'], axis=1)
KCPD2018 = DropKCPD2018[['Report_No','Reported_Date', 'Reported_Time','From_Date','From_Time','Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]

# merge all the datasets into one object
KCDPFinal = pd.concat([KCPD2010, KCPD2011, KCPD2012,KCPD2013, KCPD2014, KCPD2015, KCPD2016, KCPD2017, KCPD2018], ignore_index=True, join="inner")

# display the numbers of columns and rows
print("The number of columns and rows before cleaning:")
print(KCDPFinal.shape)

# display its info
print("\nThe dataset info\n")
KCDPFinal.info()

# Clean the dataset
KCDPFinal['City'] = 'KANSAS CITY'
KCDPFinal.Sex.fillna('U', inplace=True)
KCDPFinal.Race.fillna('U', inplace=True)
KCDPFinal.Age.fillna(round(KCDPFinal.Age.mean()), inplace = True)
KCDPFinal = KCDPFinal[KCDPFinal.IBRS.notnull()]
KCDPFinal = KCDPFinal[KCDPFinal.Rep_Dist.notnull()]
KCDPFinal = KCDPFinal[KCDPFinal.From_Time.notnull()]
KCDPFinal = KCDPFinal[KCDPFinal.From_Date.notnull()]
KCDPFinal = KCDPFinal[KCDPFinal.Beat.notnull()]
KCDPFinal = KCDPFinal[KCDPFinal.Area.notnull()]
KCDPFinal = KCDPFinal[KCDPFinal.Zip_Code.notnull()]

# To show the numbers of null values
print("\n Display the null values in each feature\n")
KCDPNulls = pd.DataFrame(KCDPFinal.isnull().sum().sort_values(ascending=False)[:25])
KCDPNulls.columns = ['Null-Values-Count']
KCDPNulls.index.name = 'Features'
print(KCDPNulls)

# Split the data and time into multiple columns
# Reported date
KCDPFinal['Reported_year'] = pd.DatetimeIndex(KCDPFinal['Reported_Date']).year
KCDPFinal['Reported_month'] = pd.DatetimeIndex(KCDPFinal['Reported_Date']).month
KCDPFinal['Reported_day'] = pd.DatetimeIndex(KCDPFinal['Reported_Date']).day

# From_Date
KCDPFinal['From_year'] = pd.DatetimeIndex(KCDPFinal['From_Date']).year
KCDPFinal['From_month'] = pd.DatetimeIndex(KCDPFinal['From_Date']).month
KCDPFinal['From_day'] = pd.DatetimeIndex(KCDPFinal['From_Date']).day

# drop the original one 
KCDPFinal = KCDPFinal.drop(['Reported_Date', 'From_Date'], axis=1)

KCDPFinal = KCDPFinal[['Report_No','Reported_year', 'Reported_month', 'Reported_day', 'Reported_Time','From_year', 'From_month', 'From_day', 'From_Time', 'Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]

KCDFinalReported_Time = KCDPFinal["Reported_Time"].str.split(":", n = 2, expand = True) 
KCDPFinal['Reported_hour'] = KCDFinalReported_Time[0]
KCDPFinal['Reported_minute'] = KCDFinalReported_Time[1]

KCDFinalFrom_Time = KCDPFinal["From_Time"].str.split(":", n = 2, expand = True) 
KCDPFinal['From_hour'] = KCDFinalFrom_Time[0]
KCDPFinal['From_minute'] = KCDFinalFrom_Time[1]

KCDPFinal = KCDPFinal[['Report_No','Reported_year', 'Reported_month', 'Reported_day', 'Reported_hour', 'Reported_minute', 'From_year', 'From_month', 'From_day', 'From_hour', 'From_minute', 'Offense','IBRS','Description','Beat','Address','City','Zip_Code','Rep_Dist','Area','DVFlag','Invl_No','Involvement','Race','Sex','Age', 'Firearm_Used_Flag']]

# display the numbers of columns and rows
print("The number of columns and rows after cleaning:")
print(KCDPFinal.shape)

print("Saving the file..")
KCDPFinal.to_csv (r'../1-Datasets/KCPD_Crime_Data/KCcrime2010To2018.csv', index = None, header=True) 