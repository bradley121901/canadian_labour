
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode


#Loading datasets from GitHub raw urls

code_url = "https://raw.githubusercontent.com/bradley121901/canadian_labour/main/2023-CSV/LFS_PUMF_EPA_FGMD_codebook.csv"
jan_url = "https://raw.githubusercontent.com/bradley121901/canadian_labour/main/2023-CSV/pub0123.csv"
may_url = "https://raw.githubusercontent.com/bradley121901/canadian_labour/main/2023-CSV/pub0523.csv"
sept_url = "https://raw.githubusercontent.com/bradley121901/canadian_labour/main/2023-CSV/pub0923.csv"


# Loading the codebook numeral to english value translations

codedf = pd.read_csv(code_url)


# Loading datasets

jandf = pd.read_csv(jan_url)
maydf = pd.read_csv(may_url)
septdf = pd.read_csv(sept_url)


# Merge datasets
surveydf = pd.concat([jandf, maydf, septdf], ignore_index=True)
surveydf['REC_NUM']= range(1, len(surveydf) + 1)
surveydf


# Create dictionary where key is the column to be translated, and value is a tuple storing the numeral to english mapping
# The first item of the tuple is the numerical value
# The second item of the tuple is the english translation

d = {}
temp = None
for index, row in codedf.iterrows():
    if pd.notnull(row['Position_Position']):
        temp = row['Variable_Variable']
        d[temp] = {}
    else:
        if '-' not in str(row['Variable_Variable']):
            if row['Variable_Variable'] != "0" and row['Variable_Variable'] != 0:
                d[temp][row['Variable_Variable'].lstrip('0')] = row['EnglishLabel_EtiquetteAnglais']
            else:
                d[temp][row['Variable_Variable']] = row['EnglishLabel_EtiquetteAnglais']
        else:
            d[temp]["blank"] = "blank"

mapping_dict = {key: value for key, value in d.items() if value != {'blank': 'blank'} and value !={'blank': 'Not applicable'}}
print(mapping_dict)


# Apply english mapping conversions to dataset

for c_name in surveydf.columns:
    if c_name.lower() in mapping_dict:
        if surveydf[c_name].dtype == 'float64':
            surveydf[c_name] = pd.to_numeric(surveydf[c_name], errors='coerce').astype('Int64')  # Convert to nullable integer
        surveydf[c_name] = surveydf[c_name].astype(str)
        surveydf[c_name] = surveydf[c_name].map(mapping_dict[c_name.lower()])

surveydf


# Converting to dollar digits
surveydf['HRLYEARN'] = surveydf['HRLYEARN'] * 0.01
surveydf['HRLYEARN']


# Convert hours worked per week to correct decimal place
surveydf['UTOTHRS'] = surveydf['UTOTHRS']*0.1
surveydf['UTOTHRS']


# creating new feature weekly income from hourly salary with usual hours worked
surveydf['WEEKINCOME'] = surveydf['HRLYEARN'] * surveydf['UTOTHRS']
surveydf['WEEKINCOME']


surveydf['ANNUALINCOME'] = surveydf['WEEKINCOME'] * 52
surveydf['ANNUALINCOME']


# dictionary to map weekly income to provincial tax bracket
tax_brackets_province = {
            "Newfoundland and Labrador": {(0, 702): 1, (703, 1404): 2, (1405, 2229): 3, (2230, float('inf')): 4},
            "Prince Edward Island": {(0, 732): 1, (733, 1465): 2, (1466, 2140): 3, (2141, float('inf')): 4},
            "Nova Scotia": {(0, 749): 1, (750, 1498): 2, (1499, 2915): 3, (2916, float('inf')): 4},
            "New Brunswick": {(0, 738): 1, (739, 1476): 2, (1477, 2293): 3, (2294, float('inf')): 4},
            "Quebec": {(0, 704): 1, (705, 1408): 2, (1409, 2181): 3, (2182, float('inf')): 4},
            "Ontario": {(0, 787): 1, (788, 1574): 2, (1575, 2885): 3, (2886, float('inf')): 4},
            "Manitoba": {(0, 740): 1, (741, 1482): 2, (1483, 2144): 3, (2145, float('inf')): 4},
            "Saskatchewan": {(0, 745): 1, (746, 1490): 2, (1491, 2173): 3, (2174, float('inf')): 4},
            "Alberta": {(0, 734): 1, (735, 1470): 2, (1471, 2107): 3, (2108, float('inf')): 4},
            "British Columbia": {(0, 775): 1, (776, 1550): 2, (1551, 2250): 3, (2251, float('inf')): 4}}

def get_tax_bracket(province, salary):
    province_tax_brackets = tax_brackets_province.get(province)
    if province_tax_brackets:
        for x, y in province_tax_brackets.items():
            if x[0] <= salary <= x[1]:
                return y
    else:
        return None

surveydf['TAXBRAC'] = surveydf.apply(lambda x: get_tax_bracket(x['PROV'], x['WEEKINCOME']), axis=1)
surveydf['TAXBRAC']


survey_desc_fill = ["YABSENT", "PROV", "CMA", "AGE_6","AGE_12","SEX", 'MARSTAT', 'EDUC', 'SCHOOLN', 'IMMIG', 'EFAMTYPE', 'AGYOWNK', 'TAXBRAC', 'SURVYEAR','SURVMNTH', 'NAICS_21', 'NOC_10', 'NOC_43', 'ESTSIZE', 'FIRMSIZE', 'WEEKINCOME', 'PERMTEMP']
for column in survey_desc_fill:
    # Calculate the most frequent value
    mode_value = surveydf[column].mode()[0]
    # Replace NaN with the most frequent value
    surveydf[column].fillna(mode_value, inplace=True)


survey_num_fill = ['LFSSTAT', 'HRLYEARN', 'UHRSMAIN', 'AHRSMAIN', 'UTOTHRS', 'ATOTHRS', 'HRSAWAY', 'WKSAWAY', 'PAIDOT', 'XTRAHRS', 'DURUNEMP', 'TENURE', 'PREVTEN', 'ANNUALINCOME']
for column in survey_num_fill:
                # Calculate average of numerical values in the column
                avg_value = surveydf[column].mean()
                # Replace NaN with the average value
                surveydf[column].fillna(avg_value, inplace=True)


surveydf


labour_force_fact = surveydf[['REC_NUM', 'LFSSTAT', 'HRLYEARN', 'UHRSMAIN', 'AHRSMAIN', 'UTOTHRS', 'ATOTHRS', 'HRSAWAY', 'WKSAWAY', 'PAIDOT', 'XTRAHRS', 'YABSENT', 'DURUNEMP', 'TENURE', 'PREVTEN', 'WEEKINCOME', 'ANNUALINCOME']]
date = surveydf[[ 'SURVYEAR','SURVMNTH']]
geographic = surveydf[[ 'PROV', 'CMA']]
demographic = surveydf[[ 'AGE_12', 'AGE_6', 'SEX', 'MARSTAT', 'EDUC', 'SCHOOLN', 'IMMIG', 'EFAMTYPE', 'AGYOWNK', 'TAXBRAC']]
job = surveydf[['NAICS_21', 'NOC_10', 'NOC_43', 'MJH', 'PERMTEMP', 'ESTSIZE', 'FIRMSIZE', 'UNION','WHYPT']]
unemployed = surveydf[['PRIORACT','FLOWUNEM', 'LKPUBAG', 'LKATADS', 'WHYLEFTO', 'WHYLEFTN','YNOLOOK', 'UNEMFTPT']]



def suggorate_key_pipeline(df, key_name):
            df = df.drop_duplicates().copy()
            df[key_name]= range(1, len(df) + 1)
            columns_except_key_name = [col for col in df.columns if col != key_name]
            labour_with_key = pd.merge(surveydf, df, on=columns_except_key_name, how='left')
            labour_with_key.drop(columns_except_key_name, axis=1, inplace=True)
            labour_force_fact.loc[:, key_name] = labour_with_key[key_name].values
            return df


date = suggorate_key_pipeline(date, "DATE_KEY")
geographic = suggorate_key_pipeline(geographic, "GEOGRAPHIC_KEY")
demographic = suggorate_key_pipeline(demographic, "DEMOGRAPHIC_KEY")
job = suggorate_key_pipeline(job, "JOB_KEY")
unemployed = suggorate_key_pipeline(unemployed, "UNEMPLOYED_KEY")


unemployed['WHYLEFT'] = unemployed['WHYLEFTO'].fillna(unemployed['WHYLEFTN'])
unemployed.drop(['WHYLEFTO', 'WHYLEFTN'], axis=1, inplace=True)


# Handle NaN in Unemployment Dimension
# Fill the first NaN row with 'Not Applicable'
unemployed.iloc[0] = 'Not Applicable'
# Drop the rest of NaN rows
unemployed.dropna(how='all', inplace=True)
# Fill remaining NaN
unemployed.fillna("Not Applicable", inplace=True)


# Elimate excessive mutual exclusive nulls within AGE_6 and AGE_12 columns by combining into one column
demographic['AGE'] = demographic['AGE_12'].fillna(demographic['AGE_6'])
demographic.drop(['AGE_12', 'AGE_6'], axis=1, inplace=True)



unemployed['UNEMPLOYED_KEY'] = range(1, len(unemployed) + 1)
unemployed = unemployed.rename(columns={'UNEMPLOYED_KEY': 'Unemployment Key', 'PRIORACT': 'Main activity before looking for work', 'YNOLOOK':'Reason for not looking for work', 'LKATADS':'Unemployed looked at job ads', 'LKPUBAG':'Unemployed used public employment agency', 'FLOWUNEM':'Flows into unemployment', 'WHYLEFT':'Reason for job leave','UNEMFTPT': 'Job seeking status'})
unemployed

# Handle NaN in Job Dimension
job.fillna("Not Applicable", inplace=True)


job = job.rename(columns={'JOB_KEY':'Job Key', 'NAICS_21':'Main Job Industry', 'NOC_10':'Employment Class', 'NOC_43':'Main Job Occupation', 'MJH': 'Single or Multiple Jobs', 'PERMTEMP':'Job Permanency Status', 'FIRMSIZE':'Firm Size', 'ESTSIZE':'Establishment Size','UNION':'Union Status','WHYPT':'Part Time Work Reason'})

job


demographic = demographic.rename(columns={'DEMOGRAPHIC_KEY':'Demographic Key', 'AGE':'Age Group', 'SEX': 'Sex', 'MARSTAT':'Martial Status', 'EDUC':'Highest Education Attained', 'SCHOOLN':'Student Status', 'IMMIG':'Immigration Status', 'EFAMTYPE':'Economic Family', 'AGYOWNK':'Youngest Child Age', 'TAXBRAC':'Tax Bracket'})
demographic



date = date.rename(columns={'DATE_KEY':'Date Key', 'SURVMNTH': 'Month', 'SURVYEAR':'Year'})
date


geographic = geographic.rename(columns={'GEOGRAPHIC_KEY':'Geographic Key', 'PROV': 'Province', 'CMA':'Census Metropolitan Area'})
geographic


labour_force_fact = labour_force_fact.rename(columns={'REC_NUM':'Labour Force Key', 'DATE_KEY': 'Date Key', 'GEOGRAPHIC_KEY':'Geographic Key', 'DEMOGRAPHIC_KEY':'Demographic Key','JOB_KEY':'Job Key', 'UNEMPLOYED_KEY': 'Unemployment Key', 'LFSSTAT':'Labour Force Status', 'HRLYEARN': 'Usual Hourly Wages', 'UHRSMAIN': 'Usual Hours worked per week at main job', 'AHRSMAIN':'Actual Hours worked per week at main job', 'ATOTHRS':'Actual Hours worked per week at all jobs', 'UTOTHRS':'Usual Hours worked per week at all jobs', 'HRSAWAY':'Hours away from work','PAIDOT':'Paid overtime hours in reference week', 'XTRAHRS':'Number of overtime or extra hours worked', 'WKSAWAY':'Numbers of weeks absent from work', 'YABSENT':'Reason of absence','PREVTEN':'Previous job tenure','TENURE':'Current job tenure','DURUNEMP':'Duration of unemployment', 'WEEKINCOME':'Weekly income', 'ANNUALINCOME': 'Annual income'})
labour_force_fact

config = {
      'host':'localhost',
      'user':'root',
      'password':'CSI4142A',
      'database':'unemployement_data'
}
try:
    conn = mysql.connector.connect(**config)
    print("Connection established")
    cursor = conn.cursor()

    #cursor.execute("DROP TABLE IF EXISTS LABOUR_FORCE_FACT;")

    for index, row in date.iterrows():
        row = row.where(pd.notnull(row), None)
        cursor.execute("INSERT INTO DATE_DIMENSION (Year, Month, `Date Key`) VALUES (%s, %s, %s)",
               (row.Year, row.Month, row['Date Key']))

    print("ddone")
    conn.commit()

    for index, row in geographic.iterrows():
        row = row.where(pd.notnull(row), None)
        cursor.execute("INSERT INTO GEOGRAPHIC_DIMENSION (Province, `Census Metropolitan Area`, `Geographic Key`) VALUES (%s, %s, %s)",
               (row.Province, row['Census Metropolitan Area'], row['Geographic Key']))

    print("gdone")
    conn.commit()

    for index, row in job.iterrows():
        row = row.where(pd.notnull(row), None)
        cursor.execute("INSERT INTO JOB_DIMENSION (`Main Job Industry`, `Employment Class`, `Main Job Occupation`, `Establishment Size`, `Firm Size`, `Part Time Work Reason`, `Union Status`,  `Job Permanency Status`, `Single or Multiple Jobs`,`Job Key`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
               (row['Main Job Industry'], row['Employment Class'], row['Main Job Occupation'], row['Establishment Size'], row['Firm Size'], row['Part Time Work Reason'], row['Union Status'], row['Job Permanency Status'],row['Single or Multiple Jobs'],row['Job Key']))

    print("jdone")
    conn.commit()

    for index, row in unemployed.iterrows():
        row = row.where(pd.notnull(row), None)
        cursor.execute("INSERT INTO UNEMPLOYED_DIMENSION (`Main activity before looking for work`, `Flows into unemployment`, `Unemployed used public employment agency`, `Unemployed looked at job ads`, `Reason for job leave`, `Reason for not looking for work`, `Job seeking status`, `Unemployment Key`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
               (row['Main activity before looking for work'], row['Flows into unemployment'], row['Unemployed used public employment agency'], row['Unemployed looked at job ads'], row['Reason for job leave'], row['Reason for not looking for work'], row['Job seeking status'], row['Unemployment Key']))

    print("udone")
    conn.commit()

    for index, row in demographic.iterrows():
        row = row.where(pd.notnull(row), None)
        cursor.execute("INSERT INTO DEMOGRAPHIC_DIMENSION (`Sex`, `Martial Status`, `Highest Education Attained`, `Immigration Status`, `Student Status`,`Economic Family`, `Youngest Child Age`, `Age Group`, `Tax Bracket`, `Demographic Key`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
               (row['Sex'], row['Martial Status'], row['Highest Education Attained'], row['Immigration Status'], row['Student Status'],row['Economic Family'], row['Youngest Child Age'], row['Age Group'], row['Tax Bracket'], row['Demographic Key']))

    print("demo")
    conn.commit()

    for index, row in labour_force_fact.iterrows():
        row = row.where(pd.notnull(row), None)
        cursor.execute("INSERT INTO LABOUR_FORCE_FACT (`Labour Force Key`, `Labour Force Status`, `Usual Hourly Wages`, `Usual Hours worked per week at main job`, `Actual Hours worked per week at main job`, `Usual Hours worked per week at all jobs`, `Actual Hours worked per week at all jobs`, `Hours away from work`, `Numbers of weeks absent from work`, `Paid overtime hours in reference week`, `Number of overtime or extra hours worked`, `Reason of absence`, `Duration of unemployment`, `Current job tenure`, `Previous job tenure`, `Weekly income`, `Annual income`,`Date Key`, `Geographic Key`, `Demographic Key`, `Job Key`, `Unemployment Key`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
               (row['Labour Force Key'], row['Labour Force Status'], row['Usual Hourly Wages'], row['Usual Hours worked per week at main job'], row['Actual Hours worked per week at main job'], row['Usual Hours worked per week at all jobs'], row['Actual Hours worked per week at all jobs'], row['Hours away from work'], row['Numbers of weeks absent from work'], row['Paid overtime hours in reference week'], row['Number of overtime or extra hours worked'], row['Reason of absence'], row['Duration of unemployment'], row['Current job tenure'], row['Previous job tenure'], row['Weekly income'],row['Annual income'],row['Date Key'], row['Geographic Key'], row['Demographic Key'], row['Job Key'], row['Unemployment Key']))

        if index % 10000 == 0:
            print(index)
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with the user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)



