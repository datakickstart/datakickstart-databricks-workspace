# Databricks notebook source
import requests
import json
# import prettytable
headers = {'Content-type': 'application/json'}
area_code = '001042'
industry_code ='000000'
data_scientist_job_code = '152051'
software_developer_job_code = '151252'
database_architect_job_code = '151243'
all_job_code = '000000'
statistic_code='03'
area_codes = ['0038060','0031080','0041740','0041860','0041940','0019740','0033100','0036740','0012060','0016980','0026900',
#               '0048620','0031140','0035380','0012580','0019820','0033460','0028140','0041180','0029820','0016740','0018140',
#               '0017140','0017460','0036420','0038900','0037980','0038300','0016700','0032820','0012420','0019100','0026420',
#               '0041620','0042660','0044060','0033340'
             ]
job_codes = ['152051','151252','000000']
# series1 = f'OEUM{area_code}{industry_code}{data_scientist_job_code}{statistic_code}'
# series2 = f'OEUM{area_code}{industry_code}{software_developer_job_code}{statistic_code}'
# series3 = f'OEUM{area_code}{industry_code}{all_job_code}{statistic_code}'
# data = json.dumps({"seriesid": [series1, series2, series3],"startyear":"2021", "endyear":"2021"}
series_list = []
for area_code in area_codes:
    for job_code in job_codes:
        series_list.append(f'OEUM{area_code}{industry_code}{job_code}{statistic_code}')
    
data = json.dumps({"seriesid": series_list,"startyear":"2021", "endyear":"2021"})

p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)

print(json_data)


# COMMAND ----------

print(json_data['Results']['series']j)

# COMMAND ----------

# for series in json_data['Results']['series']:
#     x=prettytable.PrettyTable(["series id","year","period","value","footnotes"])
#     seriesId = series['seriesID']
#     for item in series['data']:
#         year = item['year']
#         period = item['period']
#         value = item['value']
#         footnotes=""
#         for footnote in item['footnotes']:
#             if footnote:
#                 footnotes = footnotes + footnote['text'] + ','
#         if 'M01' <= period <= 'M12':
#             x.add_row([seriesId,year,period,value,footnotes[0:-1]])
#     output = open(seriesId + '.txt','w')
#     output.write (x.get_string())
#     output.close

# COMMAND ----------


