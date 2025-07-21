import requests
import pandas as pd

#step 1: Extact data from the API

url= "https://restcountries.com/v3.1/all?fields=name,region,subregion,population,area"
response = requests.get(url)
data = response.json()

#step 2: Transform data into a DataFrame
records =[]
for country in data:
    records.append({
        'name': country.get('name', {}).get('common'),
        'region': country.get('region'),
        'subregion': country.get('subregion'),
        'population': country.get('population'),
        'area': country.get('area')
    })

df = pd.DataFrame(records)

#step 3: load data into the database
#load libraries required
#create the enginer
#push the dataframe to postgreSQL database
