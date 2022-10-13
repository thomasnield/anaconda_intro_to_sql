import sqlite3
import pandas as pd
import urllib.request

# download SQLite database and connect to it 
urllib.request.urlretrieve("https://github.com/thomasnield/anaconda_intro_to_sql/blob/main/company_operations.db?raw=true", "company_operations.db")
conn = sqlite3.connect('company_operations.db')

import numpy as np
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

np.random.seed(7)

def random_dates(start, end, n, unit='D'):
    ndays = (end - start).days + 1
    return start + pd.to_timedelta(
        np.random.randint(0, ndays, n), unit=unit
    )

def weighted_coin(n, p): 
  return (np.random.uniform(0.0, 1.0, n) <= p).astype(int)

dates1 = random_dates(pd.to_datetime('2021-02-01'), pd.to_datetime('2021-05-30'), 1000)
n1 = len(dates1)

location_id = np.random.randint(0,50,n1)
overcast = weighted_coin(n1, .5)
rain = overcast * np.round(weighted_coin(n1,.6) * np.random.normal(2, 1., n1),2) 
lightning = overcast * weighted_coin(n1,.5)
hail = overcast * weighted_coin(n1, .1)
tornado = hail * weighted_coin(n1, 1.0)

import random, string

df =  pd.DataFrame({
    "REPORT_CODE": [''.join(random.choices(string.ascii_uppercase + string.digits, k=7)) for _ in range(0,n1) ],
    "REPORT_DATE" : dates1,
    "LOCATION_ID" : location_id,
    "TEMPERATURE" : np.round(np.random.normal(60, 4, n1),1),
    "OVERCAST" : overcast,
    "RAIN" : rain,
    "SNOW" : 0,
    "LIGHTNING" : lightning,
    "HAIL" : hail,
    "TORNADO" : tornado
})

dates2 = random_dates(pd.to_datetime('2020-11-01'), pd.to_datetime('2021-01-30'), 1000)
n2 = len(dates2)
snow = overcast * np.round(weighted_coin(n1,.6) * np.random.normal(2, 1., n1),2) 

df2 = pd.DataFrame({
    "REPORT_CODE": [''.join(random.choices(string.ascii_uppercase + string.digits, k=7)) for _ in range(0,n2) ],
    "REPORT_DATE" : dates2,
    "LOCATION_ID" : location_id,
    "TEMPERATURE" : np.round(np.random.normal(30, 4, n2),1),
    "OVERCAST" : overcast,
    "RAIN" : rain,
    "SNOW" : snow,
    "LIGHTNING" : 0,
    "HAIL" : 0,
    "TORNADO" : 0
})

dates3 = random_dates(pd.to_datetime('2021-06-01'), pd.to_datetime('2021-10-31'), 1000)
n3 = len(dates3)

df3 = pd.DataFrame({
    "REPORT_CODE": [''.join(random.choices(string.ascii_uppercase + string.digits, k=7)) for _ in range(0,n3) ],
    "REPORT_DATE" : dates3,
    "LOCATION_ID" : location_id,
    "TEMPERATURE" : np.round(np.random.normal(94, 12, n3),1),
    "OVERCAST" : overcast,
    "RAIN" : rain,
    "SNOW" : 0,
    "LIGHTNING" : 0,
    "HAIL" : 0,
    "TORNADO" : 0
})

# change some snow fields to null 
change = df2.sample(51).index
df2.loc[change,'SNOW'] = None

# append records
df = df.append(df2,ignore_index=True)
df = df.append(df3,ignore_index=True)

# change some rain fields to null 
change = df.sample(162).index
df.loc[change,'RAIN'] = None

df.index +=1 # shift index to be 1-based
df.to_sql("WEATHER_MONITOR",conn, index_label = "ID",if_exists="replace")
conn.execute("VACUUM;")

pd.read_sql("SELECT * FROM WEATHER_MONITOR", conn)
