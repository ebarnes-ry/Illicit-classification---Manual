from tqdm import tqdm
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime
from datetime import date
pd.set_option('display.max_columns', None)
import time
start_time = time.time()

engine = create_engine('mysql+mysqlconnector://' + 'bob' + ':' + '0akcabin3t' + '@' + 'localhost' + ':' + '3307' + '/imi', echo=False)


run_time_request='Q3_082023'
state_request ='Virginia'
county_request ='("")'
city_request = '("Arlington")'

if county_request =='("")':
    county_variable = "#"
else:
    county_variable = 'AND county_fix in '+county_request

if city_request == '("")':
    city_variable ='#'
else:
    city_variable = 'AND city_fix in '+city_request
############################# RM LOGIC QUERY ###############################

############################# AS LOGIC QUERY ###############################
if county_request =='("")':
    arcounty_variable = "#"
else:
    arcounty_variable = 'AND county in  '+county_request

if city_request == '("")':
    arcity_variable ='#'
else:
    arcity_variable = 'AND city in '+city_request


query_2='''SELECT i.imb_id,i.identifier, r.imb_identifier, i.imb_name, r.review_head_id, i.address, i.city_fix, i.county_fix, i.zip, r.visit_date, r.time_of_visit, r.paid_to_house, r.extra_tip, r.message1, r.message2, i.url, r.review_id, r.review_head_date, r.finger,r.kiss,r.lick,r.fs,r.breast_play,r.blowjob,r.ass_play,r.hand_job,r.prostate_massage
FROM imb i
LEFT JOIN reviews r
	ON i.identifier = r.imb_identifier
WHERE run_time = "'''+run_time_request+'''"
AND is_erotic = 1
AND permanently_closed = 0
'''+'''
AND state = "'''+state_request+'''"
'''+county_variable+'''
'''+city_variable+'''
'''+'''
GROUP BY r.review_head_id'''
df_kitems = pd.read_sql(query_2, engine)
df_kitems.dropna(subset=['review_head_date'], inplace = True)
df_kitems.head(200)


def calculate_sum(row):
    total = 0
    for key, val in values.items():
        if row[key] not in bad_list:
            total += val
    return total

# Load original DataFrame here (df_kitems)

# Perform necessary conversions outside the loop
today = date.today()
value05 = today - datetime.timedelta(days=365)

# Define values and bad_list as you did in your code
values = {
    'finger': 0.075,
    'kiss': 0.025,
    'lick': 0.075,
    'breast_play': 0.025, 
    'blowjob': 0.35,
    'ass_play': 0.025,
    'hand_job': 0.35,
    'prostate_massage': 0.075
}
bad_list = set(['X', 'x', '', '?'])

illicit_threshold = 0.3

# Iterate through unique business IDs
for business_id in tqdm(df_kitems['identifier'].unique()):
    
    entry = len(df_kitems[df_kitems['identifier']==business_id])
    
    mask = df_kitems['identifier'] == business_id
    temporal_df = df_kitems[mask].copy()  # Create a copy of the DataFrame
    
    list_dates = temporal_df['review_head_date']
    list_dates_diff = (today - list_dates).dt.days

    xp = [0, (today - value05).days, (today - list_dates.min()).days]
    fp = [1, 0.5, 0]
    interpolations = np.interp(list_dates_diff, xp, fp)

    value_illicit = temporal_df.apply(calculate_sum, axis=1)
    
    df_kitems.loc[mask, 'date_interpolation'] = interpolations
    df_kitems.loc[mask, 'value_illicit'] = value_illicit
    df_kitems.loc[mask, 'final_value'] = interpolations * value_illicit
    
    temporal_df.loc[:, 'date_interpolation'] = interpolations
    temporal_df.loc[:, 'value_illicit'] = value_illicit
    temporal_df.loc[:, 'final_value'] = interpolations * value_illicit
    
    exit = len(temporal_df)
    if entry == exit:
        sum_date_interpolation = temporal_df['date_interpolation'].sum()
        sum_final_value = temporal_df['final_value'].sum()

        # Calculate IMB_label and classify as illicit or not_illicit
        if sum_date_interpolation != 0:
            imb_label = sum_final_value / sum_date_interpolation
            df_kitems.loc[mask, 'IMB_label'] = imb_label

            # Label as illicit or not_illicit based on threshold
            if imb_label >= illicit_threshold:
                df_kitems.loc[mask, 'Activity_Label'] = 'illicit'
            elif imb_label <= 0.35 and imb_label >= 0.25:
                df_kitems.loc[mask, 'Activity_Label'] = 'questionable_illicit'
            else:
                df_kitems.loc[mask, 'Activity_Label'] = 'not_illicit'
        else:
            df_kitems.loc[mask, 'IMB_label'] = np.nan
            df_kitems.loc[mask, 'Activity_Label'] = np.nan


df_kitems = df_kitems.loc[:, ['IMB_label', 'Activity_Label']]

print (time.time() - start_time, 's')