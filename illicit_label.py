from tqdm import tqdm
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime
from datetime import date
pd.set_option('display.max_columns', None)
pd.set_option('mode.chained_assignment', None)

# Create the engine 
engine = create_engine('mysql+mysqlconnector://' + 'bob' + ':' + '0akcabin3t' + '@' + 'localhost' + ':' + '3307' + '/imi', echo=False)

entry_run_time = 'Q3_072023'
entry_state = 'Virginia'
entry_city = ''

# Dynamic query construction
conditions = []
if entry_run_time:
    conditions.append(f'i.run_time = "{entry_run_time}"')
if entry_state:
    conditions.append(f'i.state = "{entry_state}"')
if entry_city:
    conditions.append(f'i.city_fix = "{entry_city}"')
condition_str = ' AND '.join(conditions)

query_kitems = '''
SELECT 
    i.identifier,
    i.run_time,
    i.imb_name,
    i.address,
    i.city_fix,
    i.state,
    i.zip,
    i.county_fix,
    i.is_erotic,
    i.permanently_closed,
    i.phone,
    i.url,
    r.message1,
    r.message2,
    r.review_head_date,
    r.finger,
    r.kiss,
    r.lick,
    r.fs,
    r.breast_play,
    r.blowjob,
    r.ass_play,
    r.hand_job,
    r.prostate_massage,
    r.paid_to_house,
    r.extra_tip
FROM imi.reviews r
LEFT JOIN imi.imb i 
    ON i.identifier = r.imb_identifier
''' + (' WHERE ' + condition_str) if condition_str else ''

df_kitems = pd.read_sql(query_kitems, engine)
# Define the calculate_sum function
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

# Create new columns to store calculated values


df_kitems['date_interpolation'] = np.nan
df_kitems['value_illicit'] = np.nan
df_kitems['final_value'] = np.nan
df_kitems['IMB_label'] = np.nan

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
    temporal_df = df_kitems[mask]
    
    list_dates = temporal_df['review_head_date']
    list_dates_diff = (today - list_dates).dt.days

    xp = [0, (today - value05).days, (today - list_dates.min()).days]
    fp = [1, 0.5, 0]
    interpolations = np.interp(list_dates_diff, xp, fp)

    value_illicit = temporal_df.apply(calculate_sum, axis=1)
    
    df_kitems.loc[mask, 'date_interpolation'] = interpolations
    df_kitems.loc[mask, 'value_illicit'] = value_illicit
    df_kitems.loc[mask, 'final_value'] = interpolations * value_illicit
    
    temporal_df.loc[mask, 'date_interpolation'] = interpolations
    temporal_df.loc[mask, 'value_illicit'] = value_illicit
    temporal_df.loc[mask, 'final_value'] = interpolations * value_illicit
        
    exit = len(temporal_df)
    if entry == exit:
        #print(entry," ---- ",exit, temporal_df['imb_name'])
        


        sum_date_interpolation = temporal_df['date_interpolation'].sum()
        sum_final_value = temporal_df['final_value'].sum()

         # Calculate IMB_label and classify as illicit or not_illicit

        if sum_date_interpolation != 0:
            imb_label = sum_final_value / sum_date_interpolation
            df_kitems.loc[mask, 'IMB_label'] = imb_label
        
            # Label as illicit or not_illicit based on threshold
            if imb_label >= illicit_threshold:
                df_kitems.loc[mask, 'Activity_Label'] = 'illicit'
            else:
                df_kitems.loc[mask, 'Activity_Label'] = 'not_illicit'
        else:
            df_kitems.loc[mask, 'IMB_label'] = np.nan
            df_kitems.loc[mask, 'Activity_Label'] = np.nan
        
        
        #df_kitems.loc[:,('IMB_label','mask')] = sum_final_value/sum_date_interpolation
        #imb_label = sum_final_value/sum_date_interpolation
       # df_kitems['IMB_label'][mask] = imb_label
        #df_kitems.loc[df_kitems['IMB_label'][mask]] = sum_final_value/sum_date_interpolation

#this is new new for the adding class branch
df_kitems.head(50)