import datetime
import json
import os
import subprocess
from google.cloud import secretmanager

def new_config_file(config_file, field_name, additional_content, co_file, 
                    single_level_file, first_day_first_prev, last_day_first_prev,
                    first_day_third_prev, last_day_third_prev, sl_year, sl_month,
                    sl_first_date, sl_last_date):
    '''Modified the config file.'''

    with open(config_file, 'r') as file:
        lines = file.readlines()

    # Update the specified field with the new value
    updated_lines = []
    selection_line_found = False
    for line in lines:
        if not selection_line_found and line.strip() == '[selection]':
            updated_lines.append(f'{additional_content}\n')
            selection_line_found = True
        
        if single_level_file:
            if line.startswith('year'):
             line = f'year={sl_year}\n'
            if line.startswith("month"):
                line = f'month={sl_month}\n'
            if line.startswith("day"):
                line = f'day={sl_first_date}/to/{sl_last_date}\n'
        elif line.startswith(field_name):   
            if co_file:
                line = f'{field_name}={first_day_first_prev}/to/{last_day_first_prev}\n'
            else:
                line = f'{field_name}={first_day_third_prev}/to/{last_day_third_prev}\n'        
        updated_lines.append(line)

    with open(config_file, 'w') as file:
        file.writelines(updated_lines)

def get_month_range(date):
    '''Return the first and last date of the month from the input date.'''
    last_day = date.replace(day=1) - datetime.timedelta(days=1)
    first_day = last_day.replace(day=1)    
    return first_day, last_day

def get_single_level_dates(first_day, last_day):
    '''Return the third previous month's year,month,first day and last day.'''
    year, month = str(first_day)[:4], str(first_day)[5:7]
    first_date, last_date = str(first_day)[8:], str(last_day)[8:]  
    return (year, month, first_date, last_date)

def get_previous_month_dates():
    '''Return the first and third previous month's date from the Today's date. '''
    today = datetime.date.today()
    prev_month = today.month + 10 if today.month < 3 else today.month - 2
    third_prev_month = today.replace(month=prev_month)
    first_prev_month = today.replace(month=today.month)
    first_day_third_prev, last_day_third_prev = get_month_range(third_prev_month)
    first_day_first_prev, last_day_first_prev = get_month_range(first_prev_month)

    sl_year, sl_month, sl_first_date, sl_last_date = get_single_level_dates(
                                        first_day_third_prev, last_day_third_prev)

    return (first_day_first_prev, last_day_first_prev, first_day_third_prev,
            last_day_third_prev, sl_year, sl_month, sl_first_date, sl_last_date)

def update_config_files(directory, field_name, additional_content):
    '''Update the config file.'''
    (first_day_first_prev, last_day_first_prev, first_day_third_prev,
    last_day_third_prev, sl_year, sl_month, sl_first_date, 
                            sl_last_date)= get_previous_month_dates()

    for filename in os.listdir(directory):
        single_level_file = False
        co_file = False
        if filename.endswith('.cfg'):
            if 'sfc' in filename:
                single_level_file = True
            if 'hourly' in filename:
                co_file = True
            config_file = os.path.join(directory, filename)
            new_config_file(config_file, field_name, additional_content, co_file, 
                            single_level_file, first_day_first_prev, last_day_first_prev,
                            first_day_third_prev, last_day_third_prev, 
                            sl_year, sl_month, sl_first_date, sl_last_date)

def get_secret(api_key: str):
    '''Function to retrieve the secret value from the google cloud.'''
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": api_key})
    payload = response.payload.data.decode("UTF-8")
    secret_dict = json.loads(payload)
    return secret_dict

DIRECTORY = '/weather'
FIELD_NAME = 'date'

current_day = datetime.date.today()
Job_name = f'wx-dl-arco-era5_{current_day.month}_{current_day.year}'

PROJECT = os.environ.get('PROJECT') 
REGION = os.environ.get('REGION')
BUCKET = os.environ.get('BUCKET')
TOPIC_PATH = os.environ.get('TOPIC_PATH')
api_key1 = os.environ.get('api_key_1')
api_key2 = os.environ.get('api_key_2')

api_key_1 = get_secret(api_key1)
api_key_2 = get_secret(api_key2)

additional_content = f'[parameters.key1]\napi_url={api_key_1["api_url"]}\napi_key={api_key_1["api_key"]}\n\n[parameters.key2]\napi_url={api_key_2["api_url"]}\napi_key={api_key_2["api_key"]}\n'

update_config_files(DIRECTORY, FIELD_NAME, additional_content)

command = f'python "weather_dl/weather-dl" /weather/*.cfg --runner DataflowRunner --project {PROJECT}  --region {REGION} \
--temp_location "gs://{BUCKET}/tmp/" --disk_size_gb 260 --job_name {Job_name} \
--sdk_container_image "gcr.io/grid-intelligence-sandbox/miniconda3-beam:weather-tools-with-aria2" \
--manifest-location fs://manifest?projectId=anthromet-ingestion --topic-path {TOPIC_PATH} --experiment use_runner_v2' 

try:
    subprocess.run(command, shell=True, check=True, capture_output=True)
except subprocess.CalledProcessError as e:
    print(f'Failed to execute dataflow job due to {e.stderr.decode("utf-8")}')


