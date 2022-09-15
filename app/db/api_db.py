from sqlalchemy import event, create_engine
import pandas as pd
import time
import sys
import os
import numpy as np
import boto3
import json
import sys
from datetime import datetime

class MLOPs_DB_Connect:
    def __init__(self, hostname, username,
                port, password, database, aws_bucket, bucket_subpath
                ):
        
        self.engine_url = 'postgresql://{}:{}@{}:{}/{}'.format(
                                                            username,
                                                            password,
                                                            hostname,
                                                            port,
                                                            database
                                                            )
        self.engine = create_engine(self.engine_url)
        self.frame_bucket = boto3.resource('s3').Bucket(aws_bucket)
        self.bucket_subpath = bucket_subpath
    
    def pull_dataset_date(self, pull_strategy, custom_date=None):
        '''
        Fetch the date for which the inference is to be run
        '''
        if pull_strategy=='latest_date':
            df = self.get_table('data_table')
            df_sorted = df.loc[df['predictions_done']==False]
            df_sorted = df.sort_values(by=['date_inlet'], ascending=True)
            date_inference = str(df_sorted.iloc[-1]['date_inlet'])
        elif pull_strategy=='pull_unprocessed_dates':
            df = self.get_table('data_table')
            df_sorted = df.loc[df['predictions_done']==False]
            df_sorted = df.sort_values(by=['date_inlet'], ascending=False)
            date_inference = str(df_sorted.iloc[-1]['date_inlet'])
        elif pull_strategy=='custom_date':
            #Check if the date entry exists in the data table
            df = self.get_table('data_table')
            df_date = df.loc[df['date_inlet']==datetime.strptime(custom_date, '%Y-%m-%d').date()]
            if len(df_date):
                print("Date exists in database.")
                date_inference = custom_date
            else:
                print("Sorry! Couldn't find the custom date in data table.")
                print("Alternatively, looking into bucket for samples for custom date....")
                count = 0
                date_prefix = os.path.join(self.bucket_subpath, custom_date)
                for obj in self.frame_bucket.objects.filter(Prefix=date_prefix):
                    filename = obj.key
                    count += 1
                
                if count:
                    self.create_daily_log(custom_date, count)
                    date_inference = custom_date
                else:
                    print("Error: Unable to find the custom date(", custom_date, ")" ,"samples in both database & s3 bucket.")
                    sys.exit()
        
        return date_inference
    
    def upload_predictions(self, date, labels_path):
        #Add all the predictions from the csv file to the mlops database predictions table
        date_inference = (datetime.now().date())
        annotations_csv = pd.read_csv(labels_path)
        if len(annotations_csv):
            annotations_csv = annotations_csv.drop(columns=['imagename'])
            annotations_csv = annotations_csv.drop(columns=['frame_no'])
            annotations_csv['date_inlet'] = date
            annotations_csv['date_inference'] = date_inference
            annotations_csv = annotations_csv.rename(columns={'video':'image_name'})
            annotations_csv['bbox_confidence'] = np.nan
            annotations_csv['class_score'] = np.nan
            annotations_csv.to_sql('predictions_table', self.engine, if_exists='append', index=False)
            print("Frame predictions uploaded to MLOps database.")
        

    def update_annotations_info(self, date, labelstudio_projectid):
        #Add all the annotations to the MLOps database
        df = self.get_table('data_table')
        date_formatted  = datetime.strptime(date, "%Y-%m-%d").date() 
        df_date = df.loc[df['date_inlet']==date_formatted]
        if len(df_date):
            df.loc[df['date_inlet']==date_formatted, ['labelstudio_projectid']] = labelstudio_projectid

            df.to_sql('data_table', self.engine, if_exists='replace', index=False)
            print("Inference start datetime set in mlops db.")
        else:
            print("Sorry! but no db entries found for this date.")
            print("Please first create an entry for this date.")
        
    def update_daily_count(self, date=None):
        #Get the row with current date and update the sample count by checking the
        #no of entries in aws bucket
        count = 0
        date = (datetime.now().date()) if date is None else date
        date_prefix = os.path.join(self.bucket_subpath, str(date))
        for obj in self.frame_bucket.objects.filter(Prefix=date_prefix):
            filename = obj.key
            count += 1
        
        if count==0:
            print("No frames found for today!")
        else:
            #Checks the entry in the database for the date.
            #If there is already an entry, then replace it, otherwise create new entry
            print("Current no of frames:", count)
            df = self.get_table('data_table')
            df_date = df.loc[df['date_inlet']==date]
            
            if len(df_date):
                df.loc[df['date_inlet']==date, ['no_of_frames']] = count

                df.to_sql('data_table', self.engine, if_exists='replace', index=False)
                print("Frames count altered in the mlops db.")
            else:
                #Create new entry in the table with the date
                self.create_daily_log(date, count)
    
    def update_inference_info(self, date, model_name, model_version, weights_path):
        #Set the inference start time, model name, model version 
        #in the database for the day inference
        dt_now = datetime.now()
        df = self.get_table('data_table')
        date_formatted  = datetime.strptime(date, "%Y-%m-%d").date() 
        df_date = df.loc[df['date_inlet']==date_formatted]

        if len(df_date):
            df.loc[df['date_inlet']==date_formatted, ['datetime_inference_start']] = dt_now
            df.loc[df['date_inlet']==date_formatted, ['model_name']] = model_name
            df.loc[df['date_inlet']==date_formatted, ['model_version']] = model_version
            df.loc[df['date_inlet']==date_formatted, ['weights_path']] = weights_path

            df.to_sql('data_table', self.engine, if_exists='replace', index=False)
            print("Inference start datetime set in mlops db.")
        else:
            print("Sorry! but no db entries found for this date.")
            print("Please first create an entry for this date.")

    def update_inference_end_time(self, date):
        #Set the inference start time in the database for the day inference
        dt_now = datetime.now()
        df = self.get_table('data_table')
        date_formatted  = datetime.strptime(date, "%Y-%m-%d").date() 
        df_date = df.loc[df['date_inlet']==date_formatted]
        if len(df_date):
            df.loc[df['date_inlet']==date_formatted, ['datetime_inference_end']] = dt_now
            df.loc[df['date_inlet']==date_formatted, ['predictions_done']] = True

            df.to_sql('data_table', self.engine, if_exists='replace', index=False)
            print("Inference end datetime set in mlops db.")
        else:
            print("Sorry! but no db entries found for this date.")
            print("Please first create an entry for this date.")

    def create_daily_log(self, date, counting, model_name='', model_version='', weights_path=''):
        data_entry = pd.DataFrame()
        
        data_entry = data_entry.append({'date_inlet': date, 
                                        'predictions_done': False, 
                                        'no_of_frames': counting, 
                                        'datetime_inference_start': np.nan, 
                                        'datetime_inference_end': np.nan,
                                        'model_name':model_name,
                                        'model_version':model_version,
                                        'weights_path':weights_path,
                                        'labelstudio_projectid':None
                                        }, 
                                        ignore_index=True)
        
        data_entry.to_sql('data_table', self.engine, if_exists='append', index=False)
        print("Daily dataset entry added to the mlops db.")
    
    def get_table(self, table_name):
        '''
        Fetch the full table from the mlops database
        '''        
        df = pd.read_sql_query('select * from "{}"'.format(table_name),con=self.engine)
        return df