import pandas as  pd 
from google.cloud import bigquery

def bigquery_loader(data: pd.DataFrame,project_id:str ,dataset_id: str,table_name: str):
    try:      
        print('Initializing GBQ Client..')
        client = bigquery.Client(project=project_id)
        print('Connecting Google BigQuery Client..')

        client.dataset(dataset_id)
        table_id = project_id+'.'+ dataset_id +'.'+ table_name

        try: 
            bq_table = client.get_table(table_id)
            print(f'The table {table_id} is already created')
        except Exception:
            print("Table {} doesn't exist. You Should Created..".format(table_id))
        
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_APPEND'
        job_config.source_format = "PARQUET"
        job_config.autodetect = True

        # Load Pandas Dataframe to Bigquery
        job = client.load_table_from_dataframe(
                data,
                bq_table,
                job_config=job_config
            )

            # Waits for the job to complete.
        job.result()

        # show info
        print("Successful! Loaded {} rows into {}:{}.".format(
                job.output_rows,
                dataset_id,
                table_id)
            )
        return 200
    
    except Exception as e:
        print('[ERROR] {}'.format(e))
        return 400