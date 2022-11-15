import datetime
from etl.extractor import extract_jobs
from etl.loader import bigquery_loader

def main(request):
    try: 
        BIGQUERY_PROJECT_ID = 'youridproject'  # poner el id del proyecto
        BIGQUERY_DATASET_ID = 'jobs' # poner el id del dataset de BigQuery
        BIGQUERY_TABLE_NAME = 'O_JOBS_WEB' # poner un nombre cualquier para la tabla
        PARTITION_FIELD = 'load_timestamp'  # no tocar
        APPEND_DATA_ON_BIGQUERY = True  # cambiar a False en caso de querer que los nuevos datos reemplacen los viejos datos

        today = datetime.datetime.today()
        jobs_data = extract_jobs(day = today)
        print("saving data into Google Big Query....")

        http_status = bigquery_loader(  jobs_data,
                                        project_id= BIGQUERY_PROJECT_ID,
                                        dataset_id= BIGQUERY_DATASET_ID,
                                        table_name= BIGQUERY_TABLE_NAME)
        if http_status == 200:
            return ('Successful!', http_status)
        else:
            return ("Error. Please check the logging pannel", http_status)
    
    except Exception as e:
        error_message = "Error uploading data: {}".format(e)
        print('[ERROR] ' + error_message)
        return (error_message, '400')

