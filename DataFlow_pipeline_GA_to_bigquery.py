"""
для загрузки данных из Universal Analytics.
не для Google Аналитика 4.
"""
import os


os.system('virtualenv loadgatobq-env')
os.system('source loadgatobq-env/bin/activate')
os.system('pip3 install --quiet apache-beam[gcp]==2.32.0')



import apache_beam as beam
from apache_beam.transforms import ptransform
from datetime import datetime



project_id = 'наименование-project_id' # Используемый проект
bucket = 'bucket' # Используемая корзина
dttm = datetime.now().strftime('%Y%m%d%H%M') # для разграничения потоков 

schem_bq = 'Gdate:DATE,GclientID:STRING,GsessionDurationBucket:INT64,Gsource:STRING,GuserType:BOOLEAN,Gcountry:STRING,GdeviceCategory:STRING,GfullReferrer:STRING,GdateHourMinute:DATETIME,Gsessions:INT64,GsessinonID:STRING,GfirstDatetimeUser:DATETIME' 
# схема для таблицы



class Load_hist_ga(beam.DoFn):
# аутификация и получение отчета из GA 
    def process(self,resdata):

        import ast
        from google.cloud import bigquery
        from apiclient.discovery import build
        from oauth2client.service_account import ServiceAccountCredentials
        from datetime import datetime

        #key_file - токен из JSON файла сервисного аккаунта, т.к. нет возможности в Dataflow прочитать файл из Shell
        key_file = {
            "type": "service_account",
            "project_id": "наименование-project_id",
            "private_key_id": "1234567890",
            "private_key": "-----BEGIN PRIVATE KEY-----\n\n-----END PRIVATE KEY-----\n",
            "client_email": "название-сервисного-аккаунта@наименование-project_id.iam.gserviceaccount.com",
            "client_id": "1234567890",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/...................."
            }
        scope = 'https://www.googleapis.com/auth/analytics.readonly' # Определите области авторизации для запроса.
        view_id = '0000000000' # ID счетчика GA
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            key_file, scope)

        # Создание объекта службы.
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        # Получение отчета
        response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                'viewId': view_id,
                'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],    
                'metrics': [{'expression': 'ga:sessions'}],
                'dimensions': [{'name': 'ga:date'},{'name': 'ga:clientID'},{'name': 'ga:sessionDurationBucket'},{'name': 'ga:source'},{'name': 'ga:userType'},{'name': 'ga:country'},{'name': 'ga:deviceCategory'},{'name': 'ga:fullReferrer'},{'name': 'ga:dateHourMinute'}]
                }]
            }
        ).execute()
        #'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}], - значение по умолчанию, если не указывать иное


        #Далее трансформация отчета в лист с добавлением новых значений - SessionID и времени создания ClientID
        
        resdata = '' # консолидатор в верхнеуровневом цикле - словареподобный строчный объект из строк со значениями
                
        for report in response.get('reports', []):

            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            datatwo = '' # консолидатор в среднеуровневом цикле - строку в словареподобный строчный объект

            for xyz in range(len(dimensionHeaders)):
                dimensionHeaders[xyz] = str(dimensionHeaders[xyz]).replace('ga:','G') 
                """Rename values dimension headers"""    

            for row in report.get('data', {}).get('rows', []):
                if datatwo != '':
                    datatwo += ','
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])
                dataonce = '{'    # консолидатор в нижнеуровневом цикле - значений в строке

                for header, dimension in zip(dimensionHeaders, dimensions):
                    
                    if header == 'Gdate':
                        dtses = dimension
                        dimension = str(datetime.strptime(dimension, '%Y%m%d').date())
                    if header == 'GdateHourMinute':
                        dimension = str(datetime.strptime(dimension, '%Y%m%d%H%M%S'))
                    if header == 'GuserType':
                        if dimension == 'New Visitor': 
                            dimension = '1'
                        else: dimension = '0'
                    dataonce += str('"' + header + '": "' + dimension + '", ')
                     
                    if header == 'GclientID':
                        clid = dimension
                
                
                if clid.find('.') !=-1:
                    dtuser = str(datetime.fromtimestamp(int(clid[(clid.find('.')+1):])))
                    headus = clid[:clid.find('.')]
                else:
                    dtuser = str(datetime.fromtimestamp(int(clid[(clid.find('16',8)):])))
                    headus = clid[:clid.find('16',8)]      
                
                for i, values in enumerate(dateRangeValues):
                    for metricHeader, value in zip(metricHeaders, values.get('values')):
                        dataonce += str('"' + metricHeader.get('name').replace('ga:','G') + '": "' + value + '", ')

                datatwo += dataonce 
                
                datatwo += str('"GsessinonID": "' + dtses + '.' + headus + '", "GfirstDatetimeUser": "' + dtuser + '"}')
                # добавление значений ID сессии и времени создания ClientID
                
            resdata += datatwo 

        resdata = ast.literal_eval(resdata) # преобразования словареподобной строки в словарь
        return resdata


def run():

    argv = [
          '--project={0}'.format(project_id),
          '--job_name=jobloadlogga{0}'.format(dttm),
          '--save_main_session',
          '--staging_location=gs://{0}/tmp_ym/'.format(bucket),
          '--temp_location=gs://{0}/tmp_ym/'.format(bucket),
          '--max_num_workers=2',
          '--region=europe-north1',
          '--runner=DataflowRunner',
          '--experiments=use_beam_bq_sink'
        ]

    p = beam.Pipeline(argv=argv)
  
    ppl = (p
        |'Create data' >> beam.Create([None])
        |'Load GoogleAnalytics Log' >> beam.ParDo(Load_hist_ga())
        |'Write results' >> beam.io.gcp.bigquery.WriteToBigQuery(
            table='{0}:название-dataset.название-таблицы'.format(project_id), 
            schema=schem_bq, 
            create_disposition='CREATE_IF_NEEDED', 
            write_disposition='WRITE_APPEND',
            method='DEFAULT',
            ignore_insert_ids=True
        )
    )

    p.run()


if __name__ == '__main__':
    run()
