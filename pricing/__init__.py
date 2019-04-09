import json
import os
import os.path
import requests
import csv
import tempfile
from io import StringIO
import boto3
import time
import urllib3

create_table = """
CREATE EXTERNAL TABLE `[OFFER]`(
  [COLUMNS]
[PARTITION]
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://aws-price-magician-data/[OFFER]/'
TBLPROPERTIES (
  'classification'='csv',
  'columnsOrdered'='true',
  'compressionType'='none',
  'delimiter'=',',
  'skip.header.line.count'='1',
  'typeOfData'='file')
"""


class PriceManager:
    def __init__(self, **kwargs):
        urllib3.disable_warnings()
        self.prefix = kwargs.get("Prefix", "https://pricing.us-east-1.amazonaws.com")
        self.price_folder = kwargs.get("PriceFolder", "./price_files")
        self.ddl_folder = kwargs.get("DDLFolder", "./ddl")
        self.athena_database = kwargs.get("AthenaDatabase", "awspricedatabase")
        self.bucket_name = kwargs.get("BucketName", "aws-price-magician-data")
        self.price_url = kwargs.get('PriceUrl', 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json')
        self.client = boto3.client('athena')
        self.glue_client = boto3.client('glue')

    def load_partitions(self):
        queries = []
        files = os.listdir(self.ddl_folder)
        while len(files) > 0:
            for ddl_file in files[:3]:
                offer = ddl_file.split('_')[3].split('.')[0]
                r = self.client.start_query_execution(
                    QueryString = 'MSCK REPAIR TABLE {};'.format(offer.lower()),
                    QueryExecutionContext = {
                        'Database': self.athena_database
                    },
                    ResultConfiguration = {
                        'OutputLocation': 's3://{}/query_results'.format(self.bucket_name)
                        })
                queries.append(r['QueryExecutionId'])
                files.remove(ddl_file)

            while len(queries) > 0:
                r = self.client.batch_get_query_execution(QueryExecutionIds=queries[:50])
                for query_exec in r['QueryExecutions']:
                    if query_exec['Status'] in ['QUEUED', 'RUNNING']:
                        continue
                    else:
                        print(query_exec['Status'])
                        queries.remove(query_exec['QueryExecutionId'])
                print('Queries left: {}'.format(len(queries)))
                time.sleep(2)


    def create_tables(self):
        queries = []
        files = []
        self.glue_client.create_database(
            DatabaseInput={
                'Name': self.athena_database
            })
        for ddl_file in os.listdir(self.ddl_folder):
            files.append(ddl_file)

            with open('{}/{}'.format(self.ddl_folder, ddl_file), 'r') as ddl:
                query = ddl.read()
            print('Starting execution for ' + ddl_file)
            r = self.client.start_query_execution(
                QueryString = query,
                QueryExecutionContext = {
                    'Database': self.athena_database
                },
                ResultConfiguration = {
                    'OutputLocation': 's3://{}/query_results'.format(self.bucket_name)
                    })
            queries.append(r['QueryExecutionId'])

        while len(queries) > 0:
            r = self.client.batch_get_query_execution(QueryExecutionIds=queries[:50])
            for query_exec in r['QueryExecutions']:
                if query_exec['Status'] in ['QUEUED', 'RUNNING']:
                    continue
                else:
                    print(query_exec['Status'])
                    queries.remove(query_exec['QueryExecutionId'])
            print('Queries left: {}'.format(len(queries)))
            time.sleep(2)

    def upload_files(self):
        session = boto3.Session()
        s3 = session.resource('s3')
        bucket = s3.Bucket(self.bucket_name)

        for subdir, dirs, files in os.walk(self.price_folder):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    print('Uploading {}'.format(full_path))
                    bucket.put_object(Key=full_path[len(self.price_folder)+1:], Body=data)

    def get_master_price_list(self):
        print('Downloading master price list')
        data = requests.get(self.price_url, verify=False)
        return json.loads(data.text)

    def download_offer_file(self, path):
        print('Downloading {}'.format(path))
        r = requests.get(path, verify=False)
        tmp = tempfile.NamedTemporaryFile(delete=False)
        with open(tmp.name, 'w') as tmp_file:
            tmp_file.write('\n'.join(r.content.decode().split('\n')[5:]))
        s = StringIO(r.content.decode())
        rows = csv.reader(s)
        version = ""
        for row in rows:
            if row[0] == 'Version':
                version = row[1]
                break
        return tmp.name, version

    def get_row_location(self, row):
        has_location = 'Location' in dict(row).keys()
        if has_location and row['Location']:
            loc = row['Location']
        else:
            loc = "location-agnostic"
        return has_location, loc

    def make_folders(self, folders):
        for f in folders:
            if not os.path.exists(f):
                os.makedirs(f)

    def download_prices(self):
        values = self.get_master_price_list()
        writers = {}
        files = {}
        region_services = {}
        for offer in list(values["offers"].keys()):
            o = values["offers"][offer]
            path = "{0}{1}".format(self.prefix,
                                   o["currentVersionUrl"].replace('json',
                                                                  'csv'))
            offer_file, version = self.download_offer_file(path)
            with open(offer_file, 'rt') as csvfile:
                rows = csv.DictReader(csvfile)
                columns = []
                ddl_column_text = ""
                for row in rows:
                    has_location, loc = self.get_row_location(row)
                    if loc not in region_services:
                        region_services[loc] = []
                    if offer not in region_services[loc]:
                        region_services[loc].append(offer)
                    loc_folder = "{}/{}/location={}".format(self.price_folder, offer, loc)
                    self.make_folders([self.ddl_folder, loc_folder])
                    offer_loc = offer + loc
                    offer_file = "{}/{}.csv".format(loc_folder, offer)
                    if offer_loc not in files.keys():
                        files[offer_loc] = open(offer_file, 'w')  # noqa: E501
                        writers[offer_loc] = csv.writer(files[offer_loc],
                                                        quotechar='"',
                                                        quoting=csv.QUOTE_ALL)
                        if not columns:
                            columns = row.keys()
                            for col in columns:

                                # Athena will throw a 'duplicate column' error if we add the partition column
                                if has_location and col == 'Location':
                                    continue

                                ddl_column_text = '{}  `{}` string,\n'.format(ddl_column_text, col)

                            # strip off the trailing comma
                            ddl_column_text = ddl_column_text[:-2] + ")"
                        # write the header row
                        writers[offer_loc].writerow(dict(row))
                    writers[offer_loc].writerow(dict(row).values())

                create_ddl = create_table.replace('[COLUMNS]', ddl_column_text).replace('[OFFER]', offer)
                if has_location:
                    create_ddl = create_ddl.replace('[PARTITION]', 'PARTITIONED BY (\n  `location` string)\n')
                else:
                    create_ddl = create_ddl.replace('[PARTITION]', '\n')

                with open('{}/create_table_ddl_{}.sql'.format(self.ddl_folder, offer),'w') as f:
                    f.write(create_ddl)
                print('Offer {} processed'.format(offer))

                for key in files.keys():
                    del writers[key]
                files = {}
