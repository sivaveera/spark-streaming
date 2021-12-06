import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
import os
from confluent_kafka.schema_registry import SchemaRegistryClient,Schema

# import fastavro

schema_template = '{"type": "record","name": "DataRecord", "namespace": "", "fields": [ { "name": "data", ' \
                  '"type": { "type": "record", "name": "Data", "fields": [ ] } }, { "name": "beforeData", "type": ["null", "Data" ], ' \
                  '"default": null }, { "name": "headers", "type": { "type": "record", "name": "Headers", "namespace": "com.attunity.queue.msg", ' \
                  '"fields": [ { "name": "operation", "type": { "type": "enum", "name": "operation", "symbols": ' \
                  '[ "INSERT", "UPDATE", "DELETE", "REFRESH" ] } }, { "name": "changeSequence", "type": "string" }, { "name": "timestamp", ' \
                  '"type": "string" }, { "name": "streamPosition", "type": "string" }, { "name": "transactionId", "type": "string" }, ' \
                  '{ "name": "changeMask", "type": [ "null", "bytes" ], "default": null }, { "name": "columnMask", "type": ["null", "bytes" ], ' \
                  '"default": null }, { "name": "transactionEventCounter", "type": [ "null", "long" ], "default": null }, ' \
                  '{ "name": "transactionLastEvent", "type": [ "null", "boolean" ], "default": null } ] } } ] }'

date_type = ["null", {"type": "int", "logicalType": "date"}]
timestamp_type = ["null", {"type": "long", "logicalType": "timestamp-micros"}]
decimal_type = ["null", {"type": "bytes", "precision": 20, "scale": 6, "logicalType": "decimal"}]
string_type = ["null", "string"]
int_type = ["null", "int"]

schema_dict = json.loads(schema_template)

os.chdir('../../')
curr_dir = os.getcwd()
print(curr_dir)

# schema_df = pd.read_excel('./../../test_data/sales_inv_schema.xlsx', header=0)
schema_df = pd.read_csv(curr_dir + '/test_data/schemas_from_metadata.csv', header=0)
schema_df = schema_df[['classname', 'columnname', 'columndatatype']]
tables = schema_df['classname'].unique()

all_table_fields = {}

for table in tables:
    print('------------------Table------------------\n', table)
    fields_list = []
    # Get dataframe for an individual table
    table_df = schema_df[schema_df['classname'] == table]
    table_df = table_df[['columnname', 'columndatatype']]
    rows = table_df.values.tolist()
    for row in rows:
        if 'int' in row[1]:
            fields_list.append({'name': row[0], 'type': int_type, "default": None})
        elif 'decimal' in row[1]:
            fields_list.append({'name': row[0], 'type': decimal_type, "default": None})
        elif 'string' in row[1]:
            fields_list.append({'name': row[0], 'type': string_type, "default": None})
        elif 'date' in row[1]:
            fields_list.append({'name': row[0], 'type': date_type, "default": None})
        elif 'timestamp' in row[1]:
            fields_list.append({'name': row[0], 'type': timestamp_type, "default": None})
        else:
            print('=================== NOT FOUND ===================')
    all_table_fields[table] = fields_list

all_table_schemas = {}
for table in all_table_fields.keys():
    print('------------------Table------------------', table)
    schema_dict['fields'][0]['type']['fields'] = all_table_fields[table]
    schema_dict['namespace'] = table
    table = table.split('.')[1]
    if table == 'ecom.order_items':
        output_file_name = 'schema_utility_output/' + table + '.avsc'
        with open(output_file_name, 'w') as outfile:
            avro_schema_str = json.dumps(schema_dict, indent=4)
            outfile.write(avro_schema_str)

        topic_name = 'dev.ecom.sales.test.schema-value'

        key = dbutils.secrets.get("schema_registry", "schema_registry_key")
        secret = dbutils.secrets.get("schema_registry", "schema_registry_secret")
        schema_registry_url = dbutils.secrets.get("schema_registry", "schema_registry_url")

        schema_registry_conf = {
            'url': schema_registry_url,
            'basic.auth.user.info': '{}:{}'.format(key, secret)}

        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        deployment_schema = Schema(avro_schema_str, 'AVRO')

        schema_registry_client.set_compatibility(topic_name, level='None')
        schema_registry_client.register_schema(topic_name, deployment_schema)

    # schema_json = json.dumps(schema_dict)
    # print(schema_json)
    # all_table_schemas[table] = schema_json

# custom_schema = all_table_schemas['ecom.order_items']
# custom_schema_json = json.dumps(custom_schema)
# print('ecom.order_items', custom_schema_json)
