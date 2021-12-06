import json
import os

input_datasets = ['skechers.customer_billto_address',
                  'skechers.customer_shipto_address',
                  'skechers.customer_wishlist',
                  'skechers.customer_wishlist_items',
                  'skechers.customers',
                  'skechers.discounts',
                  'skechers.discounts_conditions',
                  'skechers.login_failure',
                  'skechers.login_success',
                  'skechers.order_attempts',
                  'skechers.order_billto_address',
                  'skechers.order_comments',
                  'skechers.order_decline',
                  'skechers.order_invoice',
                  'skechers.order_items',
                  'skechers.order_items_discounts',
                  'skechers.order_items_gift_cards',
                  'skechers.order_items_invoices',
                  'skechers.order_items_shipping_label',
                  'skechers.order_log',
                  'skechers.order_payments',
                  'skechers.order_shipto_address',
                  'skechers.order_transactions',
                  'skechers.shipping_method',
                  'skechers.skxdmcad',
                  'skechers.store',
                  'skechers.tax_overwrite',
                  'skechers.tax_rates',
                  'skechers.tax_rules']

# curr_dir = os.path.abspath(os.path.dirname(__file__))
os.chdir('../../')
curr_dir = os.getcwd()
print(curr_dir)
file_path = '/input_jsons/structured/base_job.json'
base_json_path = curr_dir + file_path

with open(base_json_path) as f:
    base_json_data = json.load(f)

print(base_json_data)

for input_dataset in input_datasets:
    params_lst = ["--job_name", "DL Prototype Test - {0}".format(input_dataset.replace('skechers.', '')),
                  "--dataset_name", "ECOM",
                  "--data_format", "JSON",
                  "--structured_type", "FULL_LOAD",
                  "--integrated_type", "FULL_LOAD",
                  "--consumption_type", "FULL_LOAD",
                  "--translation_type", "",
                  "--input", "s3://skx-dataeng-nonprod-datalake-dev/raw/attunity/{0}".format(input_dataset),
                  "--structured_output", "s3://skx-dataeng-nonprod-datalake-dev/structured/ecom/{0}".format(input_dataset),
                  "--integrated_output", "",
                  "--consumption_output", "",
                  "--data_warehouse_output_table", "",
                  "--input_schema", "",
                  "--structured_output_schema", "",
                  "--structured_output_partition_columns", "",
                  "--structured_output_table", "ecom.{0}".format(input_dataset.replace('skechers.', '')),
                  "--integrated_output_table", "",
                  "--consumption_output_table", "",
                  "--integrated_sql_stmt", "",
                  "--consumption_sql_stmt", ""
                  ]
    base_json_data['spark_python_task']['parameters'] = params_lst
    print(base_json_data['spark_python_task']['parameters'])
    output_file_path = curr_dir + '/input_jsons/structured/ecom_{0}_job.json'.format(input_dataset)
    with open(output_file_path, 'w') as json_file:
        json.dump(base_json_data, json_file, indent=4)
