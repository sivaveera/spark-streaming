# This script has been used to generate the jobs config files that have been passed as json bodies for jobs creation
import json
import os

env = 'prod'
job_names = [
        'SJ1-ECOM order_items',
        'SJ1-ECOM order_items_discounts',
        'SJ1-ECOM order_transactions',
        'SJ1-ECOM order_payments',
        'SJ1-ECOM order_comments',
        'SJ1-ECOM order_items_invoices',
        'SJ1-ECOM api_stat',
        'SJ1-ECOM order_attempts',
        'SJ1-ECOM order_billto_address',
        'SJ1-ECOM order_decline',
        'SJ1-ECOM order_invoice',
        'SJ1-ECOM order_items_gift_cards',
        'SJ1-ECOM order_log',
        'SJ1-ECOM order_shipto_address',
        'SJ1-ECOM tax_rates',
        'SJ1-ECOM tax_rules',
        'SJ1-ECOM tax_overwrite',
        'SJ1-ECOM gift_card',
        'SJ1-ECOM discounts',
        'SJ1-ECOM customers',
        'SJ1-ECOM customer_shipto_address',
        'SJ1-ECOM customer_billto_address',
        'SJ1-ECOM customers_alert_email',
        'SJ1-ECOM omni_search_log',
        'SJ1-ECOM order_items_shipping_label',
        'SJ1-APTOS_SALES exp_authorization_detail',
        'SJ1-APTOS_SALES exp_customer',
        'SJ1-APTOS_SALES exp_customer_detail',
        'SJ1-APTOS_SALES exp_discount_detail',
        'SJ1-APTOS_SALES exp_line_note',
        'SJ1-APTOS_SALES exp_merchandise_detail',
        'SJ1-APTOS_SALES exp_post_void_detail',
        'SJ1-APTOS_SALES exp_return_detail',
        'SJ1-APTOS_SALES exp_tax_detail',
        'SJ1-APTOS_SALES exp_tax_override_detail',
        'SJ1-APTOS_SALES exp_transaction_header',
        'SJ1-APTOS_SALES exp_transaction_line',
        'SJ1-APTOS_SALES exp_transaction_line_link',
        'SJ1-APTOS_INVENTORY exp_stock_control_detail',
        'SJ1-APROPOS_SALES sales',
        'SJ1-APROPOS_SALES sales_loccurr',
        'SJ1-APROPOS_SALES payment',
        'SJ1-APROPOS_SALES rt_header',
        'SJ1-APROPOS_SALES rt_detail',
        'SJ1-APROPOS_SALES rt_payment',
        'SJ1-APROPOS_SALES cashreg_hdr',
        'SJ1-APROPOS_SALES cashreg_dtl',
        'SJ1-APROPOS_SALES cash_hdr',
        'SJ1-APROPOS_SALES cash_dtl',
        'SJ1-APROPOS_SALES skdtranh',
        'SJ1-APROPOS_SALES skdtrand',
        'SJ1-APROPOS_SALES sales_rets',
        'SJ1-APROPOS_SALES nosale',
        'SJ1-APROPOS_INVENTORY ctheader',
        'SJ1-APROPOS_INVENTORY ctdetail',
        'SJ1-APROPOS_INVENTORY ct_reasons',
        'SJ1-APROPOS_INVENTORY reasons',
        'SJ1-APROPOS_INVENTORY reason_types',
        'SJ1-APROPOS_INVENTORY reasoncode',
        'SJ1-APROPOS_INVENTORY retcode',
        'SJ1-APROPOS_INVENTORY po',
        'SJ1-APROPOS_INVENTORY po_detail',
        'SJ1-APROPOS_INVENTORY po_order',
        'SJ1-APROPOS_INVENTORY shipnhdr',
        'SJ1-APROPOS_INVENTORY shipndtl',
        'SJ1-APROPOS_INVENTORY inv_detail',
        'SJ1-APROPOS_INVENTORY adjustment',
        'SJ1-APROPOS_INVENTORY adjustdtl',
        'SJ1-APROPOS_INVENTORY po_cancel',
        'SJ1-APROPOS_INVENTORY invtranarc'
]

# job_names = [
#         'SJ2-ECOM order_items',
#         'SJ2-ECOM order_items_discounts',
#         'SJ2-ECOM order_transactions',
#         'SJ2-ECOM order_payments',
#         'SJ2-ECOM order_comments',
#         'SJ2-ECOM order_items_invoices',
#         'SJ2-ECOM api_stat',
#         'SJ2-ECOM order_attempts',
#         'SJ2-ECOM order_billto_address',
#         'SJ2-ECOM order_decline',
#         'SJ2-ECOM order_invoice',
#         'SJ2-ECOM order_items_gift_cards',
#         'SJ2-ECOM order_log',
#         'SJ2-ECOM order_shipto_address',
#         'SJ2-ECOM tax_rates',
#         'SJ2-ECOM tax_rules',
#         'SJ2-ECOM tax_overwrite',
#         'SJ2-ECOM gift_card',
#         'SJ2-ECOM discounts',
#         'SJ2-ECOM customers',
#         'SJ2-ECOM customer_shipto_address',
#         'SJ2-ECOM customer_billto_address',
#         'SJ2-ECOM customers_alert_email',
#         'SJ2-ECOM omni_search_log',
#         'SJ2-ECOM order_items_shipping_label',
#         'SJ2-APTOS_SALES exp_authorization_detail',
#         'SJ2-APTOS_SALES exp_customer',
#         'SJ2-APTOS_SALES exp_customer_detail',
#         'SJ2-APTOS_SALES exp_discount_detail',
#         'SJ2-APTOS_SALES exp_line_note',
#         'SJ2-APTOS_SALES exp_merchandise_detail',
#         'SJ2-APTOS_SALES exp_post_void_detail',
#         'SJ2-APTOS_SALES exp_return_detail',
#         'SJ2-APTOS_SALES exp_tax_detail',
#         'SJ2-APTOS_SALES exp_tax_override_detail',
#         'SJ2-APTOS_SALES exp_transaction_header',
#         'SJ2-APTOS_SALES exp_transaction_line',
#         'SJ2-APTOS_SALES exp_transaction_line_link',
#         'SJ2-APTOS_INVENTORY exp_stock_control_detail',
#         'SJ2-APROPOS_SALES sales',
#         'SJ2-APROPOS_SALES sales_loccurr',
#         'SJ2-APROPOS_SALES payment',
#         'SJ2-APROPOS_SALES rt_header',
#         'SJ2-APROPOS_SALES rt_detail',
#         'SJ2-APROPOS_SALES rt_payment',
#         'SJ2-APROPOS_SALES cashreg_hdr',
#         'SJ2-APROPOS_SALES cashreg_dtl',
#         'SJ2-APROPOS_SALES cash_hdr',
#         'SJ2-APROPOS_SALES cash_dtl',
#         'SJ2-APROPOS_SALES skdtranh',
#         'SJ2-APROPOS_SALES skdtrand',
#         'SJ2-APROPOS_SALES sales_rets',
#         'SJ2-APROPOS_SALES nosale',
#         'SJ2-APROPOS_INVENTORY ctheader',
#         'SJ2-APROPOS_INVENTORY ctdetail',
#         'SJ2-APROPOS_INVENTORY ct_reasons',
#         'SJ2-APROPOS_INVENTORY reasons',
#         'SJ2-APROPOS_INVENTORY reason_types',
#         'SJ2-APROPOS_INVENTORY reasoncode',
#         'SJ2-APROPOS_INVENTORY retcode',
#         'SJ2-APROPOS_INVENTORY po',
#         'SJ2-APROPOS_INVENTORY po_detail',
#         'SJ2-APROPOS_INVENTORY po_order',
#         'SJ2-APROPOS_INVENTORY shipnhdr',
#         'SJ2-APROPOS_INVENTORY shipndtl',
#         'SJ2-APROPOS_INVENTORY inv_detail',
#         'SJ2-APROPOS_INVENTORY adjustment',
#         'SJ2-APROPOS_INVENTORY adjustdtl',
#         'SJ2-APROPOS_INVENTORY po_cancel',
#         'SJ2-APROPOS_INVENTORY invtranarc'
# ]

# Ecom job names Kafka to Audit
# job_names = [
#         'SJ1-ECOM order_items',
#         'SJ1-ECOM order_items_discounts',
#         'SJ1-ECOM order_transactions',
#         'SJ1-ECOM order_payments',
#         'SJ1-ECOM order_comments',
#         'SJ1-ECOM order_items_invoices',
#         'SJ1-ECOM api_stat',
#         'SJ1-ECOM order_attempts',
#         'SJ1-ECOM order_billto_address',
#         'SJ1-ECOM order_decline',
#         'SJ1-ECOM order_invoice',
#         'SJ1-ECOM order_items_gift_cards',
#         'SJ1-ECOM order_log',
#         'SJ1-ECOM order_shipto_address',
#         'SJ1-ECOM tax_rates',
#         'SJ1-ECOM tax_rules',
#         'SJ1-ECOM tax_overwrite',
#         'SJ1-ECOM gift_card',
#         'SJ1-ECOM discounts',
#         'SJ1-ECOM customers',
#         'SJ1-ECOM customer_shipto_address',
#         'SJ1-ECOM customer_billto_address',
#         'SJ1-ECOM customers_alert_email',
#         'SJ1-ECOM omni_search_log',
#         'SJ1-ECOM order_items_shipping_label'
# ]

# job_names = [
#         'SJ2-ECOM order_items',
#         'SJ2-ECOM order_items_discounts',
#         'SJ2-ECOM order_transactions',
#         'SJ2-ECOM order_payments',
#         'SJ2-ECOM order_comments',
#         'SJ2-ECOM order_items_invoices',
#         'SJ2-ECOM api_stat',
#         'SJ2-ECOM order_attempts',
#         'SJ2-ECOM order_billto_address',
#         'SJ2-ECOM order_decline',
#         'SJ2-ECOM order_invoice',
#         'SJ2-ECOM order_items_gift_cards',
#         'SJ2-ECOM order_log',
#         'SJ2-ECOM order_shipto_address',
#         'SJ2-ECOM tax_rates',
#         'SJ2-ECOM tax_rules',
#         'SJ2-ECOM tax_overwrite',
#         'SJ2-ECOM gift_card',
#         'SJ2-ECOM discounts',
#         'SJ2-ECOM customers',
#         'SJ2-ECOM customer_shipto_address',
#         'SJ2-ECOM customer_billto_address',
#         'SJ2-ECOM customers_alert_email',
#         'SJ2-ECOM omni_search_log',
#         'SJ2-ECOM order_items_shipping_label'
#  ]
#
#
# job_names =[
#
#         'SJ1-APTOS_SALES exp_authorization_detail',
#         'SJ1-APTOS_SALES exp_customer',
#         'SJ1-APTOS_SALES exp_customer_detail',
#         'SJ1-APTOS_SALES exp_discount_detail',
#         'SJ1-APTOS_SALES exp_line_note',
#         'SJ1-APTOS_SALES exp_merchandise_detail',
#         'SJ1-APTOS_SALES exp_post_void_detail',
#         'SJ1-APTOS_SALES exp_return_detail',
#         'SJ1-APTOS_SALES exp_tax_detail',
#         'SJ1-APTOS_SALES exp_tax_override_detail',
#         'SJ1-APTOS_SALES exp_transaction_header',
#         'SJ1-APTOS_SALES exp_transaction_line',
#         'SJ1-APTOS_SALES exp_transaction_line_link',
#         'SJ1-APTOS_INVENTORY exp_stock_control_detail'
#     ]

# job_names =[
#         'SJ2-APTOS_SALES exp_authorization_detail',
#         'SJ2-APTOS_SALES exp_customer',
#         'SJ2-APTOS_SALES exp_customer_detail',
#         'SJ2-APTOS_SALES exp_discount_detail',
#         'SJ2-APTOS_SALES exp_line_note',
#         'SJ2-APTOS_SALES exp_merchandise_detail',
#         'SJ2-APTOS_SALES exp_post_void_detail',
#         'SJ2-APTOS_SALES exp_return_detail',
#         'SJ2-APTOS_SALES exp_tax_detail',
#         'SJ2-APTOS_SALES exp_tax_override_detail',
#         'SJ2-APTOS_SALES exp_transaction_header',
#         'SJ2-APTOS_SALES exp_transaction_line',
#         'SJ2-APTOS_SALES exp_transaction_line_link',
#         'SJ2-APTOS_INVENTORY exp_stock_control_detail'
#     ]


# job_names = [
# 'SJ1-APROPOS_SALES sales',
# 'SJ1-APROPOS_SALES sales_loccurr',
# 'SJ1-APROPOS_SALES payment',
# 'SJ1-APROPOS_SALES rt_header',
# 'SJ1-APROPOS_SALES rt_detail',
# 'SJ1-APROPOS_SALES rt_payment',
# 'SJ1-APROPOS_SALES cashreg_hdr',
# 'SJ1-APROPOS_SALES cashreg_dtl',
# 'SJ1-APROPOS_SALES cash_hdr',
# 'SJ1-APROPOS_SALES cash_dtl',
# 'SJ1-APROPOS_SALES skdtranh',
# 'SJ1-APROPOS_SALES skdtrand',
# 'SJ1-APROPOS_SALES sales_rets',
# 'SJ1-APROPOS_SALES nosale'
# ]

# job_names = [
# 'SJ2-APROPOS_SALES sales',
# 'SJ2-APROPOS_SALES sales_loccurr',
# 'SJ2-APROPOS_SALES payment',
# 'SJ2-APROPOS_SALES rt_header',
# 'SJ2-APROPOS_SALES rt_detail',
# 'SJ2-APROPOS_SALES rt_payment',
# 'SJ2-APROPOS_SALES cashreg_hdr',
# 'SJ2-APROPOS_SALES cashreg_dtl',
# 'SJ2-APROPOS_SALES cash_hdr',
# 'SJ2-APROPOS_SALES cash_dtl',
# 'SJ2-APROPOS_SALES skdtranh',
# 'SJ2-APROPOS_SALES skdtrand',
# 'SJ2-APROPOS_SALES sales_rets',
# 'SJ2-APROPOS_SALES nosale'
# ]


# job_names = [
# 'SJ1-APROPOS_INVENTORY ctheader',
# 'SJ1-APROPOS_INVENTORY ctdetail',
# 'SJ1-APROPOS_INVENTORY ct_reasons',
# 'SJ1-APROPOS_INVENTORY reasons',
# 'SJ1-APROPOS_INVENTORY reason_types',
# 'SJ1-APROPOS_INVENTORY reasoncode',
# 'SJ1-APROPOS_INVENTORY retcode',
# 'SJ1-APROPOS_INVENTORY po',
# 'SJ1-APROPOS_INVENTORY po_detail',
# 'SJ1-APROPOS_INVENTORY po_order',
# 'SJ1-APROPOS_INVENTORY shipnhdr',
# 'SJ1-APROPOS_INVENTORY shipndtl',
# 'SJ1-APROPOS_INVENTORY inv_detail',
# 'SJ1-APROPOS_INVENTORY adjustment',
# 'SJ1-APROPOS_INVENTORY adjustdtl',
# 'SJ1-APROPOS_INVENTORY po_cancel',
# 'SJ1-APROPOS_INVENTORY invtranarc'
# ]

# job_names= [
# 'SJ2-APROPOS_INVENTORY ctheader',
# 'SJ2-APROPOS_INVENTORY ctdetail',
# 'SJ2-APROPOS_INVENTORY ct_reasons',
# 'SJ2-APROPOS_INVENTORY reasons',
# 'SJ2-APROPOS_INVENTORY reason_types',
# 'SJ2-APROPOS_INVENTORY reasoncode',
# 'SJ2-APROPOS_INVENTORY retcode',
# 'SJ2-APROPOS_INVENTORY po',
# 'SJ2-APROPOS_INVENTORY po_detail',
# 'SJ2-APROPOS_INVENTORY po_order',
# 'SJ2-APROPOS_INVENTORY shipnhdr',
# 'SJ2-APROPOS_INVENTORY shipndtl',
# 'SJ2-APROPOS_INVENTORY inv_detail',
# 'SJ2-APROPOS_INVENTORY adjustment',
# 'SJ2-APROPOS_INVENTORY adjustdtl',
# 'SJ2-APROPOS_INVENTORY po_cancel',
# 'SJ2-APROPOS_INVENTORY invtranarc'
# ]


os.chdir('../../../')

curr_dir = os.getcwd()
print(curr_dir)

file_path = '/job_input_jsons/streams/'+env+'/kafka_to_ct/stream_base_job.json'
# file_path = '/job_input_jsons/streams/'+env+'/ct_to_current/stream_base_job.json'

base_json_path = curr_dir + file_path

print(base_json_path)

with open(base_json_path) as f:
    base_json_data = json.load(f)

print(base_json_data)

# Define the clusters based on the env
if env.lower() == 'dev':
        ecom_cluster = '1125-test-thrum252'
        aptos_cluster = '1125-test-pease253'
        apropos_cluster = '1125-test-visor254'
elif env.lower() == 'stage':
        ecom_cluster = '1111-test-floor112'
        aptos_cluster = '1113-test-cal128'
        apropos_cluster = '1113-test-demos127'
elif env.lower() == 'prod':
        ecom_cluster = '1127-test-fine1'
        aptos_cluster = '1127-test-where3'
        apropos_cluster = '1127-test-thine2'

for job in job_names:
    params_lst = ["--job_name", job]
    base_json_data['name'] = job
    if 'ecom' in job.lower():
        base_json_data['existing_cluster_id'] = ecom_cluster
    elif 'aptos' in job.lower():
        base_json_data['existing_cluster_id'] = aptos_cluster
    elif 'apropos' in job.lower():
        base_json_data['existing_cluster_id'] = apropos_cluster

    base_json_data['spark_python_task']['parameters'] = params_lst
    print(base_json_data['spark_python_task']['parameters'])
    output_file_path = curr_dir + '/job_input_jsons/streams/'+env+'/kafka_to_ct/{0}.json'.format(job)
    # output_file_path = curr_dir + '/job_input_jsons/streams/'+env+'/ct_to_current/{0}.json'.format(job)
    with open(output_file_path, 'w') as json_file:
        json.dump(base_json_data, json_file)
