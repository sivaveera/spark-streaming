from src.utils.CommonUtils import get_or_create_spark


def log_streaming_query_status(result):
    insert_stmt = """  INSERT INTO jobs_logs.streaming_audits
  VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {}, {}, '{}')
  """.format(result['id'], result['runId'], result['batchId'], result['name'], result['sources'][0]['description'],
             result['sink']['description'], result['numInputRows'], result['sink']['numOutputRows'],
             result['timestamp'])
    get_or_create_spark().sql(insert_stmt)
