import json
import os
from pathlib import Path


if __name__ == '__main__':
    # curr_dir = os.path.abspath(os.path.dirname(__file__))
    os.chdir('../../')
    curr_dir = os.getcwd()
    print(curr_dir)
    schema_folder_path = '/schemas/'
    folders = os.listdir(curr_dir + schema_folder_path)
    for folder in folders:
        file_path = "/schemas/{0}/".format(folder)

        output_base_path = file_path + "schemas/{0}/".format(folder)
        if not os.path.exists(output_base_path):
            os.makedirs(output_base_path)

        base_path = curr_dir + file_path
        files = os.listdir(base_path)
        print(files)
        for file in files:
            json_path = base_path + file
            print("Input JSON: ", json_path)

            with open(json_path) as f:
                json_body = json.load(f)

            for col in json_body['fields'][0]['type']['fields']:
                # print(col['type'])
                if isinstance(col['type'][1], dict):
                    decimal_dict = col['type'][1]
                    if decimal_dict['logicalType'] and decimal_dict['logicalType'] == 'decimal':
                        decimal_dict['precision'] = 20
                        decimal_dict['scale'] = 6

                        avro_schema_str = json.dumps(json_body, indent=4)
                        with open(output_base_path + file, "w") as output_file:
                            output_file.write(avro_schema_str)

                        print('Changed JSON: ', json_body)
