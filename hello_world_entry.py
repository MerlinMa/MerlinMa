"""
hello_world_entry
---------
This script is intended to be an introduction to PALS excecutions
Use this script to test that your PALS deployment is working
"""
import json
import os
from PALS import PALSMethods

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """Main driving method called by PALS executor"""
    
    ############################ Initialize Results Dictionary ####################################
    dict_results = {
        "RequestKey": dict_main_entry_point_args.get('PALS').get('RequestKey'),
        "RunKey": dict_main_entry_point_args.get('PALS').get('RunKey'),
        "Messages": str(),
        "DataSchema": "Stacked",
        "Data": list(),
        "Environment": os.getenv('CONDA_DEFAULT_ENV')
        }

    ############################ Fill Results Dictionary ##########################################
    dict_results['Messages'] = 'Hello, World!'

    ############################ Exit the script ##########################################
    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    with open('test_files/test_periodic_values.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    print(main_entry_point(ENTRY_POINT_ARGS))
