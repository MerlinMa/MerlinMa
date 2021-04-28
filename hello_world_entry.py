"""
hello_world_entry
---------
This script is intended to be an introduction to PALS excecutions
Use this script to test that your PALS deployment is working

Results are accessible from the Process Studio REST API
More information found here:
    https://we.mmm.com/wiki/x/UKsKHg

For a tutorial on using this file, see here:
    https://we.mmm.com/wiki/x/Yzq5I
"""
import json
import pals_helpers
import logging_helper
import logging

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """Main driving method called by PALS executor"""

    ############################ Initialize The Logger ####################################
    log = logging_helper.initialize_logger(__name__, 'config.json')    
    log.info('Configured logger')
    
    ############################ Initialize Results Dictionary ####################################
    dict_results = {
        "RequestKey": dict_main_entry_point_args.get('PALS').get('RequestKey'),
        "RunKey": dict_main_entry_point_args.get('PALS').get('RunKey'),
        "OutputData": str(),
        "InputData" : {},
        "Messages": {"Status": "Success"}     
    }
    log.info('Initialized dict_results')

    ############################ Validate Input Data ##############################################
    # Check that data is present in dict_main_entry_point_args. If no data is present, write a message and return
    if not pals_helpers.validate_input_data(dict_main_entry_point_args):
        dict_results['Messages'].update({"Status": "Failed to validate input data"})
        dict_results['InputData'] = dict_main_entry_point_args
        return dict_results
    log.info('Validated input data')

    ############################ Transform Input Data to DataFrame ##############################
    # TODO: Here is where you might transform the input data dictionary into a dataframe
    # df_tag_data = pals_helpers.dictionary_to_dataframe(dict_main_entry_point_args)
    # logging.info('Transformed dictionary to dataframe')

    ############################ Load Machine Learning Model File #################################
    # TODO: Here is where you might load your model file
    # logging.info('Loaded model file')
    
    ############################ Execute Machine Learning Model ###################################
    # TODO: Here is where you might execute your model file
    # logging.info('Inferenced model file')

    ############################ Fill Results Dictionary ##########################################
    dict_results['OutputData'] = 'Hello, world!'
    log.info('Filled dict_results with output')

    ############################ Fill Results Dictionary ##########################################
    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    with open('test_files/test_periodic_values.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    print(main_entry_point(ENTRY_POINT_ARGS))
