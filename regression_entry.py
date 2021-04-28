"""
regression_entry.py
---------
The results are exposed via the Process Studio REST API see https://we.mmm.com/wiki/x/UKsKHg
This script MUST be manually moved into the root directory in order for PALS to find it.
"""
import pals_helpers
import logging
import logging_helper
import os

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """ Main driving method called by PALS executor """

    ############################ Initialize The Logger ####################################
    log = logging_helper.initialize_logger(__name__, 'config.json')    
    log.info('Configured logger')

    ############################ Initialize Results Dictionary ####################################
    # Initialize results dictionary with empty lists so analysis can continue if an error occurs
    dict_results = {
        "RequestKey": dict_main_entry_point_args.get('PALS').get('RequestKey'),
        "RunKey": dict_main_entry_point_args.get('PALS').get('RunKey'),
        "OutputData": [],
        'InputData' : {},
        'Messages': {"Status": "Success"}
    }
    log.info('Initialized dict_results')

    ############################ Validate Input Data ##############################################
    # Check that data is present in dict_main_entry_point_args
    # If no data is present, write a message and return
    if not pals_helpers.validate_input_data(dict_main_entry_point_args):
        dict_results['Messages'].update({"Status": "Failed to validate input data"})
        dict_results['InputData'] = dict_main_entry_point_args
        return dict_results
    log.info('Validated input data')

    ############################ Transform Input Data to DataFrame ##############################
    df_tag_data = pals_helpers.dictionary_to_dataframe(dict_main_entry_point_args)
    log.info('Transformed dictionary to dataframe')

    ############################ Load Machine Learning Model File #################################
    # This example model file contains a simple regression model which is designed to be used on simulated tag data
    # A sample of this simulated tag data is found in the provided test_periodic_values.json file
    model = pals_helpers.load_model('model.pkl')
    log.info('Loaded model file')

    ############################ Execute Machine Learning Model ###################################
    # This section might need to be customized based on the specifics of your model
    # See the documentation in pals_helpers.py for a list of supported options for output_format
    df_tag_data['Predicted_WATER_2'] = pals_helpers.predict(model, df_tag_data, output_format='list')
    log.info('Inferenced model file')

    ############################ Fill Results Dictionary ##########################################
    # Results are accessible from the Process Studio REST API
    # More information found here: https://we.mmm.com/wiki/pages/viewpage.action?pageId=504015696
    dict_results['OutputData'] = pals_helpers.dataframe_to_list(df_tag_data)
    log.info('Filled dict_results with model output')

    ############################ Return Results Dictionary ########################################
    # Results are accessible from the Process Studio REST API
    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    # Use this data to test the entry script before deploying to PALS
    import json
    with open('test_files/test_periodic_values.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    print(main_entry_point(ENTRY_POINT_ARGS))
