"""
template_entry_script
---------
This template is meant to showcase all the functionality availible for a PALS execution
Results are accessible from the Process Studio REST API
More information on the REST API found here:
    https://we.mmm.com/wiki/pages/viewpage.action?pageId=504015696
For a tutorial on using this file, see here:
    https://we.mmm.com/wiki/display/ENG/2.+Python+Regression+Model+Example
"""

import pals_helpers

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """ Main driving method called by PALS executor """

    ############################ Initialize Results Dictionary ####################################
    # In most cases we want to return timestamps, input data, and output data
    # Initialize results dictionary with empty lists so analysis can continue if an error occurs
    dict_results = {
        "RequestKey": dict_main_entry_point_args.get('PALS').get('RequestKey'),
        "RunKey": dict_main_entry_point_args.get('PALS').get('RunKey'),
        "OutputData": [],
        "InputData" : {},
        "Messages": {"Status": "Success"}     
    }

    ############################ Validate Input Data ##############################################
    # Check that data is present in dict_main_entry_point_args. If no data is present, write a message and return
    if not pals_helpers.validate_input_data(dict_main_entry_point_args):
        dict_results['Messages'].update({"Status": "Missing input data"})
        dict_results['InputData'] = dict_main_entry_point_args
        return dict_results

    ############################ Transform Input Data to DataFrame ##############################
    df_tag_data = pals_helpers.dictionary_to_dataframe(dict_main_entry_point_args)
    
    ############################ Load Machine Learning Model File #################################
    # This example model file contains a simple regression model
    model = pals_helpers.load_model('model.pkl')

    ############################ Execute Machine Learning Model ###################################
    # This section might need to be customized based on the specifics of your model
    df_tag_data['Predicted_WATER_2'] = pals_helpers.predict(model, df_tag_data, output_format='list')

    ############################ Fill Results Dictionary ##########################################
    # Results are accessible from the Process Studio REST API
    # More information found here: https://we.mmm.com/wiki/pages/viewpage.action?pageId=504015696
    dict_results = pals_helpers.dataframe_to_list(df_tag_data)

    ############################ (OPTIONAL) Send data to Azure Endpoint ###########################
    # This section will send tag data to a machine learning model in an Azure Container Instance
    # Using this package requires the installation of azure-storage-blob
    #       Run "conda install -c conda-forge azure-storage-blob"
    #       or create an environment with "conda env create -f environment.yml"
    # This works with models created by Azure Auto ML and with models that use timestamps as input
    # Get the endpoint URL from the endpoint page on ml.azure.com
    # tag_list should match the list of inputs on the Test section of the endpoint page
    # Specify the data for azure_info in config.json the same way you would if you were
    #   uploading to blob storage
    # See this tutorial for more information:
    #   https://we.mmm.com/wiki/display/ENG/Feed+data+to+Endpoint+models
    # Example:

    from azure_helper import AzureHelper
    endpoint_url = ''
    tag_list = ''
    tag_list = tag_list.split(',')
    azure_helper = AzureHelper('config.json')
    endpoint_results = azure_helper.endpoint(endpoint_url, df_tag_data, tag_list)
    dict_results['OutputData'] = endpoint_results

    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    # Use this data to test the entry script before deploying to PALS
    import json
    with open('test_files/test_periodic_values.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    RESULTS = main_entry_point(ENTRY_POINT_ARGS)

    print(RESULTS)
