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
        'Messages': [],
        'Timestamps': []
    }

    # Add an empty list for each input tag, these lists will be filled later
    for tag in dict_main_entry_point_args['InputTags']:
        dict_results[tag['Name']] = []

    # Add an empty list for each output of your machine learning model (if any exist)
    dict_results['Predicted_WATER_2'] = []

    ############################ Validate Input Data ##############################################
    # Check that data is present in dict_main_entry_point_args
    # If no data is present, write a message and return
    if not pals_helpers.validate_input_data(dict_main_entry_point_args):
        dict_results['Messages'] = 'No data in pals payload'
        return dict_results

    ############################ Transform Input Data to DataFrame ##############################
    df_tag_data = pals_helpers.dictionary_to_dataframe(dict_main_entry_point_args)

    ############################ Load Machine Learning Model File #################################
    # This example model file contains a simple regression model
    # which is designed to be used on simulated tag data
    # A sample of this simulated tag data is found in the provided test_data.json file
    model = pals_helpers.load_model('model.pkl')

    ############################ Execute Machine Learning Model ###################################
    # This section might need to be customized based on the specifics of your model
    # The use of pals_heleprs.predict is optional
    # In this example DSFLINE1_SIMULATED_WATER_2 is the tag we want to predict using regression
    # You can implement model execution code developed specifically for your model if need be
    # Remove DSFLINE1_SIMULATED_GAS_2 from the input data as it was not part of the training data
    df_input_data = df_tag_data.drop('DSFLINE1_SIMULATED_GAS_2', 1)
    # See the documentation in pals_helpers.py for a list of supported options for output_format
    predictions = pals_helpers.predict(model, df_input_data, output_format='list')

    ############################ Fill Results Dictionary ##########################################
    # Results are accessible from the Process Studio REST API
    # More information found here: https://we.mmm.com/wiki/pages/viewpage.action?pageId=504015696
    timestamps = pals_helpers.get_timestamp_list(dict_main_entry_point_args)
    dict_results['Timestamps'] = timestamps
    dict_results = pals_helpers.dataframe_to_list(df_tag_data, dict_results)
    dict_results['Predicted_WATER_2'] = predictions

    ############################ (OPTIONAL) Upload Data to Azure Blob Storage #####################
    # The storage account and container name are specified under "azure_info" in a json config file
    # Using this package requires the installation of azure-storage-blob
    #       Run "conda install -c conda-forge azure-storage-blob"
    #       or create an environment with "conda env create -f environment.yml"
    # Data can be uploaded from a file on disk (any file extension) or from memory
    # Use upload_data to upload data from memory
    # Use upload_file to upload a file on disk
    # See this tutorial for more information:
    #   https://we.mmm.com/wiki/display/ENG/Upload+data+to+Azure+Storage+via+Python

    # from azure_helper import AzureHelper
    # azure_helper = AzureHelper('config.json')
    # csv_tag_data = df_tag_data.to_csv(index_label="Timestamps")
    # azure_helper.upload_data(
    #     csv_tag_data, blob_name=timestamps[0], blob_subdir='/', overwrite=True)

    ############################ (OPTIONAL) Send data to Azure Endpoint ###########################
    # This section will send tag data to a machine learning model in an Azure Container Instance
    # Using this package requires the installation of azure-storage-blob
    #       Run "conda install -c conda-forge azure-storage-blob"
    #       or create an environment with "conda env create -f environment.yml"
    # This works with models created by Azure Auto ML and with models that use timestamps as input
    # For more info see this tutorial: 
    # Get the endpoint URL from the endpoint page on ml.azure.com 
    # tag_list should match the list of inputs on the Test section of the endpoint page
    # Specify the data for azure_info in config.json the same way you would if you were
    #   uploading to blob storage
    # See this tutorial for more information:
    #   https://we.mmm.com/wiki/display/ENG/Feed+data+to+Endpoint+models
    # Example:

    # from azure_helper import AzureHelper
    # endpoint_url = ''
    # tag_list = ''
    # tag_list = tag_list.split(',')
    # azure_helper = AzureHelper('config.json')
    # endpoint_results = azure_helper.endpoint(endpoint_url, df_tag_data, tag_list)
    # dict_results['endpoint_results'] = endpoint_results

    ############################ (OPTIONAL) Upload Data to SQL Database ###########################
    # The server, database, and default schema are specified under sql_info in the json config file
    # The PALS requestID and runID are pulled from the main entry point args
    #   If entry point args are an older version than 1.1, then requestID and runID are set to 0
    #   As of Process Studio 2.12, all entry point args should be version 1.1 or newer
    # A pandas DataFrame can be uploaded all at once using SQLHelper.upload_df
    # Data can be uploaded one tag at a time using SQLHelper.upload_tag
    # Data can be insterted manually using SQLHelper.insert
    # Any SQL query can be executed using SQLHelper.execute
    # See this tutorial for more information:
    #   https://we.mmm.com/wiki/display/ENG/Upload+data+to+SQL+database+via+Python

    # from sql_helper import SQLhelper
    # sql_connector = SQLhelper('config.json', dict_main_entry_point_args)
    # sql_connector.upload_tag('Table_5', timestamps, 'Predicted_WATER_2', dict_results['Predicted_WATER_2'])
    # sql_connector.upload_df('Table_5', timestamps, df_tag_data)

    ############################ Return Results Dictionary ########################################
    # Results are accessible from the Process Studio REST API
    # More information found here: https://we.mmm.com/wiki/pages/viewpage.action?pageId=504015696
    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    # Use this data to test the entry script before deploying to PALS
    import json
    with open('test_input_data.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    RESULTS = main_entry_point(ENTRY_POINT_ARGS)

    print(RESULTS)
