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
    model = pals_helpers.load_model('model.pkl')

    ############################ Execute Machine Learning Model ###################################
    # This section might need to be customized based on the specifics of your model
    df_tag_data['Predicted_WATER_2'] = pals_helpers.predict(model, df_tag_data, output_format='list')

    ############################ Fill Results Dictionary ##########################################
    # Results are accessible from the Process Studio REST API
    # More information found here: https://we.mmm.com/wiki/pages/viewpage.action?pageId=504015696
    dict_results = pals_helpers.dataframe_to_list(df_tag_data)

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

    from azure_helper import AzureHelper
    azure_helper = AzureHelper('config.json')
    csv_tag_data = df_tag_data.to_csv(index_label="Timestamps")
    azure_helper.upload_data(
        csv_tag_data,
        blob_name='<my_test_blob>',
        blob_subdir='/',
        overwrite=True)

    return dict_results['Messages']

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
