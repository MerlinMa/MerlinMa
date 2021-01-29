"""
template_entry_script
---------
TODO docstring

"""
import json
import pals_helpers
from azure_helper import AzureHelper

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """ Main driving method called by PALS executor """

    ############################ Initialize Helper Methods ########################################
    azure_helper = AzureHelper('config.json')

    ############################ Initialize Results Dictionary ####################################
    # In most cases we want input data and output data returned with associated timestamps
    # Initialize with empty lists so that excecution/analysis can continue if an error occurs
    dict_results = {
        'Messages': [],
        'Timestamps': [],
        'PredictedValues': []
    }
    for tag in dict_main_entry_point_args['InputTags']:
        dict_results[tag['Name']] = []

    ############################ Validate Input Data ##############################################
    if not pals_helpers.validate_input_data(dict_main_entry_point_args):
        dict_results['Messages'] = 'No data in pals payload'
        return dict_results

    ############################ Load Model File ##################################################
    model = pals_helpers.load_model('model.pkl') # comment out for initial deployment testing

    ############################ Transform input data into dataframe (optional transformation) ####
    df_tag_data = pals_helpers.dictionary_to_dataframe(dict_main_entry_point_args)

    ############################ Execute Model ####################################################
    # This section will be need to be customized to your deployment and your model
    predictions = pals_helpers.predict(model, df_tag_data, output_format='list')

    ############################ Fill Results Dictionary ##########################################
    dict_results['Timestamps'] = pals_helpers.get_timestamp_list(dict_main_entry_point_args)

    # Replace predictions with 'Hello World!' for initial deployment testing
    dict_results['PredictedValues'] = predictions

    dict_results = pals_helpers.dataframe_to_list(df_tag_data, dict_results)

    ############################ Upload Data to Azure Blob Storage ################################
    # The storage account and container are specified in a json file
    # Data can be uploaded from a file (any file extension) or from variables
    # comment out this section for initial deployment testing
    csv_tag_data = df_tag_data.to_csv(index_label="Timestamps")
    azure_helper.upload_data(csv_tag_data, blob_name='blob_from_data.csv', blob_subdir='/')
    with open('test.csv', 'w') as csv_file:
        csv_file.write(csv_tag_data)
    azure_helper.upload_file('test.csv')

    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    with open('example_file.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    RESULTS = main_entry_point(ENTRY_POINT_ARGS)

    print(RESULTS)
