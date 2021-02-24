"""
This is a sample entry script used for model deployment to Azure Endpoint

For a tutorial on how to use this file, see here:
    https://we.mmm.com/wiki/display/ENG/Deploy+locally+trained+model+to+Azure+ML
"""
import json
import os
import pickle
import pandas as pd

###################################################################################################
# FUNCTION DEFINITIONS (ignore this section, skip to line 80)
###################################################################################################
def dictionary_to_dataframe(main_entry_point_args: dict):
    """Converts the entry point args from a dictionary to a pandas DataFrame"""

    if main_entry_point_args is None:
        raise ValueError('main_entry_point_args cannot be None')

    extract_type = main_entry_point_args.get('ExtractionType')
    if extract_type in ['PeriodicStatistics', 'Periodicstatistic', 1, '1']:
        df_tag_data = __get_statistics_df(main_entry_point_args)
    elif extract_type in ['PeriodicValues', 2, '2']:
        df_tag_data = __get_values_df(main_entry_point_args)
    elif extract_type in ['RawValues', 'Rawvalue', 3, '3']:
        raise ValueError('Cannot transform RawValues dictionary to Dataframe')
    else:
        raise ValueError(f'Value for ExtractionType not recognized: {extract_type}')

    return df_tag_data

def __get_statistics_df(main_entry_point_args: dict):
    """
    Converts the entry point args from a dictionary to a pandas DataFrame
    Specifically handles periodic statistics data
    """

    df_results = pd.DataFrame()
    df_input_tags = pd.DataFrame(main_entry_point_args['InputTags'])
    dict_data = main_entry_point_args.get('PeriodicStatistics').get('Data')
    timestamps = main_entry_point_args.get('PeriodicStatistics').get('Timestamps')

    for key in dict_data.keys():
        temp = []
        for value in dict_data[key]:
            temp.append(value['Average'])
        df_results[df_input_tags.set_index('Key').loc[key, 'Name']] = temp
    df_results.index = pd.to_datetime(timestamps)

    # remove last value to prevent duplicate value from different sampling periods
    df_results = df_results[:-1]

    return df_results

def __get_values_df(main_entry_point_args: dict):
    """
    Converts the entry point args from a dictionary to a pandas DataFrame
    Specifically handles periodic values data
    """

    df_results = pd.DataFrame()
    df_input_tags = pd.DataFrame(main_entry_point_args['InputTags'])
    dict_data = main_entry_point_args.get('PeriodicValues').get('Data')
    timestamps = main_entry_point_args.get('PeriodicValues').get('Timestamps')

    for key in dict_data.keys():
        temp = []
        for value in dict_data[key]:
            temp.append(value['Value'])
        df_results[df_input_tags.set_index('Key').loc[key, 'Name']] = temp
    df_results.index = pd.to_datetime(timestamps)

    # remove last value to prevent duplicate value from different sampling periods
    df_results = df_results[:-1]

    return df_results

###################################################################################################
# AZURE FUNCTIONS
###################################################################################################
def init():
    """
    This funciton is called at the beginning of running the model
    It needs to load the model file
    """
    global MODEL
    model_file_name = 'model.pkl'
    if __debug__: # when running locally to test
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), model_file_name)
    else:         # when running in Azure ML
        filepath = os.path.join(os.getenv('AZUREML_MODEL_DIR'), model_file_name)

    # KEY STEP ##################################
    with open(filepath, 'rb') as model_file:
        MODEL = pickle.load(model_file)
    #############################################

def run(data):
    """
    This function is called when the model recieves data
    I needs to load the data, feed it to the model, and return the results
    """
    try:
        json_data = json.load(data)
        df_tag_data = dictionary_to_dataframe(json_data)
        # KEY STEP ##############################
        result = MODEL.predict(df_tag_data)
        #########################################
        return result.tolist()
    except Exception as exp:
        error = str(exp)
        return error

###################################################################################################
# LOCAL TESTING FUNCTION
###################################################################################################
if __name__ == "__main__":
    with open('test_data.json') as json_file:
        init()
        print(run(json_file))
