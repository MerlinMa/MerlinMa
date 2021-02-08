"""
pals_helpers
---------
This collection of helper functions is used to streamline the entry script
"""

import sys
import os
import json
import pickle
import pandas as pd

def validate_input_data(main_entry_point_args: dict):
    """Determins if the entry point args actually contain usable data"""

    if main_entry_point_args is None:
        raise ValueError('main_entry_point_args cannot be None')

    extract_type = main_entry_point_args.get('ExtractionType')
    if extract_type in ['PeriodicStatistics', 1, '1']:
        contains_data = bool(main_entry_point_args.get('PeriodicStatistics').get('Timestamps'))
    elif extract_type in ['PeriodicValues', 2, '2']:
        contains_data = bool(main_entry_point_args.get('PeriodicValues').get('Timestamps'))
    elif extract_type in ['RawValues', 3, '3']:
        contains_data = True
    else:
        raise ValueError(f'Value for ExtractionType not recognized: {extract_type}')

    return contains_data

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

def load_model(filename: str):
    """Loads a trained machine learning model from a local file using pickle serialization"""

    if filename is None:
        raise ValueError('filename cannot be None')

    filename = '../' + filename
    filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
    try:
        with open(filepath, 'rb') as model_file:
            try:
                model = pickle.load(model_file)
            except:
                from sklearn import __version__ as sklearn_version
                raise IOError(f'''Could not load model with sklearn version {sklearn_version}\n
                    Use a version that more closely matches
                        the version used to develop the model.''')
    except:
        raise OSError(f'Could not open file named {filename} at: {filepath}')

    return model

def load_config(filename: str):
    """Loads the config information from a json file"""

    if filename is None:
        raise ValueError('filename cannot be None')

    filename = '../' + filename
    filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
    try:
        with open(filepath) as json_file:
            config = json.load(json_file)
    except:
        raise OSError(f'Could not open file named {filename} at: {filepath}')

    return config

def get_timestamp_list(main_entry_point_args: dict):
    """Gets a list of the relavant timestamps from the tag data"""

    if main_entry_point_args is None:
        raise ValueError('main_entry_point_args cannot be None')

    extract_type = main_entry_point_args.get('ExtractionType')
    if extract_type in ['PeriodicStatistics', 'Periodicstatistic', 1, '1']:
        times_list = main_entry_point_args.get('PeriodicStatistics').get('Timestamps')
    elif extract_type in ['PeriodicValues', 2, '2']:
        times_list = main_entry_point_args.get('PeriodicValues').get('Timestamps')
    elif extract_type in ['RawValues', 'Rawvalue', 3, '3']:
        raise ValueError('Timestamp list cannot be extracted from RawValues')
    else:
        raise ValueError(f'Value for ExtractionType not recognized: {extract_type}')

    # remove last value to prevent duplicate value from different sampling periods
    times_list = times_list[:-1]

    return times_list

def dataframe_to_list(df_data: pd.DataFrame, dict_results: dict):
    """Takes a pandas DataFrame and adds it to the results dictionary"""

    if df_data is None:
        raise ValueError('df_data cannot be None')
    if dict_results is None:
        raise ValueError('dict_results cannot be None')

    for col in df_data:
        dict_results[col] = list(df_data[col])

    return dict_results

def predict(model, input_data: pd.DataFrame, output_format: str = 'list'):
    """
    Executes the model on the given input data
    Output format can be specified for compatability with later calculations
    Note: The dimension of the input data must match the expected size for the model
    """

    if model is None:
        raise ValueError('model cannot be None')
    if input_data is None:
        raise ValueError('input_data cannot be None')
    if output_format is None:
        raise ValueError('output_format cannot be None')

    np_tag_data = input_data.to_numpy()

    if np_tag_data.shape[1] == model.coef_.shape[1]:
        output = model.predict(np_tag_data)
    else:
        raise ValueError(f'''Input dimension {np_tag_data.shape[1]}
            does not match model defined dimension {model.coef_.shape[1]}''')

    if output_format in ['numpy', 'np', 'array']:
        final_output = output
    elif output_format in ['pandas', 'pd', 'dataframe', 'df_results']:
        final_output = pd.DataFrame(output)
    elif output_format in ['list']:
        output = output.tolist()
        restructured_output = []
        for lst in output:
            restructured_output.append(lst[0])
        final_output = restructured_output
    else:
        raise ValueError(f'Value for output_format not recognized: {output_format}')

    return final_output

def evaluate_filters(filters: dict, dict_main_entry_point_args: dict) -> bool:
    """ Evaluates the filter criteria against the scheduled input data """

    lst_results = []

    for key, value in filters.items():

        tagkey = value.get('key')
        condition = value.get('condition')
        value = value.get('value')
        data = dict_main_entry_point_args['Tags'].get(tagkey)

        if condition in ['Contains', 'contains', 'in', 'has']:
            lst_results.append(str(value) in str(data['Value']))
        elif condition in ['Above', 'above', '>', 'Greater', 'greater', 'greater than']:
            lst_results.append(float(data['Value']) > float(value))
        elif condition in ['Below', 'below', '<', 'Less', 'less', 'less than']:
            lst_results.append(float(data['Value']) < float(value))
        elif condition in ['Equals', 'equals', '=', '==', 'equal', 'equal to']:
            lst_results.append(float(data['Value']) == float(value))
        elif condition in ['Not Equals', 'not equals', 'not equal', '!=', '~=']:
            lst_results.append(float(data['Value']) != float(value))
        else:
            raise ValueError(
                f"In filter named \'{key}\', value for filter condition not recognized: {condition}"
            )

    return False not in lst_results
