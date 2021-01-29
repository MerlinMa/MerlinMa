import os
import pandas as pd
import numpy as np
import json
import pickle

class PALSHelpers:

    ###########################################
    # ValidateInputData
    ###########################################
    def ValidateInputData(self, dictMainEntryPointArgs:dict):
        extract_type = dictMainEntryPointArgs.get('ExtractionType')
        if extract_type in ['PeriodicStatistics', 1, '1']:
            contains_data = bool(dictMainEntryPointArgs.get('PeriodicStatistics').get('Timestamps'))
        elif extract_type in ['PeriodicValues', 2, '2']:
            contains_data = bool(dictMainEntryPointArgs.get('PeriodicValues').get('Timestamps'))
        elif extract_type in ['RawValues', 3, '3']:
            contains_data = True
        else:
            raise ValueError(f'Value for ExtractionType not recognized: {extract_type}')
        
        return contains_data

    ###########################################
    # DictionaryToDataframe
    ###########################################
    def DictionaryToDataframe(self, dictMainEntryPointArgs:dict):
        extract_type = dictMainEntryPointArgs.get('ExtractionType')
        if extract_type in ['PeriodicStatistics', 1, '1']:
            dfTagData = self.__GetStatisticsDataframe(dictMainEntryPointArgs)
        elif extract_type in ['PeriodicValues', 2, '2']:
            dfTagData = self.__GetPeriodicValuesDataframe(dictMainEntryPointArgs)
        elif extract_type in ['RawValues', 3, '3']:
            raise ValueError('Cannot transform RawValues dictionary to Dataframe')
        else:
            raise ValueError(f'Value for ExtractionType not recognized: {extract_type}')

        return dfTagData

    ###########################################
    # __GetStatisticsDataframe
    ###########################################
    def __GetStatisticsDataframe(self, statsDictionary:dict):
        df = pd.DataFrame()
        dfInputTags = pd.DataFrame(statsDictionary['InputTags'])
        dictData = statsDictionary.get('PeriodicStatistics').get('Data')
        lstTimestamps = statsDictionary.get('PeriodicStatistics').get('Timestamps')

        for key in dictData.keys():
            lstTemp = []
            for value in dictData[key]:
                lstTemp.append(value['Average'])
            df[dfInputTags.set_index('Key').loc[key,'Name']] = lstTemp
        df.index = pd.to_datetime(lstTimestamps)

        # remove last value to prevent duplicate values from different sampling periods
        df = df[:-1]
        
        return df

    ###########################################
    # __GetPeriodicValuesDataframe
    ###########################################
    def __GetPeriodicValuesDataframe(self, valuesDictionary:dict):
        df = pd.DataFrame()
        dfInputTags = pd.DataFrame(valuesDictionary['InputTags'])
        dictData = valuesDictionary.get('PeriodicValues').get('Data')
        lstTimestamps = valuesDictionary.get('PeriodicValues').get('Timestamps')

        for key in dictData.keys():
            lstTemp = []
            for value in dictData[key]:
                lstTemp.append(value['Value'])
            df[dfInputTags.set_index('Key').loc[key,'Name']] = lstTemp
        df.index = pd.to_datetime(lstTimestamps)

        # remove last value to prevent duplicate values from different sampling periods
        df = df[:-1]
        
        return df

    ###########################################
    # LoadModel
    ###########################################
    def LoadModel(self, filename:str):
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
        try:
            with open(filepath, 'rb') as model_file:
                try:
                    model = pickle.load(model_file)
                except:
                    from sklearn import __version__ as sklearn_version
                    raise IOError(f'Could not load model with sklearn version {sklearn_version}\nUse a version that more closely matches the version used to develop the model.')
        except:
            raise OSError(f'Could not open file named {filename} at: {filepath}')
        
        return model

    ###########################################
    # LoadConfig
    ###########################################
    def LoadConfig(self, filename:str):
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
        try:
            with open(filepath) as json_file: 
                config = json.load(json_file)
        except:
            raise OSError(f'Could not open file named {filename} at: {filepath}')

        return config

    ###########################################
    # GetTimestampList
    ###########################################
    def GetTimestampList(self, dictMainEntryPointArgs:dict):
        extract_type = dictMainEntryPointArgs.get('ExtractionType')
        if extract_type in ['PeriodicStatistics', 1, '1']:
            times_list = dictMainEntryPointArgs.get('PeriodicStatistics').get('Timestamps')
        elif extract_type in ['PeriodicValues', 2, '2']:
            times_list = dictMainEntryPointArgs.get('PeriodicValues').get('Timestamps')
        elif extract_type in ['RawValues', 3, '3']:
            raise ValueError('Timestamp list cannot be extracted from RawValues')
        else:
            raise ValueError(f'Value for ExtractionType not recognized: {extract_type}')

        return times_list

    ###########################################
    # DataframeToList
    ###########################################
    def DataframeToList(self, dfResults:pd.DataFrame, dictResults:dict):
        for col in dfResults:
            dictResults[col] = list(dfResults[col])
        
        return dictResults

    ###########################################
    # Predict
    ###########################################
    def Predict(self, model, inputData:pd.DataFrame, output_format:str = 'list'):
        npTagData = inputData.to_numpy()

        if npTagData.shape[1] == model.coef_.shape[1]:
            output = model.predict(npTagData)
        else:
            raise ValueError(f'Input dimension {npTagData.shape[1]} does not match model defined dimension {model.coef_.shape[1]}')

        if output_format in ['numpy', 'np', 'array']:
            output = output
        elif output_format in ['pandas', 'pd', 'dataframe', 'df']:
            output = pd.DataFrame(output)
        elif output_format in ['list']:
            output = output.tolist()
            restructured_output = []
            for l in output:
                restructured_output.append(l[0])
            output = restructured_output
        else:
            raise ValueError(f'Value for output_format not recognized: {output_format}')

        return output