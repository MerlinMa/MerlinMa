import os
import pandas as pd
import numpy as np
import json
import pickle

class PALSHelpers:

    def ValidateInputData(self, dictMainEntryPointArgs):

        # TODO: Evaluate if dictMainEntryPointArgs  contains data
        # Return True/False

        return True

    ###########################################
    # DictionaryToDataframe
    ###########################################
    def DictionaryToDataframe(self, dictMainEntryPointArgs):
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
    def __GetStatisticsDataframe(self, statsDictionary):
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

        # remove last value to prevent duplicate predicted values from different sampling periods
        df = df[:-1]
        
        return df

    ###########################################
    # __GetPeriodicValuesDataframe
    ###########################################
    def __GetPeriodicValuesDataframe(self, valuesDictionary):
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

        # remove last value to prevent duplicate predicted values from different sampling periods
        df = df[:-1]
        
        return df

    ###########################################
    # LoadModelFileFromDirectory
    ###########################################
    def LoadModel(self, filename):
        
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
        try:
            model_file = open(filepath, 'rb')
        except:
            raise OSError(f'Could not open file named {filename} at: {filepath}')

        try:
            model = pickle.load(model_file)
        except:
            from sklearn import __version__
            raise IOError(f'Could not load model with sklearn version {__version__}\nUse a version that more closely matches the version used to develop the model.')

        model_file.close()
        
        return model

    ###########################################
    # LoadConfigFile
    ###########################################
    def LoadConfigFile(self, config_filename):

        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_filename)

        with open(filepath) as json_file: 
            config = json.load(json_file)

        return config

    ###########################################
    # GetTimestampList
    ###########################################
    def GetTimestampList(self, dictMainEntryPointArgs):
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