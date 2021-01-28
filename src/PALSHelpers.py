import os

class PALSHelpers:

    def ValidateInputData(self, dictMainEntryPointArgs):

        # TODO: Evaluate if dictMainEntryPointArgs  contains data
        # Return True/False

        return True

    def TransformDictionaryToDataframe(self, dictMainEntryPointArgs):

        # TODO: Transform the dictMainEntryPointArgs dictionary into a dataframe
        # Check if contains PeriodicValues, PeriodicStatistics, or RawValues
        # Only support PeriodicValues and PeriodicStatistics currently
        pass

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

    def LoadConfigFileContents(self):

        # TODO: Check if config.json file exists in directory

        # If config file does not exist, return an empty dictionary

        # If config file does exist, load the contents into a dictionary object

        # Return the dictionary object
        pass

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