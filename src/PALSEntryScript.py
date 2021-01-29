import sys
import os
import json
import pickle
import pandas as pd
import numpy as np
from PALSHelpers import PALSHelpers
from AzureHelpers import AzureHelpers
# TODO from PALSLogging import 

def MainEntryPoint(dictMainEntryPointArgs: dict) -> dict:
    """ Main driving method called by PALS executor """

    ############################ Initialize Helper Methods ########################################
    pals_helper = PALSHelpers()
    azure_helper = AzureHelpers('config.json')
    # TODO: Instantiate the logging object

    ############################ Initialize Results Dictionary ####################################
    # In most cases we want input data and output data returned with associated timestamps
    # Initialize with empty lists so that excecution/analysis can continue if an error occurs
    dictResults = {
        'Messages': [],
        'Timestamps': [],
        'PredictedValues': []
    }
    for tag in dictMainEntryPointArgs['InputTags']:
        dictResults[tag['Name']] = []

    ############################ Validate Input Data ##############################################
    if not pals_helper.ValidateInputData(dictMainEntryPointArgs):
        # TODO implement logging here
        dictResults['Messages'] = 'No data in pals payload'
        return dictResults

    ############################ Load Model File ##################################################
    model = pals_helper.LoadModel('model.pkl') # comment out for initial deployment testing

    ############################ Transform input data into dataframe (optional transformation) ####
    dfTagData = pals_helper.DictionaryToDataframe(dictMainEntryPointArgs)

    ############################ Execute Model ####################################################
    # This section will be need to be customized to your deployment and your model
    predictions = pals_helper.Predict(model, dfTagData, output_format='list')

    ############################ Fill Results Dictionary ##########################################
    dictResults['Timestamps'] = pals_helper.GetTimestampList(dictMainEntryPointArgs)
    dictResults['PredictedValues'] = predictions # Replace Output with 'Hello World!' for initial deployment testing
    dictResults = pals_helper.DataframeToList(dfTagData, dictResults)

    ############################ Upload Data to Azure Blob Storage ################################
    # The storage account and container are specified in a json file
    # Data can be uploaded from a file (any file extension) or from variables
    # comment out this section for initial deployment testing
    csvTagData = dfTagData.to_csv(index_label="Timestamps")
    azure_helper.UploadDataToBlob(csvTagData, blob_name='blob_from_data.csv', blob_subdir='/')
    with open('test.csv', 'w') as csv_file:
        csv_file.write(csvTagData)
    azure_helper.UploadFileToBlob('test.csv')

    return dictResults

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    dictResults = dict()

    with open('PeriodicValuesTestData.json') as json_file:
        dictMainEntryPointArgs = json.load(json_file)

    dictResults = MainEntryPoint(dictMainEntryPointArgs)

    print(dictResults)