import sys
import os
import json
import pickle
import pandas as pd
import numpy as np
import PALSHelpers
import AzureHelpers

def MainEntryPoint(dictMainEntryPointArgs: dict) -> dict:
    """ Main driving method called by PALS executor """

    # TODO: Instantiate the logging object 

    # TODO: Validate dictMainEntryPointArgs contains data

    # TODO: Load config file 

    # TODO: Transform input data into dataframe (optional transformation)

    # TODO: Execute Model or Script

    # TODO: Structure Results Dictionary. Assume most cases we want input data and output data returned with associated timestamps

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