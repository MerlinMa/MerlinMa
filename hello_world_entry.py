"""
hello_world_entry
---------
This script is intended to be an introduction to PALS excecutions
Use this script to test that your PALS deployment is working
"""
import json
import os
from ProcessStudio.RESTMethods import AIMMethods
from ProcessStudio.RESTRequest import AIMRequest
from PALS import PALSMethods

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """Main driving method called by PALS executor"""
    
    ############################ Initialize Results Dictionary ####################################
    dict_results = {
        "RequestKey": dict_main_entry_point_args.get('PALS').get('RequestKey'),
        "RunKey": dict_main_entry_point_args.get('PALS').get('RunKey'),
        "Messages": str(),
        "DataSchema": "Stacked",
        "Data": list()
        }

    ############################ Transform Input Data to DataFrame ##############################
    df_data = PALSMethods.dictionary_to_dataframe(dict_main_entry_point_args)

    ############################ Load config ##########################################
    root_dir = os.path.dirname(os.path.abspath(__file__))
    dict_config = PALSMethods.load_config(os.path.join(root_dir, 'config.json'))
    dict_lookup = PALSMethods.load_config(os.path.join(root_dir, 'lookup.json'))

    ############################ Get Tag Value ##########################################
    sStartTimestampUTC = dict_main_entry_point_args.get('PeriodicValues').get('Timestamps')[0]
    sEndTimestampUTC = dict_main_entry_point_args.get('PeriodicValues').get('Timestamps')[-1]

    #dict_TagValue = get_tag_value(26, sStartTimestampUTC) 

    dict_TagValues = get_tag_values(1, sStartTimestampUTC, sEndTimestampUTC, 60)   
    lstValues = [item['Value'] for item in dict_TagValues['Items']]
    df_data['BOOL_TAG'] = lstValues

    ############################ Lookup Recipe Value ##########################################
    #dNipGap = dict_lookup.get('GROUPED_MEDIAN_NIP_GAP').get(sCurrentRecipe)    

    ############################ Fill Results Dictionary ##########################################
    dict_results.get('Data').extend(PALSMethods.dataframe_to_list(df_data))

    ############################ Exit the script ##########################################
    return dict_results

###########################################
# get_tag_value
###########################################
def get_tag_value(nTagKey: int(), sTimestampUTC: str()):

    dictResponse = dict()

    sTagValueURL = f'http://dsfpsdemo.mmm.com/processstudio/demo/workcenters/DSFLINE1/tags/{nTagKey}/value?Timestamp={sTimestampUTC}'

    nStatusCode, dictResponse = AIMRequest.Get(sTagValueURL, bBinaryMode=True)
    if ( nStatusCode < 200 or nStatusCode >= 300 ):
        AIMMethods._handle_exception ( dictResponse)
        dictResponse = {}

    return dictResponse

###########################################
# get_tag_values
###########################################
def get_tag_values(nTagKey, sStartDateTimeUTC, sEndDateTimeUTC, sSamplingPeriod):

    dictResponse = dict()
    
    sTagValuesURL = f'http://dsfpsdemo.mmm.com/processstudio/demo/workcenters/DSFLINE1/tags/{nTagKey}/values?StartTimestamp={sStartDateTimeUTC}&EndTimestamp={sEndDateTimeUTC}&SamplingPeriodS={sSamplingPeriod}'
    
    nStatusCode, dictResponse = AIMRequest.Get(sTagValuesURL, bBinaryMode=False)
    if ( nStatusCode < 200 or nStatusCode >= 300 ):
        AIMMethods._handle_exception ( dictResponse )
        dictResponse = {}

    return dictResponse


###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    with open('test_files/test_periodic_values.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    print(main_entry_point(ENTRY_POINT_ARGS))
