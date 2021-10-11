"""
hello_world_entry
---------
This script is intended to be an introduction to PALS excecutions
Use this script to test that your PALS deployment is working
"""
import json
import os
from ProcessStudio.RESTMethods import AIMMethods
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
    df_data = PALSMethods.dictionary_to_dataframe(dict_main_entry_point_args, sStatistic="Average")

    ############################ Load config ##########################################
    root_dir = os.path.dirname(os.path.abspath(__file__))
    dict_config = PALSMethods.load_config(os.path.join(root_dir, 'config.json'))
    dict_lookup = PALSMethods.load_config(os.path.join(root_dir, 'lookup.json'))

    ############################ Get Tag Value ##########################################
    sTimestamp = dict_main_entry_point_args.get('PeriodicStatistics').get('Timestamps')[0]
    sTag = dict_config.get("TAG_NAME")
    sWorkcenter = dict_config.get("WORKCENTER")
    methods = AIMMethods(dict_config.get('API_ROOT_URL'))
    dict_TagValue = methods.get_tag_value(sTagName=sTag, 
                                            sDateTime=sTimestamp, 
                                            sWorkcenterCode=sWorkcenter)
    sCurrentRecipe = dict_TagValue.get('Value').get(sTag).get('Value')
    ############################ Lookup Recipe Value ##########################################
    dNipGap = dict_lookup.get('GROUPED_MEDIAN_NIP_GAP').get(sCurrentRecipe)    
    df_data['GROUPED_MEDIAN_NIP_GAP'] = dNipGap

    ############################ Fill Results Dictionary ##########################################
    dict_results.get('Data').extend(PALSMethods.dataframe_to_list(df_data))

    ############################ Exit the script ##########################################
    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    with open('test_files/test_periodic_statistics.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    print(main_entry_point(ENTRY_POINT_ARGS))
