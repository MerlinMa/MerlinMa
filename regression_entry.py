"""
regression_entry.py
---------
The results are exposed via the Process Studio REST API see https://we.mmm.com/wiki/x/UKsKHg
This script MUST be manually moved into the root directory in order for PALS to find it.
"""
from PALS import PALSMethods
import pandas as pd
import os

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """ Main driving method called by PALS executor """

    ############################ Initialize Results Dictionary ####################################
    dict_results = {
        "RequestKey": dict_main_entry_point_args.get('PALS').get('RequestKey'),
        "RunKey": dict_main_entry_point_args.get('PALS').get('RunKey'),
        "Messages": None,
        "DataSchema": "Stacked",
        "Data": list()
    }

    root_dir = os.path.dirname(os.path.abspath(__file__))
    ############################ Validate Input Data ##############################################
    if not PALSMethods.validate_input_data(dict_main_entry_point_args):
        dict_results['Messages'] = "Failed to validate input data"
        dict_results['Data'] = dict_main_entry_point_args
        return dict_results

    ############################ Transform Input Data to DataFrame ##############################
    df_data = PALSMethods.dictionary_to_dataframe(dict_main_entry_point_args)

    ############################ Load Machine Learning Model File #################################
    model = PALSMethods.load_model(os.path.join(root_dir,'models\model.pkl'))

    ############################ Execute Machine Learning Model ###################################
    np_predictions = model.predict(df_data)
    df_predictions = pd.DataFrame(np_predictions, index=df_data.index, columns=['Predicted_Value'])

    ############################ Fill Results Dictionary ##########################################
    dict_results.get('Data').extend(PALSMethods.dataframe_to_list(df_predictions))

    ############################ Return Results Dictionary ########################################
    # Results are accessible from the Process Studio REST API
    # More information found here: https://we.mmm.com/wiki/pages/viewpage.action?pageId=504015696    
    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    # Use this data to test the entry script before deploying to PALS
    import json
    with open('test_files/test_periodic_values.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    print(main_entry_point(ENTRY_POINT_ARGS))
