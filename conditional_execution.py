"""
conditional_execution
---------
dict_results will contain a field called RunSchedelingApproved.
Based on the conditions/filters provided in the config file,
RunSchedelingApproved will be set to True or False.
The PALS execution will run if RunSchedelingApproved is True.
"""
import pals_helpers

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """ Main method to be called by PALSRunScheduler """

    ############################ Initialize Results Dictionary ####################################
    dict_results = dict()
    dict_results['RunSchedulingApproved'] = False

    ############################ Validate Input Data ##############################################
    if not dict_main_entry_point_args:
        return dict_results

    ############################ Load Config File #################################################
    config = pals_helpers.load_config('config.json')

    ############################ Evaluate Filters #################################################
    filters = config.get('FILTERS')
    bool_schedule_run = pals_helpers.evaluate_filters(filters, dict_main_entry_point_args['Tags'])

    ############################ Fill Results Dictionary ##########################################
    dict_results['RunSchedulingApproved'] = bool_schedule_run

    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    import json
    with open('test_conditional_data.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    RESULTS = main_entry_point(ENTRY_POINT_ARGS)

    print(RESULTS)
