"""
conditional_execution
---------
TODO docstring
"""
import pals_helpers

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """ Main method to be called by PALSRunScheduler """

    dict_results = dict()
    dict_results['RunSchedulingApproved'] = False

    if not pals_helpers.validate_input_data(dict_main_entry_point_args):
        return dict_results

    config = pals_helpers.load_config('config.json')

    filters = config.get('FILTERS')

    bool_schedule_run = pals_helpers.evaluate_filters(filters, dict_main_entry_point_args)

    dict_results['RunSchedulingApproved'] = bool_schedule_run

    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    import json
    with open('example_file.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    RESULTS = main_entry_point(ENTRY_POINT_ARGS)

    print(RESULTS)
