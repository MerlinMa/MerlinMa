"""
conditional_execution
---------
dict_results will contain a field called RunSchedelingApproved.
Based on the conditions/filters provided in the config file,
RunSchedelingApproved will be set to True or False.
The PALS execution will run if RunSchedelingApproved is True.
For a tutorial on using this file, see here:
    https://we.mmm.com/wiki/display/ENG/Conditional+execution
"""

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """ Main method to be called by PALSRunScheduler """

    ############################ Initialize Results Dictionary ####################################
    dict_results = dict()

    run_main = dict_main_entry_point_args.get('Tags').get('1').get('Value')

    if run_main == 1:
        dict_results['RunSchedulingApproved'] = True
    else:
        dict_results['RunSchedulingApproved'] = False

    return dict_results


###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    import json
    with open('test_files/test_conditional_data.json') as json_file:
        ENTRY_POINT_ARGS = json.load(json_file)

    RESULTS = main_entry_point(ENTRY_POINT_ARGS)

    print(RESULTS)
