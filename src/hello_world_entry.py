"""
hello_world_entry
---------
This script is intended to be an introduction to PALS excecutions

"""

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """
    Main driving method called by PALS executor
    This is were all data processing would normally occur
    For this example we return a dictionary containing the message "Hello, world!"
    Use this script to test that your PALS deployment is working

    dict_main_entry_point_args is the data gathered by PALS
    Use this function to process, upload, save, etc that data
    """

    dict_results = {'Message': 'Hello, world!'}

    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    print(main_entry_point({}))
