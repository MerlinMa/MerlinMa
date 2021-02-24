"""
hello_world_entry
---------
This script is intended to be an introduction to PALS excecutions
Use this script to test that your PALS deployment is working

Results are accessible from the Process Studio REST API
More information found here:
    https://we.mmm.com/wiki/pages/viewpage.action?pageId=504015696

For a tutorial on using this file, see here:
    https://we.mmm.com/wiki/pages/viewpage.action?pageId=549010019
"""

def main_entry_point(dict_main_entry_point_args: dict) -> dict:
    """Main driving method called by PALS executor"""
    # dict_main_entry_point_args is the data gathered by PALS
    # This is were all data processing will normally occur
    # Use this function to process, analyze, upload, save, etc.
    # For a showcase of the functionality currently developed see template_entry_script.py
    # For this example we return a dictionary containing the message "Hello, world!"

    dict_results = {'Message': 'Hello, world!'}

    return dict_results

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    print(main_entry_point({}))
