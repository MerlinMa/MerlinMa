from ProcessStudio.RESTMethods import AIMMethods
from ProcessStudio import HelperMethods
import pandas as pd
import time 
from datetime import datetime

def main():
    ################### INPUT PARAMETERS ####################
    sAPIRootURL = 'http://dsfpsdemo.mmm.com/processstudio/demo'
    sWorkcenter = 'DSFLINE1'
    sStartDatetime = '2021-01-01T00:00:00.0000000Z' # Note these are UTC time
    sEndDatetime = '2021-01-01T00:05:00.0000000Z'   # Note these are UTC time
    sSamplingPeriod = 10                   # Sampling period in seconds
    sTagCollectionName = "Energy Tags"
    sTagName = 'DSFLINE1_SIMULATED_WATER_2'
    API_start = []
    API_end = []
    API_elapsed = []

    ps = AIMMethods(sAPIRootURL)

    max_iterations = 600
    n = 0
    while n<max_iterations: 

        start = datetime.now()
        dictTagValues = ps.get_tag_values(sTagName, sStartDatetime, sEndDatetime, sWorkcenter, sSamplingPeriod)
        end = datetime.now()

        API_start.append(start)
        API_end.append(end)
        API_elapsed.append((end-start).total_seconds())

        n+=1

    df_API_query = pd.DataFrame({"Start":API_start,
                                "End":API_end,
                                "Elapsed": API_elapsed})

    df_API_query.to_csv('performance_data\API_Test_Numbers.csv')

    return df_API_query

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    print(main())


