from ProcessStudio.RESTMethods import AIMMethods
from ProcessStudio import HelperMethods
import pandas as pd
import time 
from datetime import datetime

def main():
    query_standard = dict()
    ################### INPUT PARAMETERS ####################
    sAPIRootURL = 'http://dsfpsdemo.mmm.com/processstudio/demo'
    sWorkcenter = 'DSFLINE1'
    sStartDatetime = '2021-01-01T00:00:00.0000000Z' # Note these are UTC time
    sEndDatetime = '2021-01-01T00:05:00.0000000Z'   # Note these are UTC time
    sSamplingPeriod = 10                   # Sampling period in seconds
    sTagCollectionName = "Energy Tags"
    successful_query=[]
    API_start = []
    API_end = []
    API_elapsed = []
    ps = AIMMethods(sAPIRootURL)

    seconds_to_kill= 300
    time_elapsed = 0
    while time_elapsed<seconds_to_kill: 

        start = datetime.now()
        
        try: 
            dictTagCollectionStatistics = ps.get_tag_collection_statistics(sWorkcenter, sTagCollectionName, sStartDatetime, sEndDatetime, sSamplingPeriod=sSamplingPeriod)
            successful_query.append(True)
        except:
            successful_query.append(False)
            
        end = datetime.now()
        API_start.append(start)
        API_end.append(end)
        elapsed_time = (end-start).total_seconds()
        API_elapsed.append(elapsed_time)

        time_elapsed = time_elapsed + elapsed_time

    df_API_query = pd.DataFrame({"Start_Central_Timestamp":API_start, "End_Central_Timestamp":API_end, "Query_Successful":successful_query, "Elapsed_Sec": API_elapsed})
    df_API_query.to_csv('performance_data\API_Test_Numbers_1ThreadRetrieval.csv') 
    return df_API_query

###########################################
# LOCAL TESTING FUNCTION
###########################################
if __name__ == "__main__":

    print(main())


