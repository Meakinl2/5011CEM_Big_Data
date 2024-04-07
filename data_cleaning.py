import pandas as pd
import multiprocessing as mp

import dask 
import dask.dataframe as dd
from dask.distributed import Client


if __name__ == '__main__':    
    client = Client(threads_per_worker = 4, n_workers = 5)

    ddf = dd.read_csv('Data/Trips_by_Distance.csv',
                            dtype ={
                                "Level": "string",
                                "Date": "string",
                                "State": "string",
                                "FIPS": "Int64",
                                "State_Postal_Code": "string",
                                "County_FIPS": "Int64",
                                "County_Name": "string",
                                "Population_Staying_at_Home": "Int64",
                                "Population_Not_Staying_at_Home": "Int64",
                                "Number_of_Trips": "Int64",
                                "Number_of_Trips_<1": "Int64",
                                "Number_of_Trips_1_3": "Int64",
                                "Number_of_Trips_3_5": "Int64",
                                "Number_of_Trips_5_10": "Int64",
                                "Number_of_Trips_10_25": "Int64",
                                "Number_of_Trips_25_50": "Int64",
                                "Number_of_Trips_50_100": "Int64",
                                "Number_of_Trips_100_250": "Int64",
                                "Number_of_Trips_250_500": "Int64",
                                "Number_of_Trips_>=500": "Int64",
                                "Row_ID": "string",
                                "Week": "Int64",
                                "Month": "Int64"})