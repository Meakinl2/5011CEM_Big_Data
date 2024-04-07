import pandas as pd
import multiprocessing as mp

import dask 
import dask.dataframe as dd
from dask.distributed import Client

import time
from numpy import *
import matplotlib as mt
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
# Section A
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 

# Obtains weekly averages for people stayign at home from the dataframe, based on level and joins all results together
def weekly_average(dataframe, level):
    total_avgs = {}

    if level == "National":
        dataframe = dataframe[(dataframe.Level == 'National')]
        result_dataframe = dataframe.map_partitions(weekly_average_national).compute()

    elif level == "State":
        dataframe = dataframe[(dataframe.Level == 'State')]
        result_dataframe = dataframe.map_partitions(weekly_average_state).compute()

    elif level == "County":
        dataframe = dataframe[(dataframe.Level == 'County')]
        result_dataframe = dataframe.map_partitions(weekly_average_county).compute()

    return result_dataframe

# Find Average Population Staying at Home for each Week on the National level
# Relatively quick, only calculation for each of the 129 weeks
def weekly_average_national(partition):
    avg_weekly_trips = ["A"]

    for year in ["2019","2020","2021"]:

        for week in range(53):
            
            data = partition[(partition.Week == week) & (partition.Date.str.endswith(year))]
            if len(data) <= 0: continue
            avg_weekly_trips.append(data['Population_Staying_at_Home'].mean())

    return avg_weekly_trips
        
# Find Average Population Staying at Home for each Week on the State level
# A lot slower, 52 calculations for each of the 129 weeks
def weekly_average_state(partition):
    avg_weekly_trips = ["A"]
    
    for year in ["2019","2020","2021"]:
        
        for week in range(53):

            for state in STATES:
                
                data = partition[(partition.State_Postal_Code == state) & (partition.Week == week) & (partition.Date.str.endswith(year))]
                if len(data) <= 0: continue
                avg_weekly_trips.append(data['Population_Staying_at_Home'].mean())

    return avg_weekly_trips

# Find Average Population Staying at Home for each Week on the County level
# Massively slower with over 3000 calculations for each of the 129 weeks
def weekly_average_county(partition):
    avg_weekly_trips = []
    
    for year in ["2019","2020","2021"]:
        
        for week in range(53):

            for county in COUNTIES:

                data = partition[(partition.County_Name == county) & (partition.Week == week) & (partition.Date.str.endswith(year))]
                if len(data) <= 0: continue
                avg_weekly_trips.append(data['Population_Staying_at_Home'].mean())

    return avg_weekly_trips

# Finds average trips specifically for week 32 as there is no other data in Trips_Full_Data.csv
def week32_trips_mean(dataframe):
    # 32nd week represented by 31 in Trips_by_Distance as the year's weeks start at 0
    data = dataframe[(dataframe.Level == "National") & (dataframe.Week == 31) & (dataframe.Date.str.endswith("2019"))]
    return data['Population_Staying_at_Home'].mean().compute()

# Find average total distance by each trip distance range over week 32
def week32_dist_mean(dataframe):
    dist_num_avgs = {}
    column_headers = ['Trips_<1_Mile','Trips_1_3_Miles','Trips_3_5_Miles','Trips_5_10_Miles','Trips_10_25_Miles','Trips_25_50_Miles','Trips_50_100_Miles','Trips_100_250_Miles','Trips_250_500_Miles','Trips_500+_Miles']

    for column in column_headers:
        dist_num_avgs[column] = dataframe[column].mean().compute()

    return dist_num_avgs

# Histogram for weekly number of people staying at home
def weekly_home_hist(data):
    bins = [10000000 * i for i in range(11)] # Creating Histogram bins

    # Histogram Setup and Customisation
    plt.hist(data, bins = bins, facecolor = '#2ab0ff', edgecolor = '#000000', linewidth = 2)
    plt.ticklabel_format(useOffset=False, style='plain')

    plt.title('Frequency of Average Weekly People Staying at Home')
    plt.xlabel('Number of People Staying at Home')
    plt.ylabel('Frequency')


# Histogram for frequency of 
def weekly_dist_bar(data):
    # Histogram Setup and Customisation
    plt.bar(data.keys(), data.values(), facecolor = '#2ab0ff', edgecolor = '#000000', linewidth = 2)
    plt.gcf().axes[0].yaxis.get_major_formatter().set_scientific(False)

    plt.title('Frequency of Average Weekly People Staying at Home')
    plt.xlabel('Number of People Staying at Home')
    plt.ylabel('Frequency')


# Runs the required to code to the desire outcomes for Section A
def section_A(ddf_num, ddf_dist):
    start_time = time.time()

    data = weekly_average(ddf_num, 'National')
    home_avg = []
    
    for i in range(len(data)):
        home_avg += data[i][1:] # Remove 'A' from list

    weekly_home_hist(home_avg)

    print(f"Section A Graph 1 Time: {round(time.time() - start_time, 5)}s")

    plt.show()

    start_time = time.time()

    data = week32_dist_mean(ddf_dist)
    weekly_dist_bar(data)

    print(f"Section A Graph 2 Time: {round(time.time() - start_time, 5)}s")

    plt.show()
    

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
# Section B
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 

# Finds all the dates
def min_trips(dataframe, level, specific = None):
    results_df = 0

    if level == "National":
        dataframe = dataframe[(dataframe.Level == "National")]
        results_df = dataframe.map_partitions(min_trips_national).compute()

    elif level == "State":
        dataframe = dataframe[(dataframe.Level == "State")]
        results_df = dataframe.map_partitions(min_trips_state, specific).compute()

    elif level == "County":
        dataframe = dataframe[(dataframe.Level == "County")]
        results_df = dataframe.map_partitions(min_trips_county, specific).compute()

    return results_df

# Find days with a miniimum of 10000000 people taking 10-25 trips on a National Level
def min_trips_national(partition):
    data = partition[(partition.Number_of_Trips_10_25 >= 10000000)][["Number_of_Trips_10_25", "Number_of_Trips_25_50"]]
    return data

# Find days with a miniimum of 10000000 people taking 10-25 trips in a given State
def min_trips_state(partition,state):
    data = partition[(partition.Number_of_Trips_10_25 >= 10000000) & (partition.State_Postal_Code == state)][["Date", "Number_of_Trips_10_25", "Number_of_Trips_25_50"]]
    return data

# Find days with a miniimum of 10000000 people taking 10-25 trips in a given County
def min_trips_county(partition,county):
    days = []
    data = partition[(partition.Number_of_Trips_10_25 >= 10000000) & (partition.County_Name == county)][["Date", "Number_of_Trips_10_25", "Number_of_Trips_25_50"]]
    return data

# Generates a scatter graph with appropriate foramtting given the x and y data
def trip_len_scatter(xdata,ydata):
    plt.scatter(xdata, ydata, facecolor = '#2ab0ff', edgecolor = '#169acf', alpha = 0.5)
    plt.ticklabel_format(useOffset=False, style='plain')

    plt.title('Number of People doing between 10-25 Trips vs Number of People doing between 25-50 Trips')
    plt.xlabel('Number of People doing 10-25 Trips')
    plt.ylabel('Number of People doing 25-50 Trips')
    
# Runs all corresponding functions for Section B
def section_B(ddf_num):
    start_time = time.time()

    data = min_trips(ddf_num, 'National')
    trips10 = data["Number_of_Trips_10_25"].tolist()
    trips25 = data["Number_of_Trips_25_50"].tolist()

    trip_len_scatter(trips10,trips25)

    print(f"Section B Time: {round(time.time() - start_time, 5)}s")

    plt.show()
        
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
# Section D
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 

# Generated a subplot for with a given combination of columns from both tables
def scatter_sub(trips_dist,trips_num,plot_args):
    print(f"Started: {plot_args[2]}")
    xdata = trips_dist[plot_args[0]]
    ydata = trips_num[plot_args[1]]
    plt.ioff()
    plt.scatter(xdata, ydata, facecolor = '#2ab0ff', edgecolor = '#169acf')
    plt.xlabel(plot_args[0])
    plt.ylabel(plot_args[1])
    plt.savefig(f'Plots/Distance_Scatters/plot{plot_args[2]}.png', bbox_inches='tight', pad_inches=0)
    print(f"Finished: {plot_args[2]}")

# Generates all the plots for the all the combinations of trips distance and trips num
def distance_model(trips_num, trips_dist):
    start_time = time.time()

    num_cols = ['Number_of_Trips_1_3','Number_of_Trips_3_5','Number_of_Trips_5_10','Number_of_Trips_10_25','Number_of_Trips_25_50','Number_of_Trips_50_100','Number_of_Trips_100_250','Number_of_Trips_250_500','Number_of_Trips_>=500']
    trips_num = trips_num[(trips_num.Level == "National") & (trips_num.Week == 31) & (trips_num.Date.str.endswith("2019"))]
    trips_num = trips_num[num_cols]

    dist_cols = ['Trips_<1_Mile','Trips_1_3_Miles','Trips_3_5_Miles','Trips_5_10_Miles','Trips_10_25_Miles','Trips_25_50_Miles','Trips_50_100_Miles','Trips_100_250_Miles','Trips_250_500_Miles','Trips_500+_Miles']
    trips_dist = trips_dist[dist_cols]

    p = 1
    table_args = []
    for dcol in dist_cols:
        for ncol in num_cols:
            table_args.append([dcol,ncol,p])
            p += 1

    pool = mp.get_context('spawn').Pool()

    result = pool.starmap_async(scatter_sub, [(trips_dist, trips_num, args) for args in table_args])

    result.wait()

    pool.close()
    pool.join()

    for i in range(1,91):
        img = mpimg.imread(f"Plots/Distance_Scatters/plot{i}.png")
        plt.subplot(9,10,i)
        plt.axis('off')
        plt.imshow(img)

    print(f"Plot Generation Time: {round(time.time() - start_time,2)}s")

    plt.tight_layout()
    plt.subplots_adjust(wspace=0, hspace=0)
    plt.show()
    
# Runs all the function for Section D    
def section_D(ddf_num,ddf_dist):
    distance_model(ddf_num,ddf_dist)
    pass

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
# Section E
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 

def section_E():
    pass

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
# Main Body
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 


if __name__ == '__main__':    

    setup_start_time = time.time()

    client = Client(threads_per_worker = 4, n_workers = 5)

    ddf_dist = dd.read_csv('Data/Trips_by_Distance.csv',
                            dtype = {
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
    
    ddf_full = dd.read_csv('Data/Trips_Full_Data.csv',
                           dtype = {
                               "Month_of_Date": "string",
                               "Week_of_Date": "string",
                               "Year_of_Date": "Int64",
                               "Level": "string",
                               "Date": "string",
                               "Week_Ending_Date": "string",
                               "Trips_<1_Mile": "Int64",
                               "People_Not_Staying_at_Home": "Int64",
                               "Population_Staying_at_Home": "Int64",
                               "Trips": "Int64",
                               "Trips_1_25_Miles": "Int64",
                               "Trips_1_3_Miles": "Int64",
                               "Trips_10_25_Miles": "Int64",
                               "Trips_100_250_Miles": "Int64",
                               "Trips_100+_Miles": "Int64",
                               "Trips_25_100_Miles": "Int64",
                               "Trips_25_50_Miles": "Int64",
                               "Trips_250_500_Miles": "Int64",
                               "Trips_3_5_Miles": "Int64",
                               "Trips_5_10_Miles": "Int64",
                               "Trips_50_100_Miles": "Int64",
                               "Trips_500+_Miles": "Int64"})
    
    global STATES, COUNTIES
    STATES = sorted(ddf_dist['State_Postal_Code'].unique().compute().tolist()[1:])
    COUNTIES = sorted(ddf_dist['County_Name'].unique().compute().tolist()[1:])

    print(f"\nSetup Time: {round(time.time() - setup_start_time, 5)}s")

    # section_A(ddf_dist, ddf_full)

    # section_B(ddf_dist)

    section_D(ddf_dist, ddf_full)

    # section_E(ddf_dist, ddf_full)
