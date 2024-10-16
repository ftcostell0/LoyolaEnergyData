'''
DataReader
Last Update: 10/15/24

This program reads in Loyola's BGE data and outputs as a single csv file to be used later
'''

import pandas as pd


# Class to handle data from each meter csv
class MeterData:
    # Dataframe of each meter and its respective building
    meterDF = pd.read_csv('meters.csv')

    def __init__(self, originalCSVFile):
        
        # Store initial dataframe
        self.sourceDataFrame = pd.read_csv(originalCSVFile, skiprows=4)

        # Pull metadata from initial dataframe
        self.dataType = self.sourceDataFrame.loc[0,'Type']
        self.dataUnit = self.sourceDataFrame.loc[0,'Usage Unit']
        self.meterNumber = self.sourceDataFrame.loc[0,'Meter']
        self.buildingName = MeterData.meterDF.loc[MeterData.meterDF['meterNum'] == self.meterNumber, 'building'].values[0]

        # Building characteristics
        self.buildingSquareFootage = None
        self.buildingOccupancy = None

        tempDF = self.standardizeTime(self.sourceDataFrame)

        # Output as a clean dataframe
        self.outputData = None

    # Standardize the time data
    # Electric comes in 15 minute intervals while gas comes hourly, needs to be resolved
    def standardizeTime(self, dataframe):

        # Standardize as pandas datetime, set as index, and drop unnecessary columns
        dataframe['datetime'] = pd.to_datetime(dataframe['Date'] + ' '  + dataframe['Start Time'])
        dataframe.set_index('datetime', inplace=True)
        dataframe = dataframe.drop(['Date', 'Start Time'], axis = 1)

        # Resample rules
        dataframe = dataframe.resample('h').agg({
            'Type':'first',
            'Meter':'first',
            'Usage Unit':'first',
            'Usage':'sum'
        })

        return dataframe
    

MeterData("example.csv")