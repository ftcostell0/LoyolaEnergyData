'''
DataReader
Last Update: 10/15/24

This program reads in Loyola's BGE data and outputs as a single csv file to be used later
'''

import pandas as pd


# Class to handle data from each meter csv
class MeterData:
    def __init__(self, originalCSVFile):
        
        # Store initial dataframe
        self.sourceDataFrame = pd.read_csv(originalCSVFile, skiprows=4)

        # Pull metadata from initial dataframe
        self.dataType = self.sourceDataFrame.loc[0,'Type']
        self.dataUnit = self.sourceDataFrame.loc[0,'Usage Unit']
        self.meterNumber = self.sourceDataFrame.loc[0,'Meter']
        self.buildingName = None

        # Electric data comes in 15 minute intervals while gas comes in hourly intervals
        # Electric needs to be compressed
        if self.dataType == 'Electric Usage':
            self.sourceDataFrame = self.timeCompression(self.sourceDataFrame)

        # Output as a clean dataframe
        self.outputData = None

    def timeCompression(dataframe):
        return "This is going to fucking suck"
    

    


print(MeterData("example.csv").dataType)