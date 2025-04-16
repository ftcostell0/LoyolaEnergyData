'''
DataReader
Version: 2.0
Last Update: 4/8/2025
This program reads in Loyola's BGE data and outputs as a single csv file to be used later
'''

import pandas as pd


# Class to handle data from each meter csv
class MeterData:
    # Dataframe of each account and its respective building
    accountDF = pd.read_csv('meters.csv')
    
    def __init__(self, originalCSVFile):
        # Store initial dataframe

        headerData = self.readAccountHeader(originalCSVFile)

        self.accountNumber = headerData[0]
        whiteSpaceLine = headerData[1] + 1

        self.sourceDataFrame = pd.read_csv(originalCSVFile, skiprows=whiteSpaceLine)

        # Pull metadata from initial dataframe
        self.dataUnit = self.sourceDataFrame.loc[0,'Usage Unit']
        #self.meterNumber = self.sourceDataFrame.loc[0,'Meter']
        #self.buildingName = MeterData.meterDF.loc[MeterData.meterDF['meterNum'] == self.meterNumber, 'building'].values[0]

        tempDF = self.standardizeTime(self.sourceDataFrame)

        # Output as a clean dataframe
        self.outputData = self.processData(tempDF)

    def readAccountHeader(self, filename):
        count = 0

        with open (filename, 'r') as file:
            for line in file:
                count += 1
                words = line.split(",")
                if(words[0]) == '"Account Number"':
                    return (int(words[1]), count)

    # Standardize the time data
    # Electric comes in 15 minute intervals while gas comes hourly, needs to be resolved
    def standardizeTime(self, dataframe):
        # Standardize as pandas datetime, set as index, and drop unnecessary columns
        dataframe['datetime'] = pd.to_datetime(dataframe['Date'] + ' '  + dataframe['Start Time'])
        dataframe.set_index(['datetime'], inplace=True)
        dataframe = dataframe.drop(['Date', 'Start Time'], axis = 1)

        # Resample rules
        dataframe = dataframe.resample('ME').agg({
            'Type':'first',
            'Usage Unit':'first',
            'Usage':'sum'
        })

        return dataframe
    
    # Final process of data to be outputted
    def processData(self, dataframe):
        tempDF = dataframe

        # Add columns with new data
        #tempDF['Building'] = self.buildingName
        tempDF['Account Number'] = self.acccountNumber

        # Re-index dataframe
        #tempDF.set_index('Building', append=True,inplace=True)
        tempDF.set_index('Account Number', append=True, inplace=True)

        return tempDF

def main():
    # Initialize empty dataframe to constantly append to
    outputDF = pd.DataFrame()

    # Allow for user to input multiple files
    while True:
        userInput = input("Please enter a filename or STOP to stop" + "\n")

        # Add error checking later
        if(userInput == "STOP"):
            break
        else:
            tempDF = MeterData(userInput).outputData
            outputDF = pd.concat([outputDF,tempDF])

    # Combine rows for multiple buildings into one, avoid summing gas/electric data together
    outputDF = outputDF.groupby(['Account Number','datetime','Type']).agg({
        'Usage Unit':'first',
        'Usage':'sum'
    }).reset_index()

    fileNameInput = input("What would you like to name the file?")

    # Output to csv
    outputDF.to_csv(fileNameInput + '.csv')

main()