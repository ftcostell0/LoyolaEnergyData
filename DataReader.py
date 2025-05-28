'''
DataReader
Version: 2.0
Last Update: 4/8/2025
This program reads in Loyola's BGE data and outputs as a single csv file to be used later
'''

import pandas as pd
import os

# Class to handle data from each meter csv
class MeterData:
    # Dataframe of each account and its respective building
    
    def __init__(self, originalCSVFile):
        # Store initial dataframe

        fileProcess = self.processFile(originalCSVFile)

        self.acccountNumber = int(fileProcess[0])

        self.sourceDataFrame = pd.read_csv(originalCSVFile, skiprows=fileProcess[1])
        self.sourceDataFrame = self.processColumns(self.sourceDataFrame)

        # Pull metadata from initial dataframe
        self.type = self.sourceDataFrame.loc[0,'TYPE']

        tempDF = self.standardizeTime(self.sourceDataFrame)

        # Output as a clean dataframe
        self.outputData = self.processData(tempDF)

    def processFile(self, filename):
        accountNumber = 0
        skipLines = 0

        with open (filename, 'r') as file:
            counter = 0
            for line in file:
                words = line.split(",")

                for i in range(len(words)):
                    if(words[i] == 'Account Number' or words[i] == '"Account Number"'):
                        accountNumber = words[i + 1]
                
                for i in range(len(words)):
                    if(words[i] == 'Type' or words[i] == 'TYPE'):
                        skipLines = counter
                        break

                counter += 1

        return accountNumber, skipLines
    
    def processColumns(self, dataframe):
        dataframe.columns = dataframe.columns.str.upper()
        
        dataframe = dataframe.rename(columns={
            'USAGE (KWH)': 'USAGE',   
            'USAGE (THERMS)': 'USAGE'     
        })

        columnDrops = ['END TIME', 'COST', 'NOTES', 'METER']
        dataframe = dataframe.drop(columns=columnDrops, errors='ignore')

        hasUnit = 'USAGE UNIT' in dataframe.columns
        usageType = dataframe.loc[0, 'TYPE'].split(" ")[0]
        if(usageType == 'Natural'): usageType = 'Gas'

        dataframe['TYPE'] = usageType

        if(not hasUnit):
            if((usageType == 'Electric')):
                dataframe['USAGE UNIT'] = 'kWh'
            elif((usageType == 'Gas')):
                dataframe['USAGE UNIT'] = 'Therms'
            else:
                dataframe['USAGE UNIT'] = 'N/A'
           

        return dataframe
                


    # Standardize the time data
    # Electric comes in 15 minute intervals while gas comes hourly, needs to be resolved
    def standardizeTime(self, dataframe):
        # Standardize as pandas datetime, set as index, and drop unnecessary columns
        dataframe['DATETIME'] = pd.to_datetime(dataframe['DATE'] + ' '  + dataframe['START TIME'])
        dataframe.set_index(['DATETIME'], inplace=True)
        dataframe = dataframe.drop(['DATE', 'START TIME'], axis = 1)

        # Resample rules
        dataframe = dataframe.resample('M').agg({
            'TYPE':'first',
            'USAGE UNIT':'first',
            'USAGE':'sum'
        })

        return dataframe
    
    # Final process of data to be outputted
    def processData(self, dataframe):
        tempDF = dataframe

        # Add columns with new data
        #tempDF['Building'] = self.buildingName
        tempDF['ACCOUNT NUMBER'] = self.acccountNumber

        # Re-index dataframe
        tempDF.set_index('ACCOUNT NUMBER', append=True, inplace=True)

        return tempDF

def main():
    # Initialize empty dataframe to constantly append to
    outputDF = pd.DataFrame()

    folderPath = "DataFiles/"
    for dataFile in os.listdir(folderPath):
        filePath = os.path.join(folderPath, dataFile)

        tempDF = MeterData(filePath).outputData
        outputDF = pd.concat([outputDF, tempDF])

    # Output to csv
    outputDF.to_csv('output.csv')

main()