import os
import re
import pandas as pd
import numpy as np

if __name__ == '__main__':
    
    wordListPath = os.path.dirname(__file__)+'\..\Data\Treated\list_of_words.txt'
    datasetPath = os.path.dirname(__file__)+'\..\Data\Treated\mxm_dataset_full.txt'
    outputPath = os.path.dirname(__file__)+'\..\Data\Treated\dataset_dataframe.csv'
    
    #numberOfRows = 237662 #preallocate space to speed up feeding process
    numberOfRows = 100 #preallocate space to speed up feeding process
    numberOfColumns = 100 #TEST REMOVE
    existingColumnSize = 2 #non-bow column count

    with open(wordListPath) as stream:
        print('Open word list')
        for line in stream: #only one line in LOW
            words = re.split(',', line) [0:numberOfColumns]
            print('Pre-allocate index')
            dex=np.arange(0, numberOfRows)
            print('Pre-allocate dataframe: ' + str(numberOfRows) + ' rows, ' + str(numberOfColumns) + ' columns')
            df = pd.SparseDataFrame(index=dex, columns=['track_id','mxm_track_id', *words])
            print(df)
        stream.close()
    
    index = 0
    with open(datasetPath) as stream:
        print('Open full dataset')
        for line in stream:
            words = re.split(',', line)
            print(df.loc[index])
            print([words[0], words[1], *([0] * numberOfColumns)])
            
            
            s = pd.Series([words[0], words[1], *([0] * numberOfColumns)])
            #df.loc[index] = [words[0], words[1], *([0] * numberOfColumns)]
            #print(words[2:len(words)])
            
            for word in words[2:len(words)]: #word is in format id:count, where id starts at 1, not 0
                tup = re.split(':', word)
                #print(tup) 
                columnIndex = int(tup[0])+existingColumnSize-1
                s[columnIndex] = int(tup[1])
            
            df[:,index] = s
            print(df)
            #print(df.shape)
            index += 1
            
    df.to_csv(outputPath, encoding='utf-8')
    