#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 8 09:05:27 2017

@author: shadow-2609
"""

import sqlite3

#NOTE: I come from C++ so if I say array I mean a list and a throw as an error
#raise, sorry if I say it wrong

#To get the valuetype of the row the program needs to execute pragma table_info(table)
#and fetch the data, the actual datatype will be displayed as the 2nd value
#of the tuple returned

#convert a CSV file to a compatible sqlite DB
def csvToDb(source,dest):   
    
    #if source is not a sting
    if type(source) is not str:
        raise TypeError("source must be a string!!!")
        
    #if dest is not a sting
    if type(dest) is not str:
        raise TypeError("dest must be a string!!!")
     
    #open database and the csv file
    database = sqlite3.connect(dest)
    ptFile = None

    #try to open the file, if it does not exists, throw an exception
    try:        
        ptFile = open(source,"r")
    
    except FileNotFoundError:
        raise FileNotFoundError(source + " not found! check file name")
    
    #fetch the name of the table to create and truncate the commas
    tabName = ptFile.readline()
    tabName = tabName[:tabName.find(",")]
    
    #fetch the next line which contains the name of the columns...
    headers = ptFile.readline()
    
    #...and count how many occourences there are
    elem = headers.count(",") + 1
    
    #create an array which contains all the occourences
    headNames = []
    
    #create an array which contains the datatypes
    dataTypes = []
    
    #cycle which extract each name
    for x in range(0,elem):
        #find the position of the next comma
        nextOccName = headers.find("|")
        
        #find the position of the next column
        nextOccType = headers.find(",")
        
        #append the string from the start until the next occourences of the comma
        headNames.append(headers[0:nextOccName])
        
        #append the string from the column to the end
        dataTypes.append(headers[nextOccName+1:nextOccType])
        
        #truncate the string after the comma
        headers = headers[nextOccType+1:]
        
        #NOTE: I DIDN'T NEED TO MODIFY THE CODE FOR THE LAST ELEMENT SINCE
        #IF THE NEXT "FIND" RETURNS -1, WHICH MEANS "NOT FOUND", THE APPEND
        #STATEMENT WILL WORKS LIKE THIS:
        #headNames.append(headers[0:-1]) , WHICH MEANS FROM THE START TO THE
        #END MINUS 1, WHICH RETURNS THE NAME ALREADY TRUNCATED
            
    #check if the table is already existing
    try:
        database.execute("select * from " + tabName)
    
    #if does not exists, creates it
    #NOTE: DATA WILL BE SAVED AS TEXT, NOT INT OR OTHER FORMAT
    except:    
        
        #create the base statement
        statement = "CREATE TABLE " + tabName + " ("
        
        #create a temporary index
        index = 0
        
        #for each name in the headNames array, update the statement
        for name in headNames:
            statement = statement + name + " " + dataTypes[index] + ","
            index = index + 1
            
        #remove the last comma inserted because we don't need it and close
        #the statement
        statement = statement[:-1]
        statement = statement + ")"
            
        #then execute the statement and commit the changes
        database.execute(statement)
        database.commit()

    #fetch the first line
    stringRead = ptFile.readline()

    #loop until the end of the file        
    while stringRead is not "":
        
        #create an array which will contains all the elements to insert
        elements = []
        
        #create a variable used to find the next occourence
        findNext = 0
        
        #loop until we found some match
        while findNext is not -1:
            
            #find a match
            findNext = stringRead.find(",")
            
            #if it did found a match
            if findNext is not -1:
                
                #append the next occourence
                elements.append(stringRead[0:findNext])
                
                #modify the previously read text
                stringRead = stringRead[findNext+1:]

        #last thing to do is append the last occorence
        elements.append(stringRead[:-1])
        
        #create the statement to submit to the database
        statement = 'insert into ' + tabName + ' values('
        
        #loop for each element in the array elements
        for elemList in elements:
            
            #update the statement
            statement = statement + '"' + elemList + '",'
        
        #remove the last comma inserted and close the statement
        statement = statement[:-1] + ")"
            
        #execute the statement and commit the changes
        database.execute(statement)
        database.commit()
        
        #read the next lint
        stringRead = ptFile.readline()
    database.close()
    ptFile.close()



#export one or all database to a single CSV file
def dbToCsv(source,dest,table):
    
    def fetchDataType(tabName,index):
        database = sqlite3.connect(source)
        cursor = database.execute("pragma table_info("+tabName+")")
        datas = cursor.fetchall()
        return datas[index][2]
    
    #function which returns data from the database as a string
    def fetchData(tabName):
        
        #if an element in the table list is not a string, raise a TypeError
            if type(tabName) is not str:
                raise TypeError("One or more element in the list is/are not a string!")
        
            #create a variable to return some data
            toCsv = ""
            
            #check how many columns the database has, to do that the program
            #firstly fetch everything from the table to export...
            cursor = database.execute("select * from " + tabName)
            data = cursor.fetchall()
            
            #check if there is actual data in the table
            if data.__len__() is 0:
                
                #add the name of the table 
                toCsv = tabName+(","*cursor.description.__len__())+"\n"
                
                index = 0
                
                #add the name of the columns
                for desc in cursor.description:
                    toCsv = toCsv + desc[0] + "|" + fetchDataType(tabName,index) + ","
                    index = index + 1
                toCsv = toCsv[:-1]+"\n"
                            
            else:
                #...then check the lenght of the first tuple returned
                columns = data[0].__len__()
                
                #write the name of the table and add as much comma as needed
                head = tabName + (","*columns)
                
                #then, write the string just created to the file
                toCsv = toCsv + head + "\n"
                
                index = 0
                
                #add the name of the columns
                for desc in cursor.description:
                    toCsv = toCsv + desc[0] + "|" + fetchDataType(tabName,index) + ","
                    index = index + 1
                toCsv = toCsv[:-1]+"\n"
                                                
                #cycle to all tuple in the array fetched
                for tupleData in data:
                    
                    #also cycle all the args in the tuple
                    for actualData in tupleData:
                        
                        #update the toCsv variable
                        toCsv = toCsv + actualData + ","
                
                    #remove the last comma added and add a new line
                    toCsv = toCsv[:-1] + "\n"
            
            return toCsv
        
        
        
    #if source is not a sting
    if type(source) is not str:
        raise TypeError("source must be a string!!!")
        
    #if dest is not a sting
    if type(dest) is not str:
        raise TypeError("dest must be a string!!!")
    
    #try to open the file as read, if the file is not found, throw a FileNotFoundError
    #NOTE: I DID THIS BECAUSE THE FUNCTION sqlite3.connect(source) CAN CREATE
    #AN EMPTY FILE, SO I CHECK PREVIOUSLY IF THE FILE DOES EXIST
    try:
        open(source,"r")
    
    except:
        raise FileNotFoundError(source + " not found! Check file name")
        
    #connect to the database and create the destination file    
    database = sqlite3.connect(source)
    ptFile = open(dest,"w")
    
    #if the argument table is a list
    if type(table) is list:
        
        #loop for all elements in the list
        for tableName in table:
    
            #write the file with the returned string from fetchData
            ptFile.write(fetchData(tableName))
        
    #if the argument table is a string
    elif type(table) is str:
        
        #if the program doesn't find the *all* string, which means the user
        #wants to dump all the tables
        if table.find("*all*") is -1:

            #write the file with the returned string from fetchData
            ptFile.write(fetchData(table))

        #loop here if the user wants to dump all data
        else:            
            tempList = database.execute("select name from sqlite_master")
            tablesName = tempList.fetchall()
            for name in tablesName:
                ptFile.write(fetchData(name[0]))
            
    #if table is not a string or a list, throw a TypeError to notify the user    
    else:
        raise TypeError("table is not a string nor a list. Provide one of this type of data")
    

    