#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 8 09:05:27 2017

@author: shadow-2609
"""

import sqlite3
import json

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
                toCsv = toCsv + desc[0] + "|" + fetchDataType(source,tabName,index) + ","
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
                toCsv = toCsv + desc[0] + "|" + fetchDataType(source,tabName,index) + ","
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
        database.close()
        ptFile.close()
        raise TypeError("table is not a string nor a list. Provide one of this type of data")
    
    database.close()
    ptFile.close()
    

#function to fecth the type of data of a row  by index definiton
def fetchDataTypeByIndex(source,tabName,index):
    
    #open the database indicated by the source and get a cursor
    #NOTE: PRAGMA IS AN INTERNAL SQLITE3 FUNCTION
    database = sqlite3.connect(source)
    cursor = database.execute("pragma table_info("+tabName+")")
    
    #fetch all the data from the cursor
    data = cursor.fetchall()
    
    #close the database
    database.close()
    
    #return just the third element of the indicated index
    #NOTE: THIS RETURN JUST THE THIRD INDEX OF THE SELECTED ELEMENT, WHICH
    #RAPRESENT THE DATA TYPE
    #THE STRUCTURE WORKS LIKE THIS: 0 -> INDEX, 1 -> NAME, 2 -> DATATYPES
    return data[index][2]



#function to fecth the type of data of a row 
def fetchDataType(source,tabName):
    
    #open the database indicated by the source and get a cursor
    database = sqlite3.connect(source)
    cursor = database.execute("pragma table_info("+tabName+")")
    
    #fetch all the data
    data = cursor.fetchall()
    
    #create a temporary array
    rowType = []
    
    #loop for each element in the data array
    for name in data:
        
        #append just the 3rd index of that element, which rapresent the data type
        rowType.append(name[2])
        
    #close the database
    database.close()
        
    #return the array
    return rowType



#function to fetch columns names by index definition
def fetchColumnNameByIndex(source,tabName,index):
    
    #open the database indicated by the source and get a cursor 
    database = sqlite3.connect(source)
    cursor = database.execute("pragma table_info("+tabName+")")
    
    #fetch all the data from the cursor
    datas = cursor.fetchall()
    
    #close the database
    database.close()
    
    #return just the third element of the indicated index
    return datas[index][1]



#function to fetch columns names
def fetchColumnName(source,tabName):
    
    #open the database indicated by the source and get a cursor 
    database = sqlite3.connect(source)
    cursor = database.execute("pragma table_info("+tabName+")")
    
    #fetch all the data from the cursor
    data = cursor.fetchall()
    
    #create a temporary array  
    rowNames = []
    
    #loop for each element in the data array
    for name in data:
        
        #append just the 2nd index of that element, which rapresent the data type
        rowNames.append(name[1])
        
    #close the database
    database.close()
        
    #return the array
    return rowNames



#function to fetch tables names
def fetchTablesNames(source):
    
    #open the database indicated by the source and get a cursor 
    database = sqlite3.connect(source)
    cursor = database.execute("select name from sqlite_master")
    
    #fetch all the data from the cursor
    data = cursor.fetchall()
    
    #create a temporary array      
    tabNames = []
    
    #loop for each element in the data array
    for name in data:
        
        #if the name of the returned table is not sqlite_autoindex_ procede
        #NOTE: THIS IS RETURNED IF THERE IS AN AUTOINDEXING COLUMN
        if name[0].find("sqlite_autoindex_") is -1:
            #append just the 1st index of that element
            #NOTE: WE HAVE TO GET JUST THE FIRST ELEMENT SINCE THE NAME VARIABLE
            #FROM THE DATA ARRAY IS AN ARRAY ITSELF
            tabNames.append(name[0])
            
    #close the database
    database.close()
    
    #return the array
    return tabNames   



#function to get the number of column in a table
def fetchTableColumnsCount(source,tabName):
    
    #to cheat, the program calls the function fetchColumnNames and conunts the
    #length of the returned array since it would do the same things
    total = fetchColumnName(source,tabName)
    return total.__len__()

    

#function to fetch description of a given table
def fetchInfo(source):
    
    #check if source is a string throw an exception
    if type(source) is not str:
        raise TypeError("source is not a string!!!")
        
    #fetch tables names
    names = fetchTablesNames(source)
    
    #create a variable to store ids and data type for each table
    tables = []
    
    #for each table create other 2 arrays and fill it with ids and data types
    for name in names:
        
        #create an array which contains the name, the array returned from the
        #fetchColumnName function an the array returned from the fetchDataType
        #function
        tempTables = [name,fetchColumnName(source,name),fetchDataType(source,name)]
        
        #append the just created array
        tables.append(tempTables)
        
    #return the array   
    return tables



#function to fetch all the data from a table
def fetchData(source,tabName,offset=0,lenght=0):
    
    #if source is not a string throw a TypeError
    if type(source) is not str:
        raise TypeError("source must be a string!!!")
        
    #if tabName is not a string throw a TypeError
    if type(tabName) is not str:
        raise TypeError("tabName must be a string!!!")
    
    #connect to the database
    database = sqlite3.connect(source)
    
    #create a new variable which will store all the data
    data = None
    
    #fetch everything from the table
    cursor = database.execute("select * from " + tabName)
    
    #if the offset is not 0, apply the specified offset
    if offset is not 0:
        cursor.fetchmany(offset)
        
    #if lenght is not 0, fetch the indicted amount
    if lenght is not 0:
        data = cursor.fetchmany(lenght)
        
    else:
        data = cursor.fetchall()
        
    #close the database
    database.close()
    
    return data
        


#function to convert db to a json file
def dbToJson(source,dest):

    #if source is not a sting
    if type(source) is not str:
        raise TypeError("source must be a string!!!")
        
    #if dest is not a sting
    if type(dest) is not str:
        raise TypeError("dest must be a string!!!")    

    #create an array to store tables names, column names and column datatype
    tables = []
    colName = []
    colType = []
    
    #create an array which contains the string to be converted to a json
    jsonDef = []
    
    #create a temporary json string
    tempJson = ""
    
    #fetch the names of the tables
    tables = fetchTablesNames(source)
            
    #for each table name contained in tables array
    for name in tables:
                        
        #fetch column name for a specific table
        colName = fetchColumnName(source,name)
        
        #fetch column datatype for a specific table
        colType = fetchDataType(source,name)
        
        #start writing the name of the table and the start of a new object
        tempJson = '"' + name + '": { '
        
        #count the column in the table
        count = fetchTableColumnsCount(source,name)
        
        #append the count of the columns
        tempJson = tempJson + '"column_count":' + str(count) + ',"column_type":{'
        
        #for each column add the column number and the datatype
        for x in range (0,count):
            tempJson = tempJson + '"' + str(x) + '":"' + colType[x] + '",'
            
        #remove the last comma so it can't create any problems (JSON is picky)
        tempJson = tempJson[:-1] + '},"column_name":{'
            
        #for each column daa the column number and the name
        for x in range (0,count):
            tempJson = tempJson + '"' + str(x) + '":"' + colName[x] + '",'
            
        #remove the last comma
        tempJson = tempJson[:-1] + '},'
            
        #add an array which will contain all the data
        tempJson = tempJson + '"column_data":['
        
        #fetch the data with the function fetchData
        data = fetchData(source,name)
                
        #loop for each data fetched
        for index in range(0,data.__len__()):
            
            tempJson = tempJson + "["
            
            #loop for each sub-element in the accessed in this element
            for subindex in range(0,data[index].__len__()):
                
                #append the data in an array
                tempJson = tempJson + '"' + str(data[index][subindex]) + '",'
            
            #delete the last comma and close the array
            tempJson = tempJson[:-1] + "],"
    
        #delete the last comma and close the array
        tempJson = tempJson[:-1] + "]}"
            
        #append the just created json definition to a global array
        jsonDef.append(tempJson)
         
    #delete the last comma and close the object
    tempJson = tempJson[:-1] + "}"
    
    #open a new object which will aggregate the other elements created before
    actJson = "{"
    
    #for each definition created, insert it into a string created above and
    #add a comma to add the eventual next element
    for definition in jsonDef:
        actJson = actJson + definition + ","
        
    #remove the last comma and close the last object
    actJson = actJson[:-1] + "}"
      
    #open the file in write mode
    output = open(dest,"w")
    
    #write the string created before
    output.write(actJson)
    
    #close the file
    output.close()
    
    #reopen the file as read mode to check if the written file is correct
    output = open(dest,"r")
                            
    #try to compile the object from the opened file
    json.load(output)
    
    #if everything is correct, close the file and finish the function
    #NOTE: I DID THIS SINCE IF I USE THE STRING IN THE JSON.LOADS FUNCTIONS,
    #IT THROWS AN ERROR WHICH I CANNOT FIX, PROBABLY IS A BUG SINCE I CAN 
    #COMPILE THE FILE BUT I CAN'T COMPILE THE STRING ITSELF
    output.close()
    


#convert a json file into a database
#NOTE: THE JSON FILE MUST FOLLOW THE CORRECT SYNTAX OTHERWISE IT WILL NOT
#WORK
def jsonToDb(source,dest):
    
    #if source is not a sting
    if type(source) is not str:
        raise TypeError("source must be a string!!!")
        
    #if dest is not a sting
    if type(dest) is not str:
        raise TypeError("dest must be a string!!!")  
        
    #start by opening the file
    jsonFile = open(source)
    
    #load the json file
    jsonObj = json.load(jsonFile)
    
    #close the file since we don't need it anymore
    jsonFile.close()
    
    #create an array which will contains the tables names
    tabName = []
    
    #loop for each keys in the jsonObj variable
    for name in jsonObj.keys():
        tabName.append(name)
        
    #create 2 variables, 1 will store the column name, 1 will store the column
    #type
    colName = []
    colType = []
    
    #loop for each table name    
    for name in tabName:
        
        #create 2 temporary variable to store the type and the name of the 
        #column
        tempType = []
        tempName = []
                
        #loop for each type found in the key column_type
        for ctype in jsonObj[name]["column_type"]:
            tempType.append(jsonObj[name]["column_type"][ctype])
            
        #append everything fetched from the json object
        colType.append(tempType)
            
        #loop for each name found in the key column_name
        for cname in jsonObj[name]["column_name"]:
            tempName.append(jsonObj[name]["column_name"][cname])
            
        #append everything fetched from the json object 
        colName.append(tempName)
    
    #error checking: if there is not enough name or if there is something extra
    #fetch the lenght of the array tabName
    tempLenName = tabName.__len__()
    
    #fetch the lenght of the array colType
    tempLenColType = colType.__len__()

    #fetch the lenght of the array colName
    tempLenColName = colName.__len__()
    
    #add everything into a variable
    tempTot = tempLenName+tempLenColName+tempLenColType
    
    #if everything is correct, the summed lenght is 3 times more the lenght of an array
    if tempTot is not (tempLenName*3):
        raise Exception("Fetched keys does not contains enough data or extra data is present!\n"+"Lenght of Name: " + tempLenName + "\nLenght of column name: " + tempLenColName + "\nLenght of column type: " + tempLenColType)
     
    #open or create the desired database
    database = sqlite3.connect(dest)
        
    #loop for each name contained in the array tabName
    for name in tabName:
        
        #fetch the index from the tabName variable with a given name
        index = tabName.index(name)
        
        #create a new variable to store the statement to apply to the database
        statement = "create table " + name + " ("
        
        #loop for each columns names and columns types
        for x in range (0,colName[index].__len__()):
            
            #add the column name and the column types to the statement to pass
            #to the database
            statement = statement + colName[index][x] + " " + colType[index][x] + ","
            
        #remove the last comma and add a closing parenthesis
        statement = statement[:-1]+")"
        
        #execute the statement and commit the changes
        database.execute(statement)
        database.commit()
         
        #loop for each array present in the json object for the table
        for array in jsonObj[name]["column_data"]:
            
            #start the statement with the keyword to add some data to a specific
            #table            
            statement = "insert into " + name + " values("

            #initiazlize a temporary variable which will allows us to get the
            #binded datatype for that specific record
            x = 0
            
            #for each data present in the array
            for data in array:
                
                #check if the column type is an int
                if colType[index][x].find("int") is not -1:
                    
                    #if the previous statement is true, it means that the data
                    #is an integer, hence the program doesn't need to add a
                    #quoting tag
                    statement = statement + data + ","
                
                #if the program didn't execute the last statement...
                else:
                    
                    #...it does mean that the data type for that specific record
                    #is a text, so the program needs to add an openind and a closing
                    #quoting tag
                    statement = statement + '"' + data + '",'
                    
                #increment the temporary variable
                x = x+1
                
            #delete the last comma and close the parenthesis
            statement = statement[:-1]+")"
            
            #execute the statement and commit the changes
            database.execute(statement)
            database.commit()  
                  
    #close the database
    database.close()