#include <iostream>
#include <fstream>
#include <filesystem>
#include <sstream>
#include "cjson/cJSON.h"
#include "MyVector.h"
#include "HashTable.h"
using namespace std;



string readJSON(const string& fileName)
{
    fstream file(fileName);
    if (!file.is_open())
    {
        cerr << "Error reading " << fileName << ".json file!" << endl;
        return "";
    }

    stringstream buffer;
    buffer << file.rdbuf();
    file.close();

    return buffer.str();
}

bool createDir(const string& dirName)
{
    if (filesystem::create_directory(dirName)) return true;
    else return false;
}

void createFilesInSubfolder(const cJSON* table, const int& tuplesLimit, const cJSON* structure, const string& subName)
{
    Myvector<string> columnNames; // Sozdanie i zagruzka soderjimogo v .csv
    cJSON* array = cJSON_GetObjectItem(structure, table->string);
    int arraySize = cJSON_GetArraySize(array);

    for (int i = 0; i < arraySize; i++)
    {
        cJSON* arrayItem = cJSON_GetArrayItem(array, i);
        columnNames.MPUSH(arrayItem->valuestring);
    }
    
    ofstream CSV(subName + "/1.csv"); // zapolnenie .csv
    for (int i = 0; i < columnNames.size(); i++)
    {
        CSV << columnNames[i] << " ";
    }
    CSV << endl;
    CSV.close();
    
    
    
    ofstream PKSEQ(subName + "/" + table->string +"_pk_sequence"); // cozdanie schetchika
    PKSEQ << "1";
    PKSEQ.close();

    ofstream PKLOCK(subName + "/" + table->string +"_lock"); // cozdanie mutexa
    PKLOCK << "0";
    PKLOCK.close();
}

void createDB()
{
    ofstream DBFlag("DBflag"); //flag nalichiya db
    DBFlag.close();

    string jsonContent = readJSON("schema.json"); // otkritie .json

    if (jsonContent.empty())
    {
        cerr << "Error reading schema.json file!" << endl;
        exit(-1);
    }

    cJSON* json = cJSON_Parse(jsonContent.c_str()); // parsing .json

    if (json == nullptr)
    {
        cerr << "Error parsing schema.json file!" << endl;
        exit(-1);
    }

    cJSON* tuple = cJSON_GetObjectItem(json, "tuples_limit");
    int tuplesLimit = tuple->valueint;
    cJSON* schemaName = cJSON_GetObjectItem(json, "name");
    string DataBaseName = schemaName->valuestring;
    createDir(DataBaseName); //papka DB

    cJSON* structure = cJSON_GetObjectItem(json, "structure"); //parsing structuri

    for (cJSON* table = structure->child; table != nullptr;table = table->next) // sozdanie papok tablic
    {
        string subName = DataBaseName+"/"+table->string;
        createDir(subName);
        createFilesInSubfolder(table, tuplesLimit, structure, subName);
    }

    cJSON_Delete(json);
}

void checkDB()
{
    bool isCreated = false;
    fstream jsonConfig("schema.json");
    if (jsonConfig.good() && !std::filesystem::exists("DBflag"))
    {
        createDB();
        cout << "database created!" << endl;
    } 
    else cout << "database already exists!" << endl;
}

Myvector<string> getLineNames(string rawLine)
{
    string columnName;
    Myvector<string> columnNames;
    for (int i = 0; i < rawLine.size(); i++)
    {
        if (rawLine[i] == ' ')
        {
            columnNames.MPUSH(columnName);
            columnName = "";
            continue;
        }
        else columnName += rawLine[i];
    }
    return columnNames;
}

string subString(string oldLine, int startIndex, int endIndex)
{
    string newLine = "";
    for (int i = startIndex; i < endIndex; i++)
    {
        newLine += oldLine[i];
    }
    return newLine;
}

Myvector<HASHtable<string>> readTableContent(string tableName)
{   
    Myvector<HASHtable<string>> fullTable;
    Myvector<string> columnNames;
    string firstLine;
    

    fstream tableFile("Схема 1/" + tableName + "/"+"1.csv");
    if(tableFile.bad())
    {
        cerr << "Wrong tablename!" << endl;
        exit(-1);
    }
    getline(tableFile,firstLine);
    columnNames = getLineNames(firstLine); // zapis column names po otdelnosti
    
    int tableWidth = columnNames.size();
    HASHtable<string> row(tableWidth);

    for (int i = 0; i < columnNames.size(); i++)
    {
        row.HSET(columnNames[i],columnNames[i]);
    }
    fullTable.MPUSH(row);


    string pkSeqContent;
    fstream pkSeq("Схема 1/" + tableName+ "/" + tableName + "_pk_sequence");
    getline(pkSeq, pkSeqContent);
    pkSeq.close();
    
    int amountOfLinesInTable = stoi(pkSeqContent);
    
    if (amountOfLinesInTable == 1)
    {
        return fullTable;
    }
    else
    {
        for (int i = 0; i < amountOfLinesInTable-1; i++)
        {   
            HASHtable<string> row(tableWidth);
            Myvector<string> columnValues;
            getline(tableFile,firstLine);

            columnValues = getLineNames(firstLine);
            
            for (int j = 0; j < columnValues.size(); j++)
            {
                row.HSET(columnNames[j],columnValues[j]);
            }
            
            fullTable.MPUSH(row);  
        }
          
    }
   
    
    tableFile.close();
    return fullTable;
}

void lockTable(const string& pathToDir)
{
    ofstream lockFile("Схема 1/" + pathToDir + "/" + pathToDir + "_lock");
    lockFile << 1;
    lockFile.close();
}

void unlockTable(const string& pathToDir)
{
    ofstream lockFile("Схема 1/" + pathToDir + "/" + pathToDir + "_lock");
    if (lockFile.bad()) cerr << "error opening lock";
    lockFile << 0;
    lockFile.close();
}

void insertIntoTable(Myvector<HASHtable<string>>& table, const string& pathToDir,
 const Myvector<string>& values)
{
    lockTable(pathToDir);
    unlockTable(pathToDir);
}


void handleCommands(Myvector<string>& commandVector)
{
    //commandVector.print();
    if (commandVector.size() == 0)
    {
        cerr << "Empty query for program!" << endl;
        exit(0);
    }
    if (commandVector[0] == "SELECT")
    {
        
        commandVector.MDEL(0);
        cout << "SELECT FROM has been called!" << endl;
        commandVector.print();

        Myvector<string> selectedCell; // table1.column1 etc

    }
    else if (commandVector[0] == "INSERT" && commandVector[1] == "INTO")
    {
        commandVector.MDEL(0);
        commandVector.MDEL(0);

        cout << "INSERT INTO has been called!" << endl;
        string tableName = commandVector[0];

        commandVector.MDEL(0);

        if (commandVector[0] != "VALUES")
        {
            cerr << "Wrong syntax for INSERT" << endl;
            return;
        }
        
        commandVector.MDEL(0);
        
        Myvector<HASHtable<string>> fullTable = readTableContent(tableName);
        insertIntoTable(fullTable,tableName,commandVector);
        cout << "ROFLOfsdfsfsdf";
    }
    else if (commandVector[0] == "DELETE" && commandVector[1] == "FROM")
    {
        commandVector.MDEL(0);
        commandVector.MDEL(0);
        cout << "DELETE FROM has been called!" << endl;
        commandVector.print();
    }
}

Myvector<string> handleUserInput(const string& input)
{
    string command = "";
    Myvector<string> commandArray;
    
    for (int i = 0; i < input.size(); i++)
    {
        if (input[i] == ' '|| input[i] == '\'' || input[i] == ','|| input[i] == '(' || input[i] == ')')
        {
            if (command.size() != 0) commandArray.MPUSH(command);
            command = "";
        }
        else
        {
            command += input[i];
        }
    }
    return commandArray;
}

int main()
{
    // Pomnit` pro probeli v .csv

    //checkDB();
    string input;
    getline(cin, input);
    Myvector<string> commandVector = handleUserInput(input);
    handleCommands(commandVector);

    //Myvector<HASHtable<string>> tablica1 = readTableContent("таблица1");
    //cout << tablica1.size() << endl;
    //tablica1[0].print();
    
    return 0;
}