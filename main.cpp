#include <iostream>
#include <fstream>
#include <filesystem>
#include <sstream>
#include "cjson/cJSON.h"
#include "MyVector.h"
#include <cstdio>
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

Myvector<HASHtable<string>> readTableContent(const string& tableName, Myvector<string>& columnNames)
{   
    Myvector<HASHtable<string>> fullTable;
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

void increaseSequence(const string& pathToDir)
{
    string currentNum;

    ifstream pk_seqRead("Схема 1/" + pathToDir + "/" + pathToDir + "_pk_sequence");
    if (pk_seqRead.bad()) cerr << "Error with pk_seq";
    getline(pk_seqRead,currentNum,'\n');
    pk_seqRead.close();

    ofstream pk_seqWrite("Схема 1/" + pathToDir + "/" + pathToDir + "_pk_sequence");
    if (pk_seqWrite.bad()) cerr << "Error with pk_seq";
    pk_seqWrite << stoi(currentNum)+1;
    pk_seqWrite.close();
    
}

void decreaseSequence(const string& pathToDir)
{
    string currentNum;

    ifstream pk_seqRead("Схема 1/" + pathToDir + "/" + pathToDir + "_pk_sequence");
    if (pk_seqRead.bad()) cerr << "Error with pk_seq";
    getline(pk_seqRead,currentNum,'\n');
    pk_seqRead.close();

    ofstream pk_seqWrite("Схема 1/" + pathToDir + "/" + pathToDir + "_pk_sequence");
    if (pk_seqWrite.bad()) cerr << "Error with pk_seq";
    pk_seqWrite << stoi(currentNum)-1;
    pk_seqWrite.close();
    
}

bool isFileEmpty(const std::string& filePath) {
    std::ifstream file(filePath, std::ios::ate);  // ios::ate открывает файл в конце
    return file.tellg() == 0;  // Если позиция указателя 0, значит файл пустой
}

void writeOutTableFile(Myvector<HASHtable<std::string>>& table, 
                       const std::string& pathToDir, 
                       Myvector<std::string>& columnNames)
{
    // Пути к файлам
    std::string sequencePath = "Схема 1/" + pathToDir + "/" + pathToDir + "_pk_sequence";
    std::string tablePath = "Схема 1/" + pathToDir + "/1.csv";

    // Чтение количества строк из sequence-файла
    std::string pk_seqInput;
    std::ifstream pk_seq(sequencePath);
    if (!pk_seq.is_open()) {
        std::cerr << "Error: Could not open sequence file." << std::endl;
        return;
    }
    std::getline(pk_seq, pk_seqInput);
    int amountOfLines = std::stoi(pk_seqInput);
    pk_seq.close();

    // Открываем CSV файл в режиме добавления (или создаем новый с очисткой)
    std::ofstream tableFile(tablePath, std::ios::out | std::ios::trunc);
    if (!tableFile.is_open()) {
        std::cerr << "Error: Could not open or create table file." << std::endl;
        return;
    }

    // --- Записываем заголовки только один раз (если файл пустой) ---
    if (isFileEmpty(tablePath)) {
        for (size_t j = 0; j < columnNames.size(); ++j) {
            tableFile << columnNames[j];
            if (j < columnNames.size() - 1) {
                tableFile << " ";  // Запятая между заголовками
            }
        }
        tableFile << " ";
        tableFile << '\n';  // Переход на новую строку после заголовков
    }

    // --- Записываем строки данных ---
    for (int i = 1; i < table.size(); ++i) {
        for (size_t j = 0; j < columnNames.size(); ++j) {
            tableFile << table[i].HGET(columnNames[j]);
            if (j < columnNames.size() - 1) {
                tableFile << " ";  // Запятая между значениями
            }
        }
        tableFile << " ";
        tableFile << '\n';  // Переход на новую строку после каждой строки данных
    }

    tableFile.close();  // Закрываем файл

    // Обновляем sequence-файл с новым количеством строк
    std::ofstream pk_seq_out(sequencePath, std::ios::out | std::ios::trunc);
    if (pk_seq_out.is_open()) {
        pk_seq_out << table.size();  // Обновляем количество строк
        pk_seq_out.close();
    }
}

void insertIntoTable(Myvector<HASHtable<string>>& table, const string& pathToDir,
                     Myvector<string>& values, Myvector<string>& columnNames)
{
    lockTable(pathToDir);  // Блокируем таблицу

    int tableWidth = columnNames.size();
    HASHtable<string> loadline;

    if (tableWidth == values.size()) {
        // Равное количество колонок и значений
        for (int i = 0; i < tableWidth; i++) {
            loadline.HSET(columnNames[i], values[i]);
        }
        table.MPUSH(loadline);  // Добавляем строку в таблицу
    } 
    else if (tableWidth > values.size()) {
        // Больше колонок, чем значений — заполняем пустыми ячейками
        for (int i = 0; i < values.size(); i++) {
            loadline.HSET(columnNames[i], values[i]);
        }
        for (int i = values.size(); i < tableWidth; i++) {
            loadline.HSET(columnNames[i], "EMPTY_CELL");
        }
        table.MPUSH(loadline);
    } 
    else {
        // Больше значений, чем колонок — разделяем на несколько строк
        int index = 0;
        while (index < values.size()) {
            loadline = {};  // Начинаем новую строку

            for (int i = 0; i < tableWidth && index < values.size(); i++, index++) {
                loadline.HSET(columnNames[i], values[index]);
            }

            // Если строка неполная, заполняем оставшиеся ячейки
            for (int i = loadline.size(); i < tableWidth; i++) {
                loadline.HSET(columnNames[i], "EMPTY_CELL");
            }

            table.MPUSH(loadline);  // Добавляем строку в таблицу
        }
    }

    // Увеличиваем счетчик строк и записываем таблицу в файл
    increaseSequence(pathToDir);
    writeOutTableFile(table, pathToDir, columnNames);

    unlockTable(pathToDir);  // Разблокируем таблицу
}


void selectColumns(std::string tableNameFirst, std::string firstValue, 
                   std::string tableNameSecond, std::string secondValue,
                   Myvector<string> condition)
{
    // Чтение содержимого первой таблицы
    condition.print();
    Myvector<std::string> columnNamesFirst;
    Myvector<HASHtable<std::string>> firstTable = readTableContent(tableNameFirst, columnNamesFirst);

    // Чтение содержимого второй таблицы
    Myvector<std::string> columnNamesSecond;
    Myvector<HASHtable<std::string>> secondTable = readTableContent(tableNameSecond, columnNamesSecond);

    // Проверка на наличие данных в таблицах
    if (firstTable.size() == 0 || secondTable.size() == 0) {
        std::cerr << "Error: One or both tables are empty or could not be read.\n";
        return;
    }

    // Проверка существования колонок в первой таблице
    bool foundFirstValue = false;
    for (int i = 0; i < columnNamesFirst.size(); i++) {
        if (columnNamesFirst[i] == firstValue) {
            foundFirstValue = true;
            break;
        }
    }

    // Проверка существования колонок во второй таблице
    bool foundSecondValue = false;
    for (int i = 0; i < columnNamesSecond.size(); i++) {
        if (columnNamesSecond[i] == secondValue) {
            foundSecondValue = true;
            break;
        }
    }

    // Если одна из колонок не найдена, выводим ошибку
    if (!foundFirstValue || !foundSecondValue) {
        std::cerr << "Error: One or both column names not found in the tables.\n";
        return;
    }

    // Вывод заголовков
    std::cout << firstValue << " | " << secondValue << "\n";
    std::cout << "--------------------\n";

    // Выполнение декартова произведения: перебор строк из первой и второй таблиц
    for (int i = 1; i < firstTable.size(); i++) {
        for (int j = 1; j < secondTable.size(); j++) {
            std::cout << firstTable[i].HGET(firstValue) << " | " 
                      << secondTable[j].HGET(secondValue) << "\n";
        }
    }
}

Myvector<string> handleCondition(string condition)
{
    Myvector<string> elements;
    string word = "";
    
    for (int i = 0; i < condition.size(); i++)
    {
        // Проверяем, чтобы символ не был точкой, пробелом или апострофом
        if (condition[i] != '.' && condition[i] != ' ' && condition[i] != '\'')
        {
            // Если символ - '=', добавляем предыдущее слово, если оно не пустое
            if (condition[i] == '=')
            {
                if (!word.empty())
                {
                    elements.MPUSH(word);
                    word = ""; // очищаем word для нового слова
                }
                elements.MPUSH("="); // добавляем знак равенства как отдельный элемент
            }
            else
            {
                word += condition[i]; // добавляем символ к текущему слову
            }
        }
        else
        {
            // Если встретили пробел или другой разделительный символ, добавляем слово в вектор
            if (!word.empty())
            {
                elements.MPUSH(word);
                word = ""; // очищаем word для нового слова
            }
        }
    }

    // Добавляем последнее слово, если оно не пустое
    if (!word.empty())
    {
        elements.MPUSH(word);
    }

    return elements;
}

bool checkCondition(Myvector<std::string> conditionArray, HASHtable<std::string>& line) {
    if (conditionArray.size() == 0) {
        return false; // Если массив пуст, возвращаем false
    }
    
    bool totalFlag = true; // Изначально предполагаем, что выражение истинно
    bool currentCondition = true; // Состояние текущего условия

    for (size_t i = 0; i < conditionArray.size() - 2 ; i++) {
        const std::string& token = conditionArray[i];

        if (token == "AND") {
            // Проверяем следующее условие
            if (i + 1 < conditionArray.size()) {
                const std::string& nextColumn = conditionArray[i + 1];
                const std::string& nextValue = conditionArray[i + 2];

                currentCondition = (line.HGET(nextColumn) == nextValue);
                totalFlag = totalFlag && currentCondition; // Обновляем флаг для AND
            }
            i += 2; // Пропускаем обработанные значения
        } else if (token == "OR") {
            // Проверяем следующее условие
            if (i + 1 < conditionArray.size()) {
                const std::string& nextColumn = conditionArray[i + 1];
                const std::string& nextValue = conditionArray[i + 2];

                currentCondition = (line.HGET(nextColumn) == nextValue);
                totalFlag = totalFlag || currentCondition; // Обновляем флаг для OR
            }
            i += 2; // Пропускаем обработанные значения
        } else {
            // Это обычное условие, проверяем его
            const std::string& column = token;
            const std::string& value = conditionArray[i + 2];
            currentCondition = (line.HGET(column) == value);
            totalFlag = totalFlag && currentCondition; // Обновляем общий флаг
            i += 2; // Пропускаем проверенное условие
        }
    }
    return totalFlag;
}

void deleteFromTable(string tableName, string condition)
{
    lockTable(tableName);
    Myvector<string> columnNames;
    Myvector<string> conditionArray = handleCondition(condition);
    conditionArray.MDEL(0);
    Myvector<HASHtable<std::string>> table = readTableContent(tableName, columnNames);
    for (int i = 0; i < table.size(); i++)
    {
        if (checkCondition(conditionArray, table[i]))
        {   
            table[i].HSET(conditionArray[0],"EMPTY_CELL");
            table[i].print();
        }
    }
    writeOutTableFile(table,tableName,columnNames);
    unlockTable(tableName);
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

        string firstTable = commandVector[0];
        commandVector.MDEL(0);
        string firstValue = commandVector[0];
        commandVector.MDEL(0);
        string secondTable = commandVector[0];
        commandVector.MDEL(0);
        string secondValue = commandVector[0];
        commandVector.MDEL(0);
        Myvector<string> condition = handleCondition(commandVector[0]);
        selectColumns(firstTable, firstValue,  secondTable, secondValue,
        condition);
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
        Myvector<string> columnNames;
        Myvector<HASHtable<string>> fullTable = readTableContent(tableName, columnNames);
        insertIntoTable(fullTable,tableName,commandVector, columnNames);
        
    }
    else if (commandVector[0] == "DELETE" && commandVector[1] == "FROM")
    {
        commandVector.MDEL(0);
        commandVector.MDEL(0);
        cout << "DELETE FROM has been called!" << endl;
        string tableName = commandVector[0];
        commandVector.MDEL(0);
        commandVector.MDEL(0);
        deleteFromTable(tableName, commandVector[0]);
        
    }
}

Myvector<string> handleUserInput(const string& input)
{
    string command = "";
    string conditions = "";  // Переменная для хранения всего, что идёт после WHERE
    Myvector<string> commandArray;
    bool isWhereClause = false;  // Флаг для определения секции WHERE

    for (int i = 0; i < input.size(); i++)
    {
        // Если встречаем WHERE, всё, что идёт после, должно быть одним элементом
        if (!isWhereClause && i + 5 <= input.size() && input.substr(i, 5) == "WHERE")
        {
            if (command.size() != 0) commandArray.MPUSH(command);
            commandArray.MPUSH("WHERE");  // Добавляем ключевое слово WHERE
            command = "";
            i += 5;  // Пропускаем слово WHERE
            isWhereClause = true;  // Активируем флаг секции WHERE
            continue;
        }

        if (isWhereClause)
        {
            // Всё, что идёт после WHERE, добавляем как одно целое в переменную conditions
            conditions += input[i];
        }
        else
        {
            // До WHERE обрабатываем, разделяя по символам-разделителям
            if (input[i] == ' ' || input[i] == '\'' || input[i] == ',' || 
                input[i] == '(' || input[i] == ')' || input[i] == '.' || input[i] == '=')
            {
                if (command.size() != 0) commandArray.MPUSH(command);
                command = "";
            }
            else
            {
                command += input[i];
            }
        }
    }

    // Добавляем последнее значение в массив, если оно не пустое
    if (command.size() != 0 && !isWhereClause) commandArray.MPUSH(command);

    // Добавляем всю секцию WHERE в массив команд как одно целое
    if (!conditions.empty()) commandArray.MPUSH(conditions);

    return commandArray;
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
    string input;
    getline(cin, input);
    Myvector<string> commandVector = handleUserInput(input);
    handleCommands(commandVector);
}

int main()
{
    setlocale(LC_ALL, "RU");
    

    checkDB();
    
    return 0;
}
