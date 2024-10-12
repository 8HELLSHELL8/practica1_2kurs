#include <iostream>
#include <fstream>
#include <filesystem>
#include <sstream>
#include "cjson/cJSON.h"
#include "MyVector.h"
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

void createDB()
{
    
}


bool checkDB()
{
    fstream jsonConfig("schema.json");
    return jsonConfig.good();
}

int main(int argv, char** argc)
{

    createDir("123");
    return 0;
}