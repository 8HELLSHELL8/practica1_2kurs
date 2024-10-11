#include <iostream>
#include <fstream>
#include <filesystem>
#include <sstream>
#include "cjson/cJSON.h"
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

bool createDir()
{
    if (filesystem::create_directory("123")) return true;
    else return false;
}


int main()
{

    createDir();
    return 0;
}