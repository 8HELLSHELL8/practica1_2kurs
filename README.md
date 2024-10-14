void writeOutTableFile(Myvector<HASHtable<string>>& table, const string& pathToDir, 
                       Myvector<string>& columnNames)
{
    // Чтение файла последовательности (sequence)
    string pk_seqInput;
    ifstream pk_seq("Схема 1/" + pathToDir + "/" + pathToDir + "_pk_sequence");

    if (!pk_seq.is_open()) {
        cerr << "Error: Could not open sequence file." << endl;
        return;
    }

    getline(pk_seq, pk_seqInput);
    int amountOfLines = stoi(pk_seqInput);
    pk_seq.close();

    // Открываем файл в режиме перезаписи (очистка перед записью)
    string currentTable = "1.csv";
    ofstream tableFile("Схема 1/" + pathToDir + "/" + currentTable, 
                       std::ios::out | std::ios::trunc);

    if (!tableFile.is_open()) {
        cerr << "Error: Could not write to table file." << endl;
        return;
    }

    // --- Запись заголовков (только один раз) ---
    for (int j = 0; j < columnNames.size(); j++) {
        tableFile << columnNames[j];
        if (j < columnNames.size() - 1) {
            tableFile << ",";  // Запятая между заголовками
        }
    }
    tableFile << '\n';  // Переход на новую строку после заголовков

    // --- Запись строк данных ---
    for (int i = 0; i < amountOfLines && i < table.size(); i++) {
        for (int j = 0; j < columnNames.size(); j++) {
            tableFile << table[i].HGET(columnNames[j]);
            if (j < columnNames.size() - 1) {
                tableFile << ",";  // Запятая между значениями
            }
        }
        tableFile << '\n';  // Переход на новую строку после каждой строки данных
    }

    tableFile.close();  // Закрываем файл
}
