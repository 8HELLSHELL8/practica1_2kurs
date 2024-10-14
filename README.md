void writeOutTableFile(Myvector<HASHtable<string>>& table, const string& pathToDir, 
                       Myvector<string>& columnNames)
{
    // Read the sequence file to determine the number of lines to write
    string pk_seqInput;
    ifstream pk_seq("Схема 1/" + pathToDir + "/" + pathToDir + "_pk_sequence");
    
    if (!pk_seq.is_open()) {
        cerr << "Error: Could not open sequence file." << endl;
        return;
    }

    getline(pk_seq, pk_seqInput);
    int amountOfLines = stoi(pk_seqInput);
    pk_seq.close();

    // Prepare the output CSV file
    string currentTable = "1.csv";
    ofstream tableFile("Схема 1/" + pathToDir + "/" + currentTable);

    if (!tableFile.is_open()) {
        cerr << "Error: Could not write to table file." << endl;
        return;
    }

    // Write the column headers first
    for (int j = 0; j < columnNames.size(); j++) {
        tableFile << columnNames[j];
        if (j < columnNames.size() - 1) {
            tableFile << ",";  // Add commas between headers
        }
    }
    tableFile << '\n';  // New line after headers

    // Write the table content row by row
    for (int i = 0; i < amountOfLines && i < table.size(); i++) {
        for (int j = 0; j < columnNames.size(); j++) {
            tableFile << table[i].HGET(columnNames[j]);
            if (j < columnNames.size() - 1) {
                tableFile << ",";  // Add commas between values
            }
        }
        tableFile << '\n';  // New line after each row
    }

    tableFile.close();
}
