void insertIntoTable(Myvector<HASHtable<string>>& table, const string& pathToDir,
                     Myvector<string>& values, Myvector<string>& columnNames)
{
    lockTable(pathToDir);

    int tableWidth = columnNames.size();
    HASHtable<string> loadline;

    if (tableWidth == values.size())
    {
        // Case 1: Equal size between column names and values
        for (int i = 0; i < tableWidth; i++)
        {
            loadline.HSET(columnNames[i], values[i]);
        }
        table.MPUSH(loadline);
        increaseSequence(pathToDir);
    }
    else if (tableWidth > values.size())
    {
        // Case 2: More columns than values, fill remaining with "EMPTY_CELL"
        for (int i = 0; i < values.size(); i++)
        {
            loadline.HSET(columnNames[i], values[i]);
        }
        for (int i = values.size(); i < tableWidth; i++)
        {
            loadline.HSET(columnNames[i], "EMPTY_CELL");
        }
        table.MPUSH(loadline);
        increaseSequence(pathToDir);
    }
    else if (tableWidth < values.size())
    {
        // Case 3: More values than columns, split across multiple rows
        int index = 0;
        while (index < values.size())
        {
            loadline.clear();  // Start a new row

            for (int i = 0; i < tableWidth && index < values.size(); i++, index++)
            {
                loadline.HSET(columnNames[i], values[index]);
            }

            // If this row is incomplete, fill remaining columns with "EMPTY_CELL"
            for (int i = loadline.size(); i < tableWidth; i++)
            {
                loadline.HSET(columnNames[i], "EMPTY_CELL");
            }

            table.MPUSH(loadline);  // Push the row to the table
            increaseSequence(pathToDir);
        }
    }

    // Write the table data to file
    writeOutTableFile(table, pathToDir, columnNames);

    unlockTable(pathToDir);
}
