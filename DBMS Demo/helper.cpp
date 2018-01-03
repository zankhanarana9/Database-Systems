
#include "helper.h"

map<string, table> helper::sysTableTemp;
//static std::map<string, table> sysTableTemp;
 
table helper::createObject(vector <string> query, int numRows) { 
	string tableName = query[2];
	table object;
	object.setTableName(tableName);
	object.getTableName();
	object.fetchColumnInformation(query);
	object.setNumRows(numRows);
	return object;
}


vector <string> helper::getSelectColumns(vector <string> itemizedQuery1) {

	vector <string> selectColumnNames;
	int k;
	for (k = 1; k < itemizedQuery1.size(); k++) {
		if (itemizedQuery1[k] == "FROM") {
			for (int j = 1; j < k; j++) {
				selectColumnNames.push_back(itemizedQuery1[j]);
			}
		}
	}
		return selectColumnNames;
}

int helper::parseInsertQuery(vector <string> itemizedQuery, std::map<string, table> myMap, string query, table &tableObject) {

//	string tableName = query[2];
	vector <string> getData;
	bool flag = 0;
	int key = 0;
	string object = "";
	bool fileCheck;
	bool pkCheck;
	string tableName;
	string selectTableName;
	string insertSelect;
	tableName = itemizedQuery[2];
	string compareName;
	std::map<string, table>::iterator it;
	int numRows;
	vector <string> data;
	vector <string> selectColumn;
	vector <string> executeQuery;
	vector <string> itemizedQuery1;
	string tempQuery = query;
/*	for (it = myMap.begin(); it != myMap.end(); ++it) {
		compareName = it->first;
		if (tableName == compareName) {
			tableObject = it->second;
			break;
		}
	}*/

	for (int i = 0; i < itemizedQuery.size(); i++) {
		executeQuery.push_back(itemizedQuery[i]);
	}

	numRows = tableObject.fetchNumRows();

//Use this part for insert /select query

	if (itemizedQuery[3] == "SELECT") {
		for (int i = 3; i < itemizedQuery.size(); i++) {
			insertSelect.append(itemizedQuery[i]);
			if (i < itemizedQuery.size() - 1)
				insertSelect.append(" ");
		}
		itemizedQuery1 = getItemizedQuery(insertSelect);
		selectColumn = getSelectColumns(itemizedQuery1);
		selectTableName = itemizedQuery1[selectColumn.size() + 2];
		bool columnCheck = checkColumns(selectColumn, myMap, tableName, selectTableName);
		data = parseSelect(insertSelect, myMap, "null", 0);
		
		if (columnCheck) {
			executeQuery = createItemizedQuery(tempQuery, itemizedQuery, data);
			tempQuery = createStringQuery(myMap, itemizedQuery, data, tableName);
		//	tempQuery = insertStringQuery(tableName, data);
			
		}
	} 

	getData = insertData(executeQuery);
//	pkCheck = helper::checkPrimaryKey(tableName, myMap, getData, numRows);
	string check = isTableExists(tableName, myMap);
	if (check != "null") {
		bool isTrue = checkColumnFormat(tableName, myMap, getData, tempQuery);
		if (isTrue) {
			fileCheck = helper::ifFileExists(tableName, numRows);
			if (fileCheck) {
				pkCheck = helper::checkPrimaryKey(tableName, myMap, getData, numRows);
				if (pkCheck) {
					numRows++;
					tableObject.updateNumRows(numRows);
					helper::createBinaryFile(tableName, myMap, getData);
				}
				else {
					cout << "primary key violation" << endl;
				}
			}
			else cout << "Unable to open the file" << endl;
		}
	}

	else cout << "Table " << tableName << " does not exist!" << endl;

	return numRows;
}

table helper::getTableObject(std::map<string, table> myMap, string tableName) {
	
	string compareName;
	table temp;
	string returnObject = "";
	std::map<string, table>::iterator it;
	string check = isTableExists(tableName, myMap);
	for (it = myMap.begin(); it != myMap.end(); ++it) {
		compareName = it->first;
		if (tableName == compareName) {
			temp = it->second;
			break;
		}
	}

//	Check if the table is a Temp table	
	return temp;
}

bool helper::checkColumns(vector <string> selectColumns, std::map<string, table> myMap, string insertTableName, string selectTableName) {

	table tableObject = getTableObject(myMap, insertTableName);
	//table tableObject1 = getTableObject(myMap, selectTableName);
	if (tableObject.columnName.size() <= selectColumns.size()) {
		return true;
	}
	else return false;
}

string helper::createStringQuery(std::map<string, table> myMap, vector <string> insertQuery, vector <string> data, string tableName) {
	
	table tableObject = getTableObject(myMap, tableName);
	string query1;
	int i;
	for (i = 0; i < insertQuery.size(); i++) {
		if (insertQuery[i] != "SELECT") {
			query1.append(insertQuery[i]);
			query1.append(" ");
		}	
		else break;
	}

	query1.append("VALUES (");
	for (int i = 0; i < data.size(); i++) {
		if (tableObject.columnType.at(i) == "CHAR") {
			query1.append("'");
			query1.append(data[i]);
			query1.append("'");
		}
		else {
			query1.append(data[i]);
		}
		if (i < data.size() - 1)
			query1.append(",");
	}

	query1.append(");");

	return query1;
}

vector <string> helper :: createItemizedQuery(string tempQuery, vector <string> insertQuery, vector <string> data) {
	vector <string> query1;
	int i;
	for (i = 0; i < insertQuery.size(); i++) {
		if (insertQuery[i] != "SELECT")
			query1.push_back(insertQuery[i]);
		else break;
	}

	query1.push_back("VALUES");
	for (int i = 0; i < data.size(); i++) {
		query1.push_back(data[i]);
	}

	return query1;
}

string helper::isTableExists(string name, std::map<string, table> myMap) {

	
	string compareName;
	string returnObject = "";
	std::map<string, table>::iterator it;
	for (it = myMap.begin(); it != myMap.end(); ++it) {
		compareName = it->first;
		table temp = it->second;
		if (name == compareName) {
			returnObject = name;
			break;
		}
	}	 
	if (it != myMap.end()) {
		return returnObject;
	}
	else {
		return "null";
	}
} 

vector <string> helper::sqlRead(string fileName) {
	string line;
	string delimeter = ";";
	size_t pos = 0; 
	vector <string> query;
	string multiLine = "";
	try {
		ifstream file(fileName);
		if (file.is_open())
		{
			while (getline(file, line))
			{
				size_t prev = 0;
				int start = line.find("/*");
				int stop = line.find("*/");
				if (start > 0 && stop > 0){
					line.erase(start, stop);
				}
				if ((pos = line.find(delimeter)) != std::string::npos) {
					multiLine = multiLine + " " + line.substr(prev, pos + 1);
					query.push_back(multiLine);
					line.erase(prev, pos+1);
					multiLine.erase();
				}
				else {
					multiLine = multiLine + " " + line;
					line.erase();
				}
			}
			file.close();
			
		}

		else cout << "Unable to open file";
	}
	catch (exception e) {
		cout << "Error";
	}
	return query;
}

bool helper::checkColumnFormat(string name,  std::map<string, table> myMap, vector <string> data, string tempQuery) {

	table temp = getTableObject(myMap, name);
	string check1 = "";
	string check2 = "";
	int search = 0;
	int pos = 0;
	pos = tempQuery.find("(");
	if (pos!= string::npos) {
		tempQuery.erase(0, pos);
	}
	if (temp.columnName.size() != data.size()) {
		return false;
	}
	else {
		for (int i = 0; i < temp.columnName.size(); i++) {
			if (temp.columnType.at(i) == "INT") {
				search = (tempQuery.find(data.at(i)));
				if (search < tempQuery.size()) {
					try {
						check1 = tempQuery.at(search);
						int check = stoi(check1);
						if (std::isnan((float)check)) {
							return false;
						}
						else {
							cout << "The data is: " << data.at(i)  << endl;
							continue;
						}
					}
					catch (exception e) {
						cout << "Invalid int format" << endl;
					}
				}
			}
				else if (temp.columnType.at(i) == "CHAR") {
					search = tempQuery.find(data.at(i));
					if (search < tempQuery.size()) {
						check1 = tempQuery.at(search - 1);
						check2 = tempQuery.at(data.at(i).size() + search);
						if (check1 == "'" && check2 == "'") {
							cout << "The data is: " << data.at(i) << endl;
							continue;
						}
						else {
							return false;
						}
					}
				}
			}
		}
	return true;
}

vector <string> helper::insertData(vector <string> query) {
	vector <string> insertData;
	int i;
	for (i = 0; query.size(); i++) {
		if (query[i] == "VALUES") {
			break;
		}
	}

	for (int j = i + 1; j < query.size(); j++) {
		insertData.push_back(query[j]);
	}

	return insertData;
}


void helper::handleDropQuery(vector <string> itemizedQuery) {
	string dropTable;
	string fileName;
	dropTable = itemizedQuery[2];
	fileName = dropTable + ".tbl";
	char file[10];
	strcpy(file, fileName.c_str());
	cout << "deleting file " << fileName << endl;
	if (remove(file) != 0)
		perror("Error deleting file");
	else
		puts("File successfully deleted");
	string lineFinder;
	string line;
	int pos;
	bool flag = 1;
	bool flagFound = 1;
	unsigned int curLine = 0;
	ofstream myfile1;
	int i = 0;
	myfile1.open("writeFile.txt");
	ifstream myfile("catalog.txt");
	if (myfile.is_open()) {
		while (getline(myfile, line)) {
			flag = 1;
			curLine++;
			if (line.find("tablename", 0) != string::npos) {
				lineFinder = line;
				pos = lineFinder.find(dropTable, 0);
				if (pos != -1) {
					flag = 0;
					flagFound = 0;
				}
			}
			if (!flagFound && i < 6) {
				flagFound = 0;
				i++;
			}
			else flagFound = 1;
			if (flag && flagFound) {
				myfile1 << line << "\n";
			}
		}
	}
	else cout << "Unable to open the file";
	myfile.close();

	char oldname[] = "catalog.txt";

	if (remove(oldname) != 0)
		perror("Error deleting file");
	else
		puts("File successfully deleted");

	return;
}

void helper::createCatalogFile(std::map<string, table> myMap) {

//	table tableObject = getTableObject(myMap, tableName);
	int recordsize;
	int totalSize;
	std::map<string, table>::iterator it;
	ofstream catalogOutput;
	catalogOutput.open("catalog.txt");

	table tableObject;

	for (it = myMap.begin(); it != myMap.end(); ++it) {
		tableObject = it->second;
		recordsize = 0;
		for (int i = 0; i < tableObject.columnLength.size(); i++)
			recordsize += tableObject.columnLength.at(i);

		totalSize = tableObject.numRows * recordsize;
		catalogOutput << "tablename=" << tableObject.objTableName << endl;
		catalogOutput << "columns=";
		for (int i = 0; i < tableObject.columnType.size(); i++) {
			catalogOutput << tableObject.columnName.at(i) << ":" << tableObject.columnType.at(i);
			if (i < tableObject.columnType.size() - 1)
				catalogOutput << ",";
		}
		catalogOutput << endl;
		catalogOutput << "primary key=" << tableObject.primaryKey << endl;
		catalogOutput << "recordsize=" << recordsize << endl;
		catalogOutput << "totalsize=" << totalSize << endl;
		catalogOutput << "records=" << tableObject.numRows << endl;
	}

	
	
	catalogOutput.close();
	return;
}

bool helper::ifFileExists(string tableName, int numRows) {

	if (numRows == 0) {
		return true;
	}

	string filename = tableName + ".tbl";
	ifstream myfile(filename);
	if (myfile.is_open()) {
		myfile.close();
		return true;
	}
	else return false;
}

bool helper::checkPrimaryKey(string name, std::map<string, table> myMap, vector <string> insertData, int numRows) {

	string compareName;
	table tableObject;
	string returnObject = "";
	std::map<string, table>::iterator it;
	string line;
	bool ifTrue = false;
	string fileName = name + ".tbl";
	string primKeyCol;
	string comparePK;
	int index;
	ifstream myfile(fileName);
	int rowSize = 0;
	int pos = 0;
	string c;
	string check;
	string memblock;


	if (numRows == 0) {
		return true;
	}
	else {

		tableObject = getTableObject(myMap, name);
	/*	for (it = myMap.begin(); it != myMap.end(); ++it) {
			compareName = it->first;
			if (name == compareName) {
				tableObject = it->second;
				break;
			}
		}*/

		primKeyCol = tableObject.primaryKey;

		if (primKeyCol == "") {
			return true;
		}

		for (index = 0; index < tableObject.columnName.size(); index++) {
			comparePK = tableObject.columnName[index];
			if (primKeyCol == comparePK) {
				break;
			}
		}

		rowSize = tableObject.getRowSize();

		if (myfile.is_open()) {
			while (getline(myfile, line)) {
				pos = 0;
				//	pos = j * rowSize;
				for (int k = 0; k < tableObject.columnType.size(); k++) {
					string temp = insertData[k];
					if (tableObject.columnType[k] == "INT") {
						if (insertData[k].length() != tableObject.columnLength[k])
							temp = (bitset<4>(stoi(insertData[k]))).to_string();
						c = temp.c_str();
						if (tableObject.primaryKey == tableObject.columnName[k]) {
							memblock = line.substr(pos, tableObject.columnLength[k]);
							check = string(memblock);
						}
					}
					else if (tableObject.columnType[k] == "CHAR") {
						if (insertData[k].length() != tableObject.columnLength[k])
							insertData[k].insert(insertData[k].begin(), tableObject.columnLength[k] - insertData[k].length(), '0');
						c = insertData[k].c_str();
					}
					pos += tableObject.columnLength[k];
					if (c == check) {
						ifTrue = true;
						cout << "Successful" << endl;
					}
				}
			}
		}
		else {
			cout << "Unable to open the file" << endl;
		}
		if (ifTrue) return false;
		else return true;
	}
}

void helper::createBinaryFile(string name, std::map<string, table> myMap, vector <string> insertData) {

	table tableObject = getTableObject(myMap, name);
	fstream tableFile;
	string temp = "";
	tableFile.open(name + ".tbl", ios::out | ios::binary | ios::app);
	for (int j = 0; j < tableObject.columnType.size(); j++) {
		temp = insertData[j];
		if (tableObject.columnType[j] == "INT") {
			if (insertData[j].length() != tableObject.columnLength[j]) 
				temp = (bitset<4>(stoi(insertData[j]))).to_string();
			const char * c = temp.c_str();
			tableFile.write(c, strlen(c));
			cout << "c: " << c << endl;
		}
		else if (tableObject.columnType[j] == "CHAR") {
			if (insertData[j].length() != tableObject.columnLength[j])
				insertData[j].insert(insertData[j].begin(), tableObject.columnLength[j] - insertData[j].length(), '#');
			const char * c = insertData[j].c_str();
			cout << "c: " << c << endl;
			tableFile.write(c, strlen(c));
		}
	}

	const char * newLine = "\r\n";
	tableFile.write(newLine, strlen(newLine));
	return;
}

vector <string> helper::getItemizedQuery(string query) {
	
	vector <string> itemizedQuery;
	size_t prev = 0, pos;
	//char delim;
	while ((pos = query.find_first_of(" ,;", prev)) != string::npos) { //while pos of delimiter != end of string
		if (pos > prev)
			itemizedQuery.push_back(query.substr(prev, pos - prev));
		prev = pos + 1;
	}
	if (prev < query.size()) {
		itemizedQuery.push_back(query.substr(prev, query.size() - 1));
	}

	return itemizedQuery;
}

table helper::createTempTableObject(vector <string> query, int numRows) {

	string tableName = query[2];
	table object;
	object.setTableName(tableName);
	object.getTableName();
	object.fetchColumnInformation(query);
	object.setNumRows(numRows);
	return object;
}

vector <string> helper::createTempTableQuery(string aliasTableName, std::map<string, table> myMap, vector <string> selectColumnNames, vector <string> tableName) {

	vector <string> createQuery;
	string check = isTableExists(aliasTableName, myMap);
	table tableObject;
	int j = 0;
	vector <string> handledTableName;
	vector <string> handledDot;
	int left = 0;
	int right = 0;
	vector <table> storeObject;


	if (check == "null") {
		cout << "Temp table " << aliasTableName << " can be created" << endl;
		createQuery.push_back("CREATE");
		createQuery.push_back("Table");
		createQuery.push_back(aliasTableName);

		for (int i = 0; i < selectColumnNames.size(); i++) {
			//string col = columnName[i];
			for (int j = 0; j < tableName.size(); j++) {
				tableObject = getTableObject(myMap, tableName[j]);
				if (selectColumnNames[i].find(".") != string::npos) {
					handledDot = handleDotOperator(selectColumnNames[i]);
					if (handledDot[0] == tableObject.objTableName) {
						for (int trial = 0; trial < tableObject.columnName.size(); trial++) {
							if (handledDot[1] == tableObject.columnName[trial]) {
								createQuery.push_back(handledDot[1]);
								createQuery.push_back(tableObject.columnType.at(trial));
								if (tableObject.columnType.at(trial) == "CHAR") {
									createQuery.push_back(to_string(tableObject.columnLength.at(trial)));
								}
							}
						}
					}
				}
				else {
					for (int trial = 0; trial < tableObject.columnName.size(); trial++) {
						if (selectColumnNames[i] == tableObject.columnName[trial]) {
							createQuery.push_back(selectColumnNames[i]);
							createQuery.push_back(tableObject.columnType.at(trial));
							if (tableObject.columnType.at(trial) == "CHAR") {
								createQuery.push_back(to_string(tableObject.columnLength.at(trial)));
							}
						}
					}
				}
			}
		}
	}
	return createQuery;
}
	


vector <string> helper::parseSelectQuery(string tempQuery, std::map<string, table> myMap) {

	vector <string> itemizedQuery;
	vector <string> evaluate;
	vector <string> selectColumnNames;
	string aliasTableName;

	size_t prev = 0, pos, end;
	//char delim;
	while ((pos = tempQuery.find("(", prev)) != string::npos) { //while pos of delimiter != end of string
		if ((end = tempQuery.find(")", prev)) != string::npos) {
			itemizedQuery.push_back(tempQuery.substr(pos + 1, end - pos - 1));
			aliasTableName.append(tempQuery.substr(end + 1, tempQuery.find(';', prev) - 1 - end));
		}
		else {
			aliasTableName.append("null");
		}
		tempQuery.erase(pos , end - pos + 1);
	}


	itemizedQuery.push_back(tempQuery);

	for (int i = 0; i < itemizedQuery.size(); i++) {
		cout << "i:" << i << " " << itemizedQuery[i] << endl;
		if (i == 0 && i != itemizedQuery.size() - 1) { //3 nested selects
			evaluate = parseSelect(itemizedQuery.at(i), myMap, aliasTableName, 1); //main table map
		}
		else if (i == itemizedQuery.size() -1 ) {
			if (i == 0) {
				evaluate = parseSelect(itemizedQuery.at(i), myMap, aliasTableName, 0);
			} else
				evaluate = parseSelect(itemizedQuery.at(i), sysTableTemp, aliasTableName, 0); //	
		}
		else {
			evaluate = parseSelect(itemizedQuery.at(i), sysTableTemp, aliasTableName, 1); //	
		}
		
	}


	return evaluate;
}


vector <string> helper::parseSelect(string query, std::map<string, table> myMap, string aliasTableName, bool nested) {

	string tableName;
	vector <string> createTableName;
	vector <string> itemizedQuery1;
	size_t prev = 0, pos;
	string condition;
	int k;
	bool fileCheck;
	vector <string> evaluate;
	vector <string> selectColumnNames;
	itemizedQuery1.clear();
	cout << "Parsed Select........" << endl;
	vector <string> createTempTable;
	int numRows = 0;
	string joinConditon;
	string joinTable1;
	string joinTable2;
	int search;
	int number = 1;
	itemizedQuery1 = getItemizedQuery(query);
	selectColumnNames = getSelectColumns(itemizedQuery1);
	
	tableName = itemizedQuery1[selectColumnNames.size() + 2];
	table tableObject = getTableObject(myMap, tableName);
	numRows = tableObject.numRows;

	if (nested) {
		createTableName.push_back(itemizedQuery1[selectColumnNames.size() + 2]);
		createTempTable = createTempTableQuery(aliasTableName, myMap, selectColumnNames, createTableName);
		table tempObject = createTempTableObject(createTempTable, 0);  //do not call if last query
		updateSysTable(tempObject);
	}


	for (k = 1; k < itemizedQuery1.size(); k++) {

		if (itemizedQuery1[k] == "JOIN")  {
			createTableName.push_back(itemizedQuery1[k - 1]);
			createTableName.push_back(itemizedQuery1[k + 1]);
			joinConditon  = getJoincondition(query);
			goto skip;
		} else if (itemizedQuery1[k] == "WHERE") {
			break;
		}
	}
		
	for (int j = k + 1; j < itemizedQuery1.size(); j++) {
		condition.append(itemizedQuery1[j]);
	}
	   
	skip: fileCheck = helper::ifFileExists(tableName, numRows);
	if (fileCheck) {
		if (condition.size() > 0) {
			evaluate = parseSelectWhere(tableName, selectColumnNames, myMap, itemizedQuery1, condition, query, aliasTableName, nested);
		}
		else if (joinConditon.size() > 0) {
			if (aliasTableName == "") {
				aliasTableName.append("A");
				aliasTableName.append(to_string(number));
				number++;
			}
			parseSelectJoin(createTableName, joinConditon, query, myMap, aliasTableName);
		} else {
			evaluate = selectEvaluate(-1, tableName, selectColumnNames, myMap, itemizedQuery1, numRows, aliasTableName, nested);
		}
	}
	return evaluate;
}

void helper::parseSelectJoin(vector <string> createTableName, string joinCondition, string query, std::map<string, table> myMap, string aliasTableName) {

	vector <string> createTempTable;
	vector <string> selectColumnNames;
	vector <string> itemizedQuery1;
	string columnLeft;
	string columnRight;
	int found;
	string app;
	bool check;
	vector <string> evaluate;
	vector <string> columnLeftVector;
	vector <string> columnRightVector;


	itemizedQuery1 = getItemizedQuery(query);
	selectColumnNames = getSelectColumns(itemizedQuery1);
	createTempTable = createTempTableQuery(aliasTableName, myMap, selectColumnNames, createTableName);
	table tempObject = createTempTableObject(createTempTable, 0);  
	updateSysTable(tempObject);


	found = joinCondition.find("=");
	for (int i = 0; i < joinCondition.size(); i++) {
		if (i < found) {
			app = joinCondition[i];
			columnLeft.append(app);
		}
		else if (i > found) {
			app = joinCondition[i];
			columnRight.append(app);
		}
	}
	columnLeftVector = handleDotOperator(columnLeft);
	columnRightVector = handleDotOperator(columnRight);
	evaluateJoin(columnLeftVector, columnRightVector, aliasTableName, myMap, selectColumnNames);
}

vector <bool> getFlagValue(vector <string> combinedVector, table tableObject) {

	vector <bool> flag;
	string selectColumn = combinedVector[1];
	for (int k = 0; k < tableObject.columnName.size(); k++) {
			if (selectColumn == tableObject.columnName[k]) {
				flag.push_back(1);
			}
			else flag.push_back(0);
		}

	return flag;
}



vector<string> helper::evaluateJoin(vector <string> columnLeftVector, vector <string> columnRightVector, string aliasTableName, std::map<string, table> myMap, vector <string> selectColumnNames) {

	streampos size;
	string memblock;
	string fileNameLeft = columnLeftVector[0] + ".tbl";
	string fileNameRight = columnRightVector[0] + ".tbl";
	vector <bool> flagLeft;
	vector <bool> flagRight;
	vector <string> insertTempData;
	table tableObjectLeft;
	table tableObjectRight;
	std::map<string, table>::iterator it;
	string compareName;
	string check;
	int l;
	string temp;
	int lineNumber = 0;
	vector <string> evaluate;
	int numRowsLeft;
	int numRowsRight;
	int lineNumberLeft = 0;
	int lineNumberRight = 0;
	int tempNumRows;
	string insertStringQuery;
	vector <string> insertQuery;
	table* tableObject1 = NULL;

	tableObjectLeft = helper::getTableObject(myMap, columnLeftVector[0]);
	tableObjectRight = helper::getTableObject(myMap, columnRightVector[0]);
	numRowsLeft = tableObjectLeft.fetchNumRows();
	numRowsRight = tableObjectRight.fetchNumRows();

	//Check for columns in the select query
	int rowSizeLeft = tableObjectLeft.getRowSize();
	int rowSizeRight = tableObjectRight.getRowSize();

	int j = 0;

	flagLeft = getFlagValue(columnLeftVector, tableObjectLeft);
	flagRight = getFlagValue(columnRightVector, tableObjectRight);


	for (it = sysTableTemp.begin(); it != sysTableTemp.end(); ++it) {
		compareName = it->first;
		if (aliasTableName == compareName) {
			tableObject1 = &it->second;
			break;
		}
	}

	int pos = 0;
	int pos1 = 0;
	//open the file
	//temp will store fetched value from left table
	//memblock will store fetched value from right table
	string lineLeft;
	string lineRight;
	ifstream myfile(fileNameLeft);
	ifstream myfile1(fileNameRight);
	if (myfile.is_open() && myfile1.is_open())
	{
		while (getline(myfile, lineLeft)) {
			lineNumberLeft++;
				pos1 = 0;
				pos = 0;
				lineNumberRight = 0;
				//linenumber = j *rowSizeLeft;
				for (int i = 0; i < flagLeft.size(); i++) {
					if (flagLeft[i] == 1) {
						temp = lineLeft.substr(pos, tableObjectLeft.columnLength[i]);

						while (getline(myfile1, lineRight)) {
							lineNumberRight++;
								//linenumber = j *rowSizeLeft;
								for (int right = 0; right < flagRight.size(); right++) {
									if (flagRight[right] == 1) {
										memblock = lineRight.substr(pos, tableObjectRight.columnLength[right]);
										if (temp == memblock) {
											insertTempData = getJoinData(selectColumnNames, tableObjectLeft, tableObjectRight, lineNumberLeft, lineNumberRight);
											if (insertTempData.size() == selectColumnNames.size()) {
												insertStringQuery = helper::insertStringQuery(aliasTableName, insertTempData);
												insertQuery = insertTempTableQuery(aliasTableName, insertTempData);
												tempNumRows = parseInsertQuery(insertQuery, sysTableTemp, insertStringQuery, *tableObject1);
											}
											//evaluate = selectEvaluateJoin();
										}
									}
									pos1 += tableObjectRight.columnLength[right];
								}
						}
						//pos = 0;
					}
					myfile1.clear();
					myfile1.seekg(0, ios::beg);
					pos += tableObjectLeft.columnLength[i];
				}
		}
	}
	else {
		cout << "unable to open file";
	}
	//createtemptable();
	myfile.close();
	myfile1.close();
	if (evaluate.size() == 0) {
		evaluate.push_back("null");
		return evaluate;
	}
	else
		return evaluate;
}

vector <string> helper :: getJoinData(vector <string> selectColumnNames, table tableObjectLeft, table tableObjectRight, int lineNumberLeft, int lineNumberRight) {

	vector <string> insertJoinData;
	vector <string> handledTableNameLeft;
	vector <string> handledTableNameRight;
	vector <string> handledDot;
	string fileNameLeft = tableObjectLeft.objTableName + ".tbl";
	string fileNameRight = tableObjectRight.objTableName + ".tbl";
	
	bool* flagLeft;
	int sizeLeft = tableObjectLeft.columnName.size();
	flagLeft = new bool[sizeLeft];
	
	bool* flagRight;
	int sizeRight = tableObjectRight.columnName.size();
	flagRight = new bool[sizeRight];

	string memblock;
	string temp;
	int pos = 0;
	int pos1 = 0;
	string lineLeft;
	string lineRight;
	ifstream myfile(fileNameLeft);
	ifstream myfile1(fileNameRight);
	table tableObject;
	int left = 0;
	int right = 0;

	int lineNoLeft = 0;
	int lineNoRight = 0;

	for (int i = 0; i < selectColumnNames.size(); i++) {
		//string col = columnName[i];
		if (selectColumnNames[i].find(".") != string::npos){
			handledDot = handleDotOperator(selectColumnNames[i]);
			if (handledDot[0] == tableObjectLeft.objTableName)
				handledTableNameLeft.push_back(handledDot[1]);
			else handledTableNameRight.push_back(handledDot[1]);
		}
		else if (selectColumnNames[i] == tableObjectLeft.columnName[left]) {
			left++;
			handledTableNameLeft.push_back(selectColumnNames[i]);
		}	

		else if (selectColumnNames[i] == tableObjectRight.columnName[right]) {
			right++;
			handledTableNameRight.push_back(selectColumnNames[i]);
		}
	}
	


	int j = 0;

	//Setting the flag values for table 1
	for (int i = 0; i < sizeLeft; i++) {
		flagLeft[i] = 0;
	}

	while (j != handledTableNameLeft.size())  {
		for (int k = 0; k < tableObjectLeft.columnName.size(); k++) {
			if (handledTableNameLeft[j] == tableObjectLeft.columnName[k]) {
				flagLeft[k] = 1;
				j++;
				break;
			}
		}
	}
	
	for (int i = 0; i < sizeLeft; i++) {
		cout << flagLeft[i];
	}

	//Setting the flag value for table 2

	j = 0;
	for (int i = 0; i < sizeRight; i++) {
		flagRight[i] = 0;
	}

	while (j != handledTableNameRight.size())  {
		for (int k = 0; k < tableObjectRight.columnName.size(); k++) {
			if (handledTableNameRight[j] == tableObjectRight.columnName[k]) {
				flagRight[k] = 1;
				j++;
				break;
			}
		}
	}

	for (int i = 0; i < sizeRight; i++) {
		cout << flagRight[i];
	}

	left = 0;
	right = 0;
	
	if (myfile.is_open() && myfile1.is_open()) {
		for (int i = 0; i < selectColumnNames.size(); i++) {
			myfile1.clear();
			myfile1.seekg(0, ios::beg);
			myfile.clear();
			myfile.seekg(0, ios::beg);
			lineNoLeft = 0;
			lineNoRight = 0;
			//string col = columnName[i];
			if (selectColumnNames[i].find(".") != string::npos){
				handledDot = handleDotOperator(selectColumnNames[i]);
				if (handledDot[0] == tableObjectLeft.objTableName) {
					while (getline(myfile, lineLeft)) {
						pos = 0;
						//linenumber++;
						lineNoLeft++;
						if (lineNoLeft == lineNumberLeft) {
							for (int trial = 0; trial < tableObjectLeft.columnName.size(); trial++) {
								if (handledDot[1] == tableObjectLeft.columnName[trial]) {
									if (flagLeft[trial] == 1) {
										memblock = lineLeft.substr(pos, tableObjectLeft.columnLength[trial]);
										insertJoinData.push_back(memblock);
									}
								}
								pos += tableObjectLeft.columnLength[trial];
							}
						}
					}
				}
				else {
					while (getline(myfile1, lineRight)) {
						pos1 = 0;
						//linenumber++;
						lineNoRight++;
						if (lineNoRight == lineNumberRight) {
							for (int trialRight = 0; trialRight < tableObjectRight.columnName.size(); trialRight++) {
								if (handledDot[1] == tableObjectRight.columnName[trialRight]) {
									if (flagRight[trialRight] == 1) {
										memblock = lineRight.substr(pos1, tableObjectRight.columnLength[trialRight]);
										insertJoinData.push_back(memblock);
									}
								}
								pos1 += tableObjectRight.columnLength[trialRight];
							}
						}
					}
				}
			}
			else if (selectColumnNames[i] == tableObjectLeft.columnName[left]) {
				left++;
				while (getline(myfile, lineLeft)) {
					pos = 0;
					//linenumber++;
					lineNoLeft++;
					if (lineNoLeft == lineNumberLeft) {
						for (int trial = 0; trial < tableObjectLeft.columnName.size(); trial++) {
							if (handledDot[1] == tableObjectLeft.columnName[trial]) {
								if (flagLeft[trial] == 1) {
									memblock = lineLeft.substr(pos, tableObjectLeft.columnLength[trial]);
									insertJoinData.push_back(memblock);
								}
							}
							pos += tableObjectLeft.columnLength[trial];
						}
					}
				}
			}

			else if (selectColumnNames[i] == tableObjectRight.columnName[right]) {
				right++;
				while (getline(myfile1, lineRight)) {
					pos1 = 0;
					//linenumber++;
					lineNoRight++;
					if (lineNoRight == lineNumberRight) {
						for (int trialRight = 0; trialRight < tableObjectRight.columnName.size(); trialRight++) {
							if (handledDot[1] == tableObjectRight.columnName[trialRight]) {
								if (flagRight[trialRight] == 1) {
									memblock = lineRight.substr(pos1, tableObjectRight.columnLength[trialRight]);
									insertJoinData.push_back(memblock);
								}
							}
							pos1 += tableObjectRight.columnLength[trialRight];
						}
					}
				}
			}
		}
	}
	else {
		cout << "unable to open file";
	}
	
	myfile.close();
	myfile1.close();

	return insertJoinData;
}



vector <string> helper::handleDotOperator(string columns) {

	int found;
	string column;
	string tableName;
	found = columns.find(".");
	string app;
	vector <string> combine;

	for (int i = 0; i < columns.size(); i++) {
		if (i < found) {
			app = columns[i];
			tableName.append(app);
		}
		else if (i > found) {
			app = columns[i];
			column.append(app);
		}
	}

	combine.push_back(tableName);
	combine.push_back(column);

	return combine;
}
string helper::getJoincondition(string query) {

	string joinCondition;
	int search;
	vector <string> itemizedQuery1 = getItemizedQuery(query);
	for (int i = 0; i < itemizedQuery1.size(); i++) {
		if (itemizedQuery1[i] == "JOIN") {
			for (search = i; search < itemizedQuery1.size(); search++) {
				if (itemizedQuery1[search] == "ON") {
					goto outer;
				}
			}
		}
	}

	outer: for (int j = search + 1; j < itemizedQuery1.size(); j++) {
		joinCondition.append(itemizedQuery1[j]);
	}

	return joinCondition;
}

vector <string> helper::parseSelectWhere(string selectTableName, vector <string> selectColumnNames, std::map<string, table> myMap, vector <string> itemizedQuery, string condition, string tempQuery, string aliasTableName, bool nested) {


	string whereColumn;
	string whereValue;
	int found;
	string app;
	bool check;
	vector <string> evaluate;

	found = condition.find("=");
	for (int i = 0; i < condition.size(); i++) {
		if (i < found) {
			app = condition[i];
			whereColumn.append(app);
		} else if (i > found) {
			app = condition[i];
			whereValue.append(app);
		}
	}
	//check = checkColumnFormat(selectTableName, myMap, whereValue, tempQuery);
	
	evaluate = evaluateSelectWhere(selectTableName, selectColumnNames, myMap, itemizedQuery, whereColumn, whereValue, aliasTableName, nested);

	return evaluate;
}

vector <string> helper::evaluateSelectWhere(string tableName, vector <string> selectColumnNames, std::map<string, table> myMap, vector <string> itemizedQuery, string whereColumn, string whereValue, string aliasTableName, bool nested) {

	streampos size;
	string memblock;
	string filename;
	filename = tableName + ".tbl";
	vector <bool> flag;
	table tableObject;
	std::map<string, table>::iterator it;
	string compareName;
	int rowSize = 0;
	string check;
	int l;
	string temp = whereValue;
	int lineNumber = 0;
	vector <string> evaluate;
	int numRows;

	tableObject = getTableObject(myMap, tableName);
	numRows = tableObject.fetchNumRows();
	//Check for columns in the select query
	for (int i = 0; i < tableObject.columnLength.size(); i++) {
		rowSize += tableObject.columnLength[i];
	}

	int j = 0;
	for (int k = 0; k < tableObject.columnName.size(); k++) {
		if (j != selectColumnNames.size())  {
			if (whereColumn == tableObject.columnName[k]) {
				flag.push_back(1);
				j++;
			}
			else flag.push_back(0);
		}
		else {
			flag.push_back(0);
		}
	}

	cout << "Flag value" << endl;
	for (int i = 0; i < flag.size(); i++) {
		cout << "flag "<< i << " : " << flag[i] << endl;
	}
	int pos = 0;
	//Open the file

	string line;
	ifstream myfile(filename);
	if (myfile.is_open())
	{
		while (getline(myfile, line)) {
			pos = 0;
			lineNumber++;
			//	pos = j * rowSize;
				//lineNumber = j *rowSize;
				for (int i = 0; i < flag.size(); i++) {
					if (flag[i] == 1) {
						memblock = line.substr(pos, tableObject.columnLength[i]);
						for (l = 0; l < tableObject.columnName.size(); l++) {
							if (whereColumn == tableObject.columnName[l])
								break;
						}

						if (tableObject.columnType[l] == "INT" && temp.length() != tableObject.columnLength[l]) {
							temp = (bitset<4>(stoi(temp))).to_string();
						}
						else if (tableObject.columnType[l] == "CHAR" && temp.length() != tableObject.columnLength[l]){
							temp.insert(temp.begin(), tableObject.columnLength[l] - temp.length(), '#');
							//temp = whereValue;
						}
						
						if (temp == memblock) {
							evaluate = selectEvaluate(lineNumber, tableName, selectColumnNames, myMap, itemizedQuery, numRows, aliasTableName, nested);
						}
					}
					pos += tableObject.columnLength[i];
				}
				//pos = 0;
		}
	}
	else {
		cout << "Unable to open file";
		
	}
	//createTempTable();
	myfile.close();

	if (evaluate.size() == 0) {
		evaluate.push_back("null");
		return evaluate;
	}
	else 
		return evaluate;
}


vector <string> helper::selectEvaluate(int lineNumber, string tableName, vector <string> selectColumnNames, std::map<string, table> myMap, vector <string> itemizedQuery, int numRows, string aliasTableName, bool nested) {

	streampos size;
	string memblock;
	string filename;
	filename = tableName + ".tbl";
	vector <bool> flag1;
	table tableObject;
	std::map<string, table>::iterator it;
	string compareName;
	int rowSize = 0;
	int checkLine = 0;
	int pos = 0;
	vector <string> evaluate;
	int lineNo = 0;

	vector <string> insertTempData;
	vector <string> insertQuery;
	int tempNumRows;
	table* tableObject1 = NULL;
	string insertStringQuery;

	tableObject = getTableObject(myMap, tableName);

	for (it = sysTableTemp.begin(); it != sysTableTemp.end(); ++it) {
		compareName = it->first;
		if (aliasTableName == compareName) {
			tableObject1 = &it->second;
			break;
		}
	}

	//Check for columns in the select query
	for (int i = 0; i < tableObject.columnLength.size(); i++) {
		rowSize += tableObject.columnLength[i];
	}

	int j = 0;
	for (int k = 0; k < tableObject.columnName.size(); k++) {
		if (j != selectColumnNames.size())  {
			if (selectColumnNames[j] == tableObject.columnName[k]) {
				flag1.push_back(1);
				j++;
			}
			else flag1.push_back(0);
		}
		else {
			flag1.push_back(0);
		}
	}
	
	/*for (int i = 0; i < flag1.size(); i++) {
		cout << "i: " << flag1[i] << endl;
	*/
	int numExec = 0;
	string line;
	ifstream myfile(filename);

	
	if (myfile.is_open())
	{
		while (getline(myfile, line)) {
			lineNo++;
			if (lineNo == lineNumber || lineNumber == -1) {
				//do {
					for (int i = 0; i < flag1.size(); i++) {
						if (flag1[i] == 1) {
							memblock = line.substr(pos, tableObject.columnLength[i]);
							insertTempData.push_back(memblock);
							evaluate.push_back(memblock);
							cout << " " << memblock << endl;   //dump to output file and formatting remaining. fetches correct information
							if (nested && insertTempData.size() == selectColumnNames.size()) {
								insertStringQuery = helper::insertStringQuery(aliasTableName, insertTempData);
								insertQuery = insertTempTableQuery(aliasTableName, insertTempData);
								tempNumRows = parseInsertQuery(insertQuery, sysTableTemp, insertStringQuery, *tableObject1);
							}
						}
						pos += tableObject.columnLength[i];
					}
					if (lineNumber == -1) {
						numExec++;
					}
				//} while (numExec < numRows && numExec != 0);
			}
			pos = 0;
			//for (int j = 0; j < numRows; j++) 
		}
	}
	else {
		cout << "Unable to open file";
		evaluate.push_back("null");
		return evaluate;
	}

	//createTempTable();
	myfile.close();
	return evaluate;
}


void helper::updateSysTable(table tempObject) {

	sysTableTemp.insert(std::pair<std::string, table>(tempObject.objTableName, tempObject));
	return;
}


vector <string> helper::insertTempTableQuery(string aliasTableName, vector <string> insertTempData) {

	vector <string> createQuery;
	string check = isTableExists(aliasTableName, sysTableTemp);

	if (check != "null") {
		createQuery.push_back("INSERT");
		createQuery.push_back("INTO");
		createQuery.push_back(aliasTableName);
		createQuery.push_back("VALUES");
		for (int i = 0; i < insertTempData.size(); i++){
			createQuery.push_back(insertTempData.at(i));
		}

		return createQuery;
	}
}

string helper::insertStringQuery(string aliasTableName, vector <string> insertTempData) {

	string createQuery;
	string check = isTableExists(aliasTableName, sysTableTemp);
	table tableObject = getTableObject(sysTableTemp, aliasTableName);

	if (check != "null") {
		createQuery.append("INSERT ");
		createQuery.append("INTO ");
		createQuery.append(aliasTableName);
		createQuery.append(" ");
		createQuery.append("VALUES (");
		
		for (int i = 0; i < insertTempData.size(); i++) {
			if (tableObject.columnType.at(i) == "CHAR") {
				createQuery.append("'");
				createQuery.append(insertTempData[i]);
				createQuery.append("'");
			}
			else {
				createQuery.append(insertTempData[i]);
			}
			if (i < insertTempData.size() - 1)
				createQuery.append(",");
		}

		createQuery.append(");");

		return createQuery;
	}
}

void helper :: deleteTempTableFiles() {

	string fileName;
	std::map<string, table>::iterator it;
	for (it = sysTableTemp.begin(); it != sysTableTemp.end(); ++it) {
		fileName = it->first;
		fileName.append(".tbl");
		const char *cstr = fileName.c_str();
		if (remove(cstr) != 0)
			perror("Error deleting file");
		else
			puts("File successfully deleted");
	}
}


void helper::handleShowTable(vector <string> itemizedQuery, std::map<string, table> myMap) {

	string tableName = itemizedQuery[2];
	table tableObject = getTableObject(myMap, tableName);
	cout << tableObject.objTableName << endl << endl;
	for (int i = 0; i < tableObject.columnName.size(); i++) {
		cout << tableObject.columnName.at(i) << "\t";
	}
}
