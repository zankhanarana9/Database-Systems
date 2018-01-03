

#include "table.h"
#include "helper.h"   
#include <map>
#include <stdio.h>

//typedef std::map<string, table> myMap;
string tempQuery;
int numRows;
table* tableObject;

int main(int argc, char* argv[]) {

	string input;
	string inputParsing = argv[1];
	size_t index = inputParsing.find("script=");
	if (index != std::string::npos) {
		input = inputParsing.substr(7);
	}

	vector <string> itemizedQuery;
	string sqlFile = input;
	std :: map<string, table> sysTable;
	string tableName = "";
	vector <string> sqlQuery;
	vector <table> storeObject;
	std::map<string, table>::iterator it;
	//cout << "Enter the file name: " << endl;
	//getline(cin, sqlFile);
	string compareName;
	int numObj = 0;

	sqlQuery = helper::sqlRead(sqlFile);
	for (int i = 0; i < sqlQuery.size(); i++) {
		cout <<sqlQuery[i] << endl;
	}

	int stringLCase = 0;
	for (int i = 0; i < sqlQuery.size(); i++) {
		for (int j = 0; j < sqlQuery[i].size(); j++) {
			if (sqlQuery[i].at(j) == '\'')
				stringLCase++;
			if (stringLCase % 2 == 0)
				sqlQuery[i].at(j) = toupper(sqlQuery[i].at(j));
		}

	}


	for (int i = 0; i < sqlQuery.size(); i++) {
		size_t prev = 0, pos;
		tempQuery = sqlQuery[i];
	

		while ((pos = sqlQuery[i].find_first_of(" ,;()'", prev)) != string::npos) { //while pos of delimiter != end of string
			if (pos > prev)
				itemizedQuery.push_back(sqlQuery[i].substr(prev, pos - prev));
			prev = pos + 1;
		}

		
		for (int j = 0; j < itemizedQuery.size(); j++) {
			cout << " " << itemizedQuery[j] << endl;
		} 
		if (itemizedQuery[0] == "CREATE") {
		/*	tableName = itemizedQuery[2];
			string check = helper::isTableExists(tableName, sysTable);
			if (check != "null") {
				
			} */
			numRows = 0;
			storeObject.push_back(table());
			storeObject[numObj] = helper::createObject(itemizedQuery, numRows);
			sysTable.insert(std::pair<std::string, table>(itemizedQuery[2], storeObject[numObj]));
			numObj++;
		}
		else if (itemizedQuery[0] == "DROP") {

			ifstream myfile("catalog.txt");
			if (myfile.is_open()) {
				myfile.close();
				helper::handleDropQuery(itemizedQuery);
				int result;
				char oldname[] = "writeFile.txt";
				char newname[] = "catalog.txt";
				result = rename(oldname, newname);
				if (result == 0)
					puts("File successfully renamed");
				else
					perror("Error renaming file");
			}
			else {
				cout << "Catalog file does not exist";
			}
			
		} else {
			cout << endl;
			if (itemizedQuery[0] == "INSERT") {
				tableName = itemizedQuery[2];
				for (it = sysTable.begin(); it != sysTable.end(); ++it) {
					compareName = it->first;
					if (tableName == compareName) {
						tableObject = &it->second;
						break;
					}
				}
				numRows = helper::parseInsertQuery(itemizedQuery, sysTable, tempQuery, *tableObject);

			}
			else if (itemizedQuery[0] == "SELECT")
				vector <string> evaluate = helper::parseSelectQuery(tempQuery, sysTable);
			else if (itemizedQuery[0] == "SHOW")
				helper::handleShowTable(itemizedQuery, sysTable);
		}
		itemizedQuery.clear();
		pos = 0;
		prev = 0;
	}
	helper::createCatalogFile(sysTable);
	table temp;
	for (it = sysTable.begin(); it != sysTable.end(); ++it) {
		temp = it->second;
		for (int i = 0; i< temp.columnType.size(); i++)
			std::cout << it->first << " => " << temp.columnType[i] << '\n';
	}
	helper::deleteTempTableFiles();
	return 0;
}