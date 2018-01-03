
#pragma once
#include <string>
#include <map>
#include <vector>
#include <iostream>
#include <iterator>
#include <fstream>
#include <sstream>
#include <string.h>
#include <algorithm>
#include <cmath>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <bitset>

#include "table.h"

using namespace std;

//typedef std::map<string, table> myMap1;
class helper {
private :
   static std::map<string, table> sysTableTemp;

public:
	static table createObject(vector <string> query, int numRows);
	static int parseInsertQuery(vector <string> query, std::map<string, table> myMap, string tempQuery, table &tableObject);
	static string isTableExists(string name, std::map<string, table> myMap);
	static vector <string> sqlRead(string fileName);
	static bool checkColumnFormat(string tableName,std::map<string, table> myMap, vector <string> query, string tempQuery);
	static vector <string> insertData(vector <string> query);
	static void createRow(table tableName, vector <string> insertData, int numRows);
	static void createCatalogFile(std::map<string, table> myMap);
	static bool ifFileExists(string tableName, int numRows);
	static bool checkPrimaryKey(string tableName, std::map<string, table> myMap, vector <string> insertData, int numRows);
	static void createBinaryFile(string tableName,std::map<string, table> myMap, vector <string> insertData);
	static table createTempTableObject(vector <string> query, int numRows);
	static vector <string> parseSelectQuery(string tempQuery, std::map<string, table> myMap);
	static vector <string> parseSelect(string query, std::map<string, table> myMap, string aliasTableName, bool nested);
	static vector <string> parseSelectWhere(string selectTableName, vector <string> selectColumnNames, std::map<string, table> myMap, vector <string> itemizedQuery, string condition, string tempQuery, string aliasTableName, bool nested);
	static vector <string> evaluateSelectWhere(string selectTableName, vector <string> selectColumnNames, std::map<string, table> myMap, vector <string> itemizedQuery, string whereCoulumn, string whereValue, string aliasTableName, bool nested);
	static vector <string> selectEvaluate(int lineNumber, string selectTableName, vector <string> selectColumnNames, std::map<string, table> myMap, vector <string> itemizedQuery, int numRows, string aliasTableName, bool nested);
	static void parseInsertSelect(vector <string> query, std::map<string, table> myMap, string tempQuery);
	static vector <string> createItemizedQuery(string tempQuery, vector <string> insertQuery, vector <string> data);
	static string createStringQuery(std::map<string, table> myMap, vector <string> insertQuery, vector <string> data, string tableName);
	static bool checkColumns(vector <string> selectColumns, std::map<string, table> myMap, string insertTableName, string selectTableName);
	static vector <string> getSelectColumns(vector <string> itemizedQuery1);
	static table getTableObject(std::map<string, table> myMap, string tableName);
	static vector <string> getItemizedQuery(string tempQuery);
	static void handleDropQuery(vector <string> itemizedQuery);
	static vector <string> createTempTableQuery(string aliasTableName, std::map<string, table> myMap, vector <string> columnName, vector <string> tableName);
	static void updateSysTable(table tempTable);
	static vector <string> insertTempTableQuery(string aliasTableName, vector <string> insertTempData);
	static string insertStringQuery(string aliasTableName, vector <string> insertTempData);
	static void deleteTempTableFiles();
	static string getJoincondition(string query);
	static void parseSelectJoin(vector <string> createTableName, string joinCondtion, string query, std::map<string, table> myMap, string aliasTableName);
	static vector <string> handleDotOperator(string columns);
	static vector <string> evaluateJoin(vector <string> columnLeftVector, vector <string> columnRightVector, string aliasTableName, std::map<string, table> myMap, vector <string> selectColumnNames);
	static vector <string> getJoinData(vector <string> selectColumnNames, table tableObjectLeft, table tableObjectRight, int lineNumberLeft, int lineNumberRight);
	static void handleShowTable(vector <string> itemizedQuery, std::map<string, table> myMap);
};