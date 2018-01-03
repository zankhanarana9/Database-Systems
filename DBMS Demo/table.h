
#pragma once
#include <string.h>
#include <string>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <fstream>
#include <map>

using namespace std;

class table {
public:
	string objTableName;
	vector <string> columnName;
	vector <string> columnType;
	vector <int> columnLength;
	string primaryKey;
	int numRows;
	int rowSize;
public:
	table();
	string getTableName();
	void setTableName(string name);
	void fetchColumnInformation(vector <string> query);
	void setNumRows(int num);
	int fetchNumRows();
	void updateNumRows(int num);
	int getRowSize();
};