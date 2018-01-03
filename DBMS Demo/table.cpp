#include "table.h"


string table::getTableName() {
	return objTableName;
}

void table::setTableName(string name) {
	objTableName = name;
}

void table::fetchColumnInformation(vector <string> query) {
	for (int i = 1; i < query.size(); i++) {
		if (query[i] == "KEY")
			primaryKey = query[i + 1];
		else if (query[i] == "INT") {
			columnLength.push_back(sizeof(int));
			columnType.push_back ("INT");
			columnName.push_back(query[i - 1]);
		}
		else if (query[i] == "CHAR") {
			columnLength.push_back(atoi(query.at(i + 1).c_str()));
			columnType.push_back ("CHAR");
			columnName.push_back(query[i - 1]);
		}
	}
}


void table::setNumRows(int num) {
	numRows = num;
}

int table::fetchNumRows() {
	return numRows;
}

void table::updateNumRows(int num) {
	numRows = num;
}

int table::getRowSize() {

	rowSize = 0;
	for (int i = 0; i < columnLength.size(); i++) {
		rowSize += columnLength[i];
	}

	return rowSize;
}

table::table(void){ }