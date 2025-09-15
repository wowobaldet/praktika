#pragma once
#include "structs.h"

string ReadingJson();

string ParseText(string text,  int& index);

Structure* FillStructure(string schema, int& index);

Schema FillSchema(string schema);

void CreateFiles(Schema database_schema);

bool isOverLimit(string filepath, int limit, int& order_num);

bool isEmpty(string filepath);

bool isLocked(string filepath);

void LockorUnlockTable(string filepath, string action);

void INSERT(string insert_data, int strings_limit, string schema_name);

