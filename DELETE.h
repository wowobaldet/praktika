#pragma once
#include "SELECT.h"

void InsertUpdatedData(string new_data, string filepath, int tuples_limit);

void replaceTable(string new_text, string filepath, int tuples_limit);

bool checkLine(string line_or, string del_text);

void CutOff(string& text_or, string del_text);

void DELETE(string insert_data, string filepath, int tuples_limit);