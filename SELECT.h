#pragma once
#include "funcs.h"

Structure* ParseSelected(string tabl_colon);

string GetSelected(string& text, SelTabs_node* tables);

string GetData(SelTabs_node* tables, string& filepath);

void LeaveOnlySelected(SelTabs_node* tables);

void FROM(Structure* selected_colons, string schema_name, string tables_from, SelectedTables& sel_tabl);

void CrossJoin(string& text1, string text2);

string GetValue(string line, int colon_num);

void Compare(string& table_data, int& colon_num, string& data_to_compare_right);

void Compare(string& full_text, int colon_num_left, int colon_num_right);

int findTablAndNum(string& full_text, string tabl_name, string colon_name);

void Filter(string& full_text, string& tabl_colon, string& aftersign_data);

void Connect(string& orig_text, string new_text);

void WHERE(string& full_text, string input_data);

void PrintFilteredSelected(string full_text, Structure* tables_colons);

void PrintSelected(SelectedTables& sel_tabls);

void SELECT(string input_data, string schema_name);