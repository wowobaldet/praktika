#include "SELECT.h"

Structure* ParseSelected(string tabl_colon) {
    Structure* SelectedData = new Structure;
    string name = "";
    int index = 0;
    for (index; tabl_colon[index] != '.'; index++) {
        if (index >= tabl_colon.size()) throw string("wrong enter");
        name += tabl_colon[index];
    }
    index++;
    SelectedData->name = name;
    name = "";
    for (index; index < tabl_colon.size(); index++) name += tabl_colon[index];
    if (name[name.size()-1] == ',') name.pop_back();
    SelectedData->colonms = name;
    return SelectedData;
}

string GetSelected(string& text, SelTabs_node* tables) {
    int number_runner = 0;
    istringstream text_iss(text);
    string word = "";
    string new_text = "";
    ColonNums* runner = tables->colons_head;
    while (runner != nullptr) {
        while (getline(text_iss, word, ',')) {
            if (number_runner == runner->num) {
                new_text += word + ',';
                break;
            }
            number_runner++;
        }
        number_runner++;
        runner = runner->next;
    }
    if(new_text[new_text.size()-1] == ',') new_text.pop_back(); 
    return new_text;
}

string GetData(SelTabs_node* tables, string& filepath) {
    int num_csv = 1;
    string lines = "";
    while(!(isEmpty(filepath + '/' + tables->tabl_name + '/' + to_string(num_csv) + ".csv"))) {
        ifstream read_data(filepath + '/' + tables->tabl_name + '/' + to_string(num_csv) + ".csv");
        string current_lines = "";
        getline(read_data, current_lines, '\0');
        if (num_csv > 1) {
            current_lines = '\n' + current_lines;
        }
        lines += current_lines;
        num_csv++;
    }
    return lines;
}

void LeaveOnlySelected(SelTabs_node* tables){
    string new_text = "";
    istringstream read_lines(tables->table_data);
    string selected_data = "";
    while(getline(read_lines, new_text)) {
        selected_data += GetSelected(new_text, tables) + '\n';
    }
    tables->table_data = selected_data;
}

void FROM(Structure* selected_colons, string schema_name, string tables_from, SelectedTables& sel_tabl) {
    string tabl = "";
    istringstream iss_tables(tables_from);
    while(iss_tables >> tabl) {
        if (tabl[tabl.size()-1] == ',') tabl.pop_back();
        sel_tabl.PushTabl(tabl);
    }
    if (selected_colons != nullptr) sel_tabl.GetColons(selected_colons, schema_name);
    SelTabs_node* runner = sel_tabl.head;
    while (runner != nullptr) {
        runner->table_data = GetData(runner, schema_name);
        runner = runner->next;
    }
}

void CrossJoin(string& text1, string text2) {
    istringstream txt1_iss(text1);
    istringstream txt2_iss(text2);
    string final_res = "";
    string txt1_cur = "";
    string txt2_cur = "";
    getline(txt1_iss, txt1_cur);
    getline(txt2_iss, txt2_cur);
    final_res += txt1_cur + ',' + txt2_cur + '\n';
    while (getline(txt1_iss, txt1_cur)) {
        istringstream txt2_iss_while(text2);
        getline(txt2_iss_while, txt2_cur);
        while (getline(txt2_iss_while, txt2_cur)) {
            final_res += txt1_cur + ',' + txt2_cur + '\n';
        }
    }
    final_res.pop_back();
    text1 = final_res;
}

string GetValue(string line, int colon_num){
    istringstream line_iss(line);
    string word = "";
    while (getline(line_iss, word, ',')) {
        if(colon_num <= 0) break;
        colon_num--;
    }
    return word;
}

void Compare(string& table_data, int& colon_num, string& data_to_compare_right) {
    string line = "";
    istringstream line_iss(table_data);
    getline(line_iss, line, '\n');
    string new_text = line + '\n';
    while(getline(line_iss, line, '\n')){
        if(GetValue(line, colon_num) == data_to_compare_right){
            new_text += line + '\n';
        }
    }
    new_text.pop_back();
    table_data = new_text;
}

void Compare(string& full_text, int colon_num_left, int colon_num_right){
    string line = "";
    istringstream text_iss(full_text);
    getline(text_iss, line, '\n');
    string new_text = line + '\n';
    while(getline(text_iss, line, '\n')){
        string word1 = GetValue(line, colon_num_left);
        string word2 = GetValue(line, colon_num_right);
        if(word1 == word2){
            new_text += line + '\n';
        }
    }
    full_text = new_text;
}

int findTablAndNum(string& full_text, string tabl_name, string colon_name){
    string line = "";
    istringstream text_iss(full_text);
    getline(text_iss, line);
    string word = "";
    istringstream line_iss(line);
    int num = 0;
    while(getline(line_iss, word, ',')) {
        if (word == tabl_name) {
            break;
        } 
        num++;
    }
    if (tabl_name == colon_name) return num;
    num++;
    while(getline(line_iss, word, ',')) {
        if (word == colon_name) return num;
        num++;
    }
    return num;
}

void Filter(string& full_text, string& tabl_colon, string& aftersign_data) {
    Structure* data_to_compare_left = new Structure;
    data_to_compare_left->PushBack(ParseSelected(tabl_colon));
    int colon_num  = findTablAndNum(full_text, data_to_compare_left->name + "_pk", data_to_compare_left->colonms);
    if (aftersign_data[0] == '\'') {
        int i = 1;
        string word = "";
        while(aftersign_data[i] != '\'') {
            word += aftersign_data[i];
            i++;
        }
        aftersign_data = word;
        Compare(full_text, colon_num, aftersign_data);
    } else {
        Structure* data_to_compare_right = new Structure;
        data_to_compare_right->PushBack(ParseSelected(aftersign_data));
        int colon_num_right = findTablAndNum(full_text, data_to_compare_right->name + "_pk", data_to_compare_right->colonms);
        Compare(full_text, colon_num, colon_num_right);
    }
}

void Connect(string& orig_text, string new_text){
    string connected_text = "";
    string line_or = "";
    istringstream iss_or(orig_text);
    while (getline(iss_or, line_or, '\n')){
        string line_new = "";
        istringstream iss_new(new_text);
        while(getline(iss_new, line_new, '\n')) {
            if(line_or != line_new) {
                connected_text += line_new + '\n';
            }
        }
    }
    orig_text += '\n' + connected_text;
}

void WHERE(string& full_text, string input_data) {
    istringstream iss(input_data);
    string word = "";
    while (word != "WHERE") iss >> word;
    string tabl_colon = "";
    string sign = "";
    string aftersign_data = "";
    iss >> tabl_colon;
    iss >> sign;
    iss >> aftersign_data;
    if (sign != "=") {
        throw string("not correct enter");
        return;
    }
    string full_text_copy = full_text;
    Filter(full_text, tabl_colon, aftersign_data);
    iss >> word;
    while (word == "AND" || word == "OR"){
        if (word == "AND") {
            tabl_colon = "";
            sign = "";
            aftersign_data = "";
            iss >> tabl_colon;
            iss >> sign;
            iss >> aftersign_data;
            Filter(full_text, tabl_colon, aftersign_data);
            if(!(iss >> word)) break;
        }
        if (word == "OR"){
            tabl_colon = "";
            sign = "";
            aftersign_data = "";
            iss >> tabl_colon;
            iss >> sign;
            iss >> aftersign_data;
            string new_text = full_text_copy;
            Filter(new_text, tabl_colon, aftersign_data);
            Connect(full_text, new_text);
            if(!(iss >> word)) break; 
        }
    }
}

void PrintFilteredSelected(string full_text, Structure* tables_colons){
    SelTabs_node* nums_array = new SelTabs_node;
    Structure* runner = tables_colons;
    while(runner != nullptr) {
        nums_array->PushNum(findTablAndNum(full_text, runner->name+"_pk", runner->colonms));
        runner = runner->next;
    }
    string result = "";
    string line = "";
    istringstream line_iss(full_text);
    while (getline(line_iss, line, '\n')){
        ColonNums* nums_runner = nums_array->colons_head;
        while (nums_runner != nullptr) {
            result += GetValue(line, nums_runner->num) + ',';
            nums_runner = nums_runner->next;
        }
        result.pop_back();
        result += '\n';
    }
    cout << result << endl;
}

void PrintSelected(SelectedTables& sel_tabls){
    string text = "";
    SelTabs_node* runner = sel_tabls.head;
    text = runner->table_data;
    runner = runner->next;
    while (runner != nullptr) {
        CrossJoin(text, runner->table_data);
        runner = runner->next;
    }
    cout << text << endl;
}

void SELECT(string input_data, string schema_name){
    string data = "";
    istringstream iss(input_data);
    iss >> data;
    string selected_data = "";
    Structure* selected_colons = new Structure;
    iss >> data;
    while (data != "FROM") {
        selected_colons->PushBack(ParseSelected(data));
        iss >> data;
    }
    SelectedTables sel_tabl;
    iss >> data;
    string tables_from = "";
    while (data != "WHERE") {
        tables_from += data + ' ';
        if (!(iss >> data)) break;
    }
    FROM(selected_colons, schema_name, tables_from, sel_tabl);
    if (data == "WHERE") {
        string text = "";
        SelTabs_node* runner = sel_tabl.head;
        text = runner->table_data;
        runner = runner->next;
        while (runner != nullptr) {
            CrossJoin(text, runner->table_data);
            runner = runner->next;
        }
        WHERE(text, input_data);
        PrintFilteredSelected(text, selected_colons);
        return;
    }
    SelTabs_node* runner = sel_tabl.head;
    while (runner != nullptr) {
        LeaveOnlySelected(runner);
        runner = runner->next;
    }
    PrintSelected(sel_tabl);
}