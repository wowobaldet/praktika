#include "structs.h"



string ReadingJson(){
    ifstream fileReader("schema.json");
    string jsonText = "";
    getline(fileReader, jsonText, '\0');
    fileReader.close();
    return jsonText;
}

string ParseText(string text,  int& index){
    while (text[index] != '\"' && text[index] != '\'') {
        if (index == text.size()){
            return "-0";
        } else if (text[index] == '}') {
            index++;
            return "}";
        } else if (text[index] == ',') {
            index++;
            return ",";
        } else if (text[index] == ']') {
            index++;
            return "]";
        } 
        index++;
    }
    index++;
    string word = "";
    while (index != text.size() && text[index] != '\"' && text[index] != '\'') {
        word += text[index];
        index++;
    }
    if (index >= text.size()) throw string("not correct enter");
    index++;
    return word;
}

Structure* FillStructure(string schema, int& index){
    string result = "";
    Structure* newStruct = new Structure;
    Structure* StructRunner = newStruct;
    while (result != "}") {
        result = ParseText(schema, index);
        StructRunner->name = result;
        result = "";
        StructRunner->colonms += StructRunner->name + "_pk,";
        while (true) {
            result = ParseText(schema, index);
            if (result == "]") break;
            StructRunner->colonms += result;
        }
        result = ParseText(schema, index);
        if (result == "}") break;
        StructRunner->next = new Structure;
        StructRunner = StructRunner->next;
        
    }
    return newStruct;
};

Schema FillSchema(string schema) {
    Schema newSchema;
    int index = 0;
    string result = "";
    while (result != "-0"){
        result = ParseText(schema, index);
        if (result == "name")  {
            newSchema.name = ParseText(schema, index);
        }
        if (result == "tuples_limit") {
            string newTuples_Limit = "";
            while (!isdigit(schema[index])) index++;
            while (schema[index] != ','){
                newTuples_Limit += schema[index];
                index++;
            }
            newSchema.tuples_limit = stoi(newTuples_Limit);
            index++;
        }
        if (result == "structure") {
            newSchema.tables = FillStructure(schema, index);
        }
    }
    return newSchema;
}


void CreateFiles(Schema database_schema) {
    system(("rm -rf \"" + database_schema.name + '\"').c_str());
    system(("mkdir \"" + database_schema.name + "\"").c_str());
    Structure* runnerStruct = database_schema.tables;
    while(runnerStruct != nullptr){
        system(("cd \"" + database_schema.name + "\"" + "&& mkdir \"" + runnerStruct->name + "\"").c_str());
        ofstream fileWriter(database_schema.name + "/" + runnerStruct->name + "/1.csv", ios_base::out);
        fileWriter << runnerStruct->colonms;
        fileWriter.close();
        ofstream makePkFile(database_schema.name + "/" + runnerStruct->name + "/" + runnerStruct->name + "_pk_sequence.txt", ios_base::out);
        makePkFile << "0";
        makePkFile.close();
        runnerStruct = runnerStruct->next;
    }
}

bool isOverLimit(string filepath, int limit, int& order_num) {
    ifstream fileReader(filepath + '/' + to_string(order_num) + ".csv");
    string line = "";
    while (getline(fileReader,line)) limit--;
    fileReader.close();
    if (limit <= 0) {
        return true;
    }
    return false;
}

bool isEmpty(string filepath){
    ifstream fileReader(filepath);
    string line_checker = "";
    if (getline(fileReader, line_checker)) {
        fileReader.close();
        return false;
    }
    return true;
}

void INSERT(string insert_data, int strings_limit, string schema_name){
    istringstream iss(insert_data);
    string data = "";
    iss >> data;
    iss >> data;
    string tabl = "";
    iss >> tabl;
    string filepath = (schema_name + "/" + tabl);
    fstream pk;
    pk.open(filepath + "/" + tabl + "_pk_sequence.txt", ios::in);
    pk >> data;
    int pk_int = stoi(data);
    pk.close();
    pk.open(filepath + "/" + tabl + "_pk_sequence.txt", ios::out);
    pk_int += 1;
    pk << pk_int;
    pk.close();

    string new_data = to_string(pk_int) + ',';
    int index = 0;
    data = "";
    while (data != "-0") {
        new_data += data;
        data = ParseText(insert_data, index);
    }
    int order_num = 1;
    while (isOverLimit (filepath, strings_limit, order_num)) order_num++;
    pk.open(filepath + "/" + to_string(order_num) + ".csv", ios::app);
    if (!isEmpty(filepath + "/" + to_string(order_num) + ".csv")) {
        pk << '\n' + new_data;
        pk.close();
        return;
    };
    pk << new_data;
    pk.close();
    return;
}

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

void InsertUpdatedData(string new_data, string filepath, int tuples_limit){
    int order_num = 1;
    while (isOverLimit (filepath, tuples_limit, order_num)) order_num++;
    cout << new_data << endl;
    ofstream file_writer(filepath + "/" + to_string(order_num) + ".csv", ios_base::app);
    if (!isEmpty(filepath + "/" + to_string(order_num) + ".csv")) {
        file_writer << '\n' + new_data;
        file_writer.close();
        return;
    };
    file_writer << new_data;
    file_writer.close();
}


void replaceTable(string new_text, string filepath, int tuples_limit) {
    istringstream text_iss(new_text);
    string new_data = "";
    getline(text_iss, new_data, '\n');
    ofstream put_names(filepath+"/1.csv", ios::out);
    put_names << new_data;
    put_names.close();
    while(getline(text_iss, new_data, '\n')){
        InsertUpdatedData(new_data, filepath, tuples_limit);
    }

}

bool checkLine(string line_or, string del_text){
    string line_del = "";
    istringstream del_iss(del_text);
    bool isForADel = 0;
    while(getline(del_iss, line_del, '\n')) {
        if (line_or == line_del) {
            isForADel = 1;
            break;
        }
    }
    return isForADel;
}

void CutOff(string& text_or, string del_text){
    string line_or = "";;
    istringstream or_iss(text_or);
    string new_text = "";
    getline(or_iss, line_or, '\n');
    new_text += line_or + '\n';
    while (getline(or_iss, line_or, '\n')) {
        if(!checkLine(line_or, del_text)){
            new_text += line_or + '\n';
        }
    }
    text_or = new_text;
}

void DELETE(string insert_data, string filepath, int tuples_limit) {
    istringstream del_iss(insert_data);
    string data = "";
    del_iss >> data;
    del_iss >> data;
    del_iss >> data;
    SelectedTables del_tabl;
    FROM(nullptr, filepath, data, del_tabl);
    del_iss >> data;
    string copy_data = del_tabl.head->table_data;
    if (data == "WHERE") {
        WHERE(copy_data, insert_data);
    }
    CutOff(del_tabl.head->table_data, copy_data);
    replaceTable(del_tabl.head->table_data, filepath + "/" + del_tabl.head->tabl_name, tuples_limit);
}

void EnteringCommand(string input_data, Schema sch) {
    istringstream read_command(input_data);
    string command_word = "";
    read_command >> command_word;
    try {
        if (command_word == "INSERT") INSERT(input_data, sch.tuples_limit, sch.name);
        if (command_word == "SELECT") SELECT(input_data, sch.name);
        if (command_word == "DELETE") DELETE(input_data, sch.name, sch.tuples_limit);
    } catch (string& error) {
        throw string(error);
    }
}


int main() {
    string result = ReadingJson();
    string word = "";
    int index = 0;
    string words = "";
    Schema sch = FillSchema(result);
    string choix = "";
    cout << result  <<"\nIf you want to update your schema enter 1" << endl;
    cin >> choix;
    if (choix == "1") CreateFiles(sch);
    cout << "Enter your command: " << endl;
    string command = "";
    while (command != "quit") {
        try {
            getline(cin, command);
            EnteringCommand(command, sch);
        } catch (string& error) {
            cout << "ERROR: " << error << endl;
        }
    }

}