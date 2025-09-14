#include <iostream>
#include <sstream>
#include <fstream>

using namespace std;

struct Structure {
    string name;
    string colonms;
    Structure* next;

    Structure() {
        name = "";
        colonms = "";
        next = nullptr;
    }

    void PushBack(Structure* new_data) {
        if(name == "" || colonms == "") {
            name = new_data->name;
            colonms = new_data->colonms;
            return;
        }

        if (next == nullptr) {
            next = new_data;
            return;
        }
        Structure* runner = next;
        while (runner->next != nullptr) {
            runner = runner->next;
        }
        runner->next = new_data;
        return;
    }
};

struct Schema {
    string name;
    int tuples_limit;
    Structure* tables;
};

struct ColonNums {
    int num;
    ColonNums* next = nullptr;
};

struct SelTabs_node {
    string tabl_name;
    ColonNums* colons_head = nullptr;
    SelTabs_node* next = nullptr;
    string table_data;

    void PushNum(int new_num) {
        if (colons_head == nullptr) {
            colons_head = new ColonNums{new_num, nullptr};
            return;
        }
        ColonNums* runner = colons_head;
        while(runner->next != nullptr) {
            runner = runner->next;
        }
        runner->next = new ColonNums{new_num, nullptr};
        return;
    }
};

struct SelectedTables {
    SelTabs_node* head = nullptr;

    void PushTabl(string tabl_name) {
        if (head == nullptr) {
            head = new SelTabs_node{tabl_name, nullptr, nullptr, ""};
            return;
        }
        SelTabs_node* runner = head;
        while (runner->next != nullptr){
            runner = runner->next;
        }
        runner->next = new SelTabs_node{tabl_name, nullptr, nullptr};
    }

    int GetColonsNum(string colon, string filepath) {
        ifstream read_names(filepath + "/1.csv");
        string colon_names = "";
        getline(read_names, colon_names, '\n');
        int colon_num = 0;
        istringstream colon_geter(colon_names);
        string word = "";
        while(getline(colon_geter, word, ',')) {
            if (word[word.size()-1] == ',') word.pop_back();
            if (colon == word) break;
            colon_num++;
        }
        return colon_num;
    }

    void GetColons(Structure* colons, string filepath) {
        SelTabs_node* runner = head;
        while (runner != nullptr){
            Structure* colon_runner = colons;
            while (colon_runner != nullptr) {
                if (colon_runner->name == runner->tabl_name) {
                    runner->PushNum(GetColonsNum(colon_runner->colonms, filepath + '/' + runner->tabl_name));
                }
                colon_runner = colon_runner->next;
            }
            runner = runner->next;
        }
    }
};


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

void FROM(Structure* selected_colons, string input_data, string schema_name, string tables_from, SelectedTables& sel_tabl) {
    string tabl = "";
    istringstream iss_tables(tables_from);
    while(iss_tables >> tabl) {
        if (tabl[tabl.size()-1] == ',') tabl.pop_back();
        sel_tabl.PushTabl(tabl);
    }
    sel_tabl.GetColons(selected_colons, schema_name);
    SelTabs_node* runner = sel_tabl.head;
    while (runner != nullptr) {
        runner->table_data = GetData(runner, schema_name);
        runner = runner->next;
    }
}

void Compare(string& table_data, int& colon_num, string& data_to_compare_right) {
    string line = "";
    istringstream line_iss(table_data);
    getline(line_iss, line, '\n');
    string new_text = line + '\n';
    while(getline(line_iss, line, '\n')){
        int colon_runner = 0;
        string word = "";
        istringstream word_iss(line);
        while(getline(word_iss, word, ',')){
            if (colon_runner == colon_num) {
                if (word == data_to_compare_right) {
                    new_text += line + '\n';
                    break;
                }
            }
            colon_runner++;
        }
    }
    new_text.pop_back();
    table_data = new_text;
}

void Compare(string& table_data_left, int& colon_num_left, string& table_data_right, int& colon_num_right){
    string line_left = "";
    string line_right = "";
    istringstream left_iss(table_data_left);
    istringstream right_iss(table_data_right);
    string new_text_left = "";
    string new_text_right = "";
    getline(left_iss, line_left, '\n');
    getline(right_iss, line_right, '\n');
}

SelTabs_node* findTablAndNum (SelectedTables& sel_tabls, Structure* data_to_compare, string& filepath, int& colon_num){
    SelTabs_node* runner = sel_tabls.head;
    while (runner != nullptr) {
        if(runner->tabl_name == data_to_compare->name) {
            break;
        }
    }
    if (runner == nullptr) {
        throw string("don't have that table");
    }
    colon_num = sel_tabls.GetColonsNum(data_to_compare->colonms, filepath + '/' + runner->tabl_name);
    return runner;
}

void Filter(SelectedTables& sel_tables, string& tabl_colon, string& aftersign_data, string& filepath) {
    Structure* data_to_compare_left = new Structure();
    data_to_compare_left->PushBack(ParseSelected(tabl_colon));
    int colon_num = 0;
    SelTabs_node* current_tabl = findTablAndNum(sel_tables, data_to_compare_left, filepath, colon_num);
    if (aftersign_data[0] == '\'') {
        int i = 1;
        string word = "";
        while(aftersign_data[i] != '\'') {
            word += aftersign_data[i];
            i++;
        }
        aftersign_data = word;
        Compare(current_tabl->table_data, colon_num, aftersign_data);
    } else {
        Structure* data_to_compare_right;
        data_to_compare_right->PushBack(ParseSelected(aftersign_data));
        int colon_num_right = 0;
        SelTabs_node* current_tabl_right = findTablAndNum(sel_tables, data_to_compare_right, filepath, colon_num_right);
        
    }
}

void WHERE(SelectedTables& sel_tabls, string input_data, string& filepath) {
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
    Filter(sel_tabls, tabl_colon, aftersign_data, filepath);
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
    FROM(selected_colons, input_data, schema_name, tables_from, sel_tabl);
    if (data == "WHERE") {
        WHERE(sel_tabl, input_data, schema_name);
    }
    SelTabs_node* runner = sel_tabl.head;
    while (runner != nullptr) {
        LeaveOnlySelected(runner);
        runner = runner->next;
    }
    PrintSelected(sel_tabl);
}

void EnteringCommand(string input_data, Schema sch) {
    istringstream read_command(input_data);
    string command_word = "";
    read_command >> command_word;
    try {
        if (command_word == "INSERT") INSERT(input_data, sch.tuples_limit, sch.name);
        if (command_word == "SELECT") SELECT(input_data, sch.name);
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
    cout << result << "\Enter your command: " << endl;
    //CreateFiles(sch);
    //INSERT("INSERT INTO таблица1 VALUES('1', '2', '3', '4')", sch.tuples_limit, sch.name);
    string command = "";
    while (command != "end") {
        try {
            getline(cin, command);
            EnteringCommand(command, sch);
        } catch (string& error) {
            cout << "ERROR: " << error << endl;
        }
    }

}