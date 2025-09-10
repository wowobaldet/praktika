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

    int GetColonsNum(string colon, string all_colons) {
        int colon_num = 0;
        istringstream colon_geter(all_colons);
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
        ifstream read_names(filepath + "/" + head->tabl_name + "/1.csv");
        string colon_names = "";
        getline(read_names, colon_names, '\n');
        while (runner != nullptr){
            Structure* colon_runner = colons;
            while (colon_runner != nullptr) {
                if (colon_runner->name == runner->tabl_name) {
                    runner->PushNum(GetColonsNum(colon_runner->colonms, colon_names));
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
    while (text[index] != '\"' && text[index] != '\'') {
        word += text[index];
        index++;
    }
    index++;
    return word;
}

void DeleteLast(Structure* struct_original){
    Structure* struct_to_del = struct_original;
    while (struct_to_del->next->name != "") struct_to_del = struct_to_del->next;
    struct_to_del->next = nullptr;
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

string GetSelected(string& text, SelectedTables tables) {
    int number_runner = 0;
    istringstream text_iss(text);
    string word = "";
    string new_text = "";
    while(getline(text_iss, word, ',')) {
        ColonNums* runner = tables.head->colons_head;
        while (runner != nullptr) {
            if (runner->num == number_runner) {
                new_text += word + ',';
            }
            number_runner++;
            runner = runner->next;
        }
    }
    return new_text;
}

void PrintSelected(SelectedTables& tables, string& filepath){
    ifstream read_table(filepath + '/' + tables.head->tabl_name + "/1.csv");
    string new_text = "";
    while(getline(read_table, new_text)) {
        cout << GetSelected(new_text, tables) << endl;
    }
}

void FROM (Structure* selected_colons, string input_data, string schema_name, string tables_from, SelectedTables& sel_tabl) {
    string tabl = "";
    istringstream iss_tables(tables_from);
    while(iss_tables >> tabl) {
        if (tabl[tabl.size()-1] == ',') tabl.pop_back();
        sel_tabl.PushTabl(tabl);
    }
    sel_tabl.GetColons(selected_colons, schema_name);
    //PrintSelected(sel_tabl, schema_name);
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
    
}


int main() {
    string result = ReadingJson();
    string word = "";
    int index = 0;
    string words = "";
    Schema sch = FillSchema(result);
    //CreateFiles(sch);
    //INSERT("INSERT INTO таблица1 VALUES('1', '2', '3', '4')", sch.tuples_limit, sch.name);
    SELECT("SELECT таблица1.колонка4, таблица1.колонка1, таблица2.колонка2 FROM таблица1, таблица2", sch.name);

}