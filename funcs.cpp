#include "funcs.h"

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
        ofstream makeLockFile(database_schema.name + "/" + runnerStruct->name + "/" + runnerStruct->name + "_lock.txt", ios_base::out);
        makeLockFile << "FREE";
        makeLockFile.close();
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

bool isLocked(string filepath) {
    ifstream read_lock(filepath, ios_base::in);
    string result = "";
    read_lock >> result;
    if (result == "FREE") return 0;
    return 1;
}

void LockorUnlockTable(string filepath, string action) {
    ofstream make_lock(filepath, ios_base::out);
    make_lock << action;
    make_lock.close();
}