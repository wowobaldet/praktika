#include "funcs.h"

void INSERT(string insert_data, int strings_limit, string schema_name){
    istringstream iss(insert_data);
    string data = "";
    iss >> data;
    iss >> data;
    string tabl = "";
    iss >> tabl;
    if (isLocked(schema_name+"/"+tabl+"/"+tabl+"_lock.txt")) throw string ("this table isn't free");
    LockorUnlockTable(schema_name+"/"+tabl+"/"+tabl+"_lock.txt", "LOCKED");
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
        LockorUnlockTable(schema_name+"/"+tabl+"/"+tabl+"_lock.txt", "FREE");
        return;
    };
    pk << new_data;
    pk.close();
    LockorUnlockTable(schema_name+"/"+tabl+"/"+tabl+"_lock.txt", "FREE");
    return;
}