#include "DELETE.h"

void InsertUpdatedData(string new_data, string filepath, int tuples_limit){
    int order_num = 1;
    while (isOverLimit (filepath, tuples_limit, order_num)) order_num++;
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
    system(("rm -rf " + filepath + "/*.csv").c_str());
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
    string tabl_name = data;
    if (isLocked(filepath + '/' + tabl_name + '/' + tabl_name + "_lock.txt")) throw string("this table isn't free");
    LockorUnlockTable((filepath + '/' + tabl_name + '/' + tabl_name + "_lock.txt"), "LOCKED");
    SelectedTables del_tabl;
    FROM(nullptr, filepath, data, del_tabl);
    del_iss >> data;
    string copy_data = del_tabl.head->table_data;
    if (data == "WHERE") {
        WHERE(copy_data, insert_data);
    }
    CutOff(del_tabl.head->table_data, copy_data);
    replaceTable(del_tabl.head->table_data, filepath + "/" + del_tabl.head->tabl_name, tuples_limit);
    LockorUnlockTable((filepath + '/' + tabl_name + '/' + tabl_name + "_lock.txt"), "FREE");
}