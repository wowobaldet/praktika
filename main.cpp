#include "DELETE.h"


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