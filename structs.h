#pragma once
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