#include <tbb/tbb.h>
#include <iostream>
#include <cmath>

using namespace std;

void test1(void){
    tbb::flow::graph g;
    tbb::flow::continue_node<tbb::flow::continue_msg>
            h(g, [](const tbb::flow::continue_msg &){ cout << "Hello "; });

    tbb::flow::continue_node<tbb::flow::continue_msg>
            w(g, [](const tbb::flow::continue_msg &){ cout << "Hello "; });

    tbb::flow::make_edge(h, w);

    h.try_put(tbb::flow::continue_msg());

    g.wait_for_all();
}

void test2(void){
    int size = 20000000;
    double *input = new double[size];
    double *output = new double[size];


    for (int i = 0; i < size; ++i) {
        input[i] = i;
    }

    for (int i = 0; i < size; ++i) {
        output[i] = sqrt(sin(input[i]) * sin(input[i]) + cos(input[i]) * cos(input[i]));
    }
    cout << output[size-1];

}

void test3(void){
    int size = 20000000;
    double *input = new double[size];
    double *output = new double[size];


    for (int i = 0; i < size; ++i) {
        input[i] = i;
    }

    tbb::parallel_for(0, size, 1, [=](int i) {
        output[i] = sqrt(sin(input[i]) * sin(input[i]) + cos(input[i]) * cos(input[i]));
    });

}

void test4(void){
    struct MyHashCompare {
        static size_t hash( const string& x ) {
            size_t h = 0;
            for( const char* s = x.c_str(); *s; ++s )
                h = (h*17)^*s;
            return h;
        }
        //! True if strings are equal
        static bool equal( const string& x, const string& y ) {
            return x==y;
        }
    };
    typedef tbb::concurrent_hash_map<string, int, MyHashCompare> StringTable;
    StringTable table;

    vector<string> vec;
    int N = 1000;
    for (int i = 0; i < N; ++i) {
        vec.push_back(to_string(i));
    }

    tbb::parallel_do( vec.begin(), vec.end(), [&table](string& val){
        StringTable::accessor a;
        table.insert( a, val);

        if (stoi(val) == 100){
            a->second += 200;
        } else {
        a->second += 3;}
        a.release();
    });

    StringTable::accessor a;
    table.find(a, "100");


    cout << a->first << " " << a-> second << endl;
    table.erase(a);
    a.release();
    cout << table.find(a, "200") << endl;
    cout << a->first << " " << a-> second << endl;
    a.release();

}


int main(void){
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    test4();
    std::chrono::steady_clock::time_point end= std::chrono::steady_clock::now();

    std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() <<std::endl;

    return 0;
}
