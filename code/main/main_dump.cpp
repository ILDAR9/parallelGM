#include "rg.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <ctime>
#include "dsp.h"
#include <cmath>

using namespace std;

 #define INPUT_FILE_PATH "/Users/ildarnurgaliev/projects/dsp/GM_multy/data/rg_erdos_renyi_cuda.csv" //MAC
//#define INPUT_FILE_PATH "/home/ildar/diploma/GM_cuda/data/rg_erdos_renyi_cuda.csv" //dainfos

int mainG() {
    Graph *g;
    int N = 200;
    //    ERDOS-REUNYI
    int avg = 10;
    int k = 2;
    g = new Erdos_Renyi(N);
    reinterpret_cast< Erdos_Renyi * >( g )->build(avg, k); //avg degree value and k-size
    string file_name = "/Users/ildarnurgaliev/projects/dsp/GM_cuda/data/rg_erdos_renyi_cuda.csv";
    g->writeGraph(file_name);
    cout << file_name << endl;
}


//1000 0.85 1 10 for test
int main2(int argc, char *argv[]) {
    srand(time(NULL));

    //Inputs: Type of graph, number of nodes, number of labels, average degree, turns, filename
//    int N = atoi(argv[1]);
    double ps = atof(argv[2]); // edge sampling probability
    double pt = atof(argv[3]); // nodes sampling probability
    int nb_seed = atoi(argv[4]);
//    int a_c = atoi(argv[5]);  //ToDO delete for SeedOnce

    Graph *g;
//    BARABASI-ALBERT
//      g = new Barabasi_Albert(N);
//      reinterpret_cast< Barabasi_Albert * >( g )->build(3);
//    ERDOS-REUNYI
//    int avg = 20;
//    int k = 2;
//    g = new Erdos_Renyi(N);
//    reinterpret_cast< Erdos_Renyi * >( g )->build(avg, k); //avg degree value and k-size

//    READ GRAPH
    g = new Graph(false);
    cout << "Start read graph: " << INPUT_FILE_PATH << endl;
    g->readGraph(INPUT_FILE_PATH, false);

    int N = g->getNNodes();
    cout << "Graph created" << endl;

    showParams(N, g->getNEdges(), ps, pt, 2);

    cout << "Generate G1" << endl;
    Graph *lg = new Graph(*g, ps, pt);
    cout << "Generate G2" << endl;
    Graph *rg = new Graph(*g, ps, pt);
    // To find the correct mappinngs between the two graphs
    std::map<int, int> linttonodeID = lg->getinttonodeID();
    std::map<int, int> rinttonodeID = rg->getinttonodeID();
    std::map<int, int> lnodeIDtoint = lg->getnodeIDtoint();
    std::map<int, int> rnodeIDtoint = rg->getnodeIDtoint();

    int same = 0;
    for (int i = 0; i < lg->getNNodes(); i++) {
        if (rnodeIDtoint.find(linttonodeID[i]) != rnodeIDtoint.end()) {
            same += 1;
        }
    }
    cout << "same = " << same << ";  N * pt * pt = " << N * pt * pt << endl;

    std::list<Match> seed;
    std::set<std::string> seedcheck;
   /* for (int i = 0; i < nb_seed; i++) {
        bool flag = true;

        while (flag) {
            int node = rand() % N;
            std::stringstream sstm;
            sstm << node << "$" << node;
            std::string ID = sstm.str();

            if (seedcheck.find(ID) == seedcheck.end() && lnodeIDtoint.find(node) != lnodeIDtoint.end() &&
                rnodeIDtoint.find(node) != rnodeIDtoint.end()) {
                Match m;
                m.lnode = lnodeIDtoint[node];
                m.rnode = rnodeIDtoint[node];
                m.value = 1;
                seed.push_back(m);
                seedcheck.insert(ID);
                flag = false;
            }
        }
    }*/

    for (int i = 1; i <= nb_seed; i++) {
        int node = i+10;
        std::stringstream sstm;
        sstm << node << "$" << node;
        std::string ID = sstm.str();

        if (seedcheck.find(ID) == seedcheck.end() && lnodeIDtoint.find(node) != lnodeIDtoint.end() &&
            rnodeIDtoint.find(node) != rnodeIDtoint.end()) {
            Match m;
            m.lnode = lnodeIDtoint[node];
            m.rnode = rnodeIDtoint[node];
            m.value = 1;
            seed.push_back(m);
            seedcheck.insert(ID);
        }
    }

//    DELETE up ToDo

    delete g;

    lg->display_time("START algorithm time");
//    std::set<Match, CompareMatches> matches = lg->expandTopStack(*rg, seed, 1, 6, 2 * N);
    std::set<Match, CompareMatches> matches = lg->expandWhenStuck(*rg, seed, 1, 6, 2 * N);
//    std::set<Match, CompareMatches> matches = lg->pgm(*rg, seed, 1, true);
//    std::set<Match, CompareMatches> matches = lg->expandOnce(*rg, seed, 1, a_c);
    lg->display_time("END algorithm time");

    seed.clear();
    int correct = 0, wrong = 0;

    std::list<int> correct_list;
    std::list<int> wrong_list;

//  Check results
    map<int, list<Match>> super_vmap;
    for (std::set<Match, CompareMatches>::iterator it = matches.begin(); it != matches.end(); ++it) {
        if (it->super_v >= 0) {
            Match m;
            m.lnode = it->lnode;
            m.rnode = it->rnode;
            super_vmap[it->super_v].push_back(m);
        } else {
            if (linttonodeID[it->lnode] == rinttonodeID[it->rnode]) {
                correct_list.push_back(it->lnode);
                correct++;
            }
            else {
                wrong_list.push_back(it->lnode);
                wrong++;
            }
        }
    }
    cout << "super Map size " << super_vmap.size() << endl;

    typedef map<int, list<Match>>::iterator it_type;
    for (it_type it_sup = super_vmap.begin(); it_sup != super_vmap.end(); it_sup++) {
        set<int> unqNode;
        int corr =0;
        list<Match>::iterator m = it_sup->second.begin();
        for (; m != it_sup->second.end(); m++) {
            unqNode.insert(m->lnode);
            if (m->lnode == m->rnode) {
                corr++;
            }
        }
        correct += corr;
        wrong += unqNode.size() - corr; // ToDO abs
    }

//    DEBUG
//    cout << "Correct vertexes" << endl;
//    for (std::list<int>::iterator it = correct_list.begin(); it != correct_list.end(); ++it) {
//        cout << *it << " ";
//    }
//    cout << endl;
//
//    cout << "Wrong vertexes" << endl;
//    for (std::list<int>::iterator it = wrong_list.begin(); it != wrong_list.end(); ++it) {
//        cout << *it << " ";
//    }
//    cout << endl;
//    DEBUG

    cout << "expandWhenStuck: " << endl; //ToDo set algo name
    cout << "----------RESULT----------" << endl;
    cout << "expandWhenStuck results for N = " << N << "; s = " << ps << "; nb_seed = " << nb_seed << endl;
    cout << "\tcommon nodes count G1 and G2 = " << same << endl;
    unsigned long msize = correct + wrong;
    cout << "\tmatched = " << msize << endl;
    cout << "\tcorrect matches = " << correct << "\t wrong matches = " << wrong << endl;
    double recall = (double) correct / same;
    cout << "\trecall = " << recall << endl;
    /*
     * Precision  correct/total_vertices
     */
    double precision = (double) correct / msize;
    cout << "\tprecision = " << precision << endl;
    cout << "\tF1-score = " << 2 * (precision * recall / (precision + recall)) << endl;
    delete lg;
    delete rg;
    return 0;
}

/*
 * Predict parameters: show in console
 */

//showParams(N, g->getNEdges(), ps, pt, nb_seed, k);
void showParams(double V_COUNT, double E_COUNT, double ps, double pt, int THRESHOLD_R) {
    RandomGraphProp randG;
    randG.n = (int) V_COUNT;
    randG.p = E_COUNT / ((V_COUNT * (V_COUNT - 1)) / 2.);
    randG.t = pt;
    randG.s = ps;
    double aC = a_c(randG, THRESHOLD_R);
    //SHOW CONSOLE and THEOREM1 check
    cout << "N = " << randG.n << endl;
    cout << "E = " << E_COUNT << endl;
    cout << "p = " << randG.p << endl;
    cout << "THEOREM 1:" << endl;
    cout << "\tn^(-1) << p <= n^(-5/6 - e) ";
    double e = 1. / 1000;
    cout << "\te = " << e << "\t[1/6 > e > 0]" << endl;
    double leftBound = 1. / randG.n, rightBound = pow(randG.n, -5. / 6 - e);
    cout << "\t" << leftBound << " << p <= " << rightBound <<
    " (" << (((leftBound < randG.p) && (randG.p <= rightBound)) ? "OK" : "FALSE") << ")" << endl;
    cout << "pt = " << randG.t << endl;
    cout << "ps = " << randG.s << endl;
    cout << "r = " << THRESHOLD_R << endl;
    cout << "a_c = " << aC << ": initial seed set." << endl;
    cout << "------------------------------------" << endl;
}

double a_c(RandomGraphProp &g, unsigned int r) {
    return 1. / (2 * g.n * g.t * g.t * g.p * g.p * pow(g.s, 4));

//    cout << "b_c(g, r) = " << b_c(g, r) << endl;
//    return (1 - 1. / r) * b_c(g, r);
}

double b_c(RandomGraphProp &g, unsigned int r) {
    return pow(((double) fact(r - 1) / (g.n * g.t * g.t * pow((g.p * g.s * g.s), r))), 1. / (r - 1));
}

unsigned int fact(unsigned int n) {
    return (n == 1) ? 1 : n * fact(n - 1);
}


	
