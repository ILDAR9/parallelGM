#include "rg.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include "dsp.h"
#include <cmath>

using namespace std;

map<int, int> linttonodeID;
map<int, int> rinttonodeID;
map<int, int> lnodeIDtoint;
map<int, int> rnodeIDtoint;

int main(int argc, char *argv[]) {
    srand(time(NULL));

    double pt = 1; // nodes sampling probability
    double ps = 1; // edge sampling probability
    int nb_seed = atoi(argv[1]);

    // READ GRAPH
    Graph *g;
    g = new Graph(false);
    cout << "Start read graph: " << INPUT_FILE_PATH << endl;
    g->readGraph(INPUT_FILE_PATH, false);
    int N = g->getNNodes();
    cout << "Graph created" << endl;
    showParams(N, g->getNEdges(), ps, pt, 2);

    // Generate 2 overlapping graphs
    cout << "Generate G1" << endl;
    Graph *lg = new Graph(*g, ps, pt);
    cout << "Generate G2" << endl;
    Graph *rg = new Graph(*g, ps, pt);
    delete g;

    unsigned int same = copy_and_getoverlap(lg, rg);
    cout << "same = " << same << ";  N * pt * pt = " << N * pt * pt << endl;

    // Generate seeds
    vector<Match> seed;
    gen_seed_random(seed, N, nb_seed);

    lg->display_time("START algorithm");
    auto begin = chrono::steady_clock::now();
//    set<Match, CompareMatches> matches = lg->expandWhenStuck(*rg, seed, 1, 6, 2 * N);
    vector<Match> matches = lg->expandWhenStuckParallel(*rg, seed, 1, 6, 2 * N);
    auto end= chrono::steady_clock::now();
    cout << "END algorithm: time elapsed " << chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() <<endl;

    lg->display_time("END algorithm time");
    delete lg;
    delete rg;

    // Print results
    cout << "----------RESULT----------" << endl;
    cout << "expandWhenStuck results for N = " << N << "; s = " << ps << "; nb_seed = " << nb_seed << endl;
    evaluate(matches, N, same);
    return EXIT_SUCCESS;
}

unsigned int copy_and_getoverlap(Graph *lg, Graph *rg){
    // To find the correct mappinngs between the two graphs
    linttonodeID = lg->getinttonodeID();
    rinttonodeID = rg->getinttonodeID();
    lnodeIDtoint = lg->getnodeIDtoint();
    rnodeIDtoint = rg->getnodeIDtoint();

    // Check overlaping
    int same = 0;
    for (int i = 0; i < lg->getNNodes(); i++) {
        if (rnodeIDtoint.find(linttonodeID[i]) != rnodeIDtoint.end()) {
            same += 1;
        }
    }

    return same;
}

void evaluate(vector<Match> &matches, int N, int same){
    unsigned int correct = 0, wrong = 0;

    std::list<int> correct_list;
    std::list<int> wrong_list;
    // Check results
    for (std::vector<Match>::iterator it = matches.begin(); it != matches.end(); ++it) {
        if (linttonodeID[it->lnode] == rinttonodeID[it->rnode]) {
            correct_list.push_back(it->lnode);
            correct++;
        } else {
            wrong_list.push_back(it->lnode);
            wrong++;
        }
    }

    cout << "\tcommon nodes count G1 and G2 = " << same << endl;
    unsigned int msize = correct + wrong;
    cout << "\tmatched = " << msize << endl;
    cout << "\tcorrect matches = " << correct << "\t wrong matches = " << wrong << endl;
    double recall = (double) correct / same;
    cout << "\trecall = " << recall << endl;
    double precision = (double) correct / msize;
    cout << "\tprecision = " << precision << endl;
    cout << "\tF1-score = " << 2 * (precision * recall / (precision + recall)) << endl;
}

void gen_seed_random(vector<Match> &seed, int N, int nb_seed) {
    set<string> seedcheck;
    for (int i = 0; i < nb_seed; i++) {
        bool flag = true;

        while (flag) {
            int node = rand() % N;
            stringstream sstm;
            sstm << node << "$" << node;
            string ID = sstm.str();

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
    }
}

void gen_seed_deter(vector<Match> &seed, int N, int nb_seed) {
    set<string> seedcheck;
    for (int i = 1; i <= nb_seed; i++) {
        int node = i + 10;
        stringstream sstm;
        sstm << node << "$" << node;
        string ID = sstm.str();

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
}

/*
 * Predict parameters: show in console
 * showParams(N, g->getNEdges(), ps, pt, nb_seed, k);
 */
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
}

double b_c(RandomGraphProp &g, unsigned int r) {
    return pow(((double) fact(r - 1) / (g.n * g.t * g.t * pow((g.p * g.s * g.s), r))), 1. / (r - 1));
}

unsigned int fact(unsigned int n) {
    return (n == 1) ? 1 : n * fact(n - 1);
}



