#ifndef GRAPHMATCHING_DSP_H
#define GRAPHMATCHING_DSP_H

struct RandomGraphProp {
    int n;     //total count (random graph)
    double p;  //vertex (random graph)
    double t;  //vertex (selection)
    double s;  //edges (selection)
};

unsigned int fact(unsigned int);

double a_c(RandomGraphProp &, unsigned int);

double b_c(RandomGraphProp &, unsigned int);

void gen_seed_random(std::vector<Match> &, int , int);

void gen_seed_deter(std::vector<Match> &, int , int);

void evaluate(std::vector<Match> &, int, int);

unsigned int copy_and_getoverlap(Graph *, Graph *);

/*
 * Growing a Graph Matching from handful of seeds
 * G(n,p,s,t)
 * r - given threshold
 */
void showParams(double V_COUNT, double E_COUNT, double ps, double pt, int THRESHOLD_R);

unsigned int fact(unsigned int);

#define INPUT_FILE_PATH "/Users/ildarnurgaliev/projects/dsp/GM_multy/data/rg_erdos_renyi_cuda.csv" //MAC
//#define INPUT_FILE_PATH "/home/ildar/diploma/GM_cuda/data/rg_erdos_renyi_cuda.csv" //dainfos

#endif
