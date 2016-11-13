/**
    @file
    @author Rodrigo R. Paim
    @date 17/08/2013
    @section DESCRIPTION
    This file contains three different implementations of Random Graphs: Erdos_Renyi, Watts_Strogatz and Barabasi_Albert. Each one of them inherits from Graph and presents a method to construct the mentioned random graph.
*/

#ifndef RG_H
#define RG_H

#include "graph.h"

/**
    This class implements a @f$G(n,p)@f$, or Erdos-Renyi, graph. Moreover, it can be adapted to any type of @f$G(n,p_k)@f$, where @f$ k @f$ is the size of the cliques that are created with probability @f$p_k @f$. The average degree is passed as a parameter instead of the probability of existence of a clique.
*/
class Erdos_Renyi : public Graph
{
private:

    /**
        Creates different cliques of size @f$ k @f$. This procedure is faster than listing @f$ k @f$ all the unique @f$ k @f$-tuples and creating the respective cliques with probability @f$ p_k @f$. The complexity is much smaller and also there is no rounding error (whenever the size of the clique increases, the probability drops to 0).
        @param k Size of the clique
        @param n_structures Number of structures of size @a k
        @param[out] le List with approximately @f${k \choose 2} \times n\_structures @f$ Edge - considering that edge overlap is very unlikely to happen - that are a result from the cliques construction.
    */
    void write_edges( int k, int n_structures, std::list< Edge > & le );

public:
    /**
        Constructor: Calls Graph Constructor for a labeled and undirected graph
        @param n Number of vertices
    */
    Erdos_Renyi( int n );

    /**
        Copy Constructor
        @param er Object to be copied
    */
    Erdos_Renyi( const Erdos_Renyi & er );

    /**
        Build a @f$G(n,p_k)@f$ from the average degree and the size of the cliques. It calls #write_edges and calculates the number of structures considering that no edge collisions happen: @a number_structures = @f$\frac{ \_nodes \times avg }{ { k \choose 2 } }@f$. After calling #write_edges, it calls #createGraph, giving the returned list of Edge as a parameter to create the edges of the graph.
        @param avg Average degree
        @param k Clique size
    */
    void build( int avg, int k = 2 );
};





/**
    This class implements a Watts-Strogatz graph. A lattice of size #_nodes, where each vertex has @a 2k neighbors, is created and then, with probability @f$ \beta @f$, each edge is rewired. The rewiring process is performed during the construction of the graph to reduce the associated costs.
*/
class Watts_Strogatz : public Graph
{
private:

    /**
        Create @f$ k \times \_nodes @f$ edges and rewire each one of them with probability @f$ \beta @f$. Before adding a node, it is checked whether it is to be rewired or not. In the first case, it picks one of the endpoints and choose another different node (checking if the edge already exists). If during the construction process there is an edge to one of the neighbors to be connected, it means that this one had been rewired previously and therefore it simply chooses randomly another location for the edge.
        @param k Average Degree over 2
        @param beta Rewiring probability
        @param[out] le List of Edge that are a result from the construction process.
    */
    void write_edges( int k, double beta, std::list< Edge > & le );

public:

    /**
        Constructor: Calls Graph Constructor for a labeled and undirected graph
        @param n Number of vertices
    */
    Watts_Strogatz( int n );

    /**
        Copy Constructor
        @param ws Object to be copied
    */
    Watts_Strogatz( const Watts_Strogatz & ws );

    /**
        Calls #write_edges and afterwards #createGraph with the resulting List of Edge to create the graph.
        @param k Average Degree over 2
        @param beta Rewiring probability
    */
    void build( int k, double beta );
};





class Barabasi_Albert : public Graph
{
private:

    /**
        Creates a vertex and connects it to @a init other isolated ones. Then runs the Barabasi-Albert procedure of preferential attachment for the remaining nodes.
        @param init Initial number of edges (number of nodes-1)
        @param[out] le List of Edge that are a result from the construction process.
    */
    void write_edges( int init, std::list< Edge > & le );

public:

    /**
        Constructor: Calls Graph Constructor for a labeled and undirected graph
        @param n Number of vertices
    */
    Barabasi_Albert( int n );

    /**
        Copy Constructor
        @param ba Object to be copied
    */
    Barabasi_Albert( const Barabasi_Albert & ba );

    /**
        Calls #write_edges and afterwards #createGraph with the resulting List of Edge to create the graph.
        @param init Initial number of edges (number of nodes-1)
    */
    void build( int init );
};


class Chung_Lu : public Graph
{
private:

    /**
        @param number of nodes and \beta
        @param[out] le List of Edge that are a result from the construction process.
    */
    void write_edges( double avg, double beta, std::list< Edge > & le );

public:

    /**
        Constructor: Calls Graph Constructor for a labeled and undirected graph
        @param n Number of vertices
    */
    Chung_Lu( int n );

    /**
        Copy Constructor
        @param ba Object to be copied
    */
    Chung_Lu( const Chung_Lu & cl );

    /**
        Calls #write_edges and afterwards #createGraph with the resulting List of Edge to create the graph.
        @param init Initial number of edges (number of nodes-1)
    */
    void build( double avg, double beta );
};

#endif
