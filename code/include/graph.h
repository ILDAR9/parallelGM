/**
    @file
    @author Rodrigo R. Paim
    @date 17/08/2013
    @section DESCRIPTION
    Header file of a simple graph implementation. For more information, look at Graph description.
*/

#ifndef GRAPH_H
#define GRAPH_H

#include <unordered_set>
#include "edge.h"
#include "tbb/concurrent_hash_map.h"
/**
This class is responsible for storing a graph with #_nodes vertices having IDs between 0 and n − 1 and its _nEdges labels. It can be directed (even though it is not fully implemented in some parts) or not, labeled or not. Moreover, each one of the _nLabels labels is a string, although it is not necessarily the best strategy due to high memory consumption. Each string is only stored once in #_labels to amortize this problem, even though multiple nodes may have it as a label. If the graph is undirected, each edge is stored twice.


Edges are stored sequentially in a unique giant array #__edges__ to optimize memory requests and try to have more cache hits. On the other hand, all the edge manipulations are done with #_edges.
*/
class Graph
{
private:
    int *           __edges__; /**< Contiguous memory storage of the edges. Should not be referenced directly */

protected:
    int             _nodes; /**< Number of nodes in the graph */
    int *           _nEdges; /**< Degree of each vertex */
    std::map<int , int> _nodeIDtoint;
    std::map<int , int> _inttonodeID;
    int **          _edges; /**< Array of pointers to the edges of each node. Each pointer points to the first position the edges of a vertex is stored in #__edges__. Use always this instead of #_edges. For example, if you want to pick the first edge of node 2, you have to call #_edges[ 1 ][ 0 ] */
    bool            _directed; /**<Boolean variable to store whether the graph is directed (true) or not.*/

    /**
        Reserves memory for a graph with @a n nodes. It basically allocates memory for #_nEdges and #_edges
        @param n Value for the number of vertices
    */
    void    initialize( int n );
    std::string createMatchID( int i, int j );
    void untokenizeMatchID( std::string str, int & first, int & second );
    typedef std::vector<Match>::iterator seed_iterator;
    typedef std::vector<Match>::iterator match_iterator;
    typedef std::set<Match, CompareMatches>::iterator pair_iterator;
    typedef std::map<std::string, int>::iterator score_iterator;
    struct MyHashCompare {
        static size_t hash( const std::string& x ) {
            size_t h = 0;
            for( const char* s = x.c_str(); *s; ++s )
                h = (h*17)^*s;
            return h;
        }
        //! True if strings are equal
        static bool equal( const std::string& x, const std::string& y ) {
            return x==y;
        }
    };
    typedef tbb::concurrent_hash_map<std::string,int,MyHashCompare> PairTable;

public:
    /**
        Default constructor. Sets all int parameters to 0 and points all the arrays to NULL
        @param directed Value for #_directed
        @param labeled Value for #_labeled
    */
    Graph( bool directed = true);
    Graph( const Graph & g , double s );
    Graph( const Graph & g , double s , double t);


    Graph( const Graph & g );




    /**
        Destructor: Cleans #_labels and #_nodeLabel if #_nLabels is away from 0; cleans #__edges__, #_edges and #_nEdges if _nodes is away from 0. If these previous variables are 0, then it does not do anything to avoid Segmentation Fault
    */
    virtual ~Graph(); //Destructor


    /**
        Getter for #_nodes
        @return #_nodes
    */
    int             getNNodes();
    std::map<int , int> getnodeIDtoint();
    std::map<int , int> getinttonodeID();

    /**
        Calculates the number of edges in the graph
        @return @f$ \sum_{i=0}^{\_nodes-1} \_nEdges[ i ] @f$ if #_directed, @f$ \frac{1}{2}\sum_{i=0}^{\_nodes-1} \_nEdges[ i ] @f$ otherwise
    */
    int             getNEdges();

    /**
        Getter for #_labeled
        @return #_labeled
    */

    /**
        Getter for #_directed
        @return #_directed
    */
    bool            isDirected();

    /**
        Getter for the number of neighbors of a vertex
        @param index Target vertex
        @return #_nEdges[ index ]
    */
    int             getNNeighbors( int index );

    /**
        Getter for the array of neighbors of a vertex
        @param index Target vertex
        @return #_edges[ index ]
    */
    int *           getNeighbors( int index );

    /**
        Getter for the label of a vertex
        @param index Target vertex
        @return #_nodeLabel[ index ]
    */

    /**
        Method to check if an edge connecting @a u and @a v exists or not. As the edge set of a vertex is sorted, a binary search is performed, reducing the complexity to @f$log \; \delta @f$, where @f$\delta @f$ is the degree of @a u
        @param u Source vertex
        @param v Destination vertex
        @return #_edges[ u ][ binary_search( u, v ) ] == v
    */
    bool            edgeExists( int u, int v );

    /**
        Creates the graph from a list of Edge. This list must not contain Edge repetitions, because no check is performed. Iterates over all the entries once to discover the degree of each node and them store everything in the second iteration.
        @param nodes Number of nodes
        @param le List of Edge with the edges to be included in the graph
    */
    void            createGraph( int nodes, std::list< Edge > & le );

    /**
        Reads the graph edges from a text file. This file must be in a specific format: the first line has one int corresponding to #_nodes and all the other lines have two values, corresponding to the endpoints of the edges. These values must be in the range @f$[0,\_nodes-1]@f$ and edges cannot be repeated. If the graph is not directed, each edge will be stored twice, so the order is not important in this case (take care about this).
        @param filename Text file in the forementioned format with the graph structure
        @deprecated I/O cost makes this approach unfeasible for big graphs
    */
    void            readGraph( std::string filename, bool is_shuffled );

    void            writeGraph( std::string output_file );

    /*
     * Show itermediate results of algorithm
     */
    void showInterResults(std::vector<Match> & matches, Graph & g);
    /**
        Compares the edge set @f$ E @f$ of the object with the edge set @f$ E' @f$ of another graph and returns the Sørensen–Dice coefficient of them two. The graph to be compared with must have the same vertex set of the current object and each vertex must have the same unique ID in them both.
        @param g Graph whose edge set will be compared with the current object
        @return @f$ 2\frac{\vert E \cap E' \vert}{\vert E \vert + \vert E' \vert} @f$
    */
	double          compareGraph( const Graph & g );
    /*
     * expand seed set by many noisy candidate pairs whenever there are no other unused pair
     */
    std::set<Match, CompareMatches> expandWhenStuck(Graph & g, std::list< Match> & seed,int r, int push_level, int push_size);
    /*
     * Parallel ExpandWhenStuck
     */
    std::vector<Match> expandWhenStuckParallel(Graph & g, std::vector< Match> & seed,int r, int push_level, int push_size);

	std::string retrieveString( char* buf );
	void display_time(const char *str);
	void clearing(const Graph & g, std::set<Match, CompareMatches> matches,int r, std::string filename);


};




#endif


