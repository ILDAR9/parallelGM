/**
    @mainpage Code Description
    This website contains a short description of the codes and algorithms I programmed during my summer internship at LCA4 - IC - EPFL (École Polytechnique Fédérale de Lausanne), Switzerland from the 3rd June 2013 to the 16th August 2013. Intermediate knowledge of C/C++ is required at some places (templates, abstract classes, STL containers, iterators, etc.).

    Class, method, variable and function descriptions can be referenced also in the header files. The source code contains only implementation comments and important information for a programmer. If you plan to include more features in the code, you should probably see some of the .cpp. Otherwise, a brief read of these webpages are sufficient for understanding the code behavior itself.

    Some methods and functions are not in a very proper state, because they were only coded in the last days of the project. They may present some bugs and may be quite messy. The MPI part is pretty new and should be used carefully! Before launching a solver, try to calculate the best value for the number of turns and check if the memory will be sufficient or not. Avoid to use the deprecated functions and methods and always take a look at the warnings.

    Good luck!

    @author Rodrigo R. Paim
    @date 17/08/2013 (last update in the documentation)

    @todo
        -# Change every counter on the number of edges to long long (to deal with billions of edges)
        -# Copy the pointer to the graph instead of creating a new graph in LightEgoNet
        -# Change the distance function in the calculations for min-cost max-flow and min-cost greedy algorithms
        -# Use Lemon library to solve the flow problems (code is optimized and more stable)
        -# Implement an edge prunning procedure to be used with partial egonets
        -# Use other heuristics for the greedy matchers
        -# Implement @f$G(n,p_2) + G(n,p_3) + ...@f$
*/



#pragma once

#ifndef DEFINITIONS_H
#define DEFINITIONS_H
#include <list>
#include <map>
#include <string>
#include <utility>

#define SEPARATOR "$" /**< String separator used as a token for tokenization and untokenization */

#define TMP_DIR "./_tmp" /**< @deprecated Used for file operations. Should be removed in next version! */
#define TMP_EGO TMP_DIR "/tmp_ego" /**< @deprecated Used for file operations. Should be removed in next version! */
#define TMP_LABEL TMP_DIR "/tmp_label" /**< @deprecated Used for file operations. Should be removed in next version! */
#define TMP_GRAPH TMP_DIR "/tmp_graph" /**< @deprecated Used for file operations. Should be removed in next version! */

/** Exceptions that can be raised in the middle of the code. Normally, whenever one of them occurs, the filename and line are displayed */
enum EXCEPTIONS {
                    NO_GRAPH, /**< Graph not created when method was called */
                    ALLOC_EXCEPTION, /**< Memory reallocation */
                    SEGFAULT /**< Common Segmentation Fault */
                };
/**
    Simple structure to store an edge where the endpoints have integer values.
*/
struct Edge
{
    int u, /**< Source endpoint */
        v; /**< Destination endpoint */
};

struct Match
{
	int lnode;
	int rnode;
	int value;
    int super_v = -1;
};




struct CompareMatches
{
  bool operator()(const Match & lhs, const Match & rhs) {
	  if(lhs.value == rhs.value)
	  {
		  if(lhs.lnode == rhs.lnode)
		  {
			  return ((((lhs.rnode * 0xf7f7f7f7) ^ 0x8364abf7) * 0xf00bf00b) ^ 0xf81bc437) > ((((rhs.rnode * 0xf7f7f7f7) ^ 0x8364abf7) * 0xf00bf00b) ^ 0xf81bc437);
		  }
		  return ((((lhs.lnode ^ 0xf7f7f7f7) * 0x8364abf7) ^ 0xf00bf00b) * 0xf81bc437) < ((((rhs.lnode ^ 0xf7f7f7f7) * 0x8364abf7) ^ 0xf00bf00b) * 0xf81bc437 );
	  }
	  else
	  {
		  return lhs.value > rhs.value;
	  }
  }
};




struct COMM
{
	int comm;
	int value;
};


struct CompareCOMM
{
bool operator()(const COMM & lhs, const COMM & rhs) {
	  if(lhs.value == rhs.value)
	  {
		  return lhs.comm > rhs.comm;
	  }
	  else
	  {
		  return lhs.value > rhs.value;
	  }
  }
};
struct NodeVal
{
	int rnode;
	double value;
};
struct CompareNodeVal
{
bool operator()(const NodeVal & lhs, const NodeVal & rhs) {
	  if(lhs.value == rhs.value)
	  {
		  return lhs.rnode > rhs.rnode;
	  }
	  else
	  {
		  return lhs.value > rhs.value;
	  }
  }
};
/**
    Template function to perform a binary search in a sorted array.
    @param arr_size Length of @a arr
    @param arr Array where the search is performed. It must be sorted in descending order!
    @param val Value to be searched
    @return First index @a idx of @a arr such that @f$ arr[ idx ] \leq val @f$
*/
template <typename T>
int binary_search( int arr_size, T * arr, T val )
{
    int start, middle, end;

    start   = 0;
    end     = arr_size-1;
    middle  = ( start + end )/2;

    if ( !( arr[ start ] < val ) )
    {
        return start;
    }

    if ( !( val < arr[ end ] ) )
    {
        return end;
    }

    while ( start < end )
    {
        if ( arr[ middle ] < val )
        {
            start = middle;
        }
        else if ( val < arr[ middle ] )
        {
            end = middle;
        }
        else
        {
            return middle;
        }

        if ( middle == ( start + end )/2 )
        {
            break;
        }

        middle = ( start + end )/2;
    }

    return middle;
}

/**
    Template function to perform the union of two multiset in the form of list. Each one of the input lists must be sorted in descending order on the key value. If an element has cardinality @a 2 in one list, it must appear @a twice in the same list, for example.
    @param arr_1 First multiset
    @param arr_2 Second multiset
    @return Number of elements in the union of the two @a arr_1 and @a arr_2
*/
template <typename T>
int multiset_union( const std::list< T > & arr_1, const std::list< T > & arr_2 )
{
    int ret = 0;
    typename std::list< T >::const_iterator it_1 = arr_1.begin(), it_2 = arr_2.begin();

    while ( it_1 != arr_1.end() or it_2 != arr_2.end() )
    {
        ret++;

        if ( it_1 == arr_1.end() ) ++it_2;
        else if ( it_2 == arr_2.end() ) ++it_1;
        else if ( * it_1 < * it_2 ) ++it_1;
        else if ( * it_2 < * it_1 ) ++it_2;
        else { ++it_1; ++it_2; }
    }
    return ret;
}


/**
    Template function to perform the intersection of two multiset in the form of list. Each one of the input lists must be sorted in descending order on the key value. If an element has cardinality @a 2 in one list, it must appear @a twice in the same list, for example.
    @param arr_1 First multiset
    @param arr_2 Second multiset
    @return Number of elements in the intersection of the two @a arr_1 and @a arr_2
*/
template <typename T>
int multiset_intersection( const std::list< T > & arr_1, const std::list< T > & arr_2 )
{
    int ret = 0;
    typename std::list< T >::const_iterator it_1 = arr_1.begin(), it_2 = arr_2.begin();

    while ( it_1 != arr_1.end() or it_2 != arr_2.end() )
    {
        if ( it_1 == arr_1.end() or it_2 == arr_2.end() ) break;
        else if ( * it_1 < * it_2 ) ++it_1;
        else if ( * it_2 < * it_1 ) ++it_2;
        else { ++it_1; ++it_2; ret++; }
    }
    return ret;
}

#endif
