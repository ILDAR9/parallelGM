/**
    @file
    @author Rodrigo R. Paim
    @date 17/08/2013
    @section DESCRIPTION
    This is the header file of an edge container where the endpoints have labels and fingerprints, called LabeledEdge. It presents some comparators used for sorting and complex structures declarations. Finally, it has tokenization and untokenization functions declarations that are used for creating unique identifiers for a LabeledEdge.
*/


#ifndef EDGE_H
#define EDGE_H

#include <set>
#include <vector>

#include "definitions.h"

#define EdgeSeparator SEPARATOR /**< Token used to separate strings and ints */


/**
    This class represents an edge container where the endpoints have labels and fingerprints. Each object is sortable and can be represented in a unique way tokenizing its parameters. Moreover, it is possible to discover which KU Class a LabeledEdge object belongs to and also what its Matching Class is.
*/
class Edges
{
private:

    int _from; /** Source endpoint's unique ID */
    int _to; /**< Destination endpoint's unique ID */
    int _times; /**< Number of times the same LabeledEdge appeared in the graph */


    const static int defaultTimes = 1; /**< Default value for #_times */

public:

    /**
        Getter for #_from
        @return #_from
    */
    Edges(int from, int to);

    int getFrom() const;

    /**
        Getter for #_to
        @return #_to
    */

    int getTo() const;

    /**
        Tokenizes the source endpoint's information
        @return #_from tokenized with #_fromID
    */

    int getTimes() const;

    /**
        Setter for #_fromID
        @param val Value for #_fromID
    */
    void setFrom( int val );

    /**
        Setter for #_toID
        @param val Value for #_toID
    */
    void setTo( int val );

    /**
        Setter for #_times
        @param val Value for #_times
    */
    void setTimes( int val );



    friend bool operator<( const Edges & a, const Edges & b );

   /**
	   Comparator for parameters #_from, #_to, #_fromID, #_toID
	   @param a First LabeledEdge
	   @param b Second LabeledEdge
	   @return @a a < @a b
   */

   friend bool classCompare( const Edges & a , const Edges & b );

   /**
	   Comparator for parameters #_times, #_from, #_to, #_fromID, #_toID. Specially in this case, #_times is compared with a "greater than" sign because it is used for a max-heap that is organized using a set.
	   @param a First LabeledEdge
	   @param b Second LabeledEdge
	   @return @a a < @a b
   */
   friend bool timesCompare( const Edges &, const Edges & );

   friend bool edgeCompare( const Edges & a, const Edges & b );

};

//Friend Functions
bool edgeCompare( const Edges & a, const Edges & b );
bool fingerprintCompare( const Edges & a, const Edges & b );
bool classCompare( const Edges & a, const Edges & b );
bool timesCompare( const Edges & a, const Edges & b );




//Type definitions

typedef std::list< Edges * > ListEdge; /**< List of pointers to LabeledEdge */
typedef std::set< Edges * > SetEdge; /**< Set of pointers to LabeledEdge */
typedef std::vector< Edges * > VectorEdge; /**< List of pointers to LabeledEdge */
typedef std::map< std::string, int > StringInt; /**< Map of a string to an int */
typedef std::map< std::string, ListEdge > LabelClass; /**< Map of a string to a ListEdge
                                                           @see HeavyEgoNet::joinGraphs
                                                           @see LightEgoNet::parallelJoinGraphs
                                                      */



#endif
