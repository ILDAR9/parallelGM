#include "edge.h"
#include <iostream>


Edges::Edges( int from, int to )
{
    _from = from;
    _to   = to;
    _times         = defaultTimes;
}

int Edges::getTimes() const
{
    return _times;
}


void Edges::setTimes( int val )
{
    _times = val;
}


void Edges::setTo( int val )
{
    _to = val;
}

void Edges::setFrom( int val )
{
    _from = val;
}


int Edges::getTo() const
{
    return _to;
}

int Edges::getFrom() const
{
    return _from;
}


//Friend functions

bool operator<( const Edges & a, const Edges & b )
{
    return timesCompare( a, b );
}



bool classCompare( const Edges & a, const Edges & b )
{
    if ( a._from == b._from )
    {
    	return a._to < b._to;
    }
    return a._from < b._from;
}

bool edgeCompare( const Edges & a, const Edges & b )
{
	if ( a._from == b._from )
	    {
	    	return a._to < b._to;
	    }
	    return a._from < b._from;
}

bool timesCompare( const Edges & a, const Edges & b ) //The use of > instead of a < is due to the reason that it is used in a max-heap
{
    if ( a._times == b._times )
        return edgeCompare( a, b );
    return a._times > b._times;
}

