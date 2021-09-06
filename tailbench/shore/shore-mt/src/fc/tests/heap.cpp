/*<std-header orig-src='shore'>

 $Id: heap.cpp,v 1.10.2.3 2009/10/30 23:49:03 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "w_heap.h"
#include <w.h>
#include <cstring>


template<class T> class CmpLessFunction
{
    public:
        bool                gt(const T& x, const T& y) const;
        bool                cmp(const T& x, const T& y) const;
};


template<class T> class CmpGreaterFunction
{
    public:
        bool                gt(const T& x, const T& y) const;
        bool                cmp(const T& x, const T& y) const;
};


template<class T>
inline bool CmpLessFunction<T>::gt(const T& x, const T& y) const
{
    return x < y;
}
template<class T>
inline bool CmpLessFunction<T>::cmp(const T& x, const T& y) const
{
    return (x < y) ? 1 : (x==y)? 0 : -1;
}


template<class T>
inline bool CmpGreaterFunction<T>::gt(const T& x, const T& y) const
{
    return x > y;
}
template<class T>
inline bool CmpGreaterFunction<T>::cmp(const T& x, const T& y) const
{
    return (x > y) ? 1 : (x==y)? 0 : -1;
}



#define BY_SMALLEST_HEAP_ORDER
#ifdef BY_SMALLEST_HEAP_ORDER
typedef CmpLessFunction<char> CmpFunction;
#else
typedef CmpGreaterFunction<char> CmpFunction;
#endif

typedef Heap<char, CmpFunction> CharHeap;

#ifdef EXPLICIT_TEMPLATE
template class CmpLessFunction<char>;
template class CmpGreaterFunction<char>;
template class Heap<char, CmpLessFunction<char> >;
template class Heap<char, CmpGreaterFunction<char> >;
#endif

void ReplaceAllHeadsWithLargest(CharHeap& heap)
{
    char second = 0;
    for (int i = 0; i < heap.NumElements(); ++i)  {
        if (i > 1)  {
            second = heap.Second();
        }
        heap.First() = '|';
        heap.ReplacedFirst();
        if (i > 1)  {
            w_assert1(second == heap.First());
        }
        heap.CheckHeap();
        cout << "\n\n-------------------\n\n";
        heap.Print(cout);
    }
}


void ReplaceAllHeadsWithSmallest(CharHeap& heap)
{
    char second = 0;
    for (int i = 0; i < heap.NumElements(); ++i)  {
        if (i > 1)  {
            second = heap.Second();
        }
        heap.First() = '!';
        heap.ReplacedFirst();
        if (i > 1)  {
            w_assert1(second == heap.First());
        }
        heap.CheckHeap();
        cout << "\n\n-------------------\n\n";
        heap.Print(cout);
    }
}


int main()
{
    char sentence[] = "the quick brown fox jumped over the lazy dogs.";
    int maxArraySize = strlen(sentence);
    char *array;

    CmpFunction cmp;

    // test building, second and replace for a variable number of elements
    for (int arraySize = 0; arraySize < maxArraySize; ++arraySize)  {
        CharHeap heap(cmp);

        cout << "\n\n ========== TEST # " << arraySize << " =========== \n\n";
        array = new char[arraySize + 1];
        strncpy(array, sentence, arraySize);
        array[arraySize] = 0;
        for (int i = 0; i < arraySize; i++)  {
            if (array[i] == ' ')  {
                array[i] = '_';
            }
            heap.AddElementDontHeapify(array[i]);
        }
        cout << "\n\n ========== TEST # P1." << arraySize << " =========== \n\n";
        heap.Print(cout);
        heap.Heapify();
        cout << "\n\n ========== TEST # P2." << arraySize << " =========== \n\n";
        heap.Print(cout);
#ifdef BY_SMALLEST_HEAP_ORDER
        ReplaceAllHeadsWithLargest(heap);
#else
        ReplaceAllHeadsWithSmallest(heap);
#endif
        cout << "\n\n ========== TEST # P3." << arraySize << " =========== \n\n";
        heap.Print(cout);
        delete[] array;
    }

    // test building, second, and replace when all are equal
    {
        CharHeap heap(cmp);
        char array2[] = "AAAAAAAAAAAAAAAAAAAAAAA";
        for (unsigned int i = 0; i < strlen(array2); ++i)  {
            heap.AddElementDontHeapify(array2[i]);
        }
        cout << "\n\n ========== TEST # P4. =========== \n\n";
        heap.Print(cout);
        heap.Heapify();
        heap.CheckHeap();
        cout << "\n\n ========== TEST # P5. =========== \n\n";
        heap.Print(cout);
#ifdef BY_SMALLEST_HEAP_ORDER
        ReplaceAllHeadsWithLargest(heap);
#else
        ReplaceAllHeadsWithSmallest(heap);
#endif
    }

    // test removing from a heap
    {
        int i;
        CharHeap heap(cmp);
        for (i = 0; i < maxArraySize; ++i)  {
            heap.AddElementDontHeapify(sentence[i]);
        }
        heap.Heapify();
        cout << "\n\n*******************\n\n";
        for (i = 0; i < maxArraySize; ++i)  {
            cout << heap.RemoveFirst();
        }
        cout << endl;
        w_assert1(heap.NumElements() == 0);

        for (i = 0; i < maxArraySize; ++i)  {
            heap.AddElement(sentence[i]);
            heap.CheckHeap();
        }
        cout << "\n\n*******************\n\n";
        for (i = 0; i < maxArraySize; ++i)  {
            cout << heap.RemoveFirst();
        }
        cout << endl;
    }
    return 0;
}

