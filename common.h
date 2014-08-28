#pragma once

#include <stdint.h>

namespace mongo {

    typedef unsigned char       uchar;
    typedef uint16_t            ushort;
    typedef uint32_t            uint;
    typedef uint64_t            uid;
    typedef uint64_t            off64_t;

    // page number packed byte length, (48 bits * 12..24 bits/page max = 60..72 bits)
    #define BtId                6

    // debugging
    #define __LOC__         __FILE__,__FUNCTION__,__LINE__
    #define __OSS__(X)      std::ostringstream __oss__; __oss__ << X; std::string __ss__ = __oss__.str();

#ifdef STANDALONE
    #define uassert( X, Y, Z ) assert( Z )
#endif

    // page number constants
    #define ALLOC_page          0    // allocation & lock manager hash table
    #define ROOT_page           1    // root of the btree
    #define LEAF_page           2    // first page of leaves
    #define LATCH_page          3    // pages for lock manager

    // number of levels to create in a new BTree
    #define MIN_lvl             2

    // number of latch manager slots
    #define BT_latchtable       1024

    // file open modes
    #define BT_ro               0x6f72
    #define BT_rw               0x7772

    // min / max page sizes
    #define BT_minbits          12
    #define BT_maxbits          24
    #define BT_minpage          (1 << BT_minbits)
    #define BT_maxpage          (1 << BT_maxbits)

}   // namespace mongo

