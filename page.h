
#pragma once

#include "common.h"

namespace mongo {

    //
    //  page key slot
    //
    struct Slot {
        uint off:BT_maxbits;        // page offset for key start
        uint dead:1;                // set for deleted key
    };
    
    // page accessors
    class BLTKey;
    class BLTVal;

    #define slotptr(page, slot) \
        (((mongo::Slot *)(page+1)) + (slot-1))

    #define keyptr(page, slot) \
        ((mongo::BLTKey *)((unsigned char*)(page) + slotptr(page, slot)->off))

    #define valptr(page, slot) \
        ((mongo::BLTVal *)(keyptr(page,slot)->key + keyptr(page,slot)->len))

    //
    //  index page
    //
    class Page {
    public:
        uint  cnt;              // count of keys in page
        uint  act;              // count of active keys
        uint  min;              // next key offset
        uchar bits:7;           // page size in bits
        uchar free:1;           // page is on free chain
        uchar lvl:6;            // level of page
        uchar kill:1;           // page is being deleted
        uchar dirty:1;          // page has deleted keys
        uchar right[BtId];      // page number to right
    };

}   // namespace mongo
