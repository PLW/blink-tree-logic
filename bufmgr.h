
#pragma once

#include "common.h"
#include "page.h"
#include "latchmgr.h"

namespace mongo {

	//
	//    The memory mapping pool table buffer manager entry
	//
	struct PoolEntry {
	    uid  basepage;              // mapped base page number
	    char* map;                  // mapped memory pointer
	    ushort slot;                // slot index in this array
	    ushort pin;                 // mapped page pin counter
	    void* hashprev;             // previous pool entry for the same hash idx
	    void* hashnext;             // next pool entry for the same hash idx
	};
	
	#define CLOCK_bit 0x8000        // bit in pool->pin
	
	//
	//  The loadpage interface object
	//
	struct PageSet {
	    uid page_no;                // current page number
	    Page* page;               // current page pointer
	    PoolEntry* pool;               // current page pool
	    LatchSet* latch;          // current page latch set
	};
	
	//
	//    The object structure for Btree access
	//
	class BufMgr {
	public:
        // factory
	    static BufMgr* create( char* name,
                                uint mode,
                                uint bits,
                                uint poolsize,
                                uint segsize,
                                uint hashsize );

	    static void  destroy( BufMgr* mgr );

        // segment pool interface
        PoolEntry* findpool( uid page_no, uint idx );
        void    linkhash( PoolEntry* pool, uid page_no, int idx );
        BTERR   mapsegment( PoolEntry* pool, uid page_no );
        void    unpinpool( PoolEntry* pool );
        PoolEntry* pinpool( uid page_no );

        // page interface
        Page* page( PoolEntry* pool, uid page_no );
        uid     newpage( Page* page );
        void    lockpage( BLTLockMode mode, LatchSet *set );
        void    unlockpage( BLTLockMode mode, LatchSet *set );
        int     findslot( PageSet* set, uchar* key, uint len );
        int     loadpage( PageSet* set, uchar* key, uint len, uint lvl, BLTLockMode lock );
        void    freepage( PageSet* set );

        // latch interface
        void    latchlink( ushort hashidx, ushort victim, uid page_no );
        void    unpinlatch( LatchSet *set );
        LatchSet* pinlatch( uid page_no );
	
	public:
	    uint page_size;             // page size    
	    uint page_bits;             // page size in bits    
	    uint seg_bits;              // seg size in pages in bits
	    uint mode;                  // read-write mode
	    int idx;                    // file handle
        int err;                    // last error code
	    ushort poolcnt;             // highest page pool node in use
	    ushort poolmax;             // highest page pool node allocated
	    ushort poolmask;            // total number of pages in mmap segment - 1
	    ushort hashsize;            // size of Hash Table for pool entries
	    volatile uint evicted;      // last evicted hash table slot
	    ushort* hash;               // pool index for hash entries
	    SpinLatch* latch;           // latches for hash table slots
	    LatchMgr* latchmgr;       // mapped latch page from allocation page
	    LatchSet* latchsets;      // mapped latch set from latch pages
	    PoolEntry* pool;               // memory pool page segment
	};

}   // namespace mongo
