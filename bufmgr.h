//@file bufmgr.h

/*
*    Copyright (C) 2014 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#pragma once

#ifndef STANDALONE
#include "mongo/db/storage/bltree/blterr.h"
#include "mongo/db/storage/bltree/common.h"
#include "mongo/db/storage/bltree/page.h"
#include "mongo/db/storage/bltree/latchmgr.h"
#else
#include "blterr.h"
#include "common.h"
#include "page.h"
#include "latchmgr.h"
#endif

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
        Page* page;                 // current page pointer
        PoolEntry* pool;            // current page pool
        LatchSet* latch;            // current page latch set
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
        PoolEntry* pinpool( uid page_no );
        void       unpinpool( PoolEntry* pool );
        void       linkhash( PoolEntry* pool, uid page_no, int idx );
        BTERR      mapsegment( PoolEntry* pool, uid page_no );

        // page interface
        Page* page( PoolEntry* pool, uid page_no );
        uid   newpage( Page* page );
        void  lockpage( BLTLockMode mode, LatchSet *set );
        void  unlockpage( BLTLockMode mode, LatchSet *set );
        int   findslot( PageSet* set, uchar* key, uint len );
        int   loadpage( PageSet* set, uchar* key, uint len, uint lvl, BLTLockMode lock );
        void  freepage( PageSet* set );

        // latch interface
        void      latchlink( ushort hashidx, ushort victim, uid page_no );
        void      unpinlatch( LatchSet *set );
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
        LatchMgr* latchmgr;         // mapped latch page from allocation page
        LatchSet* latchsets;        // mapped latch set from latch pages
        PoolEntry* pool;            // memory pool page segment
    };

}   // namespace mongo
