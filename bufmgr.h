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

/*
*  This module contains derived code.   The original
*  copyright notice is as follows:
*
*    This work, including the source code, documentation
*    and related data, is placed into the public domain.
*  
*    The orginal author is Karl Malbrain (malbrain@cal.berkeley.edu)
*  
*    THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY
*    OF ANY KIND, NOT EVEN THE IMPLIED WARRANTY OF
*    MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE,
*    ASSUMES _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE
*    RESULTING FROM THE USE, MODIFICATION, OR
*    REDISTRIBUTION OF THIS SOFTWARE.
*/

#pragma once

#include "common.h"
#include "blterr.h"
#include "latchmgr.h"

#include <assert.h>
#include <iostream>
#include <string>
#include <unistd.h>

namespace mongo {

/**
 *    The buffer manager manages a pool of 'segments'.   Each segment is
 *    memory mapped.  Segment base addresses are multiples of the segment
 *    size.
 *
 *    Each segment contains some number of 'pages'.  Pages are configured
 *    to have size some power of two between 9 .. 24, (i.e.) 512B .. 16MB.
 *
 *    A small number of pages are allocated to contain latches and other
 *    internal housekeeping data.   The remaining pages comprise the B-Link
 *    tree nodes.
 *
 *    The pages are assigned as follows:
 *
 *        +--------+
 *      0 | alloc  |  free list, allocation metatdata
 *        | page   | 
 *        +--------+
 *      1 | root   |
 *        | node   |
 *        +--------+
 *      2 | left   |
 *        | leaf   |
 *        +--------+ --
 *      3 |        |   |
 *        |        |   |
 *        +--------+   |
 *      4 |        |   |
 *        |        |   |
 *        +--------+    > page latch table
 *           ...       |
 *                     |
 *        +--------+   |
 *  127+3 |        |   |
 *        |        |   |
 *        +--------+ --
 *        |        |   |
 *        |        |   |
 *        +--------+   |
 *        |        |   |
 *        |        |   |
 *        +--------+    > PoolEntry pointer table
 *           ...       | 
 *        +--------+   |
 *        |        |   |
 *        |        |   |
 *        +--------+ --
 *        |        |   |
 *        |        |   |
 *        +--------+   |
 *        |        |   |
 *        |        |   |
 *        +--------+    > B-Link tree nodes
 *            ...      |
 *        +--------+   |
 *        |        |   |
 *        |        |   |
 *        +--------+ --
 *
 *
 *    Each page contains:
 *
 *      +--------+
 *        | header |  |
 *        +--------+
 *        | slots  |  | increasing addresses
 *          ...       V 
 *        +--------+
 *          ...      <--_min == next available key offset
 *        | keys   |
 *        +--------+
 *
 *    Fixed width slots at low addresses grow upward, and a set of
 *    corresponding variable width keys at high addreses grow downward.
 *    The slots are managed as a packed ordered array suitable for binary
 *    search.   The keys are managed as a simple heap.
 *
 *    The page header includes
 *
 *      uint32_t _cnt;              // count of keys in page
 *         uint32_t _act;              // count of active keys
 *         uint32_t _min;              // next key offset
 *         uchar _bits:7;              // page size in bits, only really need 5 bits here
 *         uchar _free:1;              // page is on free list
 *         uchar _level:6;             // level of page, between 0 == leaf, and 64 > max == root
 *         uchar _kill:1;              // page is being deleted
 *         uchar _dirty:1;             // page has deleted keys
 *         uchar _right[IdLength];     // page number to right: all nodes on a given
 *                                     //    level form a singly linked list.
 *
 *     The slot entries include
 *
 *         uint32_t _off:BLT_maxbits;  // page offset for key start
 *         uint32_t _dead:1;           // set for deleted key
 *         uint32_t _tod;              // timestamp of key insertion
 *         uchar    _id[IdLength];     // id associated with key
 *
 *    When a key insertion determines that the gap between the highest
 *    address slot and the lowest address key is too small to fit the
 *    key to be inserted, then
 *    
 *        1. cleanPage is called to remove deleted keys, repacking the key heap to
 *            reclaim space.  If there is still not enough space, then
 *        2. splitPage is called to allocated a new right sibling page, and half the
 *            keys are moved into the new page.
 *        3. a new separator key is inserted into the parent node.
 *
 *    The recursion is controlled by passing a 'level' parameter. Insertion
 *    at level = L, invokes insertion of a separator key at level = L+1.
 *    Leaf nodes are at level 0.  When the level == rootLevel, it may be
 *    necessary to split the root node.
 *
 */

    class Page;

    struct PoolEntry {
        PageNo _basePage;       // mapped base page number
        char*  _map;            // mapped memory pointer
        ushort _slot;           // slot index of this pool entry then segment pool hash table
        ushort _pin;            // mapped page pin counter
        void*  _hashPrev;       // previous pool entry for the same hash index
        void*  _hashNext;       // next pool entry for the same hash index
    };
    
    /**
    */
    class BufferMgr {
    public:

        /**
        *  Factory method: open/create new buffer pool manager.
        *  @param name       -  db file name
        *  @param bits       -  lg( page size )
        *  @param poolSize   -  mapped page pool size, (e.g.) 8192
        *  @param segSize    -  segment size in pages in bits
        *  @param hashSize   -  hash table size, (e.g.) poolSize >> 3
        */
        static BufferMgr* create( const char* name,
                                  uint bits,
                                  uint poolMax,
                                  uint segSize,
                                  uint hashSize );

        /**
        *  Create helper method
        */
        bool mapLatches( const char* thread );

        /**
        *  Release resources, deallocate.
        */
        void close( const char* thread );

        /**
        *  Find segment in pool: must be called with hashslot hashIndex locked.
        *  @param pageNo  -  
        *  @param hashIndex  -  
        *  @return node, otherwise NULL if not there
        */
        PoolEntry* findPoolEntry( PageNo pageNo, uint hashIndex, const char* thread );
        
        /**
        *  Add segment to hash table.
        *  @param pool  -  
        *  @param pageNo  -  
        *  @param hashIndex  -  
        */
        void linkHash( PoolEntry* pool, PageNo pageNo, int hashIndex, const char* thread );
        
        /**
        *  Map new buffer pool segment to virtual memory.
        *  @param pool  -  
        *  @param pageNo  -    
        *  @return OK if successful, otherwise error
        */
        BLTERR mapSegment( PoolEntry* pool, PageNo pageNo, const char* thread );
        
        /**
        *  Calculate page within pool.
        *  @param pool  -  
        *  @param pageNo  -  
        *  @return pointer to page in pool for given pageNo
        */
        Page* page( PoolEntry* pool, PageNo pageNo, const char* thread );
        
        /**
        *  Release pool pin.
        *  @param pool  -  
        */
        void unpinPoolEntry( PoolEntry* pool, const char* thread );
        
        /**
        *  Find or place requested page in segment pool.
        *  @param pageNo  -  
        *  @return pool table entry, incrementing pin
        */
        PoolEntry* pinPoolEntry( PageNo pageNo, const char* thread );
        
        /**
        *  Place write, read, or parent lock on requested page.
        *  @param lockMode  -  
        *  @param set  -  
        */
        void lockPage( BLTLockMode lockMode, LatchSet* set, const char* thread );
        
        /**
        *  Remove write, read, or parent lock on requested page.
        *  @param lockMode  -  
        *  @param set  -  
        */
        void unlockPage( BLTLockMode lockMode, LatchSet* set, const char* thread );
        
        /**
        *  Allocate a new page and write given page into it.
        *  @param page  -  
        *  @return page id of new page
        */
        PageNo newPage( Page* page, const char* thread );
        
        /**
        *  Find slot in page for given key at a given level.
        *  @param set  -  
        *  @param key  -  
        *  @param keylen  -  
        */
        int findSlot( PageSet* set, const uchar* key, uint keylen, const char* thread );
        
        /**
        *  Find and load page at given level for given key:
        *    leave page read or write locked as requested.
        *  @param set       - output: page set 
        *  @param key       - input: key within page  
        *  @param keylen    - input: length of key within page
        *  @param level     - input: (1-depth) to start looking (used during recursive lookups)
        *  @param lockMode  - input: intent_access/delete/read/write/parent_mod
        *  @param thread    - input: current thread
        *  @return 
        */
        int loadPage( PageSet* set, const uchar* key, uint keylen, uint level, BLTLockMode lockMode, const char* thread );
        
        /**
        *  Return page to free list: page must be delete and write locked.
        *  @param set  -  
        */
        void freePage( PageSet* set, const char* thread );

        /**
        *
        */
        int readPage( Page* page, PageNo pageNo, const char* thread );

        /**
        *
        */
        int writePage( Page* page, PageNo pageNo, const char* thread );

        /**
        *
        */
        int lastErr() const { return _err; }

        /**
        *
        */
        std::string decodeLastErr() const;

        /**
        *
        */
        void latchAudit( const char* thread );

        // accessors
        uint getPageSize() const { return _pageSize; }
        uint getPageBits() const { return _pageBits; }
        uint getSegBits() const { return _segBits; }
        int  getFD() const { return _fd; }
        LatchMgr* getLatchMgr() const { return _latchMgr; }

    protected:
        uint _pageSize;             // page size    
        uint _pageBits;             // page size in bits    
        uint _segBits;              // seg size in pages in bits
        int  _fd;                   // file descriptor
        ushort _poolCnt;            // highest page pool node in use
        ushort _poolMax;            // highest page pool node allocated
        ushort _poolMask;           // total number of pages in mmap segment - 1
        ushort _hashSize;           // size of hash table for pool entries
        volatile uint _evicted;     // last evicted hash table slot
        int _err;					// most recent error

        /*
        *  _hashSize contiguously allocated ushort
        *  _hash[ hashIndex ] => slot,
        *  _pool[ slot ] => PoolEntry
        */
        ushort* _hash;

        /*
        *  _hashSize contiguously allocated SpinLatch objects
        *  _latch[ hashIndex ] => pointer to latch for _hash[ hashIndex ]
        */
        SpinLatch* _latch;

        /*
        *  mapped latch page from allocation page
        */
        LatchMgr* _latchMgr;

        /*
        *  _poolMax contiguously allocated PoolEntry objects (segments)
        */
        PoolEntry* _pool;

        /*
         *  Page of zeros allocated once for efficiency
         */
        Page* _zero;

    };

}   // namespace mongo

