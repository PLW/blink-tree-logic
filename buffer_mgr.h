//@file buffer_mgr.h
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

#include "common.h"
#include "blterr.h"
#include "latchmgr.h"

#include <assert.h>
#include <iostream>
#include <string>
#include <unistd.h>

namespace mongo {

    class Page;

	struct Pool {
        PageId _basePage;       // mapped base page number
        char*  _map;            // mapped memory pointer
        ushort _slot;           // slot index in this array
        ushort _pin;            // mapped page pin counter
        void*  _hashPrev;       // previous pool entry for the same hash idx
        void*  _hashNext;       // next pool entry for the same hash idx
    };
	
    /**
    */
	class BufferMgr {
    public:

        /**
        *  Factory method: open/create new buffer pool manager.
        *  @param name       -  db file name
        *  @param mode       -  rw / ro
        *  @param bits       -  lg( page size ), (e.g.) 16 for 64K pages
        *  @param poolSize   -  mapped page pool size, (e.g.) 8192
        *  @param segSize    -  segment size
        *  @param hashSize   -  hash table size
        */
        static BufferMgr* create( const char* name,
                                  uint mode,
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
        *  @param pageId  -  
        *  @param hashIndex  -  
		*  @return node, otherwise NULL if not there
        */
		Pool* findPool( PageId pageId, uint hashIndex, const char* thread );
		
        /**
		*  Add segment to hash table.
        *  @param pool  -  
        *  @param pageId  -  
        *  @param hashIndex  -  
        */
		void linkHash( Pool* pool, PageId pageId, int hashIndex, const char* thread );
		
        /**
		*  Map new buffer pool segment to virtual memory.
        *  @param pool  -  
        *  @param pageId  -  
        *  @return OK if successful, otherwise error
        */
		BLTERR mapSegment( Pool* pool, PageId pageId, const char* thread );
		
        /**
		*  Calculate page within pool.
        *  @param pool  -  
        *  @param pageId  -  
        *  @return pointer to page in pool for given PageId
        */
		Page* page( Pool* pool, PageId pageId, const char* thread );
		
        /**
		*  Release pool pin.
        *  @param pool  -  
        */
		void unpinPool( Pool* pool, const char* thread );
		
        /**
		*  Find or place requested page in segment pool.
        *  @param pageId  -  
		*  @return pool table entry, incrementing pin
        */
		Pool* pinPool( PageId pageId, const char* thread );
		
        /**
		*  Place write, read, or parent lock on requested page.
        *  @param mode  -  
        *  @param set  -  
        */
		void lockPage( LockMode mode, LatchSet* set, const char* thread );
		
        /**
		*  Remove write, read, or parent lock on requested page.
        *  @param mode  -  
        *  @param set  -  
        */
		void unlockPage( LockMode mode, LatchSet* set, const char* thread );
		
        /**
		*  Allocate a new page and write given page into it.
        *  @param page  -  
        *  @return page id of new page
        */
		PageId newPage( Page* page, const char* thread );
		
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
        *  @param set  -  
        *  @param key  -  
        *  @param keylen  -  
        *  @param level  -  
        *  @param mode  -  
        *  @return
        */
		int loadPage( PageSet* set, const uchar* key, uint keylen, uint level, LockMode mode, const char* thread );
		
        /**
		*  Return page to free list: page must be delete and write locked.
        *  @param set  -  
        */
		void freePage( PageSet* set, const char* thread );

        /**
        *
        */
        int readPage( Page* page, PageId pageId, const char* thread );

        /**
        *
        */
        int writePage( Page* page, PageId pageId, const char* thread );

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
	    uint _mode;                 // read-write mode
	    int _fd;                    // file descriptor
	    ushort _poolCnt;            // highest page pool node in use
	    ushort _poolMax;            // highest page pool node allocated
	    ushort _poolMask;           // total number of pages in mmap segment - 1
	    ushort _hashSize;           // size of Hash Table for pool entries
	    volatile uint _evicted;     // last evicted hash table slot
	    ushort* _hash;              // pool index for hash entries <-- XXX fix this name!
	    SpinLatch* _latch;          // latches for hash table slots <-- XXX fix this name!
	    LatchMgr* _latchMgr;        // mapped latch page from allocation page
	    Pool* _pool;                // memory pool page segments
	    Page* _zero;                // page frame for zeroes at end of file
        int _err;                   // most recent error
	};

}   // namespace mongo

