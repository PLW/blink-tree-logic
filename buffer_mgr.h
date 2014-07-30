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

    class Page;

	struct Pool {
        PageNo _basePage;       // mapped base page number
        char*  _map;            // mapped memory pointer
        ushort _slot;           // slot index in this array
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
		Pool* findPool( PageNo pageNo, uint hashIndex, const char* thread );
		
        /**
		*  Add segment to hash table.
        *  @param pool  -  
        *  @param pageNo  -  
        *  @param hashIndex  -  
        */
		void linkHash( Pool* pool, PageNo pageNo, int hashIndex, const char* thread );
		
        /**
		*  Map new buffer pool segment to virtual memory.
        *  @param pool  -  
        *  @param pageNo  -    
        *  @return OK if successful, otherwise error
        */
		BLTERR mapSegment( Pool* pool, PageNo pageNo, const char* thread );
		
        /**
		*  Calculate page within pool.
        *  @param pool  -  
        *  @param pageNo  -  
        *  @return pointer to page in pool for given pageNo
        */
		Page* page( Pool* pool, PageNo pageNo, const char* thread );
		
        /**
		*  Release pool pin.
        *  @param pool  -  
        */
		void unpinPool( Pool* pool, const char* thread );
		
        /**
		*  Find or place requested page in segment pool.
        *  @param pageNo  -  
		*  @return pool table entry, incrementing pin
        */
		Pool* pinPool( PageNo pageNo, const char* thread );
		
        /**
		*  Place write, read, or parent lock on requested page.
        *  @param lockMode  -  
        *  @param set  -  
        */
		void lockPage( LockMode lockMode, LatchSet* set, const char* thread );
		
        /**
		*  Remove write, read, or parent lock on requested page.
        *  @param lockMode  -  
        *  @param set  -  
        */
		void unlockPage( LockMode lockMode, LatchSet* set, const char* thread );
		
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
		int loadPage( PageSet* set, const uchar* key, uint keylen, uint level, LockMode lockMode, const char* thread );
		
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
	    ushort _hashSize;           // size of Hash Table for pool entries
	    volatile uint _evicted;     // last evicted hash table slot

        /*
        *  _hashSize contiguosly allocated ushort
        *  _hash[ hashIndex ] => slot,
        *  _pool[ slot ] => Pool
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
        *  _poolMax contiguously allocated Pool objects (segments)
        */
	    Pool* _pool;

	    Page* _zero;                // page frame for zeroes at end of file
        int _err;                   // most recent error
	};

}   // namespace mongo

