//@file bufmgr.cpp
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

#ifndef STANDALONE
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "mongo/db/storage/mmap_v1/bltree/bufmgr.h"
#include "mongo/db/storage/mmap_v1/bltree/blterr.h"
#include "mongo/db/storage/mmap_v1/bltree/bltkey.h"
#include "mongo/db/storage/mmap_v1/bltree/common.h"
#include "mongo/db/storage/mmap_v1/bltree/latchmgr.h"
#include "mongo/db/storage/mmap_v1/bltree/logger.h"
#include "mongo/db/storage/mmap_v1/bltree/page.h"
#else
#include "bufmgr.h"
#include "blterr.h"
#include "bltkey.h"
#include "common.h"
#include "latchmgr.h"
#include "logger.h"
#include "page.h"
#include <assert.h>
#endif

#include <errno.h>
#include <fcntl.h>
#include <memory.h>
#include <stdlib.h>
#include <sstream>
#include <sys/mman.h>

#define BUFMGR_TRACE    false

namespace mongo {

    BufferMgr* BufferMgr::create( const char* name,
                                  uint bits0,
                                  uint poolMax,
                                  uint segBits,
                                  uint hashSize ) {
    
        if (BUFMGR_TRACE) Logger::logDebug( "main", "", __LOC__ );

		uassert( -1, "db name NULL", NULL != name );

        // determine sanity of page size and buffer pool
        uint bits = bits0;
        if (bits > BLT_maxbits) {
            __OSS__( "defaulting to BLT_maxbits = " << BLT_maxbits );
            Logger::logInfo( "main", __ss__, __LOC__ );
            bits = BLT_maxbits;
        }
        else if( bits < BLT_minbits ) {
            __OSS__( "defaulting to BLT_minbits = " << BLT_minbits );
            Logger::logInfo( "main", __ss__, __LOC__ );
            bits = BLT_minbits;
        }
    
        if (!poolMax) {
            Logger::logError( "main", "must specify buffer pool size.  bailing out.", __LOC__ );
            return NULL;    // must have buffer pool
        }
    
		// allocate buffer manager
        BufferMgr* mgr = (BufferMgr*)calloc( 1, sizeof(BufferMgr) );    // zero init

        int fd = open( name, O_RDWR | O_CREAT, 0666 );
        if (-1 == fd) {
            __OSS__( "open( " << name << " ) syserr: " << strerror(errno) );
            Logger::logError( "main", __ss__, __LOC__ );
            free(mgr);
            return NULL;
        }
        mgr->_fd = fd;

		// allocate latch manager
        LatchMgr* latchMgr = (LatchMgr*)malloc( BLT_maxpage );  // (e.g.) 16MB

        // read basic allocation metadata from file, if present
        off_t fileSize = lseek( fd, 0L, SEEK_END );
        if (fileSize) {
            if (BLT_minpage == pread( fd, latchMgr, BLT_minpage, 0 )) {
                bits = latchMgr->_alloc->_bits;
            }
            else {
                __OSS__( "pread( " << name << " ) syserr: " << strerror(errno) );
                Logger::logError( "main", __ss__, __LOC__ );
                free(mgr);
                free(latchMgr);
                return NULL;
            }
        }
    
        uint pageSize = (1 << bits);
        mgr->_pageSize = pageSize;
        mgr->_pageBits = bits;
        mgr->_poolMax  = poolMax;

        // _poolMask maps pageNo to base pageNo of PoolEntry segment
        mgr->_poolMask = (1 << segBits) - 1;	// low order segBits bits set
        mgr->_segBits  = segBits;
        mgr->_hashSize = hashSize;

        mgr->_pool  = (PoolEntry*) calloc( poolMax, sizeof(PoolEntry) );	// zero init
        mgr->_hash  =    (ushort*) calloc( hashSize, sizeof(ushort) );      //  "    "
        mgr->_latch = (SpinLatch*) calloc( hashSize, sizeof(SpinLatch) );   //  "    "

        mgr->_zero  = (Page*)malloc( pageSize ); 
        memset( mgr->_zero, 0, pageSize );
    
        if (fileSize) {
            if (!mgr->mapLatches( "main" )) return NULL;
            free( latchMgr );
            return mgr;
        }
    
        // initialize an empty bltree with
        //   [0] latch page
        //   [1] root page
        //   [2] page of leaves
        //   [3..k] page(s) of latches

        memset( latchMgr, 0, pageSize );

        uint latchSetsPerPage = pageSize / sizeof(LatchSet);
        uint nlatchPage = BLT_latchtableSize / latchSetsPerPage + 1; 	// round up

        Page::putPageNo( latchMgr->_alloc->_right, MIN_level + 1 + nlatchPage );	// first free page

        latchMgr->_alloc->_bits = bits;
        latchMgr->_nlatchPage = nlatchPage;
        latchMgr->_latchTotal = nlatchPage * latchSetsPerPage;
    
        // initialize latch manager hash table : # hash entries available in rest of page 0
        uint latchHashSize = (pageSize - sizeof(LatchMgr)) / sizeof(HashEntry);
    
        // size of hash table = total number of latchsets : #latches may actually be smaller, use that
        if (latchHashSize > latchMgr->_latchTotal) {
            latchHashSize = latchMgr->_latchTotal;
        }
    
        latchMgr->_latchHashSize = latchHashSize;
    
        if (write( fd, latchMgr, pageSize ) < pageSize) {	// latches available as shared  memory
            __OSS__( "write( " << name << " ) syserr: " << strerror(errno) );
            Logger::logError( "main", __ss__, __LOC__ );
            mgr->close( "main" );
            return NULL;
        }
    
        memset( latchMgr, 0, pageSize );
        latchMgr->_alloc->_bits = bits;
    
		// initialize root page and leaf page
        for (uint level = MIN_level; level--; ) {
            Page::slotptr(latchMgr->_alloc, 1)->_off = pageSize - 3;	// top 3 bytes are +infty key
            Page::putPageNo( Page::slotptr(latchMgr->_alloc, 1)->_id,	// level==0 -> leaf, level==1 -> root, initially
                            	level ? MIN_level - level + 1 : 0); 	// next docid, (e.g.) 2, then 0
            BLTKey* key = Page::keyptr(latchMgr->_alloc, 1);
            key->_len = 2;        	// create stopper key: +infty
            key->_key[0] = 0xff;
            key->_key[1] = 0xff;

            latchMgr->_alloc->_min = pageSize - 3;
            latchMgr->_alloc->_level = level;
            latchMgr->_alloc->_cnt = 1;
            latchMgr->_alloc->_act = 1;

            if (write(fd, latchMgr, pageSize) < pageSize) {
                __OSS__( "write( " << name << " ) syserr: " << strerror(errno) );
                Logger::logError( "main", __ss__, __LOC__ );
                mgr->close( "main" );
                return NULL;
            }
        }
    
        // clear out latch manager locks
        //   and rest of pages to round out segment
        memset( latchMgr, 0, pageSize);
        uint last = MIN_level + 1;		// (e.g.) 3
    
        while (last <= ((MIN_level + 1 + nlatchPage) | mgr->_poolMask)) {	// (i.e.) entire first segment
            pwrite( fd, latchMgr, pageSize, (last << bits) );		// write one page
            ++last;
        }

        if (!mgr->mapLatches( "main" )) return NULL;
        free( latchMgr );
        return mgr;
    }

    #define MAPLATCHES_TRACE    false

	// note: bufmgr is shared by all threads, hence these mmap's are not colliding
    bool BufferMgr::mapLatches( const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );
 
        int prot = PROT_READ | PROT_WRITE;

        uint  latchMgrSize   = _pageSize;
        off_t latchMgrOffset = ALLOC_page * _pageSize;

        _latchMgr = (LatchMgr *)mmap( 0, latchMgrSize, prot, MAP_SHARED, _fd, latchMgrOffset );

        if (_latchMgr == MAP_FAILED) {
            __OSS__( "mmap failed on 'alloc' page, syserr: " << strerror(errno) );
            Logger::logError( thread, __ss__, __LOC__ );
            close( thread );
            return false;
        }

        uint  latchSetsSize   = _latchMgr->_nlatchPage * _pageSize;
        off_t latchSetsOffset =  LATCH_page * _pageSize;

        _latchMgr->_latchSets =  (LatchSet *)mmap( 0, latchSetsSize, prot, MAP_SHARED, _fd, latchSetsOffset );

        if (MAP_FAILED == _latchMgr->_latchSets) {
            __OSS__( "mmap failed on 'latch' page, syserr: " << strerror(errno) );
            Logger::logError( thread, __ss__, __LOC__ );
            close( thread );
            return false;
        }

		madvise( _latchMgr->_latchSets, _latchMgr->_nlatchPage << _pageBits,
					MADV_RANDOM | MADV_WILLNEED );

        return true;
    }

    void BufferMgr::close( const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        // release mapped pages. note: slot zero is never used
        // 		slot 0 == NULL slot, slot 0 is vacant
        for (uint slot = 1; slot < _poolMax; ++slot) {
            PoolEntry* pool = &_pool[ slot ];
            if (pool->_slot) {
                munmap( pool->_map, (_poolMask+1) << _pageBits );
            }
        }
    
        munmap( _latchMgr->_latchSets, _latchMgr->_nlatchPage * _pageSize );
        munmap( _latchMgr, _pageSize );

        ::close( _fd );
        free( _pool );
        free( _hash );
        free( (void *)_latch );
    }
    
    /**
    *  Find segment in pool
    */
    PoolEntry* BufferMgr::findPoolEntry( PageNo pageNo, uint hashIndex, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        // compute start of hash chain
        uint slot = _hash[ hashIndex ];
        if (!slot) return NULL;

        PoolEntry* pool = &_pool[ slot ];		// XX PoolEntry -> rename --> Segment
        pageNo &= ~(_poolMask);

        while (pool->_basePage != pageNo) {
            if ( (pool = (PoolEntry *)pool->_hashNext) ) continue;
            return NULL;
        }
        return pool;
    }

    /**
    *  Add a segment to the hash table
    */
	void BufferMgr::linkHash( PoolEntry* pool, PageNo pageNo, int hashIndex, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

		uassert( -1, "NULL == pool", NULL != pool );

	    pool->_hashPrev = pool->_hashNext = NULL;
	    pool->_basePage = pageNo & ~(_poolMask);
	    pool->_pin = CLOCK_bit + 1;
	
	    uint slot = _hash[ hashIndex ];
	    if (slot) {
	        PoolEntry* node = &_pool[ slot ];
	        pool->_hashNext = node;
	        node->_hashPrev = pool;
	    }
	    _hash[ hashIndex ] = pool->_slot;
	}
	
	BLTERR BufferMgr::mapSegment( PoolEntry* pool, PageNo pageNo, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        // ex. ( 0..31 & ~31) << 15 => 0
        // ex. (32..63 & ~31) << 15 => 1048576, etc.
        // (i.e.) each segment of 32 32KB pages == 1MB
	    off_t  segOffset = (pageNo & ~_poolMask) << _pageBits;

        // ex.
        size_t segLength = (_poolMask + 1) << _pageBits;

	    int prot = PROT_READ | PROT_WRITE;

	    if (MAP_FAILED == (pool->_map = (char *)mmap( NULL, segLength, prot, MAP_SHARED, _fd, segOffset ))) {
            __OSS__( "mmap segment " << pageNo << " failed" );
            Logger::logDebug( thread, __ss__, __LOC__ );
	        return BLTERR_map;
        }
		madvise( pool->_map, segLength, MADV_SEQUENTIAL );

	    return BLTERR_ok;
	}

	Page* BufferMgr::page( PoolEntry* pool, PageNo pageNo, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

		uassert( -1, "NULL == pool", NULL != pool );

	    uint subpage = (uint)(pageNo & _poolMask); // page within mapping
	    return (Page*)(pool->_map + (subpage << _pageBits));
	}


	void BufferMgr::unpinPoolEntry( PoolEntry *pool, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

		uassert( -1, "NULL == pool", NULL != pool );

	    __sync_fetch_and_add( &pool->_pin, -1 );
	}
	
	PoolEntry* BufferMgr::pinPoolEntry( PageNo pageNo, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

	    // lock hash table chain
	    uint hashIndex = (uint)(pageNo >> _segBits) % _hashSize;
        uint n = 0;
	    if ( (n = SpinLatch::spinWriteLock( &_latch[ hashIndex ], thread )) ) {
            __OSS__( "spinWriteLock retry overflow on thread [" << thread << "] = " << n );
            Logger::logDebug( "main", __ss__, __LOC__ ); 
        }
	
	    // look up in hash table
	    PoolEntry* pool = findPoolEntry( pageNo, hashIndex, thread );
	    if (pool) {
	        __sync_fetch_and_or( &pool->_pin, CLOCK_bit);		// XX -> check this for thread safety
	        __sync_fetch_and_add( &pool->_pin, 1);
	        SpinLatch::spinReleaseWrite( &_latch[ hashIndex ], thread );
	        return pool;
	    }
	
	    // allocate a new pool node, and add to hash table
	    uint slot = __sync_fetch_and_add(&_poolCnt, 1);
	
	    if (++slot < _poolMax) {
	        pool = &_pool[ slot ];
	        pool->_slot = slot;
	
	        if (mapSegment( pool, pageNo, thread )) {
                __OSS__( "mapSegment " << pageNo << " failed" );
                Logger::logError( thread, __ss__, __LOC__ );
	            return NULL;
            }
	
	        linkHash( pool, pageNo, hashIndex, thread );
	        SpinLatch::spinReleaseWrite( &_latch[ hashIndex ], thread );
	        return pool;
	    }
	
	    // pool table is full: find best pool entry to evict
	    __sync_fetch_and_add( &_poolCnt, -1 );
	
	    while (true) {
	        uint victim = __sync_fetch_and_add( &_evicted, 1 );
	        victim %= _poolMax;
	        pool = &_pool[ victim ];
	        uint i = (uint)(pool->_basePage >> _segBits) % _hashSize;
	
	        if (!victim) continue;
	
	        // try to get write lock, skip entry if not obtained
	        if (!SpinLatch::spinTryWrite( &_latch[i], thread )) continue;
	
	        //    skip this entry if page is pinned or clock bit is set
	        if (pool->_pin) {
	            __sync_fetch_and_and( &pool->_pin, (ushort)~CLOCK_bit );
	            SpinLatch::spinReleaseWrite( &_latch[i], thread );
	            continue;
	        }
	
	        // unlink victim pool node from hash table
            PoolEntry* node;
	        if ( (node = (PoolEntry *)pool->_hashPrev) ) {
	            node->_hashNext = pool->_hashNext;
            }
	        else if( (node = (PoolEntry *)pool->_hashNext) ) {
	            _hash[i] = node->_slot;
            }
	        else {
	            _hash[i] = 0;
            }
	
	        if ( (node = (PoolEntry *)pool->_hashNext) ) {
	            node->_hashPrev = pool->_hashPrev;
            }
	
	        SpinLatch::spinReleaseWrite( &_latch[i], thread );
	
	        // remove old file mapping
	        munmap( pool->_map, (_poolMask+1) << _pageBits );
	        pool->_map = NULL;
	
	        // create new pool mapping and link into hash table
	        if (mapSegment( pool, pageNo, thread )) {
                __OSS__( "mapSegment " << pageNo << " failed" );
                Logger::logError( thread, __ss__, __LOC__ );
                return NULL;
            }
	
	        linkHash( pool, pageNo, hashIndex, thread );
	        SpinLatch::spinReleaseWrite( &_latch[ hashIndex ], thread );
	        return pool;

	    }   // end while
	}

    /**
    *  place write, read, or parent lock on requested page
    */
    void BufferMgr::lockPage( BLTLockMode lockMode, LatchSet* set, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

		uassert( -1, "NULL == set", NULL != set );
        uint n = 0;

        switch( lockMode ) {
        case LockRead:   {
            if ( (n = SpinLatch::spinReadLock( set->_readwr, thread )) ) {
                __OSS__( "spinReadLock retry overflow on thread [" << thread << "] = " << n );
                Logger::logDebug( "main", __ss__, __LOC__ ); 
            }
            break; 
        }
        case LockWrite:  {
            if ( (n = SpinLatch::spinWriteLock( set->_readwr, thread )) ) {
                __OSS__( "spinWriteLock retry overflow on thread [" << thread << "] = " << n );
                Logger::logDebug( "main", __ss__, __LOC__ ); 
            }
            break;
        }
        case LockAccess: {
            if ( (n = SpinLatch::spinReadLock( set->_access, thread )) ) {
                __OSS__( "spinReadLock retry overflow on thread [" << thread << "] = " << n );
                Logger::logDebug( "main", __ss__, __LOC__ ); 
            }
            break;
        }
        case LockDelete: {
            if ( (n = SpinLatch::spinWriteLock( set->_access, thread )) ) {
                __OSS__( "spinWriteLock retry overflow on thread [" << thread << "] = " << n );
                Logger::logDebug( "main", __ss__, __LOC__ ); 
            }
            break;
        }
        case LockParent: {
            if ( (n = SpinLatch::spinWriteLock( set->_parent, thread )) ) {
                __OSS__( "spinWriteLock retry overflow on thread [" << thread << "] = " << n );
                Logger::logDebug( "main", __ss__, __LOC__ ); 
            }
            break;
        }
        }
    }
    
    /**
    *  remove write, read, or parent lock on requested page
    */
    void BufferMgr::unlockPage( BLTLockMode lockMode, LatchSet* set, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

		uassert( -1, "NULL == set", NULL != set );

        switch (lockMode) {
        case LockRead:   {
            SpinLatch::spinReleaseRead( set->_readwr, thread );
            break;
        }
        case LockWrite:  {
            SpinLatch::spinReleaseWrite( set->_readwr, thread );
            break;
        }
        case LockAccess: {
            SpinLatch::spinReleaseRead( set->_access, thread );
            break;
        }
        case LockDelete: {
            SpinLatch::spinReleaseWrite( set->_access, thread );
            break;
        }
        case LockParent: {
            SpinLatch::spinReleaseWrite( set->_parent, thread );
            break;
        }
        }
    }

    /**
    * allocate a new page and write page into it
    */
    PageNo BufferMgr::newPage( Page* inputPage, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );
    
		uassert( -1, "NULL == inputPage", NULL != inputPage );

        // lock allocation page
        uint n = 0;
        if ( (n = SpinLatch::spinWriteLock( _latchMgr->_lock, thread )) ) {
            __OSS__( "spinWriteLock retry overflow on thread [" << thread << "] = " << n );
            Logger::logDebug( "main", __ss__, __LOC__ ); 
        }
    
        // use empty chain first, else allocate empty page
        PageSet set[1];
        int reuse;

        PageNo newPage = Page::getPageNo( _latchMgr->_alloc[1]._right );

        if (newPage) {
            if ( (set->_pool = pinPoolEntry( newPage, thread )) ) {
                set->_page = page( set->_pool, newPage, thread );
            }
            else {
				SpinLatch::spinReleaseWrite( _latchMgr->_lock, thread );
                return 0;
            }
    
            Page::putPageNo( _latchMgr->_alloc[1]._right, Page::getPageNo( set->_page->_right ) );
            unpinPoolEntry( set->_pool, thread );
            reuse = 1;
        } else {
            newPage = Page::getPageNo( _latchMgr->_alloc->_right );
            Page::putPageNo( _latchMgr->_alloc->_right, newPage+1 );
            reuse = 0;
        }

        // unlock allocation latch
        //SpinLatch::spinReleaseWrite( _latchMgr->_lock, thread );

        if (pwrite( _fd, inputPage, _pageSize, newPage << _pageBits) < _pageSize) {
            __OSS__( "write new page syserr: " << strerror(errno) );
            Logger::logError( thread, __ss__, __LOC__ );
            _err = BLTERR_write;
			SpinLatch::spinReleaseWrite( _latchMgr->_lock, thread );
            return 0;
        }
    
        // if writing first page of pool block, zero last page in the block
        if (!reuse && (_poolMask > 0) && 0==(newPage & _poolMask)) {

            // use zero buffer to write zeros
            off_t off = (newPage | _poolMask) << _pageBits;
            if (pwrite( _fd, _zero, _pageSize, off ) < _pageSize ) {
                __OSS__( "write of zero page syserr: " << strerror(errno) );
                Logger::logError( thread, __ss__, __LOC__ );
				SpinLatch::spinReleaseWrite( _latchMgr->_lock, thread );
                return 0;
            }
        }

        // unlock allocation latch and return new page
        SpinLatch::spinReleaseWrite( _latchMgr->_lock, thread );
        return newPage;
    }

    #define LOADPAGE_TRACE  false

    int BufferMgr::loadPage( PageSet* set,
                             const uchar* key,
                             uint keylen,
                             uint level,
                             BLTLockMode inputMode,
                             const char* thread )
    {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );
    
		uassert( -1, "NULL == set", NULL != set );
		uassert( -1, "NULL == key", NULL != key );

        // start at root of btree and drill down
        PageNo pageNo     = ROOT_page;
        PageNo prevPageNo = 0;
        uint drill        = 0xff;   // current drill-down level, leaf level = 0

        BLTLockMode prevMode;
        LatchSet* prevLatch;
        PoolEntry* prevPoolEntry;

        do {
            // determine lock mode of drill level: LockRead until we find our level
            BLTLockMode lockMode = (drill == level) ? inputMode : LockRead; 

            set->_latch = _latchMgr->pinLatch( pageNo, thread );
            set->_pageNo = pageNo;

            // pin page contents
            if ( (set->_pool = pinPoolEntry( pageNo, thread )) ) {
                set->_page = page( set->_pool, pageNo, thread );
            }
            else {
                __OSS__( "pinPoolEntry failed for page: " << pageNo );
                Logger::logError( thread, __ss__, __LOC__ );
                return 0;
            }

        	// obtain access lock using lock chaining with Access mode
        	if (pageNo > ROOT_page) {
            	lockPage( LockAccess, set->_latch, thread );
        	}

            // release and unpin parent page
            if (prevPageNo) {
                unlockPage( prevMode, prevLatch, thread );
                _latchMgr->unpinLatch( prevLatch, thread );
                unpinPoolEntry( prevPoolEntry, thread );
                prevPageNo = 0;
            }

            // obtain read lock using lock chaining
            lockPage( lockMode, set->_latch, thread );

            if (set->_page->_free) {
                __OSS__( "expecting free page: " << pageNo );
                Logger::logError( thread, __ss__, __LOC__ );
                 _err = BLTERR_struct;
                return 0;
            }

			// release access lock
        	if (pageNo > ROOT_page) {
            	unlockPage( LockAccess, set->_latch, thread );
        	}

            // re-read and re-lock root after finding root level
            // Q: please explain
            if (set->_page->_level != drill) {
                if (set->_pageNo != ROOT_page) {
                    __OSS__( "level!=drill  on page: " << set->_pageNo );
                    Logger::logError( thread, __ss__, __LOC__ );
                    _err = BLTERR_struct;
                    return 0;
                }
            
                drill = set->_page->_level;

                if (inputMode != LockRead && drill == level) {
                    unlockPage( lockMode, set->_latch, thread );
                    _latchMgr->unpinLatch( set->_latch, thread );
                    unpinPoolEntry( set->_pool, thread );
                    continue;
                }
            }

            prevPageNo = set->_pageNo;
            prevLatch  = set->_latch;
            prevPoolEntry   = set->_pool;
            prevMode   = lockMode;

            // find key on page at this level, descend to requested level
            if (!set->_page->_kill) {

                // find separator or leaf key slot
                uint slot = findSlot( set, key, keylen, thread );

                if (slot) {
                    if (drill == level) return slot;

                    while (Page::slotptr(set->_page, slot)->_dead) {
                        if (slot++ < set->_page->_cnt) {
                            continue;
                        }
                        else {
                            goto slideright;
                        }
                    }

                    pageNo = Page::getPageNo( Page::slotptr(set->_page, slot)->_id );
                    if (LOADPAGE_TRACE) {
                        __OSS__( "loadPage: next pageNo = " << pageNo );
                        Logger::logDebug( thread, __ss__, __LOC__ );
                    }
                    drill--;
                    continue;
                }
            }

	slideright: //  or slide right into next page
            pageNo = Page::getPageNo( set->_page->_right );

        } while (pageNo);

        // return error on end of right chain
        _err = BLTERR_struct;
        return 0; 
    }

    /**
    *  find slot in page for given key at a given level
    */
    int BufferMgr::findSlot( PageSet *set, const uchar* key, uint keylen, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

		uassert( -1, "NULL == set", NULL != set );
		uassert( -1, "NULL == key", NULL != key );

        uint diff;
        uint higher = set->_page->_cnt;
        uint low = 1;
        uint slot;
        uint good = 0;

        // make stopper key an infinite fence value
        if (Page::getPageNo( set->_page->_right )) {
            higher++;
        }
        else {
            good++;
        }

        // low is the lowest candidate; loop ends when they meet
        //  higher is already tested >= the passed key.
        while ((diff = higher - low)) {
            slot = low + (diff >> 1);
            if (BLTKey::keycmp( Page::keyptr(set->_page, slot), key, keylen) < 0) {
                low = slot + 1;
            }
            else {
                higher = slot;
                good++;
            }
        }

        // return zero if key is on right link page
        return (good ? higher : 0);
    }

    /**
    *  Read page from permanent location in BLTIndex file
    */
    int BufferMgr::readPage( Page* page, PageNo pageNo, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

		uassert( -1, "NULL == page", NULL != page );
        
        off_t off = (pageNo << _pageBits);
        uint32_t pageSize = (1 << _pageBits);

        if (pread( _fd, page, pageSize, off ) < pageSize) {
            __OSS__( "Error reading page " << pageNo << ", syserr = " << strerror(errno) );
            Logger::logError( thread, __ss__, __LOC__ );
            return BLTERR_read;
        }
        return 0;
    }       
            
    /**         
    *  Write page to permanent location in BLTIndex file.
    *  Clear the dirty bit
    */
    int BufferMgr::writePage( Page* page, PageNo pageNo, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

		uassert( -1, "NULL == page", NULL != page );

        off_t off = (pageNo << _pageBits);
        uint32_t pageSize = (1 << _pageBits);

        page->_dirty = 0;
        if (pwrite( _fd, page, pageSize, off) < pageSize) {
            __OSS__( "Error writing page " << pageNo << ", syserr = " << strerror(errno) );
            Logger::logError( thread, __ss__, __LOC__ );
            return BLTERR_write;
        }
        else {
            __OSS__( "Writing page " << pageNo << ", thread [" << thread << "]" );
            Logger::logDebug( "main", __ss__, __LOC__ );
        }
        return 0;
    }

    /**
    *  return page to free list
    *  page must be delete anad write locked
    */
    void BufferMgr::freePage( PageSet* set, const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

		uassert( -1, "NULL == set", NULL != set );

        // lock allocation page
        uint n = 0;
        if ( (n = SpinLatch::spinWriteLock( _latchMgr->_lock, thread )) ) {
            __OSS__( "spinWriteLock retry overflow on thread [" << thread << "] = " << n );
            Logger::logDebug( "main", __ss__, __LOC__ ); 
        }

        // store chain in second right
        Page::putPageNo( set->_page->_right, Page::getPageNo( _latchMgr->_alloc[1]._right ) );
        Page::putPageNo( _latchMgr->_alloc[1]._right, set->_pageNo);
        set->_page->_free = 1;

        // unlock released page
        unlockPage( LockDelete, set->_latch, thread );
        unlockPage( LockWrite, set->_latch, thread );
        _latchMgr->unpinLatch( set->_latch, thread );
        unpinPoolEntry( set->_pool, thread );

        // unlock allocation page
        SpinLatch::spinReleaseWrite( _latchMgr->_lock, thread );
    }

    /**
    *
    */
    std::string BufferMgr::decodeLastErr() const {
        return mongo::bltstrerror( _err );
    }

    #define LATCHAUDIT_TRACE    false

    /**
    *
    */
    void BufferMgr::latchAudit( const char* thread ) {
        if (BUFMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        if (*(uint *)(_latchMgr->_lock)) {
            Logger::logDebug( thread, "Alloc page locked", __LOC__ );
        }
        *(uint *)(_latchMgr->_lock) = 0;
    
        for (ushort idx = 1; idx <= _latchMgr->_latchDeployed; idx++ ) {

            LatchSet* latchSet = &_latchMgr->_latchSets[ idx ];
            PageNo pageNo = latchSet->_pageNo;

            if (*(uint *)latchSet->_readwr ) {
                __OSS__( "latchset " << idx << " rw locked for page " << pageNo );
                Logger::logDebug( thread, __ss__, __LOC__ );
            }
            *(uint *)latchSet->_readwr = 0;
    
            if (*(uint *)latchSet->_access ) {
                __OSS__( "latchset " << idx << " access locked for page " << pageNo );
                Logger::logDebug( thread, __ss__, __LOC__ );
            }
            *(uint *)latchSet->_access = 0;
    
            if (*(uint *)latchSet->_parent ) {
                __OSS__( "latchset " << idx << " parent locked for page " << pageNo );
                Logger::logDebug( thread, __ss__, __LOC__ );
            }
            *(uint *)latchSet->_parent = 0;
    
            if (latchSet->_pin ) {
                __OSS__( "latchset " << idx << " pinned for page " << pageNo );
                Logger::logDebug( thread, __ss__, __LOC__ );
                latchSet->_pin = 0;
            }
        }
    
        for (ushort hashidx = 0; hashidx < _latchMgr->_latchHashSize; hashidx++ ) {
            if (*(uint *)(_latchMgr->_table[hashidx]._latch) ) {
                __OSS__( "hash entry " << hashidx << " locked" );
                Logger::logDebug( thread, __ss__, __LOC__ );
            }
            *(uint *)(_latchMgr->_table[hashidx]._latch) = 0;
    
            uint idx = _latchMgr->_table[hashidx]._slot;
            if (idx) {
                LatchSet* latchSet;
                do {
                    latchSet = &_latchMgr->_latchSets[ idx ];
                    PageNo pageNo = latchSet->_pageNo;

                    if (*(uint *)latchSet->_busy ) {
                        __OSS__( "latchset " << idx << " busy locked for page " << pageNo );
                        Logger::logDebug( thread, __ss__, __LOC__ );
                    }
                    *(uint *)latchSet->_busy = 0;
                    if (latchSet->_hash != hashidx ) {
                        __OSS__( "latchset " << idx << " wrong hashidx " );
                        Logger::logDebug( thread, __ss__, __LOC__ );
                    }
                    if (latchSet->_pin ) {
                        __OSS__( "latchset " << idx << " pinned for page " << pageNo );
                        Logger::logDebug( thread, __ss__, __LOC__ );
                    }
                } while ((idx = latchSet->_next));
            }
        }
    
        PageNo next   = _latchMgr->_nlatchPage + LATCH_page;
        PageNo pageNo = LEAF_page;
        Page* _frame  = (Page *)malloc( _pageSize );
    
        while (pageNo < Page::getPageNo(_latchMgr->_alloc->_right)) {
            pread( _fd, _frame, _pageSize, pageNo << _pageBits );
            if (!_frame->_free) {
                for (uint idx = 0; idx++ < _frame->_cnt - 1; ) {
                    BLTKey* key = Page::keyptr(_frame, idx+1);
                    if (BLTKey::keycmp( Page::keyptr(_frame, idx), key->_key, key->_len ) >= 0) {
                        __OSS__( "page " << pageNo << " idx" << idx << " out of order" );
                        Logger::logDebug( thread, __ss__, __LOC__ );
                    }
                }
            }
    
            if (pageNo > LEAF_page) next = pageNo + 1;
            pageNo = next;
        }
    }

}   // namespace mongo
