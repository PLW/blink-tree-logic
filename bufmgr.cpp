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
 * This is a derivative work.  The original 'C' source
 * code was put in the public domain by Karl Malbrain
 * (malbrain@cal.berkeley.edu.  The original copyright
 * notice is:
 *
 *     This work, including the source code, documentation
 *     and related data, is placed into the public domain.
 *
 *     The orginal author is Karl Malbrain.
 *
 *     THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY
 *     OF ANY KIND, NOT EVEN THE IMPLIED WARRANTY OF
 *     MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE,
 *     ASSUMES _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE
 *     RESULTING FROM THE USE, MODIFICATION, OR
 *     REDISTRIBUTION OF THIS SOFTWARE.
 *
 */

#ifndef STANDALONE
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "mongo/db/storage/bltree/blterr.h"
#include "mongo/db/storage/bltree/bltkey.h"
#include "mongo/db/storage/bltree/bltval.h"
#include "mongo/db/storage/bltree/bufmgr.h"
#include "mongo/db/storage/bltree/common.h"
#include "mongo/db/storage/bltree/latchmgr.h"
#include "mongo/db/storage/bltree/page.h"
#else
#include "blterr.h"
#include "bufmgr.h"
#include "common.h"
#include "latchmgr.h"
#include "page.h"
#include <assert.h>
#endif

#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <memory.h>
#include <sstream>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

namespace mongo {

    /**
    *   factory method
    *   open/create new BufMgr
    *   
    *   @param name  -  file name
    *   @param bits  -  page size in bits
    *   @param nodemax  -  size of page pool
    */
    BufMgr* BufMgr::create( const char* name, uint bits, uint nodemax ) {
        int flag;               // used for mmap flags
        bool initit = false;    // true => initialize new db
        PageZero* pagezero;     // page_no == 0, the metadata page
        off64_t size;           // file size
        BufMgr* mgr;            // return value
    
    	// determine sanity of page size
    	if (bits > MAXBITS) { bits = MAXBITS; }
    	else if (bits < MINBITS) { bits = MINBITS; }
    
    	// determine sanity of buffer pool
    	if (nodemax < 16) {
    		std::cerr << "Buffer pool too small: " << nodemax << std::endl;
    		return NULL;
    	}
    
    #ifdef unix
    	mgr = (BufMgr*)calloc( 1, sizeof(BufMgr) );
    	mgr->idx = open( (char*)name, O_RDWR | O_CREAT, 0666 );
    
    	if (-1 == mgr->idx) {
    		std::cerr << "Unable to open btree file" << std::endl;
    		free( mgr );
    		return NULL;
    	}
    #else
    	mgr = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, sizeof(BufMgr));
    	uint attr = FILE_ATTRIBUTE_NORMAL;
    	mgr->idx = CreateFile(name, GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ|FILE_SHARE_WRITE,
                                NULL, OPEN_ALWAYS, attr, NULL);
    
    	if (INVALID_HANDLE_VALUE == mgr->idx) {
    		GlobalFree( mgr );
    		return NULL;
        }
    #endif
    
    #ifdef unix
    	pagezero = (PageZero*)valloc( MAXPAGE );
    
    	// read minimum page size to get root info
    	//	to support raw disk partition files
    	//	check if bits == 0 on the disk.
    	if ( (size = lseek( mgr->idx, 0L, 2 )) ) {
    		if (pread( mgr->idx, pagezero, MINPAGE, 0 ) == MINPAGE) {
    			if (pagezero->alloc->bits) {
    				bits = pagezero->alloc->bits;
                }
    			else {
    				initit = true;
                }
            }
    		else {
    			free( mgr );
    			free( pagezero );
    			return NULL;
            }
        }
    	else {
    		initit = true;
        }
    #else
        uint amt[1];
    	*amt = 0;
    	pagezero = VirtualAlloc( NULL, MAXPAGE, MEM_COMMIT, PAGE_READWRITE );
    	size = GetFileSize( mgr->idx, amt );
    
    	if (size || *amt) {
    		if (!ReadFile( mgr->idx, (char *)pagezero, MINPAGE, amt, NULL )) {
    			mgr->close();
    			return NULL;
            }
    		bits = pagezero->alloc->bits;
    	} else {
    		initit = true;
        }
    #endif
    
    	mgr->page_size = 1 << bits;
    	mgr->page_bits = bits;
    
    	// calculate number of latch hash table entries
    	mgr->nlatchpage = (nodemax/16 * sizeof(HashEntry) + mgr->page_size - 1) / mgr->page_size;
    	mgr->latchhash  = ((uid)mgr->nlatchpage << mgr->page_bits) / sizeof(HashEntry);
    
    	mgr->nlatchpage += nodemax;		// size of the buffer pool in pages
    	mgr->nlatchpage += (sizeof(LatchSet) * nodemax + mgr->page_size - 1)/mgr->page_size;
    	mgr->latchtotal  = nodemax;
    
    	if (!initit) goto mgrlatch;
    
    	// initialize an empty bltree with latch page, root page, page of leaves
    	// and page(s) of latches and page pool cache
    	memset( pagezero, 0, 1 << bits );
    	pagezero->alloc->bits = mgr->page_bits;
    	BLTVal::putid( pagezero->alloc->right, MIN_lvl+1 );
    
    	if (mgr->writepage( pagezero->alloc, 0 )) {
    		std::cerr << "Unable to create btree page zero" << std::endl;
    		mgr->close();
    		return NULL;
    	}
    
    	memset( pagezero, 0, 1 << bits );
    	pagezero->alloc->bits = mgr->page_bits;
    
    	for (uint lvl=MIN_lvl; lvl--; ) {
    		uint z = (lvl ? BtId + sizeof(BLTVal) : sizeof(BLTVal));
    		slotptr(pagezero->alloc, 1)->off = mgr->page_size - 3 - z;
    		BLTKey* key = keyptr(pagezero->alloc, 1);
    		key->len = 2;		// create stopper key
    		key->key[0] = 0xff;
    		key->key[1] = 0xff;
    
            uchar value[BtId];
    		BLTVal::putid( value, MIN_lvl - lvl + 1 );
    		BLTVal* val = valptr(pagezero->alloc, 1);
    		val->len = lvl ? BtId : 0;
    		memcpy( val->value, value, val->len );
    
    		pagezero->alloc->min = slotptr(pagezero->alloc, 1)->off;
    		pagezero->alloc->lvl = lvl;
    		pagezero->alloc->cnt = 1;
    		pagezero->alloc->act = 1;
    
    		if (mgr->writepage( pagezero->alloc, MIN_lvl - lvl )) {
    			std::cerr << "Unable to create btree page zero" << std::endl;
    			mgr->close();
    			return NULL;
    		}
    	}
    
    mgrlatch:
    
    #ifdef unix
    	free( pagezero );
    #else
    	VirtualFree( pagezero, 0, MEM_RELEASE );
    #endif
    
    #ifdef unix
    	// mlock the pagezero page
    	flag = PROT_READ | PROT_WRITE;
    	mgr->pagezero = (PageZero*)mmap( 0, mgr->page_size, flag, MAP_SHARED, mgr->idx,
                                            ALLOC_page << mgr->page_bits );
    	if (MAP_FAILED == mgr->pagezero) {
    		std::cerr << "Unable to mmap btree page zero, error = "
                        << errno << std::endl;
    		mgr->close();
    		return NULL;
    	}
    	mlock( mgr->pagezero, mgr->page_size );
    
    	mgr->hashtable = (HashEntry *)mmap (0, (uid)mgr->nlatchpage << mgr->page_bits,
                                                flag, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
    	if (MAP_FAILED==mgr->hashtable) {
    		std::cerr << "Unable to mmap anonymous buffer pool pages, error = "
                        << errno << std::endl;
    		mgr->close();
    		return NULL;
    	}
    #else
    	flag = PAGE_READWRITE;
    	mgr->halloc = CreateFileMapping( mgr->idx, NULL, flag, 0, mgr->page_size, NULL );
    	if (!mgr->halloc) {
    		std::cerr << "Unable to create page zero memory mapping, error = "
                        << GetLastError() << std::endl;
    		mgr->close();
    		return NULL;
    	}
    
    	flag = FILE_MAP_WRITE;
    	mgr->pagezero = MapViewOfFile( mgr->halloc, flag, 0, 0, mgr->page_size );
    	if (!mgr->pagezero) {
    		std::cerr << "Unable to map page zero, error = "
                        << GetLastError() << std::endl;
    		mgr->close();
    		return NULL;
    	}
    
    	flag = PAGE_READWRITE;
    	size = (uid)mgr->nlatchpage << mgr->page_bits;
    	mgr->hpool = CreateFileMapping( INVALID_HANDLE_VALUE, NULL, flag, size >> 32, size, NULL );
    	if (!mgr->hpool) {
    		std::cerr << "Unable to create buffer pool memory mapping, error = "
                        << GetLastError() << std::endl;
    		mgr->close();
    		return NULL;
    	}
    
    	flag = FILE_MAP_WRITE;
    	mgr->hashtable = MapViewOfFile( mgr->pool, flag, 0, 0, size );
    	if (!mgr->hashtable) {
    		std::cerr << "Unable to map buffer pool, error = "
                        << GetLastError() << std::endl;
    		mgr->close();
    		return NULL;
    	}
    #endif
    
    	mgr->pagepool = (unsigned char *)mgr->hashtable
                            + ((uid)(mgr->nlatchpage - mgr->latchtotal) << mgr->page_bits);
    	mgr->latchsets = (LatchSet *)(mgr->pagepool - (uid)mgr->latchtotal * sizeof(LatchSet));
    	return mgr;
    }
    
    /**
    *  FUNCTION: readpage
    */
    BLTERR BufMgr::readpage( Page* page, uid page_no ) {

        off64_t off = page_no << page_bits;
    
    #ifdef unix
    	if (pread( idx, page, page_size, page_no << page_bits) < page_size ) {
    		std::cerr << "Unable to read page " << page_no << " errno = " << errno << std::endl;
    		return BLTERR_read;
    	}
    #else
        OVERLAPPED ovl[1];
        uint amt[1];
    
    	memset( ovl, 0, sizeof(OVERLAPPED) );
    	ovl->Offset = off;
    	ovl->OffsetHigh = off >> 32;
    
    	if (!ReadFile( idx, page, page_size, amt, ovl )) {
    		std::cerr << "Unable to read page " << page_no << " GetLastError = " << GetLastError() << std::endl;
    		return BLTERR_read;
    	}
    	if (*amt < page_size) {
    		std::cerr << "Unable to read page " << page_no << " GetLastError = " << GetLastError() << std::endl;
    		return BLTERR_read;
    	}
    #endif
    
    	return BLTERR_ok;
    }
    
    /**
    *  FUNCTION: writepage
    *
    *  write page to permanent location in BLTree file,
    *  and clear the dirty bit
    */
    BLTERR BufMgr::writepage( Page* page, uid page_no ) {
        off64_t off = page_no << page_bits;
    
    #ifdef unix
    	if (pwrite( idx, page, page_size, off) < page_size) {
    		return BLTERR_wrt;
        }
    #else
        OVERLAPPED ovl[1];
        uint amt[1];
    
    	memset( ovl, 0, sizeof(OVERLAPPED) );
    	ovl->Offset = off;
    	ovl->OffsetHigh = off >> 32;
    
    	if (!WriteFile( idx, page, page_size, amt, ovl )) {
    		return BLTERR_wrt;
        }
    
    	if (*amt <  page_size) {
    		return BLTERR_wrt;
        }
    #endif
    
    	return BLTERR_ok;
    }
    
    /**
    *  FUNCTION: close
    */
    void BufMgr::close() {
        LatchSet* latch;
        uint num = 0;
        Page* page;
    
    	// flush dirty pool pages to the btree
    	for (uint slot = 1; slot <= latchdeployed; slot++ ) {
    		page = (Page*)(((uid)slot << page_bits) + pagepool);
    		latch = latchsets + slot;
    
    		if (latch->dirty) {
    			writepage( page, latch->page_no );
    			latch->dirty = 0;
                num++;
    		}
            //madvise( page, page_size, MADV_DONTNEED );
    	}
    
    	std::cerr << num << " buffer pool pages flushed" << std::endl;
    
    #ifdef unix
    	munmap( hashtable, (uid)nlatchpage << page_bits );
    	munmap( pagezero, page_size );
    #else
    	FlushViewOfFile( pagezero, 0 );
    	UnmapViewOfFile( pagezero );
    	UnmapViewOfFile( hashtable );
    	CloseHandle( halloc );
    	CloseHandle( hpool );
    #endif
    
    #ifdef unix
    	::close( idx );
    	//free( mgr );
    #else
    	FlushFileBuffers( idx );
    	CloseHandle( idx );
    	//GlobalFree( mgr);
    #endif
    }
    
    /**
    *  FUNCTION: poolaudit
    */
    void BufMgr::poolaudit() {
        LatchSet *latch;
        uint slot = 0;
    
    	while( slot++ < latchdeployed ) {
    		latch = latchsets + slot;
    
    		if (*latch->readwr->rin & MASK) {
    			std::cerr << "latchset " << slot
                            << " rwlocked for page " << latch->page_no << std::endl;
            }
    		memset ((ushort *)latch->readwr, 0, sizeof(BLT_RWLock));
    
    		if (*latch->access->rin & MASK) {
    			std::cerr << "latchset " << slot
                            << " accesslocked for page " << latch->page_no << std::endl;
            }
    		memset( (ushort *)latch->access, 0, sizeof(BLT_RWLock) );
    
    		if (*latch->parent->rin & MASK) {
    			std::cerr << "latchset " << slot
                            << " parentlocked for page " << latch->page_no << std::endl;
            }
    		memset ((ushort *)latch->parent, 0, sizeof(BLT_RWLock));
    
    		if (latch->pin & ~CLOCK_bit) {
    			std::cerr << "latchset " << slot
                            << " pinned for page " << latch->page_no << std::endl;
    			latch->pin = 0;
    		}
    	}
    }

    /**
    *  FUNCTION: latchlink
    *
    *  Link latch table entry into head of latch hash table.
    */
    BLTERR BufMgr::latchlink( uint hashidx, uint slot, uid page_no,
                                uint load_it, uint* reads ) {
    
        Page* page = (Page*)(((uid)slot << page_bits) + pagepool);
        LatchSet* latch = latchsets + slot;
    
        if (latch->next = hashtable[hashidx].slot) {
            latchsets[latch->next].prev = slot;
        }
    
        hashtable[hashidx].slot = slot;
        memset( &latch->atomictid, 0, sizeof(latch->atomictid) );
        latch->page_no = page_no;
        latch->entry = slot;
        latch->split = 0;
        latch->prev = 0;
        latch->pin = 1;
    
        if (load_it) {
            if ( (err = readpage( page, page_no )) ) {
                return err;
            }
            else {
                *reads++;
            }
        }
        return (err = BLTERR_ok);
    }
    
    /**
    *  FUNCTION: mappage
    *
    *  @return cached page address
    */
    Page* BufMgr::mappage( LatchSet* latch ) {
        Page* page = (Page*)(((uid)latch->entry << page_bits) + pagepool);
        return page;
    }
    
    /**
    *  FUNCTION: pinlatch
    *
    *  find existing latchset or create new one
    *  @return with latchset pinned
    */
    LatchSet* BufMgr::pinlatch( uid page_no, uint load_it,
                                    uint* reads, uint* writes ) {
        uint hashidx = page_no % latchhash;
        LatchSet* latch;
    
        //  try to find our entry
        SpinLatch::spinwritelock( hashtable[hashidx].latch );
    
        uint slot = hashtable[hashidx].slot;
        if (slot) {
            do {
                latch = latchsets + slot;
                if (page_no == latch->page_no) break;
            } while ( (slot = latch->next) );
        }
    
        //  found our entry increment clock
        if (slot) {
            latch = latchsets + slot;
    
    #ifdef unix
            __sync_fetch_and_add( &latch->pin, 1 );
    #else
            _InterlockedIncrement16( &latch->pin );
    #endif
    
            SpinLatch::spinreleasewrite( hashtable[hashidx].latch );
            return latch;
        }
    
        //  see if there are any unused pool entries
    
    #ifdef unix
        slot = __sync_fetch_and_add( &latchdeployed, 1 ) + 1;
    #else
        slot = _InterlockedIncrement( &latchdeployed );
    #endif
    
        if (slot < latchtotal) {
            latch = latchsets + slot;
            if (latchlink( hashidx, slot, page_no, load_it, reads )) return NULL;
            SpinLatch::spinreleasewrite( hashtable[hashidx].latch );
            return latch;
        }
    
    #ifdef unix
        __sync_fetch_and_add( &latchdeployed, -1 );
    #else
        _InterlockedDecrement( &latchdeployed );
    #endif
    
        //  find and reuse previous entry on victim
        while (1) {
    
    #ifdef unix
            slot = __sync_fetch_and_add( &latchvictim, 1 );
    #else
            slot = _InterlockedIncrement( &latchvictim ) - 1;
    #endif
    
            // try to get write lock on hash chain 
            // skip entry if not obtained or has outstanding pins
            slot %= latchtotal;
    
            if (!slot) continue;
    
            latch = latchsets + slot;
            uint idx = latch->page_no % latchhash;
    
            // see we are on same chain as hashidx
            if (idx == hashidx) continue;
            if (!SpinLatch::spinwritetry( hashtable[idx].latch) ) continue;
    
            // skip this slot if it is pinned or the CLOCK bit is set
            if (latch->pin) {
                if (latch->pin & CLOCK_bit) {
    
    #ifdef unix
                    __sync_fetch_and_and( &latch->pin, ~CLOCK_bit );
    #else
                    _InterlockedAnd16( &latch->pin, ~CLOCK_bit );
    #endif
                }
                SpinLatch::spinreleasewrite( hashtable[idx].latch );
                continue;
            }
    
            //  update permanent page area in btree from buffer pool
            Page* page = (Page*)( ((uid)slot << page_bits) + pagepool );
    
            if (latch->dirty) {
                if (writepage( page, latch->page_no )) {
                    return NULL;
                }
                else {
                    latch->dirty = 0;
                    *writes++;
                }
            }
    
            //  unlink our available slot from its hash chain
            if (latch->prev) {
                latchsets[latch->prev].next = latch->next;
            }
            else {
                hashtable[idx].slot = latch->next;
            }
    
            if (latch->next) {
                latchsets[latch->next].prev = latch->prev;
            }
    
            SpinLatch::spinreleasewrite( hashtable[idx].latch );
            if (latchlink( hashidx, slot, page_no, load_it, reads )) return NULL;
            SpinLatch::spinreleasewrite( hashtable[hashidx].latch );
            return latch;
        }
    }

    /**
    *  FUNCTION: unpinlatch
    *
    *  set CLOCK bit in latch
    *  decrement pin count
    */
    void BufMgr::unpinlatch( LatchSet* latch) {
    #ifdef unix
	    if (~latch->pin & CLOCK_bit)
	    	__sync_fetch_and_or( &latch->pin, CLOCK_bit );
	        __sync_fetch_and_add( &latch->pin, -1 );
    #else
	    if (~latch->pin & CLOCK_bit)
		    _InterlockedOr16( &latch->pin, CLOCK_bit );
	        _InterlockedDecrement16( &latch->pin );
    #endif
    }

    /**
    *  FUNCTION: newpage
    *
    *  allocate a new page
    *  @return with page latched but unlocked
    */
    int BufMgr::newpage( PageSet* set, Page* contents,
                            uint* reads, uint* writes ) {
    
        // lock allocation page
        SpinLatch::spinwritelock( lock );
    
        // use empty chain first, else allocate empty page
        uid page_no = BLTVal::getid( pagezero->chain );
        if (page_no) {
            if (set->latch = pinlatch( page_no, 1, reads, writes )) {
                set->page = mappage( set->latch );
            }
            else {
                return (err = BLTERR_struct);
            }
    
            BLTVal::putid( pagezero->chain, BLTVal::getid(set->page->right) );
            SpinLatch::spinreleasewrite( lock );
            memcpy( set->page, contents, page_size );
            set->latch->dirty = 1;
            return (err = BLTERR_ok);
        }
    
        page_no = BLTVal::getid( pagezero->alloc->right );
        BLTVal::putid( pagezero->alloc->right, page_no+1 );
    
        // unlock allocation latch
        SpinLatch::spinreleasewrite( lock );
    
        // don't load cache from btree page
        if ( (set->latch = pinlatch( page_no, 0, reads, writes )) ) {
            set->page = mappage( set->latch );
        }
        else {
            return (err = BLTERR_struct);
        }
    
        memcpy( set->page, contents, page_size );
        set->latch->dirty = 1;
        return (err = BLTERR_ok);
    }
    
    /**
    *  FUNCTION:loadpage
    *
    *  find and load page at given level for given key
    *  leave page read or write locked as requested
    */
    int BufMgr::loadpage( PageSet* set, uchar* key, uint len, uint lvl, BLTLockMode lock,
                            uint* reads, uint* writes ) {
        uid page_no = ROOT_page;
        uid prevpage = 0;
        uint drill = 0xff;
        uint slot;
        LatchSet* prevlatch;

        uint mode;
        uint prevmode;
    
        // start at root of btree and drill down
        do {
    
            // determine lock mode of drill level
            mode = (drill == lvl) ? lock : LockRead; 
        
            if ( !(set->latch = pinlatch( page_no, 1, reads, writes )) ) {
              return 0;
            }
        
             // obtain access lock using lock chaining with Access mode
            if (page_no > ROOT_page) {
                lockpage( LockAccess, set->latch );
            }
        
            set->page = mappage( set->latch );
        
            // release & unpin parent page
            if (prevpage) {
              unlockpage( (BLTLockMode)prevmode, prevlatch );
              unpinlatch( prevlatch );
              prevpage = 0;
            }
        
            // skip Atomic lock on leaf page if already held
            if (!drill) {
                if (mode & LockAtomic) {
                    if (pthread_equal( set->latch->atomictid, pthread_self() )) {
                        mode &= ~LockAtomic;
                    }
                }
            }
        
             // obtain mode lock using lock chaining through AccessLock
            lockpage( (BLTLockMode)mode, set->latch );
        
            if (mode & LockAtomic) {
                set->latch->atomictid = pthread_self();
            }
        
            if (set->page->free) {
                err = BLTERR_struct;
                return 0;
            }
        
            if (page_no > ROOT_page) {
                unlockpage( LockAccess, set->latch );
            }
        
            // re-read and re-lock root after determining actual level of root
            if (set->page->lvl != drill) {
                if( set->latch->page_no != ROOT_page ) {
                    err = BLTERR_struct;
                    return 0;
                }
                    
                drill = set->page->lvl;
        
                if (lock != LockRead && drill == lvl) {
                    unlockpage( (BLTLockMode)mode, set->latch );
                    unpinlatch( set->latch );
                    continue;
                }
            }
        
            prevpage = set->latch->page_no;
            prevlatch = set->latch;
            prevmode = mode;
        
            //  find key on page at this level
            //  and descend to requested level
            if (set->page->kill) {
              goto slideright;
            }
        
            if (slot = Page::findslot( set->page, key, len )) {
                if (drill == lvl) {
                    return slot;
                }
        
                while (slotptr(set->page, slot)->dead) {
                    if (slot++ < set->page->cnt) {
                        continue;
                    }
                    else {
                        goto slideright;
                    }
                }
        
                page_no = BLTVal::getid( valptr(set->page, slot)->value );
                drill--;
                continue;
            }
        
        slideright: // slide right into next page
            page_no = BLTVal::getid( set->page->right );
    
        } while (page_no);
    
        // return error on end of right chain
        err = BLTERR_struct;
        return 0;
    }
    
    /**
    *  FUNCTION: freepage
    *
    *  return page to free list
    *  page must be delete and write locked
    */
    void BufMgr::freepage( PageSet* set ) {
    
        // lock allocation page
        SpinLatch::spinwritelock( lock );
    
        // store chain
        memcpy( set->page->right, pagezero->chain, BtId );
        BLTVal::putid( pagezero->chain, set->latch->page_no );
        set->latch->dirty = 1;
        set->page->free = 1;
    
        // unlock released page
        unlockpage( LockDelete, set->latch );
        unlockpage( LockWrite, set->latch );
        unpinlatch( set->latch );
    
        // unlock allocation page
        SpinLatch::spinreleasewrite( lock );
    }

	/**
    *  FUNCTION: lockpage
    *
	*  place write, read, or parent lock on requested page_no.
	*/
	void BufMgr::lockpage( BLTLockMode mode, LatchSet* latch ) {
		switch (mode) {
		case LockRead:
			BLT_RWLock::ReadLock( latch->readwr );
			break;
		case LockWrite:
			BLT_RWLock::WriteLock( latch->readwr );
			break;
		case LockAccess:
			BLT_RWLock::ReadLock( latch->access );
			break;
		case LockDelete:
			BLT_RWLock::WriteLock( latch->access );
			break;
		case LockParent:
			BLT_RWLock::WriteLock( latch->parent );
			break;
		case LockAtomic:
			BLT_RWLock::WriteLock( latch->atomic );
			break;
		case LockAtomic | LockRead:
			BLT_RWLock::WriteLock( latch->atomic );
			BLT_RWLock::ReadLock( latch->readwr );
			break;
		}
	}
	
	/**
    *  FUNCTION:  unlockpage
    *
	*  remove write, read, or parent lock on requested page
	*/
	void BufMgr::unlockpage( BLTLockMode mode, LatchSet* latch ) {
		switch (mode) {
		case LockRead:
			BLT_RWLock::ReadRelease( latch->readwr );
			break;
		case LockWrite:
			BLT_RWLock::WriteRelease( latch->readwr );
			break;
		case LockAccess:
			BLT_RWLock::ReadRelease( latch->access );
			break;
		case LockDelete:
			BLT_RWLock::WriteRelease( latch->access );
			break;
		case LockParent:
			BLT_RWLock::WriteRelease( latch->parent );
			break;
		case LockAtomic:
			memset( &latch->atomictid, 0, sizeof(latch->atomictid) );
			BLT_RWLock::WriteRelease( latch->atomic );
			break;
		case LockAtomic | LockRead:
			BLT_RWLock::ReadRelease( latch->readwr );
			memset( &latch->atomictid, 0, sizeof(latch->atomictid) );
			BLT_RWLock::WriteRelease( latch->atomic );
			break;
		}
	}


}   // namespace mongo

