
#ifndef STANDALONE
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "mongo/db/storage/mmap_v1/bltree/blterr.h"
#include "mongo/db/storage/mmap_v1/bltree/bltkey.h"
#include "mongo/db/storage/mmap_v1/bltree/bltval.h"
#include "mongo/db/storage/mmap_v1/bltree/bufmgr.h"
#include "mongo/db/storage/mmap_v1/bltree/common.h"
#include "mongo/db/storage/mmap_v1/bltree/latchmgr.h"
#include "mongo/db/storage/mmap_v1/bltree/page.h"
#else
#include "blterr.h"
#include "bltkey.h"
#include "bltval.h"
#include "bufmgr.h"
#include "common.h"
#include "latchmgr.h"
#include "page.h"
#include <assert.h>
#endif

#include <fcntl.h>
#include <memory.h>
#include <stdlib.h>
#include <stdio.h>
#include <sstream>
#include <sys/mman.h>
#include <unistd.h>

namespace mongo {

	//
	//  open/create new buffer manager
	//
	BufMgr* BufMgr::create( char *name,
                          uint mode,
                          uint bits,
                          uint poolmax,
                          uint segsize,
                          uint hashsize )
    {
	    uint lvl, cacheblk, last;
	    uint nlatchpage, latchhash;
	    uchar value[BtId];
	    LatchMgr* latchmgr;
	    off64_t size;
	    BufMgr* mgr;
	    BLTKey* key;
	    BLTVal* val;
	    int flag;
	
	    // determine sanity of page size and buffer pool
	    if (bits > BT_maxbits) {
	        bits = BT_maxbits;
        }
	    else if (bits < BT_minbits) {
	        bits = BT_minbits;
        }
	
	    if (!poolmax) return NULL;    // must have buffer pool
	
	    mgr = (BufMgr*)calloc( 1, sizeof(BufMgr) );
	    mgr->idx = open( (char*)name, O_RDWR | O_CREAT, 0666 );
	    if (mgr->idx == -1) {
            free( mgr );
            return NULL;
        }
	    
	    cacheblk = 4096;    // minimum mmap segment size for unix
	    latchmgr = (LatchMgr *)malloc( BT_maxpage );
	
	    // read minimum page size to get root info
	    if ( (size = lseek( mgr->idx, 0L, 2 )) ) {
	        if (pread( mgr->idx, latchmgr, BT_minpage, 0 ) == BT_minpage) {
	            bits = latchmgr->alloc->bits;
            }
	        else {
	            free( mgr );
	            free( latchmgr );
	            return NULL;
            }
	    } else if (mode == BT_ro) {
	        free( latchmgr );
	        BufMgr::destroy( mgr );
	        return NULL;
        }
	
	    mgr->page_size = 1 << bits;
	    mgr->page_bits = bits;
	    mgr->poolmax = poolmax;
	    mgr->mode = mode;
	
	    if (cacheblk < mgr->page_size) {
	        cacheblk = mgr->page_size;
        }
	
	    //  mask for partial memmaps
	    mgr->poolmask = (cacheblk >> bits) - 1;
	
	    //    see if requested size of pages per memmap is greater
	    if ( (1 << segsize) > mgr->poolmask ) {
	        mgr->poolmask = (1 << segsize) - 1;
        }
	
	    mgr->seg_bits = 0;
	
	    while( (1 << mgr->seg_bits) <= mgr->poolmask ) {
	        mgr->seg_bits++;
        }
	
	    mgr->hashsize = hashsize;
	    mgr->pool  = (PoolEntry*)calloc( poolmax, sizeof(PoolEntry) );
	    mgr->hash  = (ushort *)calloc( hashsize, sizeof(ushort) );
	    mgr->latch = (SpinLatch *)calloc( hashsize, sizeof(SpinLatch) );
	
	    if (size) goto mgrlatch;
	
	    // initialize an empty b-tree with latch page, root page,
	    //  page of leaves and page(s) of latches
	    memset( latchmgr, 0, 1 << bits );
	    nlatchpage = BT_latchtable / (mgr->page_size / sizeof(LatchSet)) + 1; 
	    BLTVal::putid( latchmgr->alloc->right, MIN_lvl+1+nlatchpage );
	    latchmgr->alloc->bits = mgr->page_bits;
	    latchmgr->nlatchpage = nlatchpage;
	    latchmgr->latchtotal = nlatchpage * (mgr->page_size / sizeof(LatchSet));
	
	    // initialize latch manager
	    latchhash = (mgr->page_size - sizeof(LatchMgr)) / sizeof(HashEntry);
	
	    // size of hash table = total number of latchsets
	    if (latchhash > latchmgr->latchtotal) {
	        latchhash = latchmgr->latchtotal;
        }
	
	    latchmgr->latchhash = latchhash;
	
	    if (write( mgr->idx, latchmgr, mgr->page_size ) < mgr->page_size ) {
	        BufMgr::destroy( mgr );
	        return NULL;
        }
	
	    memset( latchmgr, 0, 1 << bits );
	    latchmgr->alloc->bits = mgr->page_bits;
	
	    for ( lvl=MIN_lvl; lvl--; ) {
	        slotptr(latchmgr->alloc, 1)->off = mgr->page_size - 3 - (lvl ? BtId + 1: 1);
	        key = keyptr(latchmgr->alloc, 1);
	        key->len = 2;        // create stopper key
	        key->key[0] = 0xff;
	        key->key[1] = 0xff;
	
	        BLTVal::putid( value, MIN_lvl - lvl + 1 );
	        val = valptr(latchmgr->alloc, 1);
	        val->len = lvl ? BtId : 0;
	        memcpy( val->value, value, val->len );
	
	        latchmgr->alloc->min = slotptr(latchmgr->alloc, 1)->off;
	        latchmgr->alloc->lvl = lvl;
	        latchmgr->alloc->cnt = 1;
	        latchmgr->alloc->act = 1;
	
	        if (write( mgr->idx, latchmgr, mgr->page_size ) < mgr->page_size) {
	            BufMgr::destroy( mgr );
	            return NULL;
            }
	    }
	
	    // clear out latch manager locks and rest of pages to round out segment
	    memset( latchmgr, 0, mgr->page_size );
	    last = MIN_lvl + 1;
	
	    while (last <= ((MIN_lvl + 1 + nlatchpage) | mgr->poolmask)) {
	        pwrite( mgr->idx, latchmgr, mgr->page_size, last << mgr->page_bits );
	        last++;
	    }
	
	mgrlatch:
	    flag = PROT_READ | PROT_WRITE;
	    mgr->latchmgr = (LatchMgr *)mmap( 0, mgr->page_size,
                                            flag, MAP_SHARED, mgr->idx,
                                            ALLOC_page * mgr->page_size );
	    if (mgr->latchmgr == MAP_FAILED) {
	        BufMgr::destroy( mgr );
	        return NULL;
        }
	
	    mgr->latchsets = (LatchSet *)mmap( 0, mgr->latchmgr->nlatchpage * mgr->page_size,
	                                         flag, MAP_SHARED, mgr->idx,
                                             LATCH_page * mgr->page_size);
	    if (mgr->latchsets == MAP_FAILED) {
	        BufMgr::destroy( mgr );
	        return NULL;
        }
	
	    free( latchmgr );
	    return mgr;
	}
	
    //
    //  dealloc bufmgr
    //
	void BufMgr::destroy( BufMgr* mgr ) {

	    // release mapped pages
	    for( uint slot = 1; slot < mgr->poolmax; slot++ ) { // note: slot zero is not used
	        PoolEntry* poolEntry = mgr->pool + slot;
	        if (poolEntry->slot) {
	            munmap( poolEntry->map, (mgr->poolmask+1) << mgr->page_bits );
            }
	    }
	
	    munmap( mgr->latchsets, mgr->latchmgr->nlatchpage * mgr->page_size );
	    munmap( mgr->latchmgr, mgr->page_size );

	    close( mgr->idx );

	    free( mgr->pool );
	    free( mgr->hash );

	    free( (void *)mgr->latch );
        free( mgr );
	}
	
	//
	// find segment in pool
	// must be called with hashslot idx locked
	// @return NULL if not there, otherwise return node
	//
	PoolEntry* BufMgr::findpool( uid page_no, uint idx ) {
	    PoolEntry* poolEntry;
	    uint slot;
	
	    // compute start of hash chain in pool
	    if ( (slot = hash[idx]) ) {
	        poolEntry = pool + slot;
	    }
	    else {
	        return NULL;
	    }
	
	    page_no &= ~poolmask;
	
	    while ( poolEntry->basepage != page_no ) {
	        if ( (poolEntry = (PoolEntry *)poolEntry->hashnext) ) {
	            continue;
	        }
	        else {
	            return NULL;
	        }
	    }
	    return poolEntry;
	}

	//
	// add segment to hash table
	//
	void BufMgr::linkhash( PoolEntry* poolEntry, uid page_no, int idx ) {
	    PoolEntry *node;
	    uint slot;
	
	    poolEntry->hashprev = poolEntry->hashnext = NULL;
	    poolEntry->basepage = page_no & ~poolmask;
	    poolEntry->pin = CLOCK_bit + 1;
	
	    if ( (slot = hash[idx]) ) {
	        node = pool + slot;
	        poolEntry->hashnext = node;
	        node->hashprev = poolEntry;
	    }
	
	    hash[idx] = poolEntry->slot;
	}
	
	//
	//  map new buffer pool segment to virtual memory
	//
    BTERR BufMgr::mapsegment( PoolEntry* poolEntry, uid page_no ) {

	    off64_t off = (page_no & ~poolmask) << page_bits;
	    int flag;
	
	    flag = PROT_READ | ( mode == BT_ro ? 0 : PROT_WRITE );
	    poolEntry->map = (char *)mmap( 0, (poolmask+1) << page_bits, flag, MAP_SHARED, idx, off );
	    if (poolEntry->map == MAP_FAILED) {
	        return (BTERR)(err = BTERR_map);
        }
	    return (BTERR)(err = BTERR_ok);
	}
	
	//
	//    calculate page within pool
	//
	Page* BufMgr::page( PoolEntry* poolEntry, uid page_no ) {

	    uint subpage = (uint)(page_no & poolmask); // page within mapping
	    Page* page = (Page*)(poolEntry->map + (subpage << page_bits));
	    return page;
	}
	
	//
	//  link latch table entry into latch hash table
	//
	void BufMgr::latchlink( ushort hashidx, ushort victim, uid page_no ) {
	
	    LatchSet* set = latchsets + victim;
	
	    if ( (set->next = latchmgr->table[hashidx].slot) ) {
	        latchsets[set->next].prev = victim;
	    }
	
	    latchmgr->table[hashidx].slot = victim;
	    set->page_no = page_no;
	    set->hash = hashidx;
	    set->prev = 0;
	}
	
	//
	//    release latch pin
	//
	void BufMgr::unpinlatch( LatchSet* set ) {
	    __sync_fetch_and_add( &set->pin, -1 );
	}
	
	//
	//  find existing latchset or inspire new one
	//  @return latchset, pinned
	//
	LatchSet* BufMgr::pinlatch( uid page_no ) {

	    ushort hashidx = page_no % latchmgr->latchhash;
	    ushort slot, avail = 0, victim, idx;
	    LatchSet* set;
	
	    //  obtain read lock on hash table entry
	    SpinLatch::spinreadlock( latchmgr->table[hashidx].latch );
	
	    if ( (slot = latchmgr->table[hashidx].slot) ) {
	        do {
	            set = latchsets + slot;
	            if( page_no == set->page_no ) break;
	        } while ( (slot = set->next) );
	    }
	
	    if (slot) {
	        __sync_fetch_and_add(&set->pin, 1);
	    }
	
	    SpinLatch::spinreleaseread( latchmgr->table[hashidx].latch );
	
	    if (slot) return set;
	
	    //  try again, this time with write lock
	    SpinLatch::spinwritelock( latchmgr->table[hashidx].latch );
	
	    if ( (slot = latchmgr->table[hashidx].slot) ) {
	        do {
	            set = latchsets + slot;
	            if (page_no == set->page_no) break;
	            if (!set->pin && !avail) avail = slot;
	        } while ( (slot = set->next) );
	    }
	
	    //  found our entry, or take over an unpinned one
	
	    if (slot || (slot = avail)) {
	        set = latchsets + slot;
	        __sync_fetch_and_add( &set->pin, 1 );
	        set->page_no = page_no;
	        SpinLatch::spinreleasewrite( latchmgr->table[hashidx].latch );
	        return set;
	    }
	
	    //  see if there are any unused entries
	    victim = __sync_fetch_and_add( &latchmgr->latchdeployed, 1 ) + 1;
	
	    if (victim < latchmgr->latchtotal) {
	        set = latchsets + victim;
	        __sync_fetch_and_add( &set->pin, 1 );
	        latchlink( hashidx, victim, page_no );
	        SpinLatch::spinreleasewrite( latchmgr->table[hashidx].latch );
	        return set;
	    }
	
	    victim = __sync_fetch_and_add( &latchmgr->latchdeployed, -1 );
	  
	  // find and reuse previous lock entry
	  while (true) {
	 
	    // we don't use slot zero
	    victim = __sync_fetch_and_add( &latchmgr->latchvictim, 1 );
	
	    if ( (victim %= latchmgr->latchtotal) ) {
	        set = latchsets + victim;
	    }
	    else {
	        continue;
	    }
	
	    // take control of our slot from other threads
	    if (set->pin || !SpinLatch::spinwritetry( set->busy ) ) {
	        continue;
	    }
	
	    idx = set->hash;
	
	    // try to get write lock on hash chain
	    // skip entry if not obtained or has outstanding locks
	    if (!SpinLatch::spinwritetry( latchmgr->table[idx].latch )) {
	        SpinLatch::spinreleasewrite( set->busy );
	        continue;
	    }
	
	    if (set->pin) {
	        SpinLatch::spinreleasewrite( set->busy );
	        SpinLatch::spinreleasewrite( latchmgr->table[idx].latch );
	        continue;
	    }
	
	    // unlink our available victim from its hash chain
	    if (set->prev) {
	        latchsets[set->prev].next = set->next;
	    }
	    else {
	        latchmgr->table[idx].slot = set->next;
	    }
	
	    if ( set->next) {
	        latchsets[set->next].prev = set->prev;
	    }
	
	    SpinLatch::spinreleasewrite( latchmgr->table[idx].latch );
	    __sync_fetch_and_add( &set->pin, 1 );
	    latchlink( hashidx, victim, page_no );
	    SpinLatch::spinreleasewrite( latchmgr->table[hashidx].latch );
	    SpinLatch::spinreleasewrite( set->busy );
	    return set;
	  }
	}

	//
	//  release pool pin
	//
	void BufMgr::unpinpool( PoolEntry* poolEntry ) {
	    __sync_fetch_and_add( &poolEntry->pin, -1 );
	}
	
	//
	//  find or place requested page in segment-pool
	//  return pool table entry, incrementing pin
	//
	PoolEntry* BufMgr::pinpool( uid page_no ) {
	    uint slot, hashidx, idx, victim;
	    PoolEntry *poolEntry, *node;
	
	    // lock hash table chain
	    hashidx = (uint)(page_no >> seg_bits) % hashsize;
	    SpinLatch::spinwritelock( &latch[hashidx] );
	
	    // look up in hash table
	    if ( (poolEntry = findpool( page_no, hashidx )) ) {
	        __sync_fetch_and_or( &poolEntry->pin, CLOCK_bit );
	        __sync_fetch_and_add( &poolEntry->pin, 1 );
	        SpinLatch::spinreleasewrite( &latch[hashidx] );
	        return poolEntry;
	    }
	
	    // allocate a new pool nodeand add to hash table
	    slot = __sync_fetch_and_add( &poolcnt, 1 );
	
	    if (++slot < poolmax) {
	        poolEntry = pool + slot;
	        poolEntry->slot = slot;
	
	        if (mapsegment( poolEntry, page_no )) {
	            return NULL;
            }
	
	        linkhash( poolEntry, page_no, hashidx );
	        SpinLatch::spinreleasewrite( &latch[hashidx] );
	        return poolEntry;
	    }
	
	    // pool table is full: find best pool entry to evict
	    __sync_fetch_and_add( &poolcnt, -1 );
	
	    while( 1 ) {
	        victim = __sync_fetch_and_add( &evicted, 1 );
	        victim %= poolmax;
	        poolEntry = pool + victim;
	        idx = (uint)(poolEntry->basepage >> seg_bits) % hashsize;
	
	        if (!victim) continue;
	
	        // try to get write lock, skip entry if not obtained
	        if (!SpinLatch::spinwritetry( &latch[idx] )) continue;
	
	        // skip this entry if page is pinned or clock bit is set
	        if (poolEntry->pin) {
	            __sync_fetch_and_and( &poolEntry->pin, ~CLOCK_bit );
	            SpinLatch::spinreleasewrite( &latch[idx] );
	            continue;
	        }
	
	        // unlink victim pool node from hash table
	        if ( (node = (PoolEntry *)poolEntry->hashprev) ) {
	            node->hashnext = poolEntry->hashnext;
            }
	        else if ( (node = (PoolEntry *)poolEntry->hashnext) ) {
	            hash[idx] = node->slot;
            }
	        else {
	            hash[idx] = 0;
            }
	
	        if ( (node = (PoolEntry*)poolEntry->hashnext) ) {
	            node->hashprev = poolEntry->hashprev;
            }
	
	        SpinLatch::spinreleasewrite( &latch[idx] );
	
	        // remove old file mapping
	        munmap( poolEntry->map, (poolmask+1) << page_bits );
	        poolEntry->map = NULL;
	
	        // create new pool mapping and link into hash table
	        if (mapsegment( poolEntry, page_no )) {
	            return NULL;
            }
	
	        linkhash( poolEntry, page_no, hashidx );
	        SpinLatch::spinreleasewrite( &latch[hashidx] );
	        return poolEntry;
	    }
	}
	
	//
	// place write, read, or parent lock on requested page_no.
	//
	void BufMgr::lockpage( BLTLockMode mode, LatchSet* set ) {
	    switch (mode) {
	    case LockRead:
	        BLT_RWLock::ReadLock( set->readwr );
	        break;
	    case LockWrite:
	        BLT_RWLock::WriteLock( set->readwr );
	        break;
	    case LockAccess:
	        BLT_RWLock::ReadLock( set->access );
	        break;
	    case LockDelete:
	        BLT_RWLock::WriteLock( set->access );
	        break;
	    case LockParent:
	        BLT_RWLock::WriteLock( set->parent );
	        break;
	    }
	}
	
	//
	// remove write, read, or parent lock on requested page
	//
	void BufMgr::unlockpage( BLTLockMode mode, LatchSet* set ) {
	    switch (mode) {
	    case LockRead:
	        BLT_RWLock::ReadRelease( set->readwr );
	        break;
	    case LockWrite:
	        BLT_RWLock::WriteRelease( set->readwr );
	        break;
	    case LockAccess:
	        BLT_RWLock::ReadRelease( set->access );
	        break;
	    case LockDelete:
	        BLT_RWLock::WriteRelease( set->access );
	        break;
	    case LockParent:
	        BLT_RWLock::WriteRelease( set->parent );
	        break;
	    }
	}
	
	//
	//    allocate a new page and write page into it
	//
	uid BufMgr::newpage( Page* page ) {
	    PageSet set[1];
	    uid new_page;
	    int reuse;
	
	    //    lock allocation page
	    SpinLatch::spinwritelock( latchmgr->lock );
	
	    // use empty chain first else allocate empty page
	    if ( (new_page = BLTVal::getid( latchmgr->chain )) ) {
	        if ( (set->pool = pinpool( new_page )) ) {
	            set->page = this->page( set->pool, new_page );
            }
	        else {
	            return 0;
            }
	
	        BLTVal::putid( latchmgr->chain, BLTVal::getid( set->page->right ) );
	        unpinpool( set->pool );
	        reuse = 1;
	    } else {
	        new_page = BLTVal::getid( latchmgr->alloc->right );
	        BLTVal::putid( latchmgr->alloc->right, new_page+1 );
	        reuse = 0;

	        // if writing first page of pool block, set file length to last page
	        if ((new_page & poolmask) == 0) {
	            ftruncate( idx, (new_page + poolmask + 1) << page_bits );
            }
	    }

	    // unlock allocation latch
	    SpinLatch::spinreleasewrite( latchmgr->lock );
	
	    //    bring new page into pool and copy page.
	    //    this will extend the file into the new pages on WIN32.
	    if ( (set->pool = pinpool( new_page )) ) {
	        set->page = this->page( set->pool, new_page );
        }
	    else {
	        return 0;
        }
	
	    memcpy( set->page, page, page_size );
	    unpinpool( set->pool );
	
	    return new_page;
	}


	//
	//  find slot in page for given key at a given level
	//
	int BufMgr::findslot( PageSet* set, uchar* key, uint len ) {

	    uint diff, higher = set->page->cnt, low = 1, slot;
	    uint good = 0;
	
	    //      make stopper key an infinite fence value
	    if (BLTVal::getid( set->page->right )) {
	        higher++;
        }
	    else {
	        good++;
        }
	
	    // low is the lowest candidate: loop ends when they meet
	    // higher is already tested as >= the passed key.
	    while ( (diff = higher - low) ) {
	        slot = low + ( diff >> 1 );
	        if (BLTKey::keycmp( keyptr(set->page, slot), key, len ) < 0) {
	            low = slot + 1;
            }
	        else {
	            higher = slot, good++;
            }
	    }
	
	    // return zero if key is on right link page
	    return good ? higher : 0;
	}
	
	//
	//  find and load page at given level for given key
	//    leave page rd or wr locked as requested
	//
	int BufMgr::loadpage( PageSet* set, uchar* key, uint len, uint lvl, BLTLockMode lock ) {
	    uid page_no = ROOT_page, prevpage = 0;
	    uint drill = 0xff, slot;
	    LatchSet* prevlatch;
	    BLTLockMode mode, prevmode;
	    PoolEntry* prevpool;
	
	    // start at root of btree and drill down
        do {
	        // determine lock mode of drill level
	        mode = (drill == lvl) ? lock : LockRead; 
	
	        set->latch = pinlatch( page_no );
	        set->page_no = page_no;
	
	        // pin page contents
	        if ( (set->pool = pinpool( page_no )) ) {
	            set->page = page( set->pool, page_no );
            }
	        else {
	            return 0;
            }
	
	        // obtain access lock using lock chaining with Access mode
	        if (page_no > ROOT_page) {
	            lockpage( LockAccess, set->latch );
            }
	
	        // release & unpin parent page
	        if (prevpage) {
	            unlockpage( prevmode, prevlatch );
	            unpinlatch( prevlatch );
	            unpinpool( prevpool );
	            prevpage = 0;
	        }
	
	        // obtain read lock using lock chaining
	        lockpage( mode, set->latch );
	
	        if (set->page->free) {
	            err = BTERR_struct;
	            return 0;
            }
	
	        if (page_no > ROOT_page) {
	            unlockpage( LockAccess, set->latch );
            }
	
	        // re-read and re-lock root after determining actual level of root
	        if (set->page->lvl != drill) {
	            if (set->page_no != ROOT_page) {
	                err = BTERR_struct;
	                return 0;
                }
	            
	            drill = set->page->lvl;
	
	            if (lock != LockRead && drill == lvl) {
	                unlockpage( mode, set->latch );
	                unpinlatch( set->latch );
	                unpinpool( set->pool );
	                continue;
	            }
	        }
	
	        prevpage = set->page_no;
	        prevlatch = set->latch;
	        prevpool = set->pool;
	        prevmode = mode;
	
	        //  find key on page at this level
	        //  and descend to requested level
	        if (!set->page->kill) {
	            if ( (slot = findslot( set, key, len )) ) {

	                if (drill == lvl) return slot;
	
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
            }
	
        slideright: //  or slide right into next page
	        page_no = BLTVal::getid( set->page->right );
	
        } while( page_no );
	
	  // return error on end of right chain
	  err = BTERR_struct;
	  return 0;
	}
	
	//
	//  return page to free list
	//  page must be delete & write locked
	//
	void BufMgr::freepage( PageSet* set ) {
	    
	    // lock allocation page
	    SpinLatch::spinwritelock( latchmgr->lock );
	
	    // store chain
	    memcpy( set->page->right, latchmgr->chain, BtId );
	    BLTVal::putid( latchmgr->chain, set->page_no );
	    set->page->free = 1;
	
	    // unlock released page
	    unlockpage( LockDelete, set->latch );
	    unlockpage( LockWrite, set->latch );
	    unpinlatch( set->latch );
	    unpinpool( set->pool );
	
	    // unlock allocation page
	    SpinLatch::spinreleasewrite( latchmgr->lock );
	}

}   // namespace mongo
