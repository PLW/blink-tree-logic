//@file bltree.cpp

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

#ifndef STANDALONE
#include "mongo/db/storage/bltree/bltree.h"
#include "mongo/db/storage/bltree/bltval.h"
#include "mongo/db/storage/bltree/latchmgr.h"
#include "mongo/db/storage/bltree/bufmgr.h"
#else
#include "bltree.h"
#include "bltval.h"
#include "latchmgr.h"
#include "bufmgr.h"
#endif

#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <memory.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>

namespace mongo {

    /*
    *  Notes:
    *
    *  Pages are allocated from low and high ends (addresses).  Key offsets 
    *  and row-id's are allocated from low addresses, while the text of the key 
    *  is allocated from high addresses.  When the two areas meet, the page is
    *  split with a 50% rule.  This can easily be tuned.
    *  
    *  A key consists of a length byte, two bytes of index number (0 - 65534),
    *  and up to 253 bytes of key value.  Duplicate keys are discarded.
    *  Associated with each key is an opaque value of any size small enough
    *  to fit in a page.
    *  
    *  The b-tree root is always located at page 1.  The first leaf page of
    *  level zero is always located on page 2.
    *  
    *  The b-tree pages are linked with next pointers to facilitate
    *  enumerators and to provide for concurrency.
    *  
    *  When the root page fills, it is split in two and the tree height is
    *  raised by a new root at page one with two keys.
    *  
    *  Deleted keys are marked with a dead bit until page cleanup. The fence
    *  key for a node is always present
    *  
    *  Groups of pages called segments from the btree are optionally cached
    *  with a memory mapped pool. A hash table is used to keep track of the
    *  cached segments.  This behavior is controlled by the cache block
    *  size parameter to open.
    *  
    *  To achieve maximum concurrency one page is locked at a time as the
    *  tree is traversed to find leaf key in question. The right page numbers
    *  are used in cases where the page is being split or consolidated.
    *  
    *  Page 0 is dedicated to lock for new page extensions, and chains empty
    *  pages together for reuse.
    *  
    *  The ParentModification lock on a node is obtained to serialize posting
    *  or changing the fence key for a node.
    *  
    *  Empty pages are chained together through the ALLOC page and reused.
    *  
    *  Access macros to address slot and key values from the page Page slots
    *  use 1 based indexing.
    */

    //
    //  factory: BLTree access method
    //
    BLTree* BLTree::create( BufMgr* mgr ) {
        BLTree* bt = (BLTree*)malloc( sizeof(BLTree) );
        memset( bt, 0, sizeof(BLTree) );
        bt->mgr = mgr;
        bt->mem = (uchar *)malloc( 2 * mgr->page_size );
        bt->frame = (Page *)bt->mem;
        bt->cursor = (Page *)(bt->mem + 1 * mgr->page_size);
        return bt;
    }

    //
    //  close and release memory
    //
    void BLTree::close() {
        if (mem) free(mem);
    }
    
    //
    //  a fence key was deleted from a page
    //  push new fence value upwards
    //  @return error code
    //
    Status BLTree::fixfence( PageSet* set, uint lvl ) {
    
        uchar leftkey[256], rightkey[256];
        uchar value[BtId];
        uid page_no;
        BLTKey* ptr;
    
        // remove the old fence value
        ptr = keyptr(set->page, set->page->cnt);
        memcpy( rightkey, ptr, ptr->len + 1 );
    
        memset( slotptr(set->page, set->page->cnt--), 0, sizeof(Slot) );
        set->page->dirty = 1;
    
        ptr = keyptr(set->page, set->page->cnt);
        memcpy( leftkey, ptr, ptr->len + 1 );
        page_no = set->page_no;
    
        mgr->lockpage( LockParent, set->latch );
        mgr->unlockpage( LockWrite, set->latch );
    
        // insert new (now smaller) fence key
        BLTVal::putid( value, page_no );
    
    #ifndef STANDALONE
        Status s = insertkey( leftkey+1, *leftkey, lvl+1, value, BtId );
        if (!s.isOK()) return s;
    #else
        if (insertkey( leftkey+1, *leftkey, lvl+1, value, BtId )) {
            return (BTERR)mgr->err;
        }
    #endif
    
        // now delete old fence key
    #ifndef STANDALONE
        s = deletekey( rightkey+1, *rightkey, lvl+1 );
        if (!s.isOK()) return s;
    #else
        if (deletekey( rightkey+1, *rightkey, lvl+1 )) {
            return (BTERR)mgr->err;
        }
    #endif
    
        mgr->unlockpage( LockParent, set->latch );
        mgr->unpinlatch( set->latch );
        mgr->unpinpool( set->pool );

    #ifndef STANDALONE
        return Status::OK();
    #else
        return (BTERR)0;
    #endif
    }
    
    //
    //  root has a single child
    //  collapse a level from the tree
    //  @return error code
    //
    Status BLTree::collapseroot( PageSet *root ) {
    
        PageSet child[1];
        uint idx;
    
        // find the child entry and promote as new root contents
        do {
            for (idx = 0; idx++ < root->page->cnt; ) {
                if (!slotptr(root->page, idx)->dead) break;
            }
    
            child->page_no = BLTVal::getid( valptr(root->page, idx)->value );
    
            child->latch = mgr->pinlatch( child->page_no );
            mgr->lockpage( LockDelete, child->latch );
            mgr->lockpage( LockWrite, child->latch );
    
            if ( (child->pool = mgr->pinpool( child->page_no )) ) {
                child->page = mgr->page( child->pool, child->page_no);
            }
            else {

            #ifndef STANDALONE
                return Status( ErrorCodes::InternalError, "pinpool error in 'collapseroot'" );
            #else
                return (BTERR)mgr->err;
            #endif

            }
    
            memcpy( root->page, child->page, mgr->page_size );
            mgr->freepage( child );
    
        } while( root->page->lvl > 1 && root->page->act == 1 );
    
        mgr->unlockpage( LockWrite, root->latch );
        mgr->unpinlatch( root->latch );
        mgr->unpinpool( root->pool );

    #ifndef STANDALONE
        return Status::OK();
    #else
        return (BTERR)0;
    #endif

    }
    
    //
    //  find and delete key on page by marking delete flag bit
    //  if page becomes empty, delete it from the btree
    //  @return error code
    //
    Status BLTree::deletekey( uchar* key, uint len, uint lvl ) {
    
        uchar lowerfence[256], higherfence[256];
        uint slot, idx, dirty = 0, fence, found0;
        PageSet set[1], right[1];
        uchar value[BtId];
        BLTKey* ptr;
    
        if ( (slot = mgr->loadpage( set, key, len, lvl, LockWrite )) ) {
            ptr = keyptr(set->page, slot);
        }
        else {
        #ifndef STANDALONE
            return Status( ErrorCodes::InternalError, "loadpage error in 'deletekey'" );
        #else
            return (BTERR)mgr->err;
        #endif
        }
    
        // are we deleting a fence slot?
        fence = (slot == set->page->cnt);
    
        // if key is found, delete it, otherwise ignore request
        if ( (found0 = !BLTKey::keycmp( ptr, key, len )) ) {
    
            if ( (found0 = slotptr(set->page, slot)->dead) == 0 ) {
                dirty = slotptr(set->page, slot)->dead = 1;
                set->page->dirty = 1;
                set->page->act--;
    
                // collapse empty slots
                while ( (idx = set->page->cnt - 1) ) {
                    if (slotptr(set->page, idx)->dead) {
                        *slotptr(set->page, idx) = *slotptr(set->page, idx + 1);
                        memset( slotptr(set->page, set->page->cnt--), 0, sizeof(Slot) );
                    }
                    else {
                        break;
                    }
                }
            }
        }
    
        // did we delete a fence key in an upper level?
        if (dirty && lvl && set->page->act && fence) {

        #ifndef STANDALONE
            Status s = fixfence( set, lvl );
            if (!s.isOK()) {
                return s;
            }
            else {
                found = found0;
                return Status::OK();
            }
        #else
            if (fixfence( set, lvl )) {
                return (BTERR)err;
            }
            else {
                found = found0;
                return (BTERR)0;
            }
        #endif

        }
    
        // is this a collapsed root?
        if (lvl > 1 && set->page_no == ROOT_page && set->page->act == 1) {

        #ifndef STANDALONE
            Status s = collapseroot( set );
            if (!s.isOK()) {
                return s;
            }
            else {
                found = found0;
                return Status::OK();
            }
        #else
            if (collapseroot( set )) {
                return (BTERR)err;
            }
            else {
                found = found0;
                return (BTERR)0;
            }
        #endif

        }
    
        // return if page is not empty
         if (set->page->act) {
            mgr->unlockpage( LockWrite, set->latch );
            mgr->unpinlatch( set->latch );
            mgr->unpinpool( set->pool );
            found = found0;
        #ifndef STANDALONE
            return Status::OK();
        #else
            return (BTERR)0;
        #endif
        }
    
        // cache copy of fence key to post in parent
        ptr = keyptr( set->page, set->page->cnt );
        memcpy( lowerfence, ptr, ptr->len + 1 );
    
        //    obtain lock on right page
        right->page_no = BLTVal::getid( set->page->right );
        right->latch = mgr->pinlatch( right->page_no );
        mgr->lockpage( LockWrite, right->latch );
    
        // pin page contents
        if ( (right->pool = mgr->pinpool( right->page_no )) ) {
            right->page = mgr->page( right->pool, right->page_no );
        }
        else {

        #ifndef STANDALONE
            return Status::OK();
        #else
            return (BTERR)0;
        #endif

        }
    
        if (right->page->kill) {

        #ifndef STANDALONE
            return Status( ErrorCodes::InternalError, "structural error in 'deletekey'" );
        #else
            return (BTERR)(err = BTERR_struct);
        #endif

        }
    
        // pull contents of right peer into our empty page
        memcpy( set->page, right->page, mgr->page_size );
    
        // cache copy of key to update
        ptr = keyptr(right->page, right->page->cnt);
        memcpy( higherfence, ptr, ptr->len + 1 );
    
        // mark right page deleted and point it to left page
        //    until we can post parent updates
        BLTVal::putid( right->page->right, set->page_no );
        right->page->kill = 1;
    
        mgr->lockpage( LockParent, right->latch );
        mgr->unlockpage( LockWrite, right->latch );
        mgr->lockpage( LockParent, set->latch );
        mgr->unlockpage( LockWrite, set->latch );
    
        // redirect higher key directly to our new node contents
        BLTVal::putid( value, set->page_no );
    
    #ifndef STANDALONE
        Status s = insertkey( higherfence+1, *higherfence, lvl+1, value, BtId );
        if (!s.isOK()) return s;
    #else
        if (insertkey( higherfence+1, *higherfence, lvl+1, value, BtId )) {
            return (BTERR)err;
        }
    #endif
    
        // delete old lower key to our node
    #ifndef STANDALONE
        s = deletekey( lowerfence+1, *lowerfence, lvl+1 );
        if (!s.isOK()) return s;
    #else
        if (deletekey( lowerfence+1, *lowerfence, lvl+1 )) {
            return (BTERR)err;
        }
    #endif
    
        // obtain delete and write locks to right node
        mgr->unlockpage( LockParent, right->latch );
        mgr->lockpage( LockDelete, right->latch );
        mgr->lockpage( LockWrite, right->latch );
        mgr->freepage( right );
    
        mgr->unlockpage( LockParent, set->latch );
        mgr->unpinlatch( set->latch );
        mgr->unpinpool( set->pool );
        found = found0;

    #ifndef STANDALONE
        return Status::OK();
    #else
        return (BTERR)0;
    #endif

    }
    
    //
    //  find key in leaf level and
    //  @return number of value bytes, or 0 if not found
    //
    int BLTree::findkey( uchar* key, uint keylen, uchar* value, uint valmax ) {
        PageSet set[1];
        uint slot;
        BLTKey* ptr;
        BLTVal* val;
    
        if ( (slot = mgr->loadpage( set, key, keylen, 0, LockRead )) ) {
            ptr = keyptr(set->page, slot);
        }
        else {
            return 0;
        }
    
        // if key exists, return TRUE, otherwise FALSE
        int ret;
        if (!BLTKey::keycmp( ptr, key, keylen )) {
            val = valptr (set->page,slot);
            if (valmax > val->len) {
                valmax = val->len;
            }
            memcpy( value, val->value, valmax );
            ret = valmax;
        } else {
            ret = 0;
        }
    
        // unlock all locks asserted by 'loadpage'
        mgr->unlockpage( LockRead, set->latch );
        mgr->unpinlatch( set->latch );
        mgr->unpinpool( set->pool );
        return ret;
    }
    
    //
    //  check page for space available, clean if necessary
    //  @return  0  page needs splitting
    //          >0  new slot value
    //
    uint BLTree::cleanpage( Page* page, uint keylen, uint slot, uint vallen ) {
    
        uint nxt = mgr->page_size;
        uint cnt = 0, idx = 0;
        uint max = page->cnt;
        uint newslot = max;
        BLTKey* key;
        BLTVal* val;
    
        if (page->min >= (max+1) * sizeof(Slot) + sizeof(*page) + keylen + 1 + vallen + 1) {
            return slot;
        }
    
        // skip cleanup if nothing to reclaim
        if (!page->dirty) {
            return 0;
        }
    
        memcpy( frame, page, mgr->page_size );
    
        // skip page info and set rest of page to zero
        memset( page+1, 0, mgr->page_size - sizeof(*page) );
        page->dirty = 0;
        page->act = 0;
    
        // try cleaning up page first by removing deleted keys
        while (cnt++ < max) {
            if (cnt == slot) {
                newslot = idx + 1;
            }
            if (cnt < max && slotptr(frame,cnt)->dead) {
                continue;
            }
    
            // copy the key across
            key = keyptr(frame, cnt);
            nxt -= key->len + 1;
            memcpy( (uchar *)page + nxt, key, key->len + 1 );
    
            // copy the value across
            val = valptr(frame, cnt);
            nxt -= val->len + 1;
            ((uchar *)page)[nxt] = val->len;
            memcpy( (uchar *)page + nxt + 1, val, val->len );
    
            // set up the slot
            slotptr(page, idx)->off = nxt;
    
            if( !(slotptr(page, idx)->dead = slotptr(frame, cnt)->dead) )
                page->act++;
        }
    
        page->min = nxt;
        page->cnt = idx;
    
        // see if page has enough space now, or does it need splitting?
        if (page->min >= (idx+1) * sizeof(Slot) + sizeof(*page) + keylen + 1 + vallen + 1) {
            return newslot;
        }
    
        return 0;
    }
    
    //
    //  split the root and raise the height of the btree
    //  @return error code
    //
    Status BLTree::splitroot( PageSet* root, uchar* leftkey, uid page_no2 ) {
        uint nxt = mgr->page_size;
        uchar value[BtId];
        uid left;
    
        // Obtain an empty page to use, and copy the current
        // root contents into it, e.g. lower keys
        if (!(left = mgr->newpage( root->page ))) {

        #ifndef STANDALONE
            return Status( ErrorCodes::InternalError, "newpage error in 'splitroot'" );
        #else
            return (BTERR)err;
        #endif

        }
    
        // preserve the page info at the bottom
        // of higher keys and set rest to zero
        memset( root->page+1, 0, mgr->page_size - sizeof(*root->page) );
    
        // insert lower keys page fence key on newroot page as first key
        nxt -= BtId + 1;
        BLTVal::putid( value, left );
        ((uchar *)root->page)[nxt] = BtId;
        memcpy( (uchar *)root->page + nxt + 1, value, BtId );
    
        nxt -= *leftkey + 1;
        memcpy( (uchar *)root->page + nxt, leftkey, *leftkey + 1 );
        slotptr(root->page, 1)->off = nxt;
        
        // insert stopper key on newroot page and increase the root height
        nxt -= 3 + BtId + 1;
        ((uchar *)root->page)[nxt] = 2;
        ((uchar *)root->page)[nxt+1] = 0xff;
        ((uchar *)root->page)[nxt+2] = 0xff;
    
        BLTVal::putid( value, page_no2 );
        ((uchar *)root->page)[nxt+3] = BtId;
        memcpy( (uchar *)root->page + nxt + 4, value, BtId );
        slotptr(root->page, 2)->off = nxt;
    
        BLTVal::putid( root->page->right, 0 );
        root->page->min = nxt;  // reset lowest used offset and key count
        root->page->cnt = 2;
        root->page->act = 2;
        root->page->lvl++;
    
        // release and unpin root
        mgr->unlockpage( LockWrite, root->latch );
        mgr->unpinlatch( root->latch );
        mgr->unpinpool( root->pool );

    #ifndef STANDALONE
        return Status::OK();
    #else
        return (BTERR)0;
    #endif

    }
    
    //
    //  split already locked full node
    //  @return err code, with page unlocked
    //
    Status BLTree::splitpage( PageSet* set ) {
        uint cnt = 0, idx = 0, max, nxt = mgr->page_size;
        uchar fencekey[256], rightkey[256];
        uchar value[BtId];
        uint lvl = set->page->lvl;
        PageSet right[1];
        BLTKey* key = NULL;
        BLTVal* val = NULL;
    
        // split higher half of keys to 'frame'
        memset( frame, 0, mgr->page_size );
        max = set->page->cnt;
        cnt = max / 2;
        idx = 0;
    
        while( cnt++ < max ) {
            val = valptr( set->page, cnt );
            nxt -= val->len + 1;
            ((uchar *)frame)[nxt] = val->len;
            memcpy( (uchar *)frame + nxt + 1, val->value, val->len );
    
            key = keyptr(set->page, cnt);
            nxt -= key->len + 1;
            memcpy( (uchar *)frame + nxt, key, key->len + 1 );
    
            slotptr(frame, ++idx)->off = nxt;
    
            if (!(slotptr(frame, idx)->dead = slotptr(set->page, cnt)->dead) ) {
                frame->act++;
            }
        }
    
        // remember existing fence key for new page to the right
        memcpy( rightkey, key, key->len + 1 );
    
        frame->bits = mgr->page_bits;
        frame->min = nxt;
        frame->cnt = idx;
        frame->lvl = lvl;
    
        // link right node
        if (set->page_no > ROOT_page) {
            memcpy( frame->right, set->page->right, BtId );
        }
    
        // get new free page and write higher keys to it.
        if ( !(right->page_no = mgr->newpage( frame )) ) {

        #ifndef STANDALONE
            return Status( ErrorCodes::InternalError, "newpage error in 'splitpage'" );
        #else
            return (BTERR)err;
        #endif

        }
    
        // update lower keys to continue in old page
        memcpy( frame, set->page, mgr->page_size);
        memset( set->page+1, 0, mgr->page_size - sizeof(*set->page));
        nxt = mgr->page_size;
        set->page->dirty = 0;
        set->page->act = 0;
        cnt = 0;
        idx = 0;
    
        //  assemble page of smaller keys
        while( cnt++ < max / 2 ) {
            val = valptr(frame, cnt);
            nxt -= val->len + 1;
            ((uchar *)set->page)[nxt] = val->len;
            memcpy( (uchar *)set->page + nxt + 1, val->value, val->len);
    
            key = keyptr(frame, cnt);
            nxt -= key->len + 1;
            memcpy( (uchar *)set->page + nxt, key, key->len + 1);
            slotptr(set->page, ++idx)->off = nxt;
            set->page->act++;
        }
    
        // remember fence key for smaller page
        memcpy(fencekey, key, key->len + 1);
    
        BLTVal::putid( set->page->right, right->page_no );
        set->page->min = nxt;
        set->page->cnt = idx;
    
        // if current page is the root page, split it
        if ( set->page_no == ROOT_page ) {
            return splitroot( set, fencekey, right->page_no );
        }
    
        // insert new fences in their parent pages
        right->latch = mgr->pinlatch( right->page_no );
        mgr->lockpage( LockParent, right->latch );
        mgr->lockpage( LockParent, set->latch );
        mgr->unlockpage( LockWrite, set->latch );
    
        // insert new fence for reformulated left block of smaller keys
        BLTVal::putid( value, set->page_no );

    #ifndef STANDALONE
        Status s = insertkey( fencekey+1, *fencekey, lvl+1, value, BtId );
        if (!s.isOK()) return s;
    #else
        if ( insertkey( fencekey+1, *fencekey, lvl+1, value, BtId ) ) {
            return (BTERR)err;
        }
    #endif
    
        // switch fence for right block of larger keys to new right page
        BLTVal::putid( value, right->page_no );

    #ifndef STANDALONE
        s = insertkey( rightkey+1, *rightkey, lvl+1, value, BtId );
        if (!s.isOK()) return s;
    #else
        if (insertkey( rightkey+1, *rightkey, lvl+1, value, BtId )) {
            return (BTERR)err;
        }
    #endif
    
        mgr->unlockpage( LockParent, set->latch) ;
        mgr->unpinlatch( set->latch) ;
        mgr->unpinpool( set->pool) ;
        mgr->unlockpage( LockParent, right->latch) ;
        mgr->unpinlatch( right->latch) ;

    #ifndef STANDALONE
        return Status::OK();
    #else
        return (BTERR)0;
    #endif
    
    }
    
    //
    //  Insert new key into the btree at given level.
    //
    Status BLTree::insertkey( uchar* key, uint keylen, uint lvl, uchar* value, uint vallen) {
    
        PageSet set[1];
        uint slot, idx;
        uint reuse;
        BLTKey* ptr;
        BLTVal* val;
    
        while (true) {
            if ( (slot = mgr->loadpage( set, key, keylen, lvl, LockWrite )) ) {
                ptr = keyptr(set->page, slot);
            }
            else {

            #ifndef STANDALONE
                return Status( ErrorCodes::InternalError, "loadpage error in 'insertkey'" );
            #else
                if (!err) err = BTERR_ovflw;
                return (BTERR)err;
            #endif

            }
    
            // if key already exists, update id and return
            if ( (reuse = !BLTKey::keycmp( ptr, key, keylen )) ) {
                if ( (val = valptr(set->page, slot), val->len >= vallen) ) {
                    if (slotptr(set->page, slot)->dead ) {
                        set->page->act++;
                    }
                    slotptr(set->page, slot)->dead = 0;
                    val->len = vallen;
                    memcpy( val->value, value, vallen );
                    mgr->unlockpage( LockWrite, set->latch );
                    mgr->unpinlatch( set->latch );
                    mgr->unpinpool( set->pool );

                #ifndef STANDALONE
                    return Status::OK();
                #else
                    return (BTERR)0;
                #endif

                } else {
                    if (!slotptr(set->page, slot)->dead) {
                        set->page->act--;
                    }
                    slotptr(set->page, slot)->dead = 1;
                    set->page->dirty = 1;
                }
            }
    
            // check if page has enough space
            if ( (slot = cleanpage( set->page, keylen, slot, vallen )) ) {
                break;
            }
    
        #ifndef STANDALONE
            Status s = splitpage( set );
            if (!s.isOK()) return s;
        #else
            if (splitpage( set )) {
                return (BTERR)err;
            }
        #endif

        }   // end while
    
        // calculate next available slot and copy key into page
        set->page->min -= vallen + 1; // reset lowest used offset
        ((uchar *)set->page)[set->page->min] = vallen;
        memcpy( (uchar *)set->page + set->page->min +1, value, vallen );
    
        set->page->min -= keylen + 1; // reset lowest used offset
        ((uchar *)set->page)[set->page->min] = keylen;
        memcpy( (uchar *)set->page + set->page->min +1, key, keylen );
    
        for (idx = slot; idx < set->page->cnt; idx++) {
            if (slotptr(set->page, idx)->dead) break;
        }
    
        // now insert key into array before slot
        if (!reuse && idx == set->page->cnt) {
            idx++, set->page->cnt++;
        }
    
        set->page->act++;
    
        while (idx > slot) {
            *slotptr(set->page, idx) = *slotptr(set->page, idx -1), idx--;
        }
    
        slotptr(set->page, slot)->off = set->page->min;
        slotptr(set->page, slot)->dead = 0;
    
        mgr->unlockpage( LockWrite, set->latch );
        mgr->unpinlatch( set->latch );
        mgr->unpinpool( set->pool );

    #ifndef STANDALONE
        return Status::OK();
    #else
        return (BTERR)0;
    #endif

    }
    
    //
    //  cache page of keys into cursor and return starting slot for given key
    //
    uint BLTree::startkey( uchar* key, uint len ) {
        PageSet set[1];
        uint slot;
    
        // cache page for retrieval
        if ( (slot = mgr->loadpage( set, key, len, 0, LockRead )) ) {
            memcpy( cursor, set->page, mgr->page_size );
        }
        else {
            return 0;
        }
    
        cursor_page = set->page_no;
    
        mgr->unlockpage( LockRead, set->latch );
        mgr->unpinlatch( set->latch );
        mgr->unpinpool( set->pool );
        return slot;
    }
    
    //
    //  return next slot for cursor page
    //  or slide cursor right into next page
    //
    uint BLTree::nextkey( uint slot ) {
        PageSet set[1];
        uid right;
    
        do {
            right = BLTVal::getid( cursor->right );
     
            while (slot++ < cursor->cnt) {
                if (slotptr(cursor,slot)->dead) { continue; }
                // skip infinite stopper
                else if (right || (slot < cursor->cnt)) { return slot; }
                else { break; }
            }
    
            if (!right) break;
    
            cursor_page = right;
    
            if ( (set->pool = mgr->pinpool( right )) ) {
                set->page = mgr->page( set->pool, right );
            }
            else {
                return 0;
            }
    
            set->latch = mgr->pinlatch( right );
            mgr->lockpage( LockRead, set->latch );
         
            memcpy( cursor, set->page, mgr->page_size );
    
            mgr->unlockpage( LockRead, set->latch );
            mgr->unpinlatch( set->latch );
            mgr->unpinpool( set->pool );
            slot = 0;
    
        } while (true);
    
        return (BTERR)(err = 0);
    }
    
    //
    // latch audit - debugging
    //
    uint BLTree::latchaudit() {
        ushort idx, hashidx;
        uid next, page_no;
        LatchSet *latch;
        uint cnt = 0;
        BLTKey* ptr;
    
        //posix_fadvise( mgr->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
    
        if (*(ushort *)(mgr->latchmgr->lock)) {
            fprintf( stderr, "Alloc page locked\n" );
        }
        *(ushort *)(mgr->latchmgr->lock) = 0;
    
        for( idx = 1; idx <= mgr->latchmgr->latchdeployed; idx++ ) {
            latch = mgr->latchsets + idx;
            if (*latch->readwr->rin & MASK) {
                fprintf( stderr, "latchset %d rwlocked for page %.8lx\n", idx, latch->page_no );
            }
            memset( (ushort *)latch->readwr, 0, sizeof(BLT_RWLock) );
    
            if (*latch->access->rin & MASK) {
                fprintf( stderr, "latchset %d accesslocked for page %.8lx\n", idx, latch->page_no );
            }
            memset( (ushort *)latch->access, 0, sizeof(BLT_RWLock) );
    
            if (*latch->parent->rin & MASK) {
                fprintf( stderr, "latchset %d parentlocked for page %.8lx\n", idx, latch->page_no );
            }
            memset( (ushort *)latch->access, 0, sizeof(BLT_RWLock) );
    
            if (latch->pin) {
                fprintf( stderr, "latchset %d pinned for page %.8lx\n", idx, latch->page_no );
                latch->pin = 0;
            }
        }
    
        for (hashidx = 0; hashidx < mgr->latchmgr->latchhash; hashidx++) {
            if (*(ushort *)(mgr->latchmgr->table[hashidx].latch)) {
                fprintf( stderr, "hash entry %d locked\n", hashidx );
            }
    
            *(ushort *)(mgr->latchmgr->table[hashidx].latch) = 0;
    
            if ( (idx = mgr->latchmgr->table[hashidx].slot ) ) {
                do {
                    latch = mgr->latchsets + idx;
                    if (*(ushort *)latch->busy) {
                        fprintf( stderr, "latchset %d busylocked for page %.8lx\n", idx, latch->page_no );
                    }
    
                    *(ushort *)latch->busy = 0;
                    if (latch->pin ) {
                        fprintf( stderr, "latchset %d pinned for page %.8lx\n", idx, latch->page_no );
                    }
                } while ( (idx = latch->next) );
            }
        }
    
        next = mgr->latchmgr->nlatchpage + LATCH_page;
        page_no = LEAF_page;
    
        while (page_no < BLTVal::getid( mgr->latchmgr->alloc->right )) {
            off64_t off = (page_no << mgr->page_bits);
    
            pread( mgr->idx, frame, mgr->page_size, off);
    
            if (!frame->free ) {
                for( idx = 0; idx++ < frame->cnt - 1; ) {
                    ptr = keyptr(frame, idx+1);
                    if (BLTKey::keycmp( keyptr(frame, idx), ptr->key, ptr->len ) >= 0 ) {
                        fprintf( stderr, "page %.8lx idx %.2x out of order\n", page_no, idx );
                    }
                }
                if( !frame->lvl ) {
                    cnt += frame->act;
                }
            }
            if (page_no > LEAF_page) next = page_no + 1;
            page_no = next;
        }
    
        return (cnt - 1);
    }

    void BLTree::scan( std::ostream& out ) {

        PageSet set[1];
        uid page_no = LEAF_page;
        uid next;
        int count   = 0;

        do {
            if ( (set->pool = mgr->pinpool( page_no )) ) {
                set->page = mgr->page( set->pool, page_no );
            }
            else {
                break;
            }
            set->latch = mgr->pinlatch( page_no );
            mgr->lockpage( LockRead, set->latch );
            next = BLTVal::getid( set->page->right );
            count += set->page->act;

            for (uint slot = 0; slot++ < set->page->cnt; ) {
                if (next || slot < set->page->cnt) {
                    if (!slotptr(set->page, slot)->dead) {
                        BLTKey* key = keyptr(set->page, slot);
                        BLTVal* val = valptr( set->page, slot );

                        std::cout << std::string( (char*)key->key, key->len )
                                  << " -> "
                                  << std::string( (char*)val->value, val->len )
                                  << std::endl;
                    }
                }
            }
            mgr->unlockpage( LockRead, set->latch );
            mgr->unpinlatch( set->latch );
            mgr->unpinpool( set->pool );
        } while ( (page_no = next) );

        std::cout << "Scanned " << (count-1) << " documents" << std::endl;
    }

}   // namespace mongo

