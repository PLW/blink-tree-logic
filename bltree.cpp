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
#include "mongo/db/storage/bltree/page.h"
#include "mongo/db/storage/bltree/latchmgr.h"
#include "mongo/db/storage/bltree/bufmgr.h"
#include "mongo/db/storage/bltree/bltree.h"
#else
#include "page.h"
#include "latchmgr.h"
#include "bufmgr.h"
#include "bltree.h"
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

    /**
    *  close and release memory
    */
    void ~BLTree() {
    #ifdef unix
        if (mem) free( mem );
    #else
        if (mem) VirtualFree( mem, 0, MEM_RELEASE );
    #endif
    }
    
    /**
    *  FUNCTION:  create
    *
    *  open BTree access method based on buffer manager
    */ 
    BLTree* BLTree::create( BufMgr* bufMgr ) {
    
        BLTree* tree = malloc( sizeof(BLTree) );
        memset( tree, 0, sizeof(BLTree) );
        tree->bufMgr = bufMgr;
    
    #ifdef unix
        tree->mem = valloc( 2 * bufMgr->page_size );
    #else
        tree->mem = VirtualAlloc( NULL, 2 * bufMgr->page_size, MEM_COMMIT, PAGE_READWRITE );
    #endif
    
        tree->frame = (Page *)tree->mem;
        tree->cursor = (Page *)(tree->mem + 1 * bufMgr->page_size);
        return tree;
    }
    
    /**
    *  FUNCTION:  fixfence
    *
    *  a fence key was deleted from a page,
    *  push new fence value upwards
    */
    BLTERR BLTree::fixfence( PageSet* set, uint lvl ) {
        uchar leftkey[BT_keyarray];
        uchar rightkey[BT_keyarray];
        uchar value[BtId];
        BLTKey* ptr;
        uint idx;
    
        // remove the old fence value
        ptr = keyptr(set->page, set->page->cnt);
        memcpy( rightkey, ptr, ptr->len + sizeof(BLTKey) );
        memset( slotptr(set->page, set->page->cnt--), 0, sizeof(BLTSlot) );
        set->latch->dirty = 1;
    
        // cache new fence value
        ptr = keyptr(set->page, set->page->cnt);
        memcpy( leftkey, ptr, ptr->len + sizeof(BLTKey) );
    
        BufMgr::lockpage( LockParent, set->latch );
        BufMgr::unlockpage( LockWrite, set->latch );
    
        // insert new (now smaller) fence key
        BLTVal::putid( value, set->latch->page_no );
        ptr = (BLTKey*)leftkey;
    
        if (insertkey( ptr->key, ptr->len, lvl+1, value, BtId, 1 )) {
            return err;
        }
    
        // now delete old fence key
        ptr = (BLTKey*)rightkey;
    
        if (deletekey( ptr->key, ptr->len, lvl+1 )) {
            return err;
        }
    
        BufMgr::unlockpage( LockParent, set->latch );
        unpinlatch( set->latch );
        return 0;
    }
    
    /**
    *  FUNCTION: collapseroot
    *
    *  root has a single child
    *  collapse a level from the tree
    */
    BLTERR BLTree::collapseroot( PageSet* root ) {
        PageSet child[1];
        uid page_no;
        uint idx;
    
        // find the child entry and promote as new root contents
    
        do {
            for (idx = 0; idx++ < root->page->cnt; ) {
                if (!slotptr(root->page, idx)->dead) break;
            }
        
            page_no = BLTVal::getid( valptr(root->page, idx)->value );
        
            if (child->latch = pinlatch( page_no, 1, &reads, &writes )) {
                child->page = mappage( child->latch );
            }
            else {
                return err;
            }
        
            BufMgr::lockpage( LockDelete, child->latch );
            BufMgr::lockpage( LockWrite, child->latch );
        
            memcpy( root->page, child->page, mgr->page_size );
            root->latch->dirty = 1;
            freepage( child );
        
        } while (root->page->lvl > 1 && root->page->act == 1);
        
        BufMgr::unlockpage( LockWrite, root->latch );
        unpinlatch( root->latch );
        return 0;
    }
    
    /**
    *  FUNCTION:  deletepage
    *
    *  delete a page and manage keys
    *  call with page writelocked
    *  returns with page unpinned
    */
    BLTERR BLTree::deletepage( PageSet* set, BLTLockMode mode ) {
        uchar lowerfence[BT_keyarray];
        uchar higherfence[BT_keyarray];
        uchar value[BtId];
        uint lvl = set->page->lvl;
        PageSet right[1];
        uid page_no;
        BLTKey* ptr;
    
        // cache copy of fence key to post in parent
        ptr = keyptr( set->page, set->page->cnt );
        memcpy( lowerfence, ptr, ptr->len + sizeof(BLTKey) );
    
        // obtain lock on right page
        page_no = BLTVal::getid( set->page->right );
    
        if (right->latch = pinlatch( page_no, 1, &reads, &writes )) {
            right->page = mappage( right->latch );
        }
        else {
            return 0;
        }
    
        BufMgr::lockpage( LockWrite, right->latch );
        BufMgr::lockpage( mode, right->latch );
    
        // cache copy of key to update
        ptr = keyptr( right->page, right->page->cnt );
        memcpy( higherfence, ptr, ptr->len + sizeof(BLTKey) );
    
        if (right->page->kill) {
            return (err = BLTERR_struct);
        }
    
        // pull contents of right peer into our empty page
        memcpy( set->page, right->page, mgr->page_size );
        set->latch->dirty = 1;
    
        // mark right page deleted and point it to left page
        // until we can post parent updates that remove access
        // to the deleted page.
        BLTVal::putid( right->page->right, set->latch->page_no );
        right->latch->dirty = 1;
        right->page->kill = 1;
    
        BufMgr::lockpage( LockParent, right->latch );
        BufMgr::unlockpage( LockWrite, right->latch );
        BufMgr::unlockpage( mode, right->latch );
        BufMgr::lockpage( LockParent, set->latch );
        BufMgr::unlockpage( LockWrite, set->latch );
    
        // redirect higher key directly to our new node contents
        BLTVal::putid( value, set->latch->page_no );
        ptr = (BLTKey*)higherfence;
    
        if (insertkey( ptr->key, ptr->len, lvl+1, value, BtId, 1 )) {
            return err;
        }
    
        // delete old lower key to our node
        ptr = (BLTKey*)lowerfence;
    
        if (deletekey( ptr->key, ptr->len, lvl+1 )) {
            return err;
        }
    
        // obtain delete and write locks to right node
        BufMgr::unlockpage( LockParent, right->latch );
        BufMgr::lockpage( LockDelete, right->latch );
        BufMgr::lockpage( LockWrite, right->latch );
        freepage( right );
        BufMgr::unlockpage( LockParent, set->latch );
        unpinlatch( set->latch );
        found = 1;
        return 0;
    }
    
    
    //
    //  find and delete key on page by marking delete flag bit
    //  if page becomes empty, delete it from the btree
    //
    BLTERR BLTree::deletekey( uchar* key, uint len, uint lvl ) {
        uint slot;
        uint idx;
        uint found;
        uint fence;
        PageSet set[1];
        BLTKey* ptr;
        BLTVal* val;
    
        if (slot = loadpage( set, key, len, lvl, LockWrite )) {
            ptr = keyptr( set->page, slot );
        }
        else {
            return err;
        }
    
        // if librarian slot, advance to real slot
        if (slotptr( set->page, slot )->type == Librarian) {
            ptr = keyptr(set->page, ++slot);
        }
    
        fence = (slot == set->page->cnt);
    
        // if key is found delete it, otherwise ignore request
        if (found = !BLTKey::keycmp (ptr, key, len) ) {
            if (found = slotptr(set->page, slot)->dead == 0 ) {
                val = valptr(set->page,slot);
                slotptr(set->page, slot)->dead = 1;
                set->page->garbage += ptr->len + val->len + sizeof(BLKey) + sizeof(BLTVal);
                set->page->act--;
        
                // collapse empty slots beneath the fence
                while (idx = set->page->cnt - 1) {
                    if (slotptr( set->page, idx )->dead) {
                        *slotptr( set->page, idx ) = *slotptr( set->page, idx + 1 );
                        memset( slotptr(set->page, set->page->cnt--), 0, sizeof(BLTSlot) );
                    }
                    else {
                        break;
                    }
                }
            }
        }
    
        // did we delete a fence key in an upper level?
        if (found && lvl && set->page->act && fence) {
            if (fixfence( set, lvl )) {
                return err;
            }
            else {
                found = found;
                return 0;
            }
        }
    
        // do we need to collapse root?
        if (lvl > 1 && set->latch->page_no == ROOT_page && set->page->act == 1) {
            if (collapseroot( set )) {
                return err;
            }
            else {
               found = found;
               return 0;
            }
        }
    
        // delete empty page
        if( !set->page->act ) {
            return deletepage( set, 0 );
        }
        set->latch->dirty = 1;
        BufMgr::unlockpage( LockWrite, set->latch );
        unpinlatch( set->latch );
        found = found;
        return 0;
    }
    
    BLTKey* BLTree::foundkey() {
        return (BLTKey*)key;
    }
    
    //
    // advance to next slot
    //
    uint BLTree::findnext( PageSet* set, uint slot ) {
    
        if (slot < set->page->cnt) return slot + 1;
        LatchSet* prevlatch = set->latch;
        uid page_no;
        if (page_no = BLTVal::getid( set->page->right )) {
            if ( (set->latch = pinlatch( page_no, 1, &reads, &writes )) {
                set->page = mappage( set->latch );
            }
            else {
                return 0;
            }
        }
        else {
            err = BLTERR_struct;
            return 0;
        }
    
        // obtain access lock using lock chaining with Access mode
        BufMgr::lockpage( LockAccess, set->latch );
        BufMgr::unlockpage( LockRead, prevlatch );
        unpinlatch( prevlatch );
        BufMgr::lockpage( LockRead, set->latch );
        BufMgr::unlockpage( LockAccess, set->latch );
        return 1;
    }
    
    //
    // find unique key or first duplicate key in
    //   leaf level and return number of value bytes
    //   or (-1) if not found.  Setup key for foundkey
    //
    int BLTree::findkey( uchar *key, uint keylen, uchar *value, uint valmax ) {
        PageSet set[1];
        uint len;
        uint slot;
        int ret = -1;
        BLTKey *ptr;
        BLTVal *val;
    
        if ( (slot = loadpage( set, key, keylen, 0, LockRead )) ) {
            do {
                ptr = keyptr(set->page, slot);
            
                // skip librarian slot place holder
                if (LIbrarian == slotptr(set->page, slot)->type) {
                    ptr = keyptr(set->page, ++slot);
                }
            
                // return actual key found
                memcpy( key, ptr, ptr->len + sizeof(BLTKey) );
                len = ptr->len;
            
                if (Duplicate == slotptr(set->page, slot)->type) {
                    len -= BtId;
                }
            
                // not there if we reach the stopper key
                if (slot == set->page->cnt) {
                    if( !BLTVal::getid( set->page->right )) {
                        break;
                    }
                }
            
                // if key exists, return >= 0 value bytes copied
                // otherwise return (-1)
                if (slotptr(set->page, slot)->dead) continue;
            
                if (keylen == len) {
                    if (!memcmp( ptr->key, key, len )) {
                        val = valptr(set->page,slot);
                        if (valmax > val->len ) valmax = val->len;
                        memcpy( value, val->value, valmax );
                        ret = valmax;
                    }
                }
            
                break;
            
            } while( (slot = findnext( set, slot )) );
        }
    
        BufMgr::unlockpage( LockRead, set->latch );
        unpinlatch( set->latch );
        return ret;
    }
    
    /**
    *  FUNCTION: cleanpage
    *
    *  check page for space available,
    *    clean if necessary and return
    *    0 - page needs splitting
    *    >0  new slot value
    */
    uint BLTree::cleanpage( PageSet* set, uint keylen, uint slot, uint vallen ) {
        uint nxt = mgr->page_size;
        Page page = set->page;
        uint cnt = 0;
        uint idx = 0;
        uint max = page->cnt;
        uint newslot = max;
        BLTKey *key;
        BLTVal *val;
    
        if (page->min >= (max+2)*sizeof(Slot)
                            + sizeof(*page)
                            + keylen + sizeof(BLTKey)
                            + vallen + sizeof(BLTVal)) { return slot; }
    
        // skip cleanup and proceed to split
        // if there's not enough garbage to bother with.
        if (page->garbage < nxt / 5) return 0;
    
        memcpy( frame, page, mgr->page_size );
    
        // skip page info and set rest of page to zero
        memset( page+1, 0, mgr->page_size - sizeof(*page) );
        set->latch->dirty = 1;
        page->garbage = 0;
        page->act = 0;
    
        // clean up page first by removing deleted keys
        while (cnt++ < max) {
            if (cnt == slot) newslot = idx + 2;
            if (cnt < max && slotptr(frame,cnt)->dead) continue;
    
            // copy the value across
            val = valptr(frame, cnt);
            nxt -= val->len + sizeof(BLTVal);
            memcpy( (uchar *)page + nxt, val, val->len + sizeof(BLTVal));
    
            // copy the key across
            key = keyptr(frame, cnt);
            nxt -= key->len + sizeof(BLTKey);
            memcpy( (uchar *)page + nxt, key, key->len + sizeof(BLTKey) );
    
            // make a librarian slot
            if (idx) {
                slotptr(page, ++idx)->off = nxt;
                slotptr(page, idx)->type = Librarian;
                slotptr(page, idx)->dead = 1;
            }
    
            // set up the slot
            slotptr(page, ++idx)->off = nxt;
            slotptr(page, idx)->type = slotptr(frame, cnt)->type;
    
            if (!(slotptr(page, idx)->dead = slotptr(frame, cnt)->dead)) page->act++;
        }
    
        page->min = nxt;
        page->cnt = idx;
    
        // see if page has enough space now, or does it need splitting?
        if (page->min >= (idx+2) * sizeof(Slot)
                            + sizeof(*page)
                            + keylen + sizeof(BLTKey)
                            + vallen + sizeof(BLTVal) ) {
            return newslot;
        }
    
        return 0;
    }
    
    /**
    *  FUNCTION:  splitroot
    *
    *  split the root and raise the height of the btree
    */
    BLTERR BLTree::splitroot( PageSet* root, LatchSet* right) {  
        uchar leftkey[BT_keyarray];
        uint nxt = mgr->page_size;
        uchar value[BtId];
        PageSet left[1];
        uid left_page_no;
        BLTKey* ptr;
        BLTVal* val;
    
        // save left page fence key for new root
        ptr = keyptr(root->page, root->page->cnt);
        memcpy( leftkey, ptr, ptr->len + sizeof(BLTKey) );
    
        //  Obtain an empty page to use, and copy the current
        //  root contents into it, e.g. lower keys
        if (newpage( left, root->page )) return err; 
    
        left_page_no = left->latch->page_no;
        unpinlatch( left->latch );
    
        // preserve the page info at the bottom
        // of higher keys and set rest to zero
        memset( root->page+1, 0, mgr->page_size - sizeof(*root->page) );
    
        // insert stopper key at top of newroot page
        // and increase the root height
        nxt -= BtId + sizeof(BLTVal);
        BLTVal::putid( value, right->page_no );
        val = (BLTVal *)((uchar *)root->page + nxt);
        memcpy( val->value, value, BtId );
        val->len = BtId;
    
        nxt -= 2 + sizeof(BLTKey);
        slotptr(root->page, 2)->off = nxt;
        ptr = (BLTKey *)((uchar *)root->page + nxt);
        ptr->len = 2;
        ptr->key[0] = 0xff;
        ptr->key[1] = 0xff;
    
        // insert lower keys page fence key on newroot page as first key
        nxt -= BtId + sizeof(BLTVal);
        BLTVal::putid( value, left_page_no );
        val = (BLTVal *)((uchar *)root->page + nxt);
        memcpy( val->value, value, BtId );
        val->len = BtId;
    
        ptr = (BLTKey *)leftkey;
        nxt -= ptr->len + sizeof(BLTKey);
        slotptr(root->page, 1)->off = nxt;
        memcpy( (uchar *)root->page + nxt, leftkey, ptr->len + sizeof(BLTKey) );
        
        BLTVal::putid( root->page->right, 0 );
        root->page->min = nxt;        // reset lowest used offset and key count
        root->page->cnt = 2;
        root->page->act = 2;
        root->page->lvl++;
    
        // release and unpin root pages
        BufMgr::unlockpage( LockWrite, root->latch );
        unpinlatch( root->latch );
    
        unpinlatch( right );
        return 0;
    }
    
    
    /**
    *  FUNCTION:  splitpage
    *
    *  split already locked full node; leave it locked.
    *  @return pool entry for new right page, unlocked
    */
    uint BLTree::splitpage( PageSet* set ) {
        uint cnt = 0;
        uint idx = 0;
        uint max;
        uint nxt = mgr->page_size;
        uint lvl = set->page->lvl;
        PageSet right[1];
        BLTKey* key;
        BLTKey* ptr;
        BLTVal* val;
        BLTVal* src;
        uid right2;
        uint prev;
    
        //  split higher half of keys to frame
        memset( frame, 0, mgr->page_size );
        max = set->page->cnt;
        cnt = max / 2;
        idx = 0;
    
        while (cnt++ < max) {
            if (slotptr(set->page, cnt)->dead && cnt < max) continue;
            src = valptr( set->page, cnt );
            nxt -= src->len + sizeof(BLTVal);
            memcpy( (uchar *)frame + nxt, src, src->len + sizeof(BLTVal) );
    
            key = keyptr(set->page, cnt);
            nxt -= key->len + sizeof(BLTKey);
            ptr = (BLTKey*)((uchar *)frame + nxt);
            memcpy( ptr, key, key->len + sizeof(BLTKey) );
    
            // add librarian slot
            if (idx) {
                slotptr( frame, ++idx)->off = nxt;
                slotptr( frame, idx)->type = Librarian;
                slotptr( frame, idx)->dead = 1;
            }
    
            //  add actual slot
            slotptr( frame, ++idx )->off = nxt;
            slotptr( frame, idx )->type = slotptr( set->page, cnt )->type;
    
            if (!(slotptr( frame, idx )->dead = slotptr( set->page, cnt )->dead)) {
                frame->act++;
            }
        }
    
        frame->bits = mgr->page_bits;
        frame->min = nxt;
        frame->cnt = idx;
        frame->lvl = lvl;
    
        // link right node
        if (set->latch->page_no > ROOT_page) {
            BLTVal::putid( frame->right, BLTVal::getid( set->page->right ) );
        }
    
        // get new free page and write higher keys to it.
        if (newpage( right, frame )) {
            return 0;
        }
    
        memcpy( frame, set->page, mgr->page_size );
        memset( set->page+1, 0, mgr->page_size - sizeof(*set->page) );
        set->latch->dirty = 1;
    
        nxt = mgr->page_size;
        set->page->garbage = 0;
        set->page->act = 0;
        max /= 2;
        cnt = 0;
        idx = 0;
    
        if (slotptr( frame, max )->type == Librarian) { max--; }
    
        // assemble page of smaller keys
        while (cnt++ < max) {
            if (slotptr(frame, cnt)->dead) continue;
            val = valptr(frame, cnt);
            nxt -= val->len + sizeof(BLTVal);
            memcpy( (uchar *)set->page + nxt, val, val->len + sizeof(BLTVal) );
    
            key = keyptr(frame, cnt);
            nxt -= key->len + sizeof(BLTKey);
            memcpy( (uchar *)set->page + nxt, key, key->len + sizeof(BLTKey) );
    
            // add librarian slot
            if (idx) {
                slotptr(set->page, ++idx)->off = nxt;
                slotptr(set->page, idx)->type = Librarian;
                slotptr(set->page, idx)->dead = 1;
            }
    
            // add actual slot
            slotptr(set->page, ++idx)->off = nxt;
            slotptr(set->page, idx)->type = slotptr(frame, cnt)->type;
            set->page->act++;
        }
    
        BLTVal::putid( set->page->right, right->latch->page_no );
        set->page->min = nxt;
        set->page->cnt = idx;
    
        return right->latch->entry;
    }
    
    /**
    *  FUNCTION: splitkeys
    *
    *  fix keys for newly split page
    *  call with page locked,
    *  @return unlocked
    */
    BLTERR BLTree::splitkeys( PageSet* set, LatchSet* right) {
        uchar leftkey[BT_keyarray];
        uchar rightkey[BT_keyarray];
        uchar value[BtId];
        uint lvl = set->page->lvl;
        BLTKey *ptr;
    
        // if current page is the root page, split it
        if (ROOT_page == set->latch->page_no) {
            return splitroot( set, right );
        }
    
        Page* ptr = keyptr(set->page, set->page->cnt);
        memcpy( leftkey, ptr, ptr->len + sizeof(BLTKey) );
    
        page = mappage( right );
    
        ptr = keyptr(page, page->cnt);
        memcpy (rightkey, ptr, ptr->len + sizeof(BLTKey));
    
        // insert new fences in their parent pages
        BufMgr::lockpage( LockParent, right );
        BufMgr::lockpage( LockParent, set->latch );
        BufMgr::unlockpage( LockWrite, set->latch );
    
        // insert new fence for reformulated left block of smaller keys
        BLTVal::putid( value, set->latch->page_no );
        ptr = (BLTKey *)leftkey;
    
        if (insertkey( ptr->key, ptr->len, lvl+1, value, BtId, 1 )) {
            return err;
        }
    
        // switch fence for right block of larger keys to new right page
        BLTVal::putid( value, right->page_no );
        ptr = (BLTKey *)rightkey;
    
        if (insertkey( ptr->key, ptr->len, lvl+1, value, BtId, 1 )) {
            return err;
        }
    
        BufMgr::unlockpage( LockParent, set->latch );
        unpinlatch( set->latch );
        BufMgr::unlockpage( LockParent, right );
        unpinlatch( right );
        return 0;
    }
    
    /**
    *  FUNCTION:  insertslot
    *
    *  install new key and value onto page
    *  page must already be checked for adequate space
    */ 
    BLTERR BLTree::insertslot( PageSet* set, uint slot,
                                uchar *key, uint keylen,
                                uchar* value, uint vallen,
                                uint type, uint release)
    {
        uint idx;
        uint librarian;
        Slot *node;
        BLTKey *ptr;
        BLTVal *val;
    
        // if found slot > desired slot and previous slot
        // is a librarian slot, use it
        if (slot > 1) {
            if (Librarian == slotptr(set->page, slot-1)->type) slot--;
        }
    
        // copy value onto page
        set->page->min -= vallen + sizeof(BLTVal);
        val = (BLTVal*)((uchar *)set->page + set->page->min);
        memcpy( val->value, value, vallen );
        val->len = vallen;
    
        // copy key onto page
        set->page->min -= keylen + sizeof(BLTKey);
        ptr = (BLTKey*)((uchar *)set->page + set->page->min);
        memcpy( ptr->key, key, keylen );
        ptr->len = keylen;
        
        // find first empty slot
        for (idx = slot; idx < set->page->cnt; idx++) {
            if (slotptr( set->page, idx )->dead) break;
        }
    
        // now insert key into array before slot
        if (idx == set->page->cnt) {
            idx += 2;
            set->page->cnt += 2;
            librarian = 2;
        }
        else {
            librarian = 1;
        }
    
        set->latch->dirty = 1;
        set->page->act++;
    
        while (idx > slot + librarian - 1) {
            *slotptr(set->page, idx) = *slotptr( set->page, idx - librarian );
            idx--;
        }
    
        // add librarian slot
        if (librarian > 1) {
            node = slotptr( set->page, slot++ );
            node->off = set->page->min;
            node->type = Librarian;
            node->dead = 1;
        }
    
        // fill in new slot
        node = slotptr(set->page, slot);
        node->off = set->page->min;
        node->type = type;
        node->dead = 0;
    
        if (release) {
            BufMgr::unlockpage( LockWrite, set->latch );
            unpinlatch( set->latch );
        }
    
        return 0;
    }
    
    /**
    *  FUNCTION: insertkey
    *
    *  Insert new key into the btree at given level.
    *    either add a new key or update/add an existing one
    */
    BLTERR BLTree::insertkey( uchar* key, uint keylen, uint lvl,
                              void* value, uint vallen,
                              uint unique )
    {
        uchar newkey[BT_keyarray];
        uint slot;
        uint idx;
        uint len;
        uint entry;
        PageSet set[1];
        BLTKey* ptr;
        BLTKey* ins;
        uid sequence;
        BLTVal* val;
        uint type;
    
        // set up the key we're working on
        ins = (BLTKey*)newkey;
        memcpy( ins->key, key, keylen );
        ins->len = keylen;
    
        // is this a non-unique index value?
        if (unique) {
          type = Unique;
        }
        else {
            type = Duplicate;
            sequence = newdup();
            BLTVal::putid( ins->key + ins->len + sizeof(BLTKey), sequence );
            ins->len += BtId;
        }
      
        while ( true ) { // find the page and slot for the current key
            if (slot = loadpage( set, ins->key, ins->len, lvl, LockWrite) )
                ptr = keyptr(set->page, slot);
            else {
                if (!err) err = BLTERR_ovflw;
                return err;
            }
        
            // if librarian slot == found slot, advance to real slot
            if (Librarian == slotptr(set->page, slot)->type) {
                if (!BLTKey::keycmp(ptr, key, keylen)) {
                    ptr = keyptr(set->page, ++slot);
                }
            }
        
            len = ptr->len;
        
            if (Duplicate == slotptr(set->page, slot)->type) len -= BtId;
        
            // if inserting a duplicate key or unique key
            //   check for adequate space on the page
            //   and insert the new key before slot.
            if (unique && (len != ins->len || memcmp( ptr->key, ins->key, ins->len )) || !unique ) {
                if (!(slot = cleanpage( set, ins->len, slot, vallen )) ) {
                    if ( !(entry = splitpage( set )) ) {
                        return err;
                    }
                    else if (splitkeys( set, mgr->latchsets + entry )) {
                        return err;
                    }
                }
                else {
                    continue;
                }
                return insertslot( set, slot, ins->key, ins->len, value, vallen, type, 1 );
            }
        
            // if key already exists, update value and return
            val = valptr(set->page, slot);
        
            if (val->len >= vallen) {
                if (slotptr(set->page, slot)->dead) set->page->act++;
                set->page->garbage += val->len - vallen;
                set->latch->dirty = 1;
                slotptr(set->page, slot)->dead = 0;
                val->len = vallen;
                memcpy (val->value, value, vallen);
                BufMgr::unlockpage( LockWrite, set->latch );
                unpinlatch( set->latch );
                return 0;
            }
        
            // new update value doesn't fit in existing value area
            if (!slotptr(set->page, slot)->dead) {
                set->page->garbage += val->len + ptr->len + sizeof(BLTKey) + sizeof(BLTVal);
            }
            else {
                slotptr(set->page, slot)->dead = 0;
                set->page->act++;
            }
        
            if ( !(slot = cleanpage( set, keylen, slot, vallen )) ) {
                if ( !(entry = splitpage( set )) ) {
                    return err;
                }
                else if( splitkeys( set, mgr->latchsets + entry )) {
                    return err;
                }
                else {
                    continue;
                }
            }
        
            set->page->min -= vallen + sizeof(BLTVal);
            val = (BLTVal*)((uchar *)set->page + set->page->min);
            memcpy (val->value, value, vallen);
            val->len = vallen;
        
            set->latch->dirty = 1;
            set->page->min -= keylen + sizeof(BLTKey);
            ptr = (BLTKey*)((uchar *)set->page + set->page->min);
            memcpy (ptr->key, key, keylen);
            ptr->len = keylen;
            
            slotptr(set->page, slot)->off = set->page->min;
            BufMgr::unlockpage( LockWrite, set->latch );
            unpinlatch( set->latch );
            return 0;
    
        }   // end while
    
        return 0;
    }
    
    struct AtomicMod {
        uint entry;            // latch table entry number
        uint slot:30;        // page slot number
        uint reuse:1;        // reused previous page
        uint emptied:1;        // page was emptied
    };
    
    struct AtomicKey {
        uid page_no;        // page number for split leaf
        void* next;            // next key to insert
        uint entry;            // latch table entry number
        uchar leafkey[BT_keyarray];
    };
    
    /**
    *  FUNCTION: atomicpage
    *
    *  determine actual page where key is located
    *  return slot number with set page locked
    */
    uint BLTree::atomicpage( Page* source, AtomicMod* locks, uint src, PageSet* set) {
        BLTKey* key = keyptr( source, src );
        uint slot = locks[src].slot;
        uint entry;
    
        if (src > 1 && locks[src].reuse) {
            entry = locks[src-1].entry, slot = 0;
        }
        else {
            entry = locks[src].entry;
        }
    
        if (slot) {
            set->latch = mgr->latchsets + entry;
            set->page = mappage( set->latch );
            return slot;
        }
    
        // is locks->reuse set?
        // if so, find where our key
        // is located on previous page or split pages
        do {
            set->latch = mgr->latchsets + entry;
            set->page = mappage( set->latch );
    
            if ( (slot = Page::findslot( set->page, key->key, key->len )) {
                if( locks[src].reuse ) locks[src].entry = entry;
                return slot;
            }
        } while ( (entry = set->latch->split) );
    
        err = BLTERR_atomic;
        return 0;
    }
 

    /**
    *  FUNCTION: atomicinsert
    */
    BLTERR BLTree::atomicinsert( Page* source, AtomicMod* locks, uint src ) {
        BLTKey* key = keyptr(source, src);
        BLTVal* val = valptr(source, src);
        LatchSet* latch;
        PageSet set[1];
        uint entry;
    
        while ( (locks[src].slot = atomicpage( source, locks, src, set )) ) {

            if (locks[src].slot = cleanpage( set, key->len, locks[src].slot, val->len )) {
                return insertslot( set, locks[src].slot,
                                   key->key, key->len,
                                   val->value, val->len,
                                   slotptr(source, src)->type, 0 );
            }
        
            uint entry = splitpage( set );

            if (entry) {
                latch = mgr->latchsets + entry;
            }
            else {
                return err;
            }
        
            // splice right page into split chain and WriteLock it.
            latch->split = set->latch->split;
            set->latch->split = entry;
            BufMgr::lockpage( LockWrite, latch );
        }
        return (err = BLTERR_atomic);
    }
 
    
    /**
    *  FUNCTION: atomicdelete
    */
    BLTERR BLTree::atomicdelete( Page* source, AtomicMod* locks, uint src ) {
        BLTKey* key = keyptr( source, src );
        uint idx;
        uint slot;
        PageSet set[1];
    
        if ( (slot = atomicpage( source, locks, src, set )) ) {
            slotptr(set->page, slot)->dead = 1;
        }
        else {
            return (err = BLTERR_struct);
        }
    
        BLTKey* ptr = keyptr( set->page, slot );
        BLTVal* val = valptr( set->page, slot );
        set->page->garbage += ptr->len + val->len + sizeof(BLTKey) + sizeof(BLTVal);
        set->page->act--;
    
        // collapse empty slots beneath the fence
        while (idx = set->page->cnt - 1) {
            if (slotptr( set->page, idx )->dead) {
                *slotptr( set->page, idx ) = *slotptr(set->page, idx + 1);
                memset( slotptr(set->page, set->page->cnt--), 0, sizeof(BLTSlot) );
            } else {
                break;
            }
        }
    
        set->latch->dirty = 1;
        return 0;
    }
    
    /**
    *  FUNCTION: qsortcmp
    *
    *  qsort comparison function
    */
    int qsortcmp( BLTSlot* slot1, BLTSlot* slot2, Page* page ) {
        BLTKey* key1 = (BLTKey *)((uchar *)page + slot1->off);
        BLTKey* key2 = (BLTKey *)((uchar *)page + slot2->off);
        return BLTKey::keycmp( key1, key2->key, key2->len );
    }
    
    
    /**
    *  FUNCTION:  atomicmods
    *
    *  atomic modification of a batch of keys.
    *     return -1 if BLTERR is set
    *     otherwise return slot number causing the key constraint violation
    *     or zero on successful completion.
    * 
    *     source contains (key,value) pairs, with the opcode encoded in slot 'type'
    */
    int BLTree::atomicmods( Page* source ) {
    
        PageSet set[1];
        PageSet prev[1];
        uchar value[BtId];
    
        BLTKey* ptr;
        BLTKey* key;
        BLTKey* key2;
    
        LatchSet* latch;
        AtomicMod* locks;
        int result = 0;
        BLTVal* val;
    
        // one lock per source page entry
        locks = calloc( source->cnt + 1, sizeof(AtomicMod) );
        AtomicKey* head = NULL;
        AtomicKey* tail = NULL;
        AtomicKey* leaf;
    
        // First sort the list of keys into order to prevent deadlocks between threads.
        qsort_r( slotptr(source,1), source->cnt, sizeof(BLTSlot),
                    (__compar_d_fn_t)qsortcmp, source );
      
        // Validate transaction:
        //   Load the leafpage for each key
        //   and determine any constraint violations
        for (uint src = 0; src++ < source->cnt; ) {  // i.e.  for (ops in Ops) ..
            key = keyptr(source, src);
            val = valptr(source, src);
            uint slot = 0;
            bool samepage = false;
        
            // First determine if this modification falls
            //   on the same page as the previous modification
            //   *note that the far right leaf page is a special case
            if ( (samepage = src > 1) ) {
    
                // skip initial step ('set' uninitialized), otherwise see if key is on set->page
                if ( (samepage = !BLTVal::getid( set->page->right )
                      || BLTKey::keycmp( keyptr( set->page, set->page->cnt ),
                                            key->key, key->len ) > 0) ) {
                    slot = Page::findslot( set->page, key->key, key->len );
                }
                else { // release read on previous page
                    BufMgr::unlockpage( LockRead, set->latch ); 
                }
            }
        
            if (!slot) { // not on same page as previous op, get page
                if ( slot = loadpage( set, key->key, key->len, 0, LockAtomic | LockRead )) {
                    set->latch->split = 0;
                }
                else {
                    return -1;  // i.e. ERROR
                }
            }
        
            if (Librarian == slotptr( set->page, slot )->type) {
                // step past librarian slot
                ptr = keyptr(set->page, ++slot);
            }
            else {
                ptr = keyptr( set->page, slot );
             }
        
            if (!samepage) {
                locks[src].entry = set->latch->entry;
                locks[src].slot  = slot;
                locks[src].reuse = 0;
            } else {
                // i.e. entry and slot are in locks[src-1]
                locks[src].entry = 0;
                locks[src].slot  = 0;
                locks[src].reuse = 1;
            }
        
            switch (slotptr( source, src )->type) {
            case Duplicate: {
                // tbd
                break;
            }
            case Unique: {
                if (!slotptr( set->page, slot )->dead) {
        
                    // check : if not fence, or if fence, not infinite
                    if (slot < set->page->cnt || BLTVal::getid( set->page->right )) {
        
                        // then compare keys - could be keycmp
                        if (ptr->len == key->len && !memcmp( ptr->key, key->key, key->len )) {
        
                            // return constraint violation if key already exists
                            result = src;
                            BufMgr::unlockpage( LockRead, set->latch );
                            // all prev pages already read unlocked
        
                            while (src) {
                                // remove all previous atomic locks
                                if (locks[src].entry) {
                                    set->latch = mgr->latchsets + locks[src].entry;
                                    BufMgr::unlockpage( LockAtomic, set->latch );
                                    unpinlatch( set->latch );
                                }
                                src--;
                            }
        
                            free( locks );
                            return result;
                        }
                    }
                }
                break;
            }
            case Delete: {
                if (!slotptr( set->page, slot )->dead) {
                    if (ptr->len == key->len) {
                        if (!memcmp( ptr->key, key->key, key->len )) {
                            break;
                        }
                    }
                }
        
                // Key to delete doesn't exist
                result = src;
                BufMgr::unlockpage( LockRead, set->latch );
        
                while (src) {
                    if (locks[src].entry) {
                        set->latch = mgr->latchsets + locks[src].entry;
                        BufMgr::unlockpage( LockAtomic, set->latch );
                        unpinlatch( set->latch );
                    }
                    src--;
                }
        
                return result;
            }
            }   // end switch
    
        } // end for
    
    
        // unlock last loadpage lock
        if (source->cnt > 1) {
            BufMgr::unlockpage( LockRead, set->latch );
        }
    
        // obtain write lock for each page
        for (uint src = 0; src++ < source->cnt; ) {
            if (locks[src].entry) {
                BufMgr::lockpage( LockWrite, mgr->latchsets + locks[src].entry );
            }
        }
    
        // insert or delete each key
        for (uint src = 0; src++ < source->cnt; ) {
            if (Delete == slotptr( source, src )->type) {
                if (atomicdelete( source, locks, src)) {
                    return -1;
                }
                else {
                    continue;
                }
            }
            else {
                if (atomicinsert( source, locks, src )) {
                    return -1;
                }
                else {
                    continue;
                }
            }
        }
    
        // set ParentModification and release WriteLock
        // leave empty pages locked for later processing
        // build queue of keys to insert for split pages
    
        for (uint src = 0; src++ < source->cnt; ) {  // i.e.  for (ops in Ops) ..
            uint next;
            if (locks[src].reuse) continue;
    
            prev->latch = mgr->latchsets + locks[src].entry;
            prev->page = mappage( prev->latch );
        
            // pick-up all splits from original page
            uint split = (next = prev->latch->split);
        
            if (!prev->page->act) {
                locks[src].emptied = 1;
            }
    
            uint entry; 
            while ( (entry = next) ) {
                set->latch = mgr->latchsets + entry;
                set->page = mappage( set->latch );
                next = set->latch->split;
                set->latch->split = 0;
          
                // delete empty previous page
                if (!prev->page->act) {
    
                    // recycle prev: pull 'set' content into prev, free 'set'
                    memcpy( prev->page, set->page, mgr->page_size );
                    BufMgr::lockpage( LockDelete, set->latch );
                    freepage( set );
            
                    prev->latch->dirty = 1;
            
                    if (prev->page->act) {
                        locks[src].emptied = 0;
                    }
            
                    continue; // following list of 'split' entries till non-empty page
                }
          
                // at this point a non-empty split leaf page has been found
          
                // prev page is not emptied 
                locks[src].emptied = 0;
          
                // schedule previous fence key update
                ptr = keyptr(prev->page,prev->page->cnt); // i.e. fence key
          
                // leaf is an entry in a fifo queue of level >=1 updates due to splits
                leaf = malloc( sizeof(AtomicKey) );
                memcpy( leaf->leafkey, ptr, ptr->len + sizeof(BLTKey) );
                leaf->page_no = prev->latch->page_no;
                leaf->entry = prev->latch->entry;
                leaf->next = NULL;
          
                if (tail) { // initialized to NULL
                    tail->next = leaf;
                }
                else {
                    head = leaf;
                }
          
                tail = leaf;
          
                // remove empty block from the split chain
                if (!set->page->act) {
                    memcpy( prev->page->right, set->page->right, BtId );
                    BufMgr::lockpage( LockDelete, set->latch );
                    freepage( set );
                    continue;
                }
          
                BufMgr::lockpage( LockParent, prev->latch );
                BufMgr::unlockpage( LockWrite, prev->latch );
                *prev = *set;
        
            } // end while
        
            //  was entire chain emptied?
            if (!prev->page->act) continue;
        
            if (!split) {
                BufMgr::unlockpage( LockWrite, prev->latch );
                continue;
            }
        
            // process last page split in chain
            ptr = keyptr( prev->page,prev->page->cnt );
            leaf = malloc( sizeof(AtomicKey) );
            memcpy( leaf->leafkey, ptr, ptr->len + sizeof(BLTKey) );
            leaf->page_no = prev->latch->page_no;
            leaf->entry = prev->latch->entry;
            leaf->next = NULL;
        
            // add 'leaf' to fifo
            if (tail) {
                tail->next = leaf;
            }
            else {
                head = leaf;
            }
        
            tail = leaf;
            BufMgr::lockpage( LockParent, prev->latch );
            BufMgr::unlockpage( LockWrite, prev->latch );
    
        } // end for ( op in Ops )
      
        // remove Atomic locks and process any empty pages
        for (uint src = source->cnt; src; src--) {
            if (locks[src].reuse) continue;
        
            set->latch = mgr->latchsets + locks[src].entry;
            set->page = mappage( set->latch );
        
            // clear original page split field
            uint split = set->latch->split;
            set->latch->split = 0;
            BufMgr::unlockpage( LockAtomic, set->latch );
        
            // delete page emptied by our atomic action
            if (locks[src].emptied) {
                if (deletepage( set, LockAtomic )) {
                    return err;
                }
                else {
                    continue;
                }
            }
        
            if (!split) {
                unpinlatch( set->latch );
            }
        }
    
        //  add keys for any pages split during action
        if ( (leaf = head) )
            do {
                ptr = (BLTKey *)leaf->leafkey;
                BLTVal::putid( value, leaf->page_no );
                if (insertkey( ptr->key, ptr->len, 1, value, BtId, 1 )) {
                    return err;
                }
                BufMgr::unlockpage( LockParent, mgr->latchsets + leaf->entry );
                unpinlatch( mgr->latchsets + leaf->entry );
                tail = leaf->next;
                free (leaf);
             } while ( (leaf = tail) );
        }
    
        // return success
        free( locks );
        return 0;
    }
    
    
    // iterator methods
    
    /**
    *  FUNCTION:  nextkey
    *
    *   return next slot on cursor page
    *   or slide cursor right into next page
    */
    uint BLTree::nextkey( uint slot ) {
        PageSet set[1];
        uid right;
    
        do {
            right = BLTVal::getid(cursor->right);
        
            while (slot++ < cursor->cnt) {
                if (slotptr( cursor,slot )->dead) {
                    continue;
                }
                else if( right || (slot < cursor->cnt) ) { // skip infinite stopper
                    return slot;
                }
                else {
                    break;
                }
            }
        
            if (!right) break;
            cursor_page = right;
        
            if (set->latch = pinlatch( right, 1, &reads, &writes )) {
                set->page = mappage( set->latch );
            }
            else {
                return 0;
            }
        
            BufMgr::lockpage( LockRead, set->latch);
            memcpy( cursor, set->page, mgr->page_size );
            BufMgr::unlockpage( LockRead, set->latch);
            unpinlatch( set->latch );
            slot = 0;
      
        } while( 1 );
    
        return (err = 0);
    }
    
    /**
    *  FUNCTION:  startkey
    *
    *  cache page of keys into cursor and return starting slot for given key
    */
    uint BLTree::startkey( uchar* key, uint len ) {
        PageSet set[1];
        uint slot;
    
        // cache page for retrieval
        if ( (slot = loadpage( set, key, len, 0, LockRead )) {
            memcpy( cursor, set->page, mgr->page_size );
        }
        else {
            return 0;
        }
    
        cursor_page = set->latch->page_no;
        BufMgr::unlockpage( LockRead, set->latch );
        unpinlatch( set->latch );
        return slot;
    }
    
    
    BLTKey* BLTree::key( uint slot ) { return keyptr( cursor, slot ); }
    BLTVal* BLTree::val( uint slot ) { return valptr( cursor,slot ); }
    
    
    /**
    *  FUNCTION:   latchaudit
    */
    uint BLTree::latchaudit() {
        LatchSet* latch;
        uint cnt = 0;
    
        if (*(ushort *)(mgr->lock)) {
            std::err <<  "Alloc page locked\n" );
        }
    
        *(ushort *)(mgr->lock) = 0;
    
        for (ushort idx = 1; idx <= mgr->latchdeployed; idx++) {
            latch = mgr->latchsets + idx;
            if (*latch->readwr->rin & MASK) {
                std::err <<  "latchset " << idx << " rwlocked for page "
                            << latch->page_no << std::endl;
            }
    
            memset ((ushort *)latch->readwr, 0, sizeof(RWLock));
    
            if (*latch->access->rin & MASK) {
                std::err <<  "latchset " << idx << " accesslocked for page "
                            << latch->page_no << std::endl;
            }
    
            memset ((ushort *)latch->access, 0, sizeof(RWLock));
    
            if (*latch->parent->rin & MASK) {
                std::err <<  "latchset " << idx << " parentlocked for page "
                            << latch->page_no << std::endl;
            }
    
            memset ((ushort *)latch->parent, 0, sizeof(RWLock));
    
            if (latch->pin) {
                std::err <<  "latchset " << idx << " pinned for page "
                            << latch->page_no << std::endl;
                latch->pin = 0;
            }
        }
    
        for (ushort hashidx = 0; hashidx < mgr->latchhash; hashidx++) {
            if (*(ushort *)(mgr->hashtable[hashidx].latch)) {
                  std::err <<  "hash entry " << hashidx << " locked" << std::endl;
            }
      
            *(ushort *)(mgr->hashtable[hashidx].latch) = 0;
      
            ushort idx;
            if ( (idx = mgr->hashtable[hashidx].slot) ) {
                do {
                    latch = mgr->latchsets + idx;
                    if (latch->pin) {
                        std::err <<  "latchset " << idx << " pinned for page "
                            << latch->page_no << std::endl;
                    }
                } while ( (idx = latch->next) );
            }
        }
    
        uid page_no = LEAF_page;
    
        while (page_no < BLTVal::getid( mgr->pagezero->alloc->right)) {
            uid off = page_no << mgr->page_bits;

    #ifdef unix
            pread (mgr->idx, frame, mgr->page_size, off);
    #else
            DWORD amt[1];
            SetFilePointer (mgr->idx, (long)off, (long*)(&off)+1, FILE_BEGIN);
            if (!ReadFile(mgr->idx, frame, mgr->page_size, amt, NULL)) {
                return (err = BLTERR_map);
            }
            if (*amt <  mgr->page_size ) {
                return (err = BLTERR_map);
            }
    #endif

            if (!frame->free && !frame->lvl ) {
                cnt += frame->act;
            }
            page_no++;
        }
            
        cnt--;    // do not count final 'stopper' key
        std::err << "Total keys read " << cnt << std::endl;
        close();
        return 0;
    }

}   // namespace mongo

