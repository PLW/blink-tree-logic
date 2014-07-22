//@file bltindex.cpp
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

#include "bltindex.h"
#include "common.h"
#include "latchmgr.h"
#include "logger.h"
#include "buffer_mgr.h"
#include "page.h"

#include <assert.h>
#include <iostream>
#include <pthread.h>
#include <stddef.h>
#include <stdlib.h>
#include <sstream>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#define BLTINDEX_TRACE  false

namespace mongo {

    void BLTIndex::close() {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );
        if (_mem) free(_mem);
    }
    
    /**
    *  Open BLTIndex access method.
    *  @param mgr  -  
    *  @param log  -  
    *  @return
    */
    BLTIndex* BLTIndex::create( BufferMgr* mgr, const char* thread ) {
        if (BLTINDEX_TRACE) Logger::logDebug( thread, "", __LOC__ );

        assert( NULL != mgr );
        assert( NULL != thread );

        BLTIndex* blt = new BLTIndex();
        memset( blt, 0, sizeof(*blt) );
        blt->_thread    = thread;
        blt->_mgr       = mgr;
        blt->_mem       = (uchar *)malloc( 3 * mgr->getPageSize() );
        blt->_frame     = (Page*)(blt->_mem);
        blt->_zero      = (Page*)(blt->_mem + 1 * mgr->getPageSize() );
        blt->_cursor    = (Page*)(blt->_mem + 2 * mgr->getPageSize() );
        memset( blt->_zero, 0, mgr->getPageSize() );

        return blt;
    }
    
    /**
    *  a fence key was deleted from a page: push new fence value upwards
    *  @param set -  
    *  @param level -  
    *  @return
    */
    BLTERR BLTIndex::fixFenceKey( PageSet* set, uint level ) {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        assert( NULL != set );

        uchar leftKey[256];
        uchar rightKey[256];
        PageId pageId;
        BLTKey* key;
    
        // remove the old fence value
        key = Page::keyptr( set->_page, set->_page->_cnt );
        memcpy( rightKey, key, key->_len + 1);
        memset( Page::slotptr( set->_page, set->_page->_cnt-- ), 0, sizeof(Slot) );
        set->_page->_dirty = 1;
    
        key = Page::keyptr( set->_page, set->_page->_cnt);
        memcpy( leftKey, key, key->_len + 1 );
        pageId = set->_pageNo;
    
        _mgr->lockPage( LockParent, set->_latch, _thread );
        _mgr->unlockPage( LockWrite, set->_latch, _thread );
    
        // insert new (now smaller) fence key
        if (insertKey( leftKey+1, *leftKey, level+1, pageId, time(NULL) )) {
            return _err;
        }
    
        // delete old fence key
        if (deleteKey( rightKey+1, *rightKey, level+1 )) {
            return _err;
        }
    
        _mgr->unlockPage( LockParent, set->_latch, _thread );
        _mgr->getLatchMgr()->unpinLatch( set->_latch, _thread );
        _mgr->unpinPool( set->_pool, _thread );
        return BLTERR_ok;
    }
    
    /**
    *  root has a single child: collapse a level from the tree.
    *  @param root
    *  @return
    */
    BLTERR BLTIndex::collapseRoot( PageSet* root ) {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        assert( NULL != root );

        PageSet child[1];
    
        // find the child entry and promote as new root contents
        do {
            uint idx;
            for (idx = 0; idx++ < root->_page->_cnt; ) {
                if (!Page::slotptr(root->_page, idx)->_dead) break;
            }
        
            child->_pageNo = Page::getid( Page::slotptr(root->_page, idx)->_id );
        
            child->_latch = _mgr->getLatchMgr()->pinLatch( child->_pageNo, _thread );
            _mgr->lockPage( LockDelete, child->_latch, _thread );
            _mgr->lockPage( LockWrite, child->_latch, _thread );
        
            if ( (child->_pool = _mgr->pinPool( child->_pageNo, _thread )) ) {
                child->_page = _mgr->page( child->_pool, child->_pageNo, _thread );
            }
            else {
                return _err;
            }
            memcpy( root->_page, child->_page, _mgr->getPageSize() );
            _mgr->freePage( child, _thread );
    
        } while (root->_page->_level > 1 && root->_page->_act == 1 );
    
        _mgr->unlockPage( LockWrite, root->_latch, _thread );
        _mgr->getLatchMgr()->unpinLatch( root->_latch, _thread );
        _mgr->unpinPool( root->_pool, _thread );
        return BLTERR_ok;
    }
    
    /**
    *  find and delete key on page by marking delete flag bit,
    *  if page becomes empty, delete it from the btree.
    */
    BLTERR BLTIndex::deleteKey( const uchar* inputKey, uint inputKeyLen, uint level ) {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        assert( NULL != inputKey );

        uchar lowerFence[256];
        uchar higherFence[256];
        PageSet set[1];
        PageSet right[1];
        BLTKey* key;
    
        uint slot = _mgr->loadPage( set, inputKey, inputKeyLen, level, LockWrite, _thread );
        if (slot) {
            key = Page::keyptr( set->_page, slot );
        }
        else {
            return _err;
        }
    
        // are we deleting a fence slot?
        uint fence = (slot == set->_page->_cnt);
    
        // if key is found delete it, otherwise ignore request
        uint dirty = 0;
        uint found = !BLTKey::keycmp( key, inputKey, inputKeyLen );
        if (found) {
            found = Page::slotptr( set->_page, slot )->_dead;
            if (0==found) {
                dirty = Page::slotptr( set->_page, slot )->_dead = 1;
                set->_page->_dirty = 1;
                set->_page->_act--;
    
                // collapse empty slots
                uint idx;
                while ( (idx = set->_page->_cnt - 1) ) {
                    if (!Page::slotptr( set->_page, idx )->_dead ) break;
                    *Page::slotptr( set->_page, idx ) = *Page::slotptr( set->_page, idx + 1 );
                    memset( Page::slotptr( set->_page, set->_page->_cnt--), 0, sizeof(Slot) );
                }
            }
        }
    
        // did we delete a fence key in an upper level?
        if (dirty && level && set->_page->_act && fence) {
            if (fixFenceKey( set, level )) {
                return _err;
            }
            else {
                _found = found;
                return BLTERR_ok;
            }
        }
    
        // is this a collapsed root?
        if (level > 1 && set->_pageNo == ROOT_page && set->_page->_act == 1) {
            if (collapseRoot( set )) {
                return _err;
            }
            else {
                _found = found;
                return BLTERR_ok;
            }
        }
    
        // return if page is not empty
        if (set->_page->_act) {
            _mgr->unlockPage( LockWrite, set->_latch, _thread );
            _mgr->getLatchMgr()->unpinLatch( set->_latch, _thread );
            _mgr->unpinPool( set->_pool, _thread );
            _found = found;
            return BLTERR_ok;
        }
    
        // cache copy of fence key to post in parent
        key = Page::keyptr( set->_page, set->_page->_cnt);
        memcpy( lowerFence, key, key->_len + 1 );

        // obtain lock on right page
        right->_pageNo = Page::getid( set->_page->_right );
        right->_latch = _mgr->getLatchMgr()->pinLatch( right->_pageNo, _thread );
        _mgr->lockPage( LockWrite, right->_latch, _thread );

        // pin page contents
        right->_pool = _mgr->pinPool( right->_pageNo, _thread );
        if (right->_pool) {
            right->_page = _mgr->page( right->_pool, right->_pageNo, _thread );
        }
        else {
            return BLTERR_ok;
        }
    
        if (right->_page->_kill) {
            return (_err = BLTERR_struct);
        }
    
        // pull contents of right peer into our empty page
        memcpy( set->_page, right->_page, _mgr->getPageSize() );
    
        // cache copy of key to update
        key = Page::keyptr( right->_page, right->_page->_cnt );
        memcpy( higherFence, key, key->_len + 1 );
    
        // mark right page deleted: point it to left page until we can post parent updates
        Page::putid( right->_page->_right, set->_pageNo );
        right->_page->_kill = 1;
    
        _mgr->lockPage( LockParent, right->_latch, _thread );
        _mgr->unlockPage( LockWrite, right->_latch, _thread );
        _mgr->lockPage( LockParent, set->_latch, _thread );
        _mgr->unlockPage( LockWrite, set->_latch, _thread );
    
        // redirect higher key directly to our new node contents
        if (insertKey( higherFence+1, *higherFence, level+1, set->_pageNo, time(NULL)) ) {
            return _err;
        }
    
        // delete old lower key to our node
        if (deleteKey( lowerFence+1, *lowerFence, level+1 )) {
            return _err;
        }
    
        // obtain delete and write locks to right node
        _mgr->unlockPage( LockParent, right->_latch, _thread );
        _mgr->lockPage( LockDelete, right->_latch, _thread );
        _mgr->lockPage( LockWrite, right->_latch, _thread );
        _mgr->freePage( right, _thread );
    
        _mgr->unlockPage( LockParent, set->_latch, _thread );
        _mgr->getLatchMgr()->unpinLatch( set->_latch, _thread );
        _mgr->unpinPool( set->_pool, _thread );
        _found = found;
        return BLTERR_ok;
    }
    
    /**
    *  find key in leaf level and return pageId
    */
    PageId BLTIndex::findKey( const uchar* inputKey, uint inputKeyLen ) {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        assert( NULL != inputKey );

        PageSet set[1];
        PageId id = 0;
        BLTKey* key;
    
        uint slot = _mgr->loadPage( set, inputKey, inputKeyLen, 0, LockRead, _thread );
        if (slot) {
            key = Page::keyptr( set->_page, slot );
        }
        else {
            return 0;
        }
    
        // if key exists, return row-id, otherwise return 0
        if (slot <= set->_page->_cnt ) {
            if (!BLTKey::keycmp( key, inputKey, inputKeyLen )) {
                id = Page::getid( Page::slotptr( set->_page, slot )->_id );
            }
        }
    
        _mgr->unlockPage( LockRead, set->_latch, _thread );
        _mgr->getLatchMgr()->unpinLatch( set->_latch, _thread );
        _mgr->unpinPool( set->_pool, _thread );
        return id;
    }
    
    /**
    *  Check page for space available, clean if necessary.
    *  @return 0 - page needs splitting, >0  new slot value
    */
    uint BLTIndex::cleanPage( Page* page, uint amt, uint slot ) {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        assert( NULL != page );

        uint nxt = _mgr->getPageSize();
        uint idx = 0;
        uint max = page->_cnt;
        uint newslot = max;
        BLTKey* key;
    
        if (page->_min >= (max+1) * sizeof(Slot) + sizeof(*page) + amt + 1 ) {
            return slot;
        }
    
        //    skip cleanup if nothing to reclaim
        if (!page->_dirty ) return 0;
    
        memcpy( _frame, page, _mgr->getPageSize() );
    
        // skip page info and set rest of page to zero
        memset( page+1, 0, _mgr->getPageSize() - sizeof(*page) );
        page->_dirty = 0;
        page->_act = 0;
    
        // try cleaning up page first by removing deleted keys
        uint cnt = 0;
        while (cnt++ < max) {
            if (cnt == slot) newslot = idx + 1;
            if (cnt < max && Page::slotptr(_frame, cnt)->_dead) continue;
    
            // copy the key
            key = Page::keyptr(_frame, cnt);
            nxt -= key->_len + 1;
            memcpy( (uchar *)page + nxt, key, key->_len + 1 );
    
            // copy slot
            memcpy( Page::slotptr(page, ++idx)->_id, Page::slotptr(_frame, cnt)->_id, IdLength );
            if (!(Page::slotptr(page, idx)->_dead = Page::slotptr(_frame, cnt)->_dead)) {
                page->_act++;
            }
            Page::slotptr(page, idx)->_tod = Page::slotptr(_frame, cnt)->_tod;
            Page::slotptr(page, idx)->_off = nxt;
        }
    
        page->_min = nxt;
        page->_cnt = idx;
    
        // see if page has enough space now, or does it need splitting?
        if (page->_min >= (idx+1) * sizeof(Slot) + sizeof(*page) + amt + 1) {
            return newslot;
        }
    
        return 0;
    }
    
    /**
    *  split the root and raise the height of the btree
    */
    BLTERR BLTIndex::splitRoot( PageSet* root, const uchar* leftKey, PageId pageId2) {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        assert( NULL != root );
        assert( NULL != leftKey );

        uint nxt = _mgr->getPageSize();
        PageId left;
    
        //  Obtain an empty page to use, and copy the current
        //  root contents into it, e.g. lower keys
        if (!(left = _mgr->newPage( root->_page, _thread ))) {
            return _err;
        }
    
        // preserve the page info at the bottom
        // of higher keys and set rest to zero
    
        memset(root->_page+1, 0, _mgr->getPageSize() - sizeof(*root->_page));
    
        // insert lower keys page fence key on newroot page as first key
    
        nxt -= *leftKey + 1;
        memcpy( (uchar *)root->_page + nxt, leftKey, *leftKey + 1 );
        Page::putid( Page::slotptr(root->_page, 1 )->_id, left);
        Page::slotptr(root->_page, 1)->_off = nxt;
        
        // insert stopper key on newroot page
        // and increase the root height
    
        nxt -= 3;
        ((uchar *)root->_page)[nxt] = 2;
        ((uchar *)root->_page)[nxt+1] = 0xff;
        ((uchar *)root->_page)[nxt+2] = 0xff;
        Page::putid( Page::slotptr(root->_page, 2)->_id, pageId2 );
        Page::slotptr(root->_page, 2)->_off = nxt;
    
        Page::putid( root->_page->_right, 0 );
        root->_page->_min = nxt;        // reset lowest used offset and key count
        root->_page->_cnt = 2;
        root->_page->_act = 2;
        root->_page->_level++;
    
        // release and unpin root
    
        _mgr->unlockPage( LockWrite, root->_latch, _thread );
        _mgr->getLatchMgr()->unpinLatch( root->_latch, _thread );
        _mgr->unpinPool( root->_pool, _thread );
        return BLTERR_ok;
    }
    
    /**
    *  split already locked full node
    *  return unlocked.
    */
    BLTERR BLTIndex::splitPage( PageSet* set ) {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        assert( NULL != set );

        uchar fenceKey[256];
        uchar rightKey[256];
        PageSet right[1];
    
        //  split higher half of keys to _frame
        memset( _frame, 0, _mgr->getPageSize() );
        uint max = set->_page->_cnt;
        uint cnt = max / 2;
        uint idx = 0;
        uint nxt = _mgr->getPageSize();
        uint level = set->_page->_level;

        BLTKey* key;
    
        while (cnt++ < max) {
            key = Page::keyptr(set->_page, cnt);
            nxt -= key->_len + 1;

            memcpy( (uchar *)_frame + nxt, key, key->_len + 1 );
            memcpy( Page::slotptr(_frame,++idx)->_id, Page::slotptr(set->_page,cnt)->_id, IdLength );

            if (!(Page::slotptr(_frame, idx)->_dead = Page::slotptr(set->_page, cnt)->_dead)) {
                _frame->_act++;
            }

            Page::slotptr(_frame, idx)->_tod = Page::slotptr(set->_page, cnt)->_tod;
            Page::slotptr(_frame, idx)->_off = nxt;
        }
    
        // remember existing fence key for new page to the right
        memcpy( rightKey, key, key->_len + 1 );
    
        _frame->_bits = _mgr->getPageBits();
        _frame->_min = nxt;
        _frame->_cnt = idx;
        _frame->_level = level;
    
        // link right node
        if (set->_pageNo > ROOT_page) {
            memcpy( _frame->_right, set->_page->_right, IdLength );
        }
    
        // get new free page and write higher keys to it.
        if (!(right->_pageNo = _mgr->newPage( _frame, _thread ))) {
            return _err;
        }
    
        // update lower keys to continue in old page
        memcpy( _frame, set->_page, _mgr->getPageSize() );
        memset( set->_page+1, 0, _mgr->getPageSize() - sizeof(*set->_page) );
        nxt = _mgr->getPageSize();
        set->_page->_dirty = 0;
        set->_page->_act = 0;
        cnt = 0;
        idx = 0;
    
        // assemble page of smaller keys
        while (cnt++ < max / 2) {
            key = Page::keyptr(_frame, cnt);
            assert( NULL != key );
            nxt -= key->_len + 1;

            memcpy( (uchar *)set->_page + nxt, key, key->_len + 1 );
            memcpy( Page::slotptr(set->_page, ++idx)->_id, Page::slotptr(_frame,cnt)->_id, IdLength );

            Page::slotptr(set->_page, idx)->_tod = Page::slotptr(_frame, cnt)->_tod;
            Page::slotptr(set->_page, idx)->_off = nxt;

            set->_page->_act++;
        }
    
        // remember fence key for smaller page
        memcpy( fenceKey, key, key->_len + 1 );
        Page::putid(set->_page->_right, right->_pageNo);
        set->_page->_min = nxt;
        set->_page->_cnt = idx;
    
        // if current page is the root page, split it
        if (set->_pageNo == ROOT_page) {
            return splitRoot( set, fenceKey, right->_pageNo );
        }
    
        // insert new fences in their parent pages
        right->_latch = _mgr->getLatchMgr()->pinLatch( right->_pageNo, _thread );
        _mgr->lockPage( LockParent, right->_latch, _thread );
        _mgr->lockPage( LockParent, set->_latch, _thread );
        _mgr->unlockPage( LockWrite, set->_latch, _thread );
    
        // insert new fence for reformulated left block of smaller keys
        if (insertKey( fenceKey+1, *fenceKey, level+1, set->_pageNo, time(NULL))) {
            return _err;
        }
    
        // switch fence for right block of larger keys to new right page
        if (insertKey( rightKey+1, *rightKey, level+1, right->_pageNo, time(NULL))) {
            return _err;
        }
    
        _mgr->unlockPage( LockParent, set->_latch, _thread );
        _mgr->getLatchMgr()->unpinLatch( set->_latch, _thread );
        _mgr->unpinPool( set->_pool, _thread );
        _mgr->unlockPage( LockParent, right->_latch, _thread );
        _mgr->getLatchMgr()->unpinLatch( right->_latch, _thread );
        return BLTERR_ok;
    }

    /**
    *  Insert new key into the btree at given level.
    */
    BLTERR BLTIndex::insertKey( const uchar* inputKey,
                                uint inputKeyLen,
                                uint level,
                                PageId id,
                                uint tod )
    {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        assert( NULL != inputKey );

        PageSet set[1];
        uint slot;
        BLTKey* key;
    
        while (true) {
            slot = _mgr->loadPage( set, inputKey, inputKeyLen, level, LockWrite, _thread );
            if (slot) {
                key = Page::keyptr(set->_page, slot);
            }
            else {
                if (!_err) _err = BLTERR_ovflw;
                return _err;
            }

            assert( NULL != key );
    
            // if key already exists, update id and return
            if (!BLTKey::keycmp( key, inputKey, inputKeyLen )) {

                char buf[ key->_len+1 ];
                strncpy ( buf, (const char*)key->_key, key->_len );
                __OSS__( "duplicate key: " << buf );
                Logger::logInfo( _thread, __ss__, __LOC__ );

                if (Page::slotptr(set->_page, slot)->_dead) set->_page->_act++;
                Page::slotptr(set->_page, slot)->_dead = 0;
                Page::slotptr(set->_page, slot)->_tod = tod;
                Page::putid(Page::slotptr(set->_page,slot)->_id, id);
                _mgr->unlockPage( LockWrite, set->_latch, _thread );
                _mgr->getLatchMgr()->unpinLatch( set->_latch, _thread );
                _mgr->unpinPool( set->_pool, _thread );
                return BLTERR_ok;
            }
    
            // check if page has enough space
            if ((slot = cleanPage( set->_page, inputKeyLen, slot)) ) break;
            if (splitPage( set )) return _err;
        }
    
        // calculate next available slot and copy key into page
        set->_page->_min -= inputKeyLen + 1; // reset lowest used offset
        ((uchar *)set->_page)[set->_page->_min] = inputKeyLen;
        memcpy( (uchar *)set->_page + set->_page->_min +1, inputKey, inputKeyLen );
    
        uint idx;
        for (idx = slot; idx < set->_page->_cnt; idx++) {
          if (Page::slotptr(set->_page, idx)->_dead ) break;
        }
    
        // now insert key into array before slot
        if (idx == set->_page->_cnt ) {
            idx++;
            set->_page->_cnt++;
        }
        set->_page->_act++;
    
        while (idx > slot) {
            *Page::slotptr(set->_page, idx) = *Page::slotptr(set->_page, idx -1), idx--;
        }
    
        Page::putid( Page::slotptr(set->_page,slot)->_id, id );
        Page::slotptr(set->_page, slot)->_off = set->_page->_min;
        Page::slotptr(set->_page, slot)->_tod = tod;
        Page::slotptr(set->_page, slot)->_dead = 0;
    
        _mgr->unlockPage( LockWrite, set->_latch, _thread );
        _mgr->getLatchMgr()->unpinLatch( set->_latch, _thread );
        _mgr->unpinPool( set->_pool, _thread );
        return BLTERR_ok;
    }
    
    /**
    *  cache page of keys into cursor and return starting slot for given key
    */
    uint BLTIndex::startKey( const uchar* key, uint keylen ) {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        assert( NULL != key );

        PageSet set[1];
    
        // cache page for retrieval
        uint slot = _mgr->loadPage( set, key, keylen, 0, LockRead, _thread );
        if (slot) {
            memcpy( _cursor, set->_page, _mgr->getPageSize() );
        }
        else {
            return 0;
        }
    
        _cursorPage = set->_pageNo;
    
        _mgr->unlockPage( LockRead, set->_latch, _thread );
        _mgr->getLatchMgr()->unpinLatch( set->_latch, _thread );
        _mgr->unpinPool( set->_pool, _thread );
        return slot;
    }
    
    /**
    *  return next slot for cursor page
    *  or slide cursor right into next page
    */
    uint BLTIndex::nextKey( uint slot ) {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        PageSet set[1];
    
        do {
            PageId right = Page::getid(_cursor->_right);
    
            while (slot++ < _cursor->_cnt) {
                if (Page::slotptr(_cursor, slot)->_dead ) {
                    continue;
                }
                else if (right || (slot < _cursor->_cnt) ) { // skip infinite stopper
                    return slot;
                }
                else {
                    break;
                }
            }
    
            if (!right ) break;
            _cursorPage = right;
     
            if ((set->_pool = _mgr->pinPool( right, _thread ))) {
                set->_page = _mgr->page( set->_pool, right, _thread );
            }
            else {
                return 0;
            }
    
            set->_latch = _mgr->getLatchMgr()->pinLatch( right, _thread );
            _mgr->lockPage( LockRead, set->_latch, _thread );
     
            memcpy( _cursor, set->_page, _mgr->getPageSize() );
     
            _mgr->unlockPage( LockRead, set->_latch, _thread );
            _mgr->getLatchMgr()->unpinLatch( set->_latch, _thread );
            _mgr->unpinPool( set->_pool, _thread );
            slot = 0;
    
        } while (true);
    
        return (_err = BLTERR_ok);
    }
    
    BLTKey* BLTIndex::getKey( uint slot ) { return Page::keyptr(_cursor, slot); }
    PageId BLTIndex::getPageId( uint slot ) { return Page::getid(Page::slotptr(_cursor,slot)->_id); }
    uint BLTIndex::getTod( uint slot ) { return Page::slotptr(_cursor,slot)->_tod; } 

    /**
    *
    */
    void BLTIndex::latchAudit() {
        if (BLTINDEX_TRACE) Logger::logDebug( _thread, "", __LOC__ );

        if (*(uint *)(_mgr->getLatchMgr()->_lock)) {
            Logger::logDebug( _thread, "Alloc page locked", __LOC__ );
        }
        *(uint *)(_mgr->getLatchMgr()->_lock) = 0;
    
        for (ushort idx = 1; idx <= _mgr->getLatchMgr()->_latchDeployed; idx++ ) {

            LatchSet* latchSet = &_mgr->getLatchMgr()->_latchSets[ idx ];
            PageId pageId = latchSet->_pageId;

            if (*(uint *)latchSet->_readwr ) {
                __OSS__( "latchset " << idx << " rw locked for page " << pageId );
                Logger::logDebug( _thread, __ss__, __LOC__ );
            }
            *(uint *)latchSet->_readwr = 0;
    
            if (*(uint *)latchSet->_access ) {
                __OSS__( "latchset " << idx << " access locked for page " << pageId );
                Logger::logDebug( _thread, __ss__, __LOC__ );
            }
            *(uint *)latchSet->_access = 0;
    
            if (*(uint *)latchSet->_parent ) {
                __OSS__( "latchset " << idx << " parent locked for page " << pageId );
                Logger::logDebug( _thread, __ss__, __LOC__ );
            }
            *(uint *)latchSet->_parent = 0;
    
            if (latchSet->_pin ) {
                __OSS__( "latchset " << idx << " pinned for page " << pageId );
                Logger::logDebug( _thread, __ss__, __LOC__ );
                latchSet->_pin = 0;
            }
        }
    
        for (ushort hashidx = 0; hashidx < _mgr->getLatchMgr()->_latchHash; hashidx++ ) {
            if (*(uint *)(_mgr->getLatchMgr()->_table[hashidx]._latch) ) {
                __OSS__( "hash entry " << hashidx << " locked" );
                Logger::logDebug( _thread, __ss__, __LOC__ );
            }
            *(uint *)(_mgr->getLatchMgr()->_table[hashidx]._latch) = 0;
    
            uint idx = _mgr->getLatchMgr()->_table[hashidx]._slot;
            if (idx) {
                LatchSet* latchSet;
                do {
                    latchSet = &_mgr->getLatchMgr()->_latchSets[ idx ];
                    PageId pageId = latchSet->_pageId;

                    if (*(uint *)latchSet->_busy ) {
                        __OSS__( "latchset " << idx << " busy locked for page " << pageId );
                        Logger::logDebug( _thread, __ss__, __LOC__ );
                    }
                    *(uint *)latchSet->_busy = 0;
                    if (latchSet->_hash != hashidx ) {
                        __OSS__( "latchset " << idx << " wrong hashidx " );
                        Logger::logDebug( _thread, __ss__, __LOC__ );
                    }
                    if (latchSet->_pin ) {
                        __OSS__( "latchset " << idx << " pinned for page " << pageId );
                        Logger::logDebug( _thread, __ss__, __LOC__ );
                    }
                } while ((idx = latchSet->_next));
            }
        }
    
        PageId next = _mgr->getLatchMgr()->_nlatchPage + LATCH_page;
        PageId pageNo = LEAF_page;
        Page* _frame = (Page *)malloc( _mgr->getPageSize() );
    
        while (pageNo < Page::getid(_mgr->getLatchMgr()->_alloc->_right)) {
            pread( _mgr->getFD(), _frame, _mgr->getPageSize(), pageNo << _mgr->getPageBits() );
            if (!_frame->_free) {
                for (uint idx = 0; idx++ < _frame->_cnt - 1; ) {
                    BLTKey* key = Page::keyptr(_frame, idx+1);
                    if (BLTKey::keycmp( Page::keyptr(_frame, idx), key->_key, key->_len ) >= 0) {
                        __OSS__( "page " << pageNo << " idx" << idx << " out of order" );
                        Logger::logDebug( _thread, __ss__, __LOC__ );
                    }
                }
            }
    
            if (pageNo > LEAF_page) next = pageNo + 1;
            pageNo = next;
        }
    }

}   // namespace mongo
