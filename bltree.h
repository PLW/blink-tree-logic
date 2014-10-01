//@file bltree.h
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

#pragma once

#ifndef STANDALONE
#include "mongo/base/status.h"
#include "mongo/db/storage/bltree/common.h"
#include "mongo/db/storage/bltree/blterr.h"
#include "mongo/db/storage/bltree/bufmgr.h"
#include "mongo/db/storage/bltree/page.h"
#else
#include "common.h"
#include "blterr.h"
#include "bufmgr.h"
#include "page.h"
#endif

namespace mongo {

    //
    //  bltree
    //

    struct AtomicMod {
	    uint entry;			// latch table entry number
	    uint slot:30;		// page slot number
	    uint reuse:1;		// reused previous page
	    uint emptied:1;		// page was emptied
    };

    struct AtomicKey {
	    uid page_no;		// page number for split leaf
	    void* next;			// next key to insert
	    uint entry;			// latch table entry number
	    unsigned char leafkey[KEYARRAY];
    };


    class BLTree {
    public:
        // factory method
        static BLTree* create( BufMgr* mgr );
        void close();

        ~BLTree();

    public:
        // index interface
        int    findkey(   uchar* key, uint keylen, uchar* val, uint valmax );
        Status insertkey( uchar* key, uint keylen, uint lvl, uchar* val, uint vallen, uint uniq );
        Status deletekey( uchar* key, uint keylen, uint lvl );

        // transaction support
        int atomicmods( Page* source );

        // iterator interface
        uint startkey( uchar* key, uint keylen );
        uint nextkey( uint slot );

        // return current key
        BLTKey* foundkey();

        // for debugging
        uint latchaudit();
        //void scan( std::ostream& );

    protected:
        Status fixfence( PageSet* set, uint lvl );
        Status collapseroot( PageSet *root );
        Status splitroot( PageSet* root, LatchSet* right);
        Status deletepage( PageSet* set, BLTLockMode mode );
        uint   splitpage( PageSet* set );
        uint   cleanpage( PageSet* set, uint keylen, uint slot, uint vallen );

        // duplicate key tie-breaker, numeric suffix
        uid newdup();

        // atomic support
        uint atomicpage( Page* source, AtomicMod* locks, uint src, PageSet* set);
        Status atomicdelete( Page* source, AtomicMod* locks, uint src );
        Status atomicinsert( Page* source, AtomicMod* locks, uint src );
    
        Status insertslot( PageSet* set, uint slot,
                                uchar *key, uint keylen,
                                uchar* value, uint vallen,
                                uint type, uint release);

        Status splitkeys( PageSet* set, LatchSet* right );

        uint findnext( PageSet* set, uint slot );
        void freepage( PageSet* set );

        BLTKey* getKey( uint slot );
        BLTVal* getVal( uint slot );

    public:
        BufMgr* mgr;                // buffer manager for thread
        Page*   cursor;             // cached frame for start/next (never mapped)
        Page*   frame;              // spare frame for the page split (never mapped)
        uid     cursor_page;        // current cursor page number    
        uchar*  mem;                // frame, cursor, page memory buffer
        int     found;              // last delete or insert was found
        BLTERR  err;                // last error
        uchar   key[KEYARRAY];      // last found complete key
        uint     reads;             // number of reads from the btree
        uint     writes;            // number of reads to   the btree
    };

}   // namespace mongo

