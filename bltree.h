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

#pragma once

#ifndef STANDALONE
#include "mongo/base/status.h"
#include "mongo/db/storage/bltree/common.h"
#include "mongo/db/storage/bltree/blterr.h"
#include "mongo/db/storage/bltree/bltkey.h"
#include "mongo/db/storage/bltree/bufmgr.h"
#include "mongo/db/storage/bltree/page.h"
#else
#include "common.h"
#include "blterr.h"
#include "bltkey.h"
#include "bufmgr.h"
#include "page.h"
#endif

namespace mongo {

    //
    //  bltree
    //
    class BLTree {
    public:
        // factory
        static BLTree* create( BufMgr* mgr );
        void close();

    public:
        int findkey( uchar* key, uint keylen, uchar* value, uint valmax );
        Status insertkey( uchar* key, uint keylen, uint lvl, uchar* value, uint vallen);
        Status deletekey( uchar* key, uint len, uint lvl );
        uint startkey( uchar* key, uint len );
        uint nextkey( uint slot );

        // for debugging
        uint latchaudit();
        void scan( std::ostream& );

    protected:
        Status fixfence( PageSet* set, uint lvl );
        Status collapseroot( PageSet *root );
        uint cleanpage( Page* page, uint keylen, uint slot, uint vallen);
        Status splitroot( PageSet* root, uchar* leftkey, uid page_no2 );
        Status splitpage( PageSet* set );
    
    public:
        BufMgr* mgr;                 // buffer manager for thread
        Page* cursor;             // cached frame for start/next (never mapped)
        Page* frame;              // spare frame for the page split (never mapped)
        uid cursor_page;            // current cursor page number    
        uchar* mem;                 // frame, cursor, page memory buffer
        int found;                  // last delete or insert was found
        int err;                    // last error
    };

}   // namespace mongo

