//@file page.h

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
#include "mongo/db/storage/bltree/common.h"
#else
#include "common.h"
#endif

namespace mongo {

    //
    //  page key slot
    //
    struct Slot {
        uint off:BT_maxbits;        // page offset for key start
        uint dead:1;                // set for deleted key
    };
    
    // page accessors
    class BLTKey;
    class BLTVal;

    #define slotptr(page, slot) \
        (((mongo::Slot *)(page+1)) + (slot-1))

    #define keyptr(page, slot) \
        ((mongo::BLTKey *)((unsigned char*)(page) + slotptr(page, slot)->off))

    #define valptr(page, slot) \
        ((mongo::BLTVal *)(keyptr(page,slot)->key + keyptr(page,slot)->len))

    //
    //  index page
    //
    class Page {
    public:
        uint  cnt;              // count of keys in page
        uint  act;              // count of active keys
        uint  min;              // next key offset
        uchar bits:7;           // page size in bits
        uchar free:1;           // page is on free chain
        uchar lvl:6;            // level of page
        uchar kill:1;           // page is being deleted
        uchar dirty:1;          // page has deleted keys
        uchar right[BtId];      // page number to right
    };

}   // namespace mongo
