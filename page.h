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
#include "bltkey.h"
#include "bltval.h"

#include <iostream>

namespace mongo {

    struct LatchSet;
    class PoolEntry;

    #define OFFSET_MASK     0xffffffffffffUL

    /**
    *  doc locator:  file # + offset
    */
    struct DiskLoc {
        uint64_t fileno:16;
        uint64_t offset:48;

        DiskLoc( uint64_t id )
        :   fileno( id>>48 ),
            offset( id & OFFSET_MASK )
        {}

        DiskLoc& operator=( uint64_t id) {
            fileno = id>>48;
            offset = id & OFFSET_MASK;
            return *this;
        }

        DiskLoc& operator=( const DiskLoc& loc ) {
            fileno = loc.fileno;
            offset = loc.offset;
            return *this;
        }

        uint64_t pack() {
            return (((uint64_t)fileno)<<48) | offset;
        }

        friend std::ostream& operator<<( std::ostream& os, const DiskLoc& page );
    };
    
    /**
    *  Page key slot definition.
    *
    *  If BLT_maxbits is 15 or less, you can save 4 bytes
    *  for each key stored by making the first two uints
    *  into ushorts.
    * 
    *  Keys are marked dead, but remain on the page until
    *  it cleanup is called. The fence key (highest key) for
    *  the page is always present, even after cleanup.
    */
    struct Slot {
        uint32_t _off:BLT_maxbits;  // page offset for key start
        uint32_t _dead:1;           // set for deleted key

        friend std::ostream& operator<<( std::ostream& os, const Slot& slot );
    };

    /**
    *  first part of an index page.
    *  - immediately followed by the Slot array of keys.
    */
    class Page {
    public:
        // page slot accessors
        static Slot* slotptr( Page* page, uint slot ) {
            return (((Slot *)(page+1)) + (slot-1));
        }

        static BLTKey* keyptr( Page* page, uint slot ) {
            return ((BLTKey*)((uchar*)page + slotptr( page, slot )->_off));
        }

        static BLTVal* valptr( Page* page, uint slot ) {
            return (BLTVal*)(keyptr( page, slot )->_key + keyptr( page, slot )->_len);
        }

        friend std::ostream& operator<<( std::ostream& os, const Page& page );

    public:
        uint32_t _cnt;            // count of keys in page
        uint32_t _act;            // count of active keys
        uint32_t _min;            // next key offset
        uchar _bits:7;            // page size in bits
        uchar _free:1;            // page is on free chain
        uchar _level:6;           // level of page
        uchar _kill:1;            // page is being deleted
        uchar _dirty:1;           // page has deleted keys
        PageNo _right;
    };
    
    /**
    *  loadpage interface object
    */
    struct PageSet {
        PageNo      _pageNo;      // current page number
        Page*       _page;        // current page pointer
        PoolEntry*  _pool;        // current page pool
        LatchSet*   _latch;       // current page latch set
    };

}   // namespace mongo

