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

#include <string.h>

namespace mongo {

    class LatchSet;

    #define slotptr(page, slot) \
        (((mongo::Slot *)(page+1)) + (slot-1))

    #define keyptr(page, slot) \
        ((mongo::BLTKey *)((unsigned char*)(page) + slotptr(page, slot)->off))

    #define valptr(page, slot) \
        ((mongo::BLTVal *)(keyptr(page,slot)->key + keyptr(page,slot)->len))

    /**
    * Page key slot definition.
    */
    struct Slot {
        /*
        *  Slot types:
        *
        *  In addition to the Unique keys that occupy slots there are
        *  Librarian and Duplicate key slots occupying the key slot array.
        *  The Librarian slots are dead keys that serve as filler, available
        *  to add new Unique or Dup slots that are inserted into the B-tree.
        * 
        *  The Duplicate slots have had their key bytes extended by 6 bytes
        *  to contain a binary duplicate key uniqueifier.
        */
        enum Type {
            Unique,
            Librarian,
            Duplicate,
            Delete
        };
    
        uint off:MAXBITS;    // key offset
        uint type:3;         // type of slot
        uint dead:1;         // Keys are marked dead, but remain on the page until
                             // cleanup is called. The fence key (highest key) for
                             // a leaf page is always present, even after cleanup.
    };
    
    /**
    *  The key structure occupies space at the upper end of each
    *  page.  It's a length byte followed by the key bytes.
    */
    struct BLTKey {
        /**
        *  Compare two keys.
        *  @return  > 0, = 0, or < 0
        */
        static int keycmp( BLTKey* key1, uchar* key2, uint len2 ) {
            uint len1 = key1->len;
            int ans;
            if ( (ans = memcmp( key1->key, key2, len1 > len2 ? len2 : len1 )) ) {
                return ans;
            }
            return (len1 > len2 ? 1 : len1 < len2 ? -1 : 0);
        }

        unsigned char len;          // may change to ushort or uint
        unsigned char key[0];
    };
    
    /**
    *  The value structure also occupies space at the upper
    *  end of the page. Each key is immediately followed by a value.
    */
    class BLTVal {
    public:
        /**
        *  FUNCTION:  putid
        *
        *  pack pageno values
        */
        static void putid( uchar* dest, uid id );

        /**
        *  FUNCTION:  getid
        *
        *  unpack pageno values
        */
        static uid getid( uchar* src );

    public:
        unsigned char len;              // may change to ushort or uint
        unsigned char value[0];
    };
    
    #define MAXKEY       255            // maximum number of bytes in a key
    #define KEYARRAY (MAXKEY + sizeof(BLTKey))

    /*
    *  The first part of an index page.  It is immediately followed
    *  by the Slot array of keys.
    * 
    *  Note: this structure size must be a multiple of 8 bytes in order
    *  to place dups correctly.
    */
    class Page {
    public:
        /**
        *  FUNCTION:  findslot
        *
        *  find slot in page for given key at a given level
        */
        static int findslot( Page* page, uchar* key, uint keylen );

    public:
        uint cnt;                       // count of keys in page
        uint act;                       // count of active keys
        uint min;                       // next key offset
        uint garbage;                   // page garbage in bytes
        unsigned char bits:7;           // page size in bits
        unsigned char free:1;           // page is on free chain
        unsigned char lvl:7;            // level of page
        unsigned char kill:1;           // page is being deleted
        unsigned char right[BtId];      // page number to right
    };
    
    /**
    *  The loadpage interface object
    */
    class PageSet {
    public:
        Page* page;                     // current page pointer
        LatchSet* latch;                // current page latch set
    };

}   // namespace mongo


