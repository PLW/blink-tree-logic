//@file bufmgr.h

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
#include "mongo/db/storage/bltree/blterr.h"
#include "mongo/db/storage/bltree/common.h"
#include "mongo/db/storage/bltree/page.h"
#include "mongo/db/storage/bltree/latchmgr.h"
#else
#include "blterr.h"
#include "common.h"
#include "page.h"
#include "latchmgr.h"
#endif

namespace mongo {

    /**
    *  memory mapping pool table buffer manager entry
    */
    struct PoolEntry {
        uid  basepage;              // mapped base page number
        char* map;                  // mapped memory pointer
        ushort slot;                // slot index in this array
        ushort pin;                 // mapped page pin counter
        void* hashprev;             // previous pool entry for the same hash idx
        void* hashnext;             // next pool entry for the same hash idx
    };
    
    #define CLOCK_bit 0x8000        // bit in pool->pin
    
    /**
    *  structure for latch manager on ALLOC_page
    */
    struct PageZero {
        Page alloc[1];              // next page_no in right ptr
        unsigned long long dups[1]; // global duplicate key uniqueifier
        unsigned char chain[BtId];  // head of free page_nos chain
    };
    
    /**
    *  object structure for BLTree access
    */
    class BufMgr {
    public:
        /**
        *  FUNCTION: create
        *
        *  factory method
        */
        static BufMgr* create( const char* name, uint bits, uint nodemax );

        /**
        *  FUNCTION: poolaudit
        *
        *  release all resources
        */
        void close();

        /**
        *  FUNCTION: poolaudit
        */
        void poolaudit();
    
        /**
        *  FUNCTION: latchlink
        */
        BLTERR latchlink( uint hashidx, uint slot, uid page_no, uint load_it,
                            uint* reads );

        /**
        *  FUNCTION: pinlatch
        */
        LatchSet* pinlatch( uid page_no, uint loadit,
                            uint* reads, uint* writes );

        /**
        *  FUNCTION: unpinlatch
        */
        void unpinlatch( LatchSet* latch);

        /**
        *  FUNCTION: loadpage
        */
        int loadpage( PageSet*, uchar* key, uint len, uint lvl, BLTLockMode,
                            uint* reads, uint* writes );

        /**
        *  FUNCTION: mappage
        */
        Page* mappage( LatchSet* latch );

        /**
        *  FUNCTION: newpage
        */
        int newpage( PageSet* set, Page* contents,
                            uint* reads, uint* writes );

        /**
        *  FUNCTION: freepage
        */
        void  freepage( PageSet* set );

        /**
        *  FUNCTION: readpage
        */
        BLTERR readpage( Page* page, uid page_no );

        /**
        *  FUNCTION: writepage
        */
        BLTERR writepage( Page* page, uid page_no );

        /**
        *  FUNCTION: lockpage
        */
        static void  lockpage( BLTLockMode mode, LatchSet* latch );

        /**
        *  FUNCTION: unlockpage
        */
        static void  unlockpage( BLTLockMode mode, LatchSet* latch );

    public:
        uint page_size;             // page size    
        uint page_bits;             // page size in bits    

    #ifdef unix
        int idx;
    #else
        HANDLE idx;
    #endif

        PageZero *pagezero;         // mapped allocation page
        SpinLatch lock[1];          // allocation area lite latch
        uint latchdeployed;         // highest number of latch entries deployed
        uint nlatchpage;            // number of latch pages at BT_latch
        uint latchtotal;            // number of page latch entries
        uint latchhash;             // number of latch hash table slots
        uint latchvictim;           // next latch entry to examine
        HashEntry* hashtable;       // the buffer pool hash table entries
        LatchSet* latchsets;        // mapped latch set from buffer pool
        uchar* pagepool;            // mapped to the buffer pool pages

    #ifndef unix
        HANDLE halloc;              // allocation handle
        HANDLE hpool;               // buffer pool handle
    #endif

        BLTERR err;                 // last error

    };

}   // namespace mongo

