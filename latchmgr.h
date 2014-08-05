//@file latchmgr.h
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

#include "page.h"

#include <iostream>
#include <string>

namespace mongo {

    /**
    *  There are five lock types for each node in three independent sets: 
    *  Set 1:
    *      AccessIntent: Sharable.   Going to Read node.
    *      NodeDelete:   Exclusive.  Going to release node.
    *  Set 2:
    *      ReadLock:     Sharable.   Read node.
    *      WriteLock:    Exclusive.  Modify node.
    *  Set 3:
    *      ParentMod:    Exclusive.  Change node parent keys.
    *
    *                    AI  D   R   W   P
    *                   ---+---+---+---+---+
    *              AI  | Y | N | Y | Y | Y |
    *                  +---+---+---+---+---+
    *               D  | N | N | Y | Y | Y |
    *                  +---+---+---+---+---+
    *               R  | Y | Y | Y | N | Y |
    *                  +---+---+---+---+---+
    *               W  | Y | Y | N | N | Y |
    *                  +---+---+---+---+---+
    *               P  | Y | Y | Y | Y | N |
    *                  +---+---+---+---+---+
    *
    *  The three sets correspond to three spinlatches within the
    *  PageLatchSet object:
	*       SpinLatch _access[1];       // access intent/page delete
	*       SpinLatch _readwr[1];       // read/write page lock
	*       SpinLatch _parent[1];       // posting of fence key in parent
    */

	typedef enum {
	    LockAccess,
	    LockDelete,
	    LockRead,
	    LockWrite,
	    LockParent
	} LockMode;
	
	class SpinLatch {
    public:
        static uint spinReadLock( SpinLatch* latch, const char* thread );
        static void spinReleaseRead( SpinLatch* latch, const char* thread );
        static int  spinTryWrite( SpinLatch* latch, const char* thread );
        static uint spinWriteLock( SpinLatch* latch, const char* thread );
        static void spinReleaseWrite( SpinLatch* latch, const char* thread );

        friend std::ostream& operator<<( std::ostream& os, const SpinLatch& latch );
        std::string toString() const;

        SpinLatch() : _exclusive(0), _pending(0), _share(0) { _mutex[0] = 0; }

    public:
	    volatile uchar _mutex[1];
	    volatile uchar _exclusive:1;     // set for write access
	    volatile uchar _pending:1;
	    volatile uint16_t _share;        // count of read accessors
	};
	
	struct HashEntry {
	    SpinLatch _latch[1];
	    volatile uint16_t _slot;    // latch table entry at head of chain
	};
	
	struct LatchSet {
	    SpinLatch _access[1];       // access intent/page delete
	    SpinLatch _readwr[1];       // read/write page lock
	    SpinLatch _parent[1];       // posting of fence key in parent
	    SpinLatch _busy[1];         // slot is being moved between chains
	    volatile uint16_t _next;    // next entry in hash table chain
	    volatile uint16_t _prev;    // prev entry in hash table chain
	    volatile uint16_t _pin;     // number of outstanding locks
	    volatile uint16_t _hash;    // hash slot of this entry
	    volatile PageNo _pageNo;    // latch set page number

        friend std::ostream& operator<<( std::ostream& os, const LatchSet& set );
        std::string toString() const;
	};

	class LatchMgr {
    public:
        /**
        *  link latch table entry into latch hash table
        *  @param hashIndex - latch hash table index -> slot
        *  @param victim    - latchSets index of latch being added
        *  @param pageNo    - 
        *  @param thread    - 
        */
        void latchLink( ushort hashIndex, ushort victim, PageNo pageNo, const char* thread );

        /**
        *  release latch pin
        */
        void unpinLatch( LatchSet* set, const char* thread );

        /**
        *  find existing latchset or inspire new one
        *  return with latchset pinned
        */
        LatchSet* pinLatch( PageNo pageNo, const char* thread );

    public:
	    Page _alloc[2];             // next and free page numbers in right ptr
	    SpinLatch _lock[1];         // allocation area latch
	    ushort _latchDeployed;      // highest number of latch entries deployed
	    ushort _nlatchPage;         // number of latch pages at BT_latch
	    ushort _latchTotal;         // number of page latch entries
	    ushort _latchHash;          // number of latch hash table slots
	    ushort _latchVictim;        // next latch entry to examine
	    LatchSet* _latchSets;       // mapped latch set from latch pages
	    HashEntry _table[0];        // the hash table
	};

}   // namespace mongo
