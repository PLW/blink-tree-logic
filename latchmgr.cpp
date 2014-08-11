//@file latchmgr.cpp
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

#include "common.h"
#include "latchmgr.h"
#include "blterr.h"
#include "logger.h"

#include <assert.h>
#include <stdlib.h>
#include <sstream>
#include <time.h>
#include <unistd.h>

#define LATCHMGR_TRACE  false

namespace mongo {

    // SpinLatch

    #define LATCH_TRACE     false
	#define SPIN_LIMIT		500000

    /**
    *  wait until write lock mode is clear,
    *  and add 1 to the share count
    */
    uint SpinLatch::spinReadLock( SpinLatch* latch, const char* thread ) {
        if (LATCHMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        assert( NULL != latch );

        ushort prev;
		uint backoffCount = 0;
		/*
		uint count = 0;

		struct timespec spec;
		spec.tv_sec = 0;
        spec.tv_nsec = random() % 1024;
		*/
        
        do {
            // obtain latch mutex
            if (__sync_lock_test_and_set( (unsigned char*)latch->_mutex, 1 )) {
                continue;
            }

			/*
			if (count++ > SPIN_LIMIT) {
				count = 0;
				nanosleep( &spec, NULL );
				spec.tv_nsec <<= 1;
				backoffCount++;
			}
			*/

            // see if exclusive request is granted or pending
            if ( (prev = !(latch->_exclusive | latch->_pending)) ) {
                latch->_share++;
            }

            *latch->_mutex = 0;
            if (prev) { return backoffCount; }

        } while (sched_yield(), 1);

		return backoffCount;
    }
 
    /**
    *  wait for other read and write latches to relinquish
    */
    uint SpinLatch::spinWriteLock( SpinLatch* latch, const char* thread ) {
        if (LATCHMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        assert( NULL != latch );

        ushort prev;
		uint backoffCount = 0;
		/*
		uint count = 0;

		struct timespec spec;
		spec.tv_sec = 0;
        spec.tv_nsec = random() % 1024;
		*/
        

        do {
            if (__sync_lock_test_and_set( (unsigned char*)latch->_mutex, 1 )) {
                continue;
            }

			/*
			if (count++ > SPIN_LIMIT) {
				count = 0;
				nanosleep( &spec, NULL );
				spec.tv_nsec <<= 1;
				backoffCount++;
			}
			*/

            // see if shared or exclusive request is granted 
            if ((prev = !(latch->_share | latch->_exclusive))) {
                latch->_exclusive = 1;
                latch->_pending = 0;
            }
            else {
                latch->_pending = 1;
            }
            *latch->_mutex = 0;
            if (prev) { return backoffCount; }

        } while (sched_yield(), 1);

		return backoffCount;
    }
 
    /**
    *  try to obtain write lock
    *  return 1 if obtained, 0 otherwise
    */
    int SpinLatch::spinTryWrite( SpinLatch* latch, const char* thread ) {
        if (LATCHMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        assert( NULL != latch );

        if (__sync_lock_test_and_set( (unsigned char*)latch->_mutex, 1 )) {
            return 0;
        }

        // take write access if all bits are clear
        uint prev;
        if ((prev = !(latch->_exclusive | latch->_share))) {
            latch->_exclusive = 1;
        }

        *latch->_mutex = 0;
        return prev;
    }

    /**
    *  clear write latch
    */
    void SpinLatch::spinReleaseWrite( SpinLatch* latch, const char* thread ) {
        if (LATCHMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        assert( NULL != latch );

        while (__sync_lock_test_and_set( (unsigned char*)latch->_mutex, 1 )) {
            sched_yield();
        }
        latch->_exclusive = 0;
        *latch->_mutex = 0;
    }

    /**
    *  decrement reader count
    */
    void SpinLatch::spinReleaseRead( SpinLatch* latch, const char* thread) {
        if (LATCHMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        assert( NULL != latch );

        while (__sync_lock_test_and_set( (unsigned char*)latch->_mutex, 1 )) {
            sched_yield();
        }
        latch->_share--;
        *latch->_mutex = 0;
    }

    std::string SpinLatch::toString() const {
        std::ostringstream oss;
        oss << "SpinLatch["
            << " mutex = " << (bool)_mutex[0]
            << ", exclusive = " << (bool)_exclusive
            << ", pending: = " << (bool)_pending
            << ", share = " << _share << "]";
        return oss.str();
    }

    std::string LatchSet::toString() const {
        std::ostringstream oss;
        oss << "LatchSet["
            << "\n  access = " << _access[0]
            << "\n  readwr = " << _readwr[0]
            << "\n  parent = " << _parent[0]
            << "\n  busy = "   << _busy[0]
            << "\n  next = "   << _next
            << "\n  prev = "   << _prev
            << "\n  pin = "    << _pin
            << "\n  hash = "   << _hash
            << "\n  pageNo = " << _pageNo << "]";
        return oss.str();
    }
 
    std::ostream& operator<<( std::ostream& os, const SpinLatch& latch ) {
        return os << latch.toString();
    }

    std::ostream& operator<<( std::ostream& os, const LatchSet& set ) {
        return os << set.toString();
    }
 
    // LatchMgr

    /**
    *  Add victim to head of hash chain at hashIndex
    */
    void LatchMgr::latchLink( ushort hashIndex, ushort victim, PageNo pageNo, const char* thread ) {
        if (LATCHMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        LatchSet* set = &_latchSets[ victim ];

        if ((set->_next = _table[ hashIndex ]._slot)) {
            _latchSets[set->_next]._prev = victim;
        }
        _table[ hashIndex ]._slot = victim;
        set->_pageNo = pageNo;
        set->_hash = hashIndex;
        set->_prev = 0;
    }
    
    /**
    *  Unpin, i.e. decrement lock count
    */
    void LatchMgr::unpinLatch( LatchSet* set, const char* thread ) {
        if (LATCHMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );
        assert( NULL != set );
        __sync_fetch_and_add( &set->_pin, -1 );
    }

    /**
    *  find existing latch set or create a new one
    *  @return with latchset pinned (i.e.) lock count++
    */
    LatchSet* LatchMgr::pinLatch( PageNo pageNo, const char* thread ) {
        if (LATCHMGR_TRACE) Logger::logDebug( thread, "", __LOC__ );

        ushort hashIndex = pageNo % _latchHashSize;
        ushort slot;
        ushort avail = 0;
        LatchSet* set;
    
        // obtain read lock on hash table entry
        SpinLatch::spinReadLock( _table[ hashIndex ]._latch, thread );
    
        // check if non-empty slot and follow chain from there
        if ( (slot = _table[ hashIndex ]._slot) ) {
            do {
                set = &_latchSets[ slot ];
                if (pageNo == set->_pageNo) break;
            } while ( (slot = set->_next) );
        }
    
        // if found, increment in count 
        if (slot) {
            __sync_fetch_and_add( &set->_pin, 1 );  // i.e. lock count++
        }
    
        // release hash table entry
        SpinLatch::spinReleaseRead( _table[ hashIndex ]._latch, thread );
    
        // if found, then done
        if (slot) return set;
    
        // not found: try again with write lock, we are going to create new latch set
        SpinLatch::spinWriteLock( _table[ hashIndex ]._latch, thread );
    
        if ( (slot = _table[ hashIndex ]._slot) ) {
            do {
                set = &_latchSets[ slot ];
                if (pageNo == set->_pageNo) break;      // might have shown up since we last checked
                if (!set->_pin && !avail) avail = slot; // may reuse unpinned slot
            } while( (slot = set->_next) );
        }
    
        // found our entry, or take over an unpinned one
        if (slot || (slot = avail)) {
            set = &_latchSets[ slot ];
            __sync_fetch_and_add( &set->_pin, 1 );  // i.e. lock count++
            set->_pageNo = pageNo;
            SpinLatch::spinReleaseWrite( _table[ hashIndex ]._latch, thread );
            return set;
        }
    
        // not found and no unpinned entries: see if there are any unused entries
        ushort victim = __sync_fetch_and_add( &_latchDeployed, 1 ) + 1;
    
        if (victim < _latchTotal) { // i.e. an available slot
            set = &_latchSets[ victim ];
            __sync_fetch_and_add( &set->_pin, 1 );              // i.e. lock count++
            latchLink( hashIndex, victim, pageNo, thread );     // link onto hash chain
            SpinLatch::spinReleaseWrite( _table[ hashIndex ]._latch, thread );
            return set;
        }
    
        victim = __sync_fetch_and_add( &_latchDeployed, -1 );

        // find and reuse previous lock entry
        while (true) {
            victim = __sync_fetch_and_add( &_latchVictim, 1 );  // i.e. latchVictim += 1

            // we don't use slot zero
            if ( (victim %= _latchTotal) ) {
                set = &_latchSets[ victim ];
            }
            else {
                continue;
            }
    
            // try to take control of our slot from other threads
            if (set->_pin || !SpinLatch::spinTryWrite( set->_busy, thread )) continue;
    
            ushort idx = set->_hash;
    
            // try to get write lock on hash chain
            // skip entry if not obtained or has outstanding locks
            if (!SpinLatch::spinTryWrite( _table[idx]._latch, thread )) {
                SpinLatch::spinReleaseWrite( set->_busy, thread );
                continue;
            }
    
            // check again: don't use a pinned set
            // may be pinned between the two clauses of previous ||
            if (set->_pin) {
                SpinLatch::spinReleaseWrite( set->_busy, thread );
                SpinLatch::spinReleaseWrite( _table[idx]._latch, thread );
                continue;
            }
    
            // unlink our available victim from its hash chain
            if (set->_prev) {
                _latchSets[set->_prev]._next = set->_next;
            }
            else {
                _table[idx]._slot = set->_next;
            }
            if (set->_next) {
                _latchSets[set->_next]._prev = set->_prev;
            }
    
            // release latches and return the pinned latch set
            SpinLatch::spinReleaseWrite( _table[idx]._latch, thread );
            __sync_fetch_and_add( &set->_pin, 1 );      // lock count++
            latchLink( hashIndex, victim, pageNo, thread );
            SpinLatch::spinReleaseWrite( _table[ hashIndex ]._latch, thread );
            SpinLatch::spinReleaseWrite( set->_busy, thread );
            return set;
        }
    }

}   // namespace mongo

