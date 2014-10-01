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
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "mongo/db/storage/bltree/common.h"
#include "mongo/db/storage/bltree/latchmgr.h"
#include "mongo/db/storage/bltree/blterr.h"
#include "mongo/db/storage/bltree/logger.h"
#else
#include "blterr.h"
#include "common.h"
#include "latchmgr.h"
#include <assert.h>
#endif

#include <stdlib.h>
#include <sstream>
#include <unistd.h>
   
namespace mongo {
    
	//
	//	Phase-Fair reader/writer lock implementation
	//
	
#ifdef unix

	void BLT_RWLock::WriteLock( BLT_RWLock* lock ) {
		ushort tix = __sync_fetch_and_add( (ushort *)lock->ticket, 1 );
	
		// wait for our ticket to come up
		while (tix != lock->serving[0]) sched_yield();
		ushort w = PRES | (tix & PHID);
		ushort r = __sync_fetch_and_add( (ushort *)lock->rin, w );
		while (r != *lock->rout) sched_yield();
	}
	
	void BLT_RWLock::WriteRelease( BLT_RWLock* lock ) {
		__sync_fetch_and_and( (ushort *)lock->rin, ~MASK );
		lock->serving[0]++;
	}
	
	void BLT_RWLock::ReadLock( BLT_RWLock* lock ) {
	    ushort w = __sync_fetch_and_add( (ushort *)lock->rin, RINC ) & MASK;
		if (w) {
		    while (w == (*lock->rin & MASK)) sched_yield();
	    }
	}
	
	void BLT_RWLock::ReadRelease( BLT_RWLock* lock ) {
		__sync_fetch_and_add( (ushort *)lock->rout, RINC );
	}
	
#else
	
	void BLT_RWLock::WriteLock( BLT_RWLock *lock ) {
		ushort tix = _InterlockedExchangeAdd16( lock->ticket, 1 );
	
		// wait for our ticket to come up
		while (tix != lock->serving[0]) SwitchToThread();
		ushort w = PRES | (tix & PHID);
		ushort r = _InterlockedExchangeAdd16( lock->rin, w );
		while (r != *lock->rout) SwitchToThread();
	}
	
	void BLT_RWLock::WriteRelease( BLT_RWLock* lock ) {
		_InterlockedAnd16( lock->rin, ~MASK );
		lock->serving[0]++;
	}
	
	void BLT_RWLock::ReadLock( BLT_RWLock* lock ) {
	    ushort w = _InterlockedExchangeAdd16 (lock->rin, RINC) & MASK;
		if (w) {
		    while (w == (*lock->rin & MASK)) SwitchToThread();
	    }
	}
	
	void BLT_RWLock::ReadRelease( BLT_RWLock* lock ) {
		_InterlockedExchangeAdd16( lock->rout, RINC );
	}
	
#endif
	
	
	//
	//	Spin Latch Manager
	//
	
#ifdef unix
	
	/**
	*  wait until write lock mode is clear
	*  and add 1 to the share count
	*/
	void SpinLatch::spinreadlock( SpinLatch* latch ) {
	    ushort prev;
	
	    do {
		    prev = __sync_fetch_and_add( (ushort *)latch, SHARE );
	
		    //  see if exclusive request is granted or pending
		    if (!(prev & BOTH)) return;
		    prev = __sync_fetch_and_add( (ushort *)latch, -SHARE );
	    } while (sched_yield(), 1);
	}
	
	/**
	*  wait for other read and write latches to relinquish
	*/
	void SpinLatch::spinwritelock( SpinLatch* latch ) {
	    ushort prev;
	
	    do {
		    prev = __sync_fetch_and_or( (ushort *)latch, PEND | XCL );
		    if (!(prev & XCL)) {
		        if( !(prev & ~BOTH) ) {
			        return;
	            }
		        else {
			        __sync_fetch_and_and( (ushort *)latch, ~XCL );
	            }
	        }
	    } while (sched_yield(), 1);
	}
	
	/**
	*  try to obtain write lock
	*  @return 1 if obtained, 0 otherwise 
	*/
	int SpinLatch::spinwritetry( SpinLatch* latch ) {
	    ushort prev;
		prev = __sync_fetch_and_or( (ushort *)latch, XCL );
	
		//	take write access if all bits are clear
		if (!(prev & XCL)) {
		    if (!(prev & ~BOTH)) {
			    return 1;
	        }
		    else {
			    __sync_fetch_and_and( (ushort *)latch, ~XCL );
	        }
	    }
		return 0;
	}
	
	/**
	*  clear write mode
	*/
	void SpinLatch::spinreleasewrite( SpinLatch* latch ) {
	    __sync_fetch_and_and( (ushort *)latch, ~BOTH );
	}
	
	/**
	*  decrement reader count
	*/
	void SpinLatch::spinreleaseread( SpinLatch* latch ) {
		__sync_fetch_and_add( (ushort *)latch, -SHARE );
	}
	
#else
	
	/**
	*  wait until write lock mode is clear
	*  and add 1 to the share count
	*/ 
	void SpinLatch::spinreadlock( SpinLatch* latch ) {
	    ushort prev;
	
	    do {
		    prev = _InterlockedExchangeAdd16( (ushort *)latch, SHARE );
	
		    //  see if exclusive request is granted or pending
		    if (!(prev & BOTH)) return;
		    prev = _InterlockedExchangeAdd16( (ushort *)latch, -SHARE );
	    } while (SwitchToThread(), 1);
	}
	
	/**
	*  wait for other read and write latches to relinquish
	*/
	void SpinLatch::spinwritelock( SpinLatch* latch ) {
	    ushort prev;
	
	    do {
		    prev = _InterlockedOr16( (ushort *)latch, PEND | XCL );
		    if (!(prev & XCL)) {
		        if( !(prev & ~BOTH)) {
			        return;
	            }
		        else {
			        _InterlockedAnd16((ushort *)latch, ~XCL);
	            }
	        }
	    } while(SwitchToThread(), 1);
	}
	
	/**
	*  try to obtain write lock
	*  @return 1 if obtained, 0 otherwise 
	*/
	int SpinLatch::spinwritetry( SpinLatch* latch ) {
	    ushort prev = _InterlockedOr16((ushort *)latch, XCL);
	
		//	take write access if all bits are clear
		if (!(prev & XCL)) {
		    if (!(prev & ~BOTH)) {
			    return 1;
	        }
		    else {
			    _InterlockedAnd16((ushort *)latch, ~XCL);
	        }
	    }
		return 0;
	}
	
	/**
	*  clear write mode
	*/
	void SpnLatch::spinreleasewrite( SpinLatch* latch ) {
		_InterlockedAnd16( (ushort *)latch, ~BOTH );
	}
	
	/**
	*  decrement reader count
	*/
	void SpnLatch::spinreleaseread( SpinLatch* latch ) {
		_InterlockedExchangeAdd16( (ushort *)latch, -SHARE );
	}
	
#endif

}   // namespace mongo

