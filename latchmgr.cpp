

#ifndef STANDALONE
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "mongo/db/storage/mmap_v1/bltree/common.h"
#include "mongo/db/storage/mmap_v1/bltree/latchmgr.h"
#include "mongo/db/storage/mmap_v1/bltree/blterr.h"
#include "mongo/db/storage/mmap_v1/bltree/logger.h"
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
    
    void BLT_RWLock::WriteLock( BLT_RWLock* lock ) {
        ushort w, r;
        ushort tix = __sync_fetch_and_add( (ushort *)lock->ticket, 1 );

        // wait for our ticket to come up
        while( tix != lock->serving[0] ) sched_yield();
    
        w = PRES | (tix & PHID);
        r = __sync_fetch_and_add( (ushort *)lock->rin, w );

        while( r != *lock->rout ) sched_yield();
    }
    
    void BLT_RWLock::WriteRelease( BLT_RWLock* lock ) {
        __sync_fetch_and_and( (ushort *)lock->rin, ~MASK );
        lock->serving[0]++;
    }
    
    void BLT_RWLock::ReadLock( BLT_RWLock* lock ) {
        ushort w = __sync_fetch_and_add( (ushort *)lock->rin, RINC ) & MASK;
        if (w) {
          while (w == (*lock->rin & MASK)) sched_yield ();
        }
    }
    
    void BLT_RWLock::ReadRelease( BLT_RWLock* lock ) {
        __sync_fetch_and_add( (ushort *)lock->rout, RINC );
    }
    
    //
    //    Spin Latch Manager
    //        
    
    //    wait until write lock mode is clear, and add 1 to the share count
    void SpinLatch::spinreadlock( SpinLatch* latch ) {
        ushort prev;
    
        do {
            prev = __sync_fetch_and_add( (ushort *)latch, SHARE );
        
            //  see if exclusive request is granted or pending
            if( !(prev & BOTH) ) return;

            prev = __sync_fetch_and_add( (ushort *)latch, -SHARE );
        } while (sched_yield(), 1);
    }
    
    //    wait for other read and write latches to relinquish
    void SpinLatch::spinwritelock( SpinLatch* latch ) {
        ushort prev;
    
        do {
            prev = __sync_fetch_and_or((ushort *)latch, PEND | XCL);
            if (!(prev & XCL)) {
                if( !(prev & ~BOTH) ) {
                    return;
                }
                else {
                    __sync_fetch_and_and ((ushort *)latch, ~XCL);
                }
            }
        } while (sched_yield(), 1);
    }
    
    //    try to obtain write lock
    //    @return 1 if obtained, 0 otherwise
    int SpinLatch::spinwritetry( SpinLatch* latch ) {

        ushort prev = __sync_fetch_and_or( (ushort *)latch, XCL );

        //    take write access if all bits are clear
        if ( !(prev & XCL) ) {
            if( !(prev & ~BOTH) ) {
                return 1;
            }
            else {
                __sync_fetch_and_and ((ushort *)latch, ~XCL);
            }
        }
        return 0;
    }
    
    //    clear write mode
    void SpinLatch::spinreleasewrite( SpinLatch *latch ) {
        __sync_fetch_and_and( (ushort *)latch, ~BOTH );
    }
    
    //    decrement reader count
    void SpinLatch::spinreleaseread( SpinLatch *latch ) {
        __sync_fetch_and_add( (ushort *)latch, -SHARE );
    }

}   // namespace mongo

