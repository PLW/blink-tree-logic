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

#include <pthread.h>

namespace mongo {
    /*
    *    There are six lock types for each node in four independent sets: 
    *    Set 1
    *        1. AccessIntent: Sharable.
    *               Going to Read the node. Incompatible with NodeDelete. 
    *        2. NodeDelete: Exclusive.
    *               About to release the node. Incompatible with AccessIntent. 
    *    Set 2
    *        3. ReadLock: Sharable.
    *               Read the node. Incompatible with WriteLock. 
    *        4. WriteLock: Exclusive.
    *               Modify the node. Incompatible with ReadLock and other WriteLocks. 
    *    Set 3
    *        5. ParentModification: Exclusive.
    *               Change the node's parent keys. Incompatible with ParentModification. 
    *    Set 4
    *        6. AtomicModification: Exclusive.
    *               Atomic Update including node is underway. Incompatible with AtomicModification. 
    */

    enum BLTLockMode {
        LockNone      =  0,
        LockAccess    =  1,
        LockDelete    =  2,
        LockRead      =  4,
        LockWrite     =  8,
        LockParent    = 16,
        LockAtomic    = 32
    };
    
    //
    //    definition for phase-fair reader/writer lock implementation
    //
    class BLT_RWLock {
    public:
        static void WriteLock( BLT_RWLock* );
        static void WriteRelease( BLT_RWLock* );
        static void ReadLock( BLT_RWLock* );
        static void ReadRelease( BLT_RWLock* );

        ushort rin[1];
        ushort rout[1];
        ushort ticket[1];
        ushort serving[1];
    };
    
    #define PHID        0x1
    #define PRES        0x2
    #define MASK        0x3
    #define RINC        0x4
    
    /**
    *  spin latch implementation
    */
    class SpinLatch {
    public:
        static void spinreadlock( SpinLatch* );
        static void spinwritelock( SpinLatch* );
        static int  spinwritetry( SpinLatch* );
        static void spinreleasewrite( SpinLatch* );
        static void spinreleaseread( SpinLatch* );

        ushort exclusive:1;     // exclusive is set for write access
        ushort pending:1;
        ushort share:14;        // share is count of read accessors
                                //   grant write lock when share == 0
    };
    
    #define XCL         1
    #define PEND        2
    #define BOTH        3
    #define SHARE       4
    
    /**
    *  hash table entries
    */
    struct HashEntry {
        volatile uint slot;     // latch table entry at head of chain
        SpinLatch latch[1];
    };
    
    /**
    *  latch manager table structure
    */
    struct LatchSet {
        uid page_no;            // latch set page number
        BLT_RWLock readwr[1];   // read / write page lock
        BLT_RWLock access[1];   // access intent / page delete
        BLT_RWLock parent[1];   // posting of fence key in parent
        BLT_RWLock atomic[1];   // atomic update in progress
        uint split;             // right split page atomic insert
        uint entry;             // entry slot in latch table
        uint next;              // next entry in hash table chain
        uint prev;              // prev entry in hash table chain
        volatile ushort pin;    // number of outstanding threads
        ushort dirty:1;         // page in cache is dirty

    #ifdef unix
        pthread_t atomictid;    // thread id holding atomic lock
    #else
        uint atomictid;
    #endif

    };


    class LatchMgr {
    public:
    };

}   // namespace mongo

