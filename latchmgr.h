
#pragma once

#include "common.h"
#include "page.h"

namespace mongo {

    /*
      There are five lock types for each node in three independent sets: 
        1. (set 1) AccessIntent: Sharable. Going to Read node. Incompatible with NodeDelete. 
        2. (set 1) NodeDelete: Exclusive. About to release node. Incompatible with AccessIntent. 
        3. (set 2) ReadLock: Sharable. Read node. Incompatible with WriteLock. 
        4. (set 2) WriteLock: Exclusive. Modify node. Incompatible with ReadLock and WriteLock. 
        5. (set 3) ParentModification: Exclusive. Change node parent keys. Incompatible ParentModification. 
    */
    
    enum BLTLockMode {
        LockAccess,
        LockDelete,
        LockRead,
        LockWrite,
        LockParent
    };
    
    //
    //    definition for phase-fair reader/writer lock implementation
    //
    class BLT_RWLock {
    public:
        static void WriteLock( BLT_RWLock* lock );
        static void WriteRelease( BLT_RWLock* lock );
        static void ReadLock( BLT_RWLock* lock );
        static void ReadRelease( BLT_RWLock* lock );

    public:
        ushort rin[1];
        ushort rout[1];
        ushort ticket[1];
        ushort serving[1];
    };
    
    #define PHID 0x1
    #define PRES 0x2
    #define MASK 0x3
    #define RINC 0x4
    
    //    definition for spin latch implementation
    
    // exclusive is set for write access
    // share is count of read accessors
    // grant write lock when share == 0
    
    class SpinLatch {
    public:
        static void spinreadlock( SpinLatch* latch );
        static void spinwritelock( SpinLatch* latch );
        static int  spinwritetry( SpinLatch* latch );
        static void spinreleasewrite( SpinLatch* latch );
        static void spinreleaseread( SpinLatch* latch );

    public:
        ushort exclusive:1;
        ushort pending:1;
        ushort share:14;
    };
    
    #define XCL 1
    #define PEND 2
    #define BOTH 3
    #define SHARE 4
    
    //
    //  hash table entries
    //
    struct HashEntry {
        SpinLatch latch[1];
        volatile ushort slot;        // Latch table entry at head of chain
    };
    
    //
    //    latch manager table structure
    //
    class LatchSet {
    public:
        BLT_RWLock readwr[1];       // read/write page lock
        BLT_RWLock access[1];       // Access Intent/Page delete
        BLT_RWLock parent[1];       // Posting of fence key in parent
        SpinLatch busy[1];          // slot is being moved between chains
        volatile ushort next;       // next entry in hash table chain
        volatile ushort prev;       // prev entry in hash table chain
        volatile ushort pin;        // number of outstanding locks
        volatile ushort hash;       // hash slot entry is under
        volatile uid page_no;       // latch set page number

    };
    
    class LatchMgr {
    public:
        Page alloc[1];              // next page_no in right ptr
        unsigned char chain[BtId];  // head of free page_nos chain
        SpinLatch lock[1];          // allocation area lite latch
        ushort latchdeployed;       // highest number of latch entries deployed
        ushort nlatchpage;          // number of latch pages at BT_latch
        ushort latchtotal;          // number of page latch entries
        ushort latchhash;           // number of latch hash table slots
        ushort latchvictim;         // next latch entry to examine
        HashEntry table[0];         // the hash table

    };
    
}   // namespace mongo
