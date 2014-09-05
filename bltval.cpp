
#ifndef STANDALONE
#include "mongo/db/storage/bltree/bltval.h"
#else
#include "bltval.h"
#endif

namespace mongo {

    void BLTVal::BLTVal::putid( uchar* dest, uid id ) {
        int i = BtId;
        while( i-- ) {
            dest[i] = (uchar)id;
            id >>= 8;
        }
    }
    
    uid BLTVal::BLTVal::getid( uchar* src ) {
        uid id = 0;
        for (int i = 0; i < BtId; i++) {
            id <<= 8;
            id |= *src++; 
        }
        return id;
    }
    

}   // namespace mongo
