
#pragma once

#include "common.h"

namespace mongo {

    //
    // index value, either pageno / doc as opaque bytes
    //
    class BLTVal {
    public:
        // pack / unpack pageno values
        static void putid( uchar* dest, uid id );
        static uid getid( uchar* src );
        
        uchar len;
        uchar value[1];
    };

}   // namespace mongo
