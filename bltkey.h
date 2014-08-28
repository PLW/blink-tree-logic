
#pragma once

#include "common.h"

#include <string.h>

namespace mongo {

    class BLTKey {
    public:
	    //
	    //  compare two keys,
	    //  @return  > 0, = 0, or < 0
	    //
	    static int keycmp( BLTKey* key1, uchar* key2, uint len2 ) {
	        uint len1 = key1->len;
	        int ans;
	
	        if ( (ans = memcmp( key1->key, key2, len1 > len2 ? len2 : len1 )) ) {
	            return ans;
	        }
	        if (len1 > len2) return 1;
	        if (len1 < len2) return -1;
	        return 0;
	    }

	    uchar len;
	    uchar key[1];
    };

}   // namespace mongo
