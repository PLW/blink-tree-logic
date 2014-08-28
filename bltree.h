
#ifndef STANDALONE
#include "mongo/db/storage/mmap_v1/bltree/common.h"
#include "mongo/db/storage/mmap_v1/bltree/blterr.h"
#include "mongo/db/storage/mmap_v1/bltree/bltkey.h"
#include "mongo/db/storage/mmap_v1/bltree/bufmgr.h"
#include "mongo/db/storage/mmap_v1/bltree/page.h"
#else
#include "common.h"
#include "blterr.h"
#include "bltkey.h"
#include "bufmgr.h"
#include "page.h"
#endif

namespace mongo {
  
    //
    //  bltree
    //
    class BLTree {
    public:
        // factory
	    static BLTree* create( BufMgr* mgr );
        void close();

    public:
        int findkey( uchar* key, uint keylen, uchar* value, uint valmax );
        BTERR insertkey( uchar* key, uint keylen, uint lvl, uchar* value, uint vallen);
        BTERR deletekey( uchar* key, uint len, uint lvl );
        uint startkey( uchar* key, uint len );
        uint nextkey( uint slot );
        uint latchaudit(); // for debugging

    protected:
        BTERR fixfence( PageSet* set, uint lvl );
        BTERR collapseroot( PageSet *root );
        uint cleanpage( Page* page, uint keylen, uint slot, uint vallen);
        BTERR splitroot( PageSet* root, uchar* leftkey, uid page_no2 );
        BTERR splitpage( PageSet* set );
    
    public:
        BufMgr* mgr;                 // buffer manager for thread
        Page* cursor;             // cached frame for start/next (never mapped)
        Page* frame;              // spare frame for the page split (never mapped)
        uid cursor_page;            // current cursor page number    
        uchar* mem;                 // frame, cursor, page memory buffer
        int found;                  // last delete or insert was found
        int err;                    // last error
    };

}   // namespace mongo

