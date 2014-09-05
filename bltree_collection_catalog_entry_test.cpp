//@ file bltree_collection_catalog_entry_test.cpp

#include "mongo/base/status.h"
#include "mongo/db/json.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/bson_collection_catalog_entry.h"
//#include "mongo/db/storage/bltree/bltree_collection_catalog_entry.h"
#include "mongo/db/storage/bltree/common.h"
#include "mongo/db/storage/bltree/blterr.h"
#include "mongo/db/storage/bltree/bltkey.h"
#include "mongo/db/storage/bltree/bltree.h"
#include "mongo/db/storage/bltree/bltval.h"
#include "mongo/db/storage/bltree/bufmgr.h"
#include "mongo/unittest/unittest.h"

#include <iostream>
#include <stdio.h>
#include <string>
#include <string.h>
#include <vector>

using namespace std;

namespace mongo {

    std::string name( "blt.meta" ); // index file name
    uint pageBits( 15 );            // lg( page size )
    uint poolSize( 4096 );          // number of segments
    uint segBits( 5 );              // lg( seg size in pages )
    uint hashSize( 4096 );          // size of segment cache

    //
    //  TESTS
    //

    TEST( BLTreeCollectionCatalogEntry, BasicTest ) {

        std::cout << "\nTEST( BLTreeCollectionCatalogEntry, BasicTest )" << std::endl;

        char* path = (char *)name.c_str();
        remove( path );
        BufMgr* mgr = BufMgr::create( path, BT_rw, pageBits, poolSize, segBits, hashSize );
        BLTree* blt = BLTree::create( mgr );

        BSONObj o_a = fromjson(
            "{ ns:'db.coll-a',"
               "indexes:["
                   "{ spec: { _id:'bltree' }, ready: true, head_a: 123, head_b: 456789, multikey: false },"
                   "{ spec: { a:'bltree' }, ready: true, head_a: 234, head_b: 567891, multikey: false }"
               "] }" );
        
        BSONObj o_b = fromjson(
            "{ ns:'db.coll-b',"
               "indexes:["
                   "{ spec: { _id:'bltree' }, ready: true, head_a: 123, head_b: 556789, multikey: false },"
                   "{ spec: { b:'bltree' }, ready: true, head_a: 234, head_b: 667891, multikey: false }"
               "] }" );

        BSONObj o_c = fromjson(
            "{ ns:'db.coll-c',"
               "indexes:["
                   "{ spec: { _id:'bltree' }, ready: true, head_a: 123, head_b: 656789, multikey: false },"
                   "{ spec: { c:'bltree' }, ready: true, head_a: 234, head_b: 767891, multikey: false }"
               "] }" );

        const char* key_a = o_a[ "ns" ].valuestr();
        uint keylen_a = strlen( key_a );
        std::string val_a = o_a.toString();
        ASSERT_OK( blt->insertkey( (uchar *)key_a, keylen_a, 0, (uchar *)val_a.data(), val_a.size() ) );
        
        const char* key_b = o_b[ "ns" ].valuestr();
        uint keylen_b = strlen( key_b );
        std::string val_b = o_b.toString();
        ASSERT_OK( blt->insertkey( (uchar *)key_b, keylen_b, 0, (uchar *)val_b.data(), val_b.size() ) );
        
        const char* key_c = o_c[ "ns" ].valuestr();
        uint keylen_c = strlen( key_c );
        std::string val_c = o_c.toString();
        ASSERT_OK( blt->insertkey( (uchar *)key_c, keylen_c, 0, (uchar *)val_c.data(), val_c.size() ) );

        char valbuf[65536];

        const char* findKey_a = "db.coll-a";
        int n = blt->findkey( (uchar *)findKey_a, 9, (uchar *)valbuf, 65536 );
        valbuf[n] = 0;
        std::cout << "find(a) [" << n << "] :\n" << valbuf << std::endl;
        
        const char* findKey_b = "db.coll-b";
        n = blt->findkey( (uchar *)findKey_b, 9, (uchar *)valbuf, 65536 );
        valbuf[n] = 0;
        std::cout << "find(b) [" << n << "] :\n" << valbuf << std::endl;

        const char* findKey_c = "db.coll-c";
        n = blt->findkey( (uchar *)findKey_c, 9, (uchar *)valbuf, 65536 );
        valbuf[n] = 0;
        std::cout << "find(c) [" << n << "] :\n" << valbuf << std::endl;

        BufMgr::destroy( mgr );
    }

    TEST( BLTreeCollectionCatalogEntry, PersistenceTest ) {

        std::cout << "\nTEST( BLTreeCollectionCatalogEntry, PersistenceTest )" << std::endl;

        char* path = (char *)name.c_str();
        BufMgr* mgr = BufMgr::create( path, BT_rw, pageBits, poolSize, segBits, hashSize );
        BLTree* blt = BLTree::create( mgr );

        char valbuf[65536];

        const char* findKey_a = "db.coll-a";
        int n = blt->findkey( (uchar *)findKey_a, 9, (uchar *)valbuf, 65536 );
        valbuf[n] = 0;
        std::cout << "find(a) [" << n << "] :\n" << valbuf << std::endl;
        
        const char* findKey_b = "db.coll-b";
        n = blt->findkey( (uchar *)findKey_b, 9, (uchar *)valbuf, 65536 );
        valbuf[n] = 0;
        std::cout << "find(b) [" << n << "] :\n" << valbuf << std::endl;

        const char* findKey_c = "db.coll-c";
        n = blt->findkey( (uchar *)findKey_c, 9, (uchar *)valbuf, 65536 );
        valbuf[n] = 0;
        std::cout << "find(c) [" << n << "] :\n" << valbuf << std::endl;

        BufMgr::destroy( mgr );
    }

    
    TEST( BLTreeCollectionCatalogEntry, UpdateTest ) {

        std::cout << "\nTEST( BLTreeCollectionCatalogEntry, UpdateTest )" << std::endl;

        char* path = (char *)name.c_str();
        BufMgr* mgr = BufMgr::create( path, BT_rw, pageBits, poolSize, segBits, hashSize );
        BLTree* blt = BLTree::create( mgr );

        char valbuf[65536];

        const char* findKey_a = "db.coll-a";
        int n = blt->findkey( (uchar *)findKey_a, 9, (uchar *)valbuf, 65536 );
        valbuf[n] = 0;
        std::cout << "find(a) [" << n << "] :\n" << valbuf << std::endl;
        
        BSONCollectionCatalogEntry::MetaData metadata;
        metadata.parse( fromjson( valbuf ) );
        metadata.indexes[1].multikey = true;

        std::cout << "object in-place update done" << std::endl;

        const char* key = metadata.ns.c_str();
        uint keylen = metadata.ns.size();
        std::string val = metadata.toBSON().toString();

        std::cout << "key = " << key << std::endl;
        std::cout << "val = " << val << std::endl;

        ASSERT_OK( blt->insertkey( (uchar *)key, keylen, 0, (uchar *)val.data(), val.size() ) );
        
        n = blt->findkey( (uchar *)findKey_a, 9, (uchar *)valbuf, 65536 );
        valbuf[n] = 0;
        std::cout << "find(a) [" << n << "] :\n" << valbuf << std::endl;

        BufMgr::destroy( mgr );
    }

    
}   // namespace mongo


