// bltree_engine.cpp

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
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

#include "mongo/db/storage/bltree/bltree_engine.h"
#include "mongo/db/json.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/bltree/bltree_collection_catalog_entry.h"
#include "mongo/db/storage/bltree/bltree_database_catalog_entry.h"
#include "mongo/db/storage/bltree/bltree_record_store.h"
#include "mongo/db/storage/bltree/bltree_recovery_unit.h"
#include "mongo/db/storage/bltree/bltree_sorted_data_impl.h"

namespace mongo {

    /**
    *  Config format, (e.g.):
    *
    *       basePath    /data/blt/
    *       metaName    metadb
    *       openMode    rw
    *       pageBits    15
    *       poolSize    4096
    *       segBits     5
    *       hashSize    4096
    */
    Status BLTreeEngine::parseConfig(
        const std::string& path,    // expected to include trailing '/'
        BTLConfig* conf )
    {
        std::string configPath( path+"blt.conf" );
        std::ifstream in( configPath.c_str(), ios::in );
        if (!in.good()) {
            return Status( ErrorCodes::InternalError, "BLTree config file open error" );
        }
        std::string line;
        while (!in.eof()) {
            std::getline( in, line );
            if (0==line.length()) continue;
            if ('#'==line[0]) continue;
            std::size_t n = line.find( '\t' );
            if (std::string::npos == n) continue;

            std::string key = line.substr( 0, n );
            std::string val = line.substr( n+1 );

                 if ("metaName"==key) conf->_metaName = path+val;
            else if ("openMode"==key) conf->_openMode = val;
            else if ("pageBits"==key) conf->_pageBits = strtoul( val.c_str(), NULL, 10 );
            else if ("poolSize"==key) conf->_poolSize = strtoul( val.c_str(), NULL, 10 );
            else if ("segBits" ==key) conf->_segBits  = strtoul( val.c_str(), NULL, 10 );
            else if ("hashSize"==key) conf->_hashSize = strtoul( val.c_str(), NULL, 10 );
        }
    }

    BLTreeEngine::BLTreeEngine( const std::string& path ) : _path( path )
    {
        BLTConfig conf;
        parseConfig( _path, &conf );
        BufMgr* mgr = BufMgr::create( conf._metaName, conf._openMode, conf._pageBits,
                                      conf._poolSize, conf._segBits, conf._hashSize );
        _blt = BLTree::create( mgr );
    }

    BLTreeEngine::~BLTreeEngine() {
    }

    #define MAX_VALUE_SIZE  65536

    Status BLTreeEngine::get( const KeyString& inputKey, Value& val )
    {
        char valbuf[ MAX_VALUE_SIZE+1 ];
        const char* key = inputKey.data();
        int n = _blt->findkey( (uchar *)key, inputKey.size(), (uchar *)valbuf, MAX_VALUE_SIZE );
        if (0 == n) {
            return Status( ErrorCodes::InternalError, "findkey returned empty in 'get'" );
        }
        val.assign( valbuf, n );
        return Status::OK();
    }

    Status BLTreeEngine::put( const KeyString& inputKey, const BSONObj& obj )
    {
        const char* key = inputKey.data();
        std::string val = obj.toString();
        return _blt->insertkey( (uchar *)key, inputKey.size(), 0, (uchar *)val.data(), val.size() );
    }

    Status BLTreeEngine::remove( const KeyString& inputKey )
    {
        const char* key = inputKey.data();
        return _blt->deletekey( (uchar *)key, inputKey.size(), 0 );
    }

    //
    // StorageEngine interface
    //

    //
    // return a new interface to recovery unit
    // 
    RecoveryUnit* BLTreeEngine::newRecoveryUnit(
        OperationContext* ctx )
    {
        return new BLTreeRecoveryUnit( ctx );
    }

    Status BLTreeEngine::loadCollectionMetadata( BLTMetadataMap* collMap ) {
        // stubbed
        return Status::OK();
    }

    // 
    // list databases stored in this storage engine
    // you get this by enumerating all the collections, and
    // returning the unique set of db prefixes.
    // 
    Status BLTreeEngine::listDatabases(
        std::vector<std::string>* out ) const
    {
        std::set<std::string> dbs;
        BLTMetadataMap collMap;
        loadCollectionMetadata( &collMap );

        for (BLTMetadataMapIterator it = collMap.begin(); it!=collMap.end(); ++it) {
            const StringData& ns = it->first;
            std::string db = nsToDatabase( ns ); // remove collection suffixes
            if ( dbs.insert( db ).second ) {     // de-dup
                out->push_back( db );
            }
        }
    }

    // 
    // return the DatabaseCatalogEntry that describes the database
    //
    DatabaseCatalogEntry* BLTreeEngine::getDatabaseCatalogEntry(
        OperationContext* ctx,
        const StringData& db )
    {
        BLTMetadataMap collMap;
        loadCollectionMetadata( &collMap );
        boost::shared_ptr<BLTreeDatabaseCatalogEntry>& e = collMap[ db.toString() ];
        if ( !e ) {
            e = boost::make_shared<BLTreeDatabaseCatalogEntry>( this, db );
        }
        return e.get();
    }

    //
    // @return number of files flushed
    //
    int BLTreeEngine::flushAllFiles( bool sync )
    {
        // stubbed
        return 0;
    }

    Status BLTreeEngine::repairDatabase(
        OperationContext* tnx,
        const std::string& dbName,
        bool preserveClonedFilesOnFailure,
        bool backupOriginalFiles )
    {
        // stubbed
        return Status( ErrorCodes::InternalError, "repairDatabase not implemented" );
    }

    void BLTreeEngine::cleanShutdown(
        OperationContext* ctx)
    {
        _blt.close();
    }

    // 
    // close all file handles
    //
    Status BLTreeEngine::closeDatabase(
        OperationContext* ctx,
        const StringData& db )
    {
        boost::mutex::scoped_lock lk( _dbCatalogMapMutex );
        _dbCatalogMap.erase( db.toString() );
        return Status::OK();
    }

    //
    // delete all collections, their metadata, then the database
    //
    Status BLTreeEngine::dropDatabase(
        OperationContext* ctx,
        const StringData& db )
    {
        const string prefix = db.toString() + ".";
        boost::mutex::scoped_lock lk( _entryMapMutex );
        vector<string> toDrop;

        for (EntryMap::const_iterator i = _entryMap.begin(); i != _entryMap.end(); ++i ) {
            const StringData& ns = i->first;
            if ( ns.startsWith( prefix ) ) {
                toDrop.push_back( ns.toString() );
            }
        }

        for (vector<string>::const_iterator j = toDrop.begin(); j != toDrop.end(); ++j ) {
            _dropCollection_inlock( ctx, *j );
        }

        return closeDatabase( ctx, db );
    }


    //
    // internals
    //

    Status BLTreEngine::listIndexNames(
        const std::string& path,
        std::vector< std::string>* indexNames )
    {
        namespace fs = boost::filesystem;
        if (!fs::exists( path )) {
            return Status( ErrorCodes::InternalError, "path '"+path+"' not found" );
        }
        if (!fs::is_directory( path )) {
            return Status( ErrorCodes::InternalError, "path '"+path+"' is not a directory" );
        }
        fs::directory_iterator end_it;
        for (fs::directory_iterator it( path ); it!=end_it; ++it) {
            std::string fname = it->path().filename();
            size_t n = fname.find( "$" );
            if (n!=std::string::npos) {
                indexNames->push_back( fname.substr(0,n) );
            }
        }
        return Status::OK();
    }

    Status BLTreeEngine::createEntries()
    {
    }


}
