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

        string line;
        while (!in.eof()) {
            getline( in, line );
            if (0==line.length()) continue;
            if ('#'==line[0]) continue;
            size_t n = line.find( '\t' );
            if (string::npos == n) continue;

            string key = line.substr( 0, n );
            string val = line.substr( n+1 );

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
        BufMgr* mgr = BufMgr::create( conf._metaName,
                                      conf._openMode,
                                      conf._pageBits,
                                      conf._poolSize,
                                      conf._segBits,
                                      conf._hashSize );
        _blt = BLTree::create( mgr );
    }

    BLTreeEngine::~BLTreeEngine() {
    }

    Status BLTreeEngine::loadMetadataMap()
    {
        std::string key( "collection-catalog" );
        uint valmax( 65536 );
        char* value[valmax];

        // pull it out of the metadata store
        int vallen = _blt->findkey( (uchar*) key.dat(), key.size(), (uchar*)value, valmax );
        if (vallen >= valmax) {
            return Status( ErrorCodes::InternalError, "collection-catalog overflow" );
        }
        value[vallen] = 0;

        // parse into BSON
        BSONObjBuilder builder;
        Status s = JParse( value ).parse( builder );
        if (!s.isOK()) {
            return Status( ErrorCodes::InternalError, "collection-catalog parse error" );
        }

        // step through the BSON
        _collMap.clear();
        BSONObjIterator it( builder.obj() );
        while (it.more()) {
            BSONElement e = it.next();
            std::string ns = e[ "ns" ].String();
            _collMap.add( BLTMetadataMapEntry( ns, e ) );
        }
    }

    RecoveryUnit* BLTreeEngine::newRecoveryUnit(
        OperationContext* ctx )
    {
        return new BLTreeRecoveryUnit( /* tbd */ );
    }

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

    DatabaseCatalogEntry* BLTreeEngine::getDatabaseCatalogEntry(
        OperationContext* ctx,
        const StringData& db )
    {
        BLTMetadataMap collMap;
        loadCollectionMetadata( &collMap );
        boost::shared_ptr<BLTreeDatabaseCatalogEntry>& dbce = collMap[ db.toString() ];
        if ( !dbce ) {
            dbce = boost::make_shared<BLTreeDatabaseCatalogEntry>( this, db );
        }
        return dbce.get();
    }

    int BLTreeEngine::flushAllFiles(
        bool sync )
    {
        boost::mutex::scoped_lock lk( _entryMapMutex );
        for ( EntryMap::const_iterator i = _entryMap.begin(); i != _entryMap.end(); ++i ) {
            if ( i->second->cfHandle ) {
                _db->Flush( FlushOptions(), i->second->cfHandle.get() );
            }
        }
        return _entryMap.size();
    }

    Status BLTreeEngine::repairDatabase(
        OperationContext* tnx,
        const std::string& dbName,
        bool preserveClonedFilesOnFailure,
        bool backupOriginalFiles )
    {
        return Status( ErrorCodes::InternalError, "repairDatabase not yet implemented" );
    }

    void BLTreeEngine::cleanShutdown(
        OperationContext* ctx)
    {
        _blt.close();
    }

    Status BLTreeEngine::closeDatabase(
        OperationContext* ctx,
        const StringData& db )
    {
        boost::mutex::scoped_lock lk( _dbCatalogMapMutex );
        _dbCatalogMap.erase( db.toString() );
        return Status::OK();
    }

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

}
