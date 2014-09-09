// bltree_collection_catalog_entry.cpp

/**
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

#include "mongo/db/storage/bltree/bltree_collection_catalog_entry.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/bltree/bltree_engine.h"
#include "mongo/db/storage/bltree/bltree.h"

namespace mongo {

    static const int _maxAllowedIndexes = 64; // for compatability

    /**
     * bson schema for collection metadata
     * store one such document per collection in the 'blt.meta' index
     *
     *  { ns: <name for sanity>,
     *    indexes : [ { spec : <bson spec>,
     *                  ready: <bool>,
     *                  head:  <DiskLoc>,
     *                  multikey: <bool> } ]
     *  }
     *
     */

    BLTreeCollectionCatalogEntry::BLTreeCollectionCatalogEntry(
        BLTreeEngine* engine,
        const StringData& ns )
    :
        BSONCollectionCatalogEntry( ns ),
        _engine( engine ),
        _metadataKey( string( "bltmeta-" ) + ns.toString() )
    {
    }

    //
    // catalog public interface
    //

    CollectionOptions getCollectionOptions(
        OperationContext* ctx ) const
    {
    }

    int getTotalIndexCount(
        OperationContext* ctx ) const
    {
    }

    int getCompletedIndexCount(
        OperationContext* ctx ) const
    {
    }

    int getMaxAllowedIndexes() const
    {
        return _maxAllowedIndexes;
    }

    void getAllIndexes(
        OperationContext* ctx,
        std::vector<std::string>* names ) const
    {
    }

    BSONObj getIndexSpec(
        OperationContext* ctx,
        const StringData& idxName ) const
    {
        boost::mutex::scoped_lock lk( _mutex );
        Metadata md = getMetadata();
        int offset = md.findIndexOffset( indexName );
        uassert( -1, "offset < 0", offset >= 0 );
        return md.indexes[offset].spec;
    }

    bool isIndexMultikey(
        OperationContext* ctx,
        const StringData& indexName) const
    {
        boost::mutex::scoped_lock lk( _mutex );
        Metadata md = getMetadata();
        int offset = md.findIndexOffset( indexName );
        uassert( -1, "offset < 0", offset >= 0 );
        return md.indexes[offset].multikey;
    }

    bool setIndexIsMultikey(
        OperationContext* ctx,
        const StringData& indexName,
        bool multikey)
    {
        boost::mutex::scoped_lock lk( _mutex );
        Metadata md = getMetadata();
        int offset = md.findIndexOffset( indexName );
        uassert( -1, "offset < 0", offset >= 0 );
        if (md.indexes[offset].multikey == multikey) return false;
        md.indexes[offset].multikey = multikey;
        putMetadata( md );
        return true;
    }

    DiskLoc getIndexHead(
        OperationContext* ctx,
        const StringData& indexName ) const
    {
    }

    void setIndexHead(
        OperationContext* ctx,
        const StringData& indexName,
        const DiskLoc& newHead )
    {
        boost::mutex::scoped_lock lk( _mutex );
        Metadata md = getMetadata();
        int offset = md.findIndexOffset( indexName );
        uassert( -1, "offset < 0", offset >= 0 );
        md.indexes[offset].head = newHead;
        putMetadata( md );
    }

    bool isIndexReady(
        OperationContext* ctx,
        const StringData& indexName ) const
    {
        boost::mutex::scoped_lock lk( _mutex );
        Metadata md = getMetadata();
        int offset = md.findIndexOffset( indexName );
        uassert( -1, "offset < 0", offset >= 0 );
        return md.indexes[offset].ready;
    }

    Status removeIndex(
        OperationContext* ctx,
        const StringData& indexName )
    {
        boost::mutex::scoped_lock lk( _mutex );
        Metadata md = getMetadata();
        uassert( -1, "eraseIndex error", md.eraseIndex( indexName ) );
        putMetadata( md );
        _engine->getBLTree( indexName )->clear();
        return Status::OK();
    }

    Status prepareForIndexBuild(
        OperationContext* ctx,
        const IndexDescriptor* spec )
    {
        boost::mutex::scoped_lock lk( _mutex );
        Metadata md = getMetadata();
        md.indexes.push_back( IndexMetadata( spec->infoObj(), false, DiskLoc(), false ) );
        putMetadata( md );
        return Status::OK();
    }

    void indexBuildSuccess(
        OperationContext* ctx,
        const StringData& indexName )
    {
        boost::mutex::scoped_lock lk( _mutex );
        Metadata md = getMetadata();
        int offset = md.findIndexOffset( indexName );
        uassert( -1, "offset < 0", offset >= 0 );
        md.indexes[offset].ready = true;
        putMetadata( md );
    }

    void updateTTLSetting(
         OperationContext* ctx,
        const StringData& idxName,
        long long newExpireSeconds )
    {
        uassert( -1, "ttl settings change not supported in bltree yet", false );
    }

    //
    // internals
    //

    Status BLTreeCollectionCatalogEntry::getMetadata(
        OperationContext* ctx,
        BLTreeCollectionCatalogEntry::Metadata* metadata ) const
    {
        string result;
        Status status = _engine->get( _metadataKey, &result );
        if (!status.isOK()) return status;
        metadata->parse( BSONObj( result.c_str() ) );
        return Status::OK();
    }

    Status BLTreeCollectionCatalogEntry::putMetadata(
        const Metadata& in )
    {
        return _engine->put( _metadataKey, in.toBSON() );
    }

    Status BLTreeCollectionCatalogEntry::dropMetadata()
    {
        return _engine->remove( _metadataKey );
    }

}
