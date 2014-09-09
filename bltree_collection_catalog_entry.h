//@file bltree_collection_catalog_entry.h

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

#pragma once

#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/storage/bson_collection_catalog_entry.h"
#include "mongo/db/storage/bltree/bufmgr.h"
#include "mongo/db/storage/bltree/bltree.h"

namespace mongo {

    class BLTreeEngine;

    class BLTreeCollectionCatalogEntry : public BSONCollectionCatalogEntry {
    public:
        typedef StringData                  IndexName;
        typedef StringData                  NamespaceName;
        typedef long long                   ExpireSeconds;
        typedef std::vector<std::string>    IndexNameVector;
        typedef DiskLoc                     IndexHead;
        typedef CollectionOptions           Options;

        BLTreeCollectionCatalogEntry( BLTreeEngine* engine, NamespaceName);
        virtual ~BLTreeCollectionCatalogEntry(){}

        virtual int       getMaxAllowedIndexes  ( ) const;
        virtual Options   getCollectionOptions  ( OperationContext* ) const;
        virtual int       getTotalIndexCount    ( OperationContext* ) const;
        virtual int       getCompletedIndexCount( OperationContext* ) const;
        virtual BSONObj   getIndexSpec          ( OperationContext*, const IndexName& ) const;
        virtual bool      isIndexMultikey       ( OperationContext*, const IndexName& ) const;
        virtual bool      setIndexIsMultikey    ( OperationContext*, const IndexName&, bool multikey = true);
        virtual IndexHead getIndexHead          ( OperationContext*, const IndexName& ) const;
        virtual void      setIndexHead          ( OperationContext*, const IndexName&, const IndexHead& );
        virtual bool      isIndexReady          ( OperationContext*, const IndexName& ) const;
        virtual Status    removeIndex           ( OperationContext*, const IndexName& );
        virtual void      indexBuildSuccess     ( OperationContext*, const IndexName& );
        virtual void      updateTTLSetting      ( OperationContext*, const IndexName&, ExpireSeconds );
        virtual void      getAllIndexes         ( OperationContext*, IndexNameVector* ) const;
        virtual Status    prepareForIndexBuild  ( OperationContext*, const IndexDescriptor* );

        const string metadataKey() { return _metaDataKey; }

    protected:
        virtual Metadata getMetadata( OperationContext* ctx ) const;

    private:
        Status getMetadata( BLTreeCollectionCatalogEntry::Metadata* ) const;
        Status putMetadata( const Metadata& )
        Status dropMetadata()

        // the underlying bltree storage
        BLTreeEngine* _engine;

        // bltree metadata key 
        const std::string _metadataKey;

        // lock which must be acquired before calling _getMetadata_inlock().
        // Protects the metadata stored in the metadata btree.
        mutable boost::mutex _mutex;
    };

}
