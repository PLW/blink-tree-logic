// bltree_database_catalog_entry.cpp

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

#include "mongo/db/storage/bltree/bltree_database_catalog_entry.h"

#include <bltreedb/options.h>

#include "mongo/db/jsobj.h"
#include "mongo/db/storage/bltree/bltree_collection_catalog_entry.h"
#include "mongo/db/storage/bltree/bltree_engine.h"
#include "mongo/db/storage/bltree/bltree_record_store.h"
#include "mongo/util/log.h"

namespace mongo {

    BLTreeDatabaseCatalogEntry::BLTreeDatabaseCatalogEntry(
        BLTreeEngine* engine,
        const StringData& dbname )
    :
        DatabaseCatalogEntry( dbname ),
        _engine( engine )
    {
    }

    bool BLTreeDatabaseCatalogEntry::exists() const
    {
        return !isEmpty();
    }

    bool BLTreeDatabaseCatalogEntry::isEmpty() const
    {
        // TODO(XXX): make this fast
        std::list<std::string> lst;
        getCollectionNamespaces( &lst );
        return lst.empty();
    }

    void BLTreeDatabaseCatalogEntry::appendExtraStats(
        OperationContext* ctx,
        BSONObjBuilder* out,
        double scale ) const
    {
        // put some useful stats here
        out->append( "note", "BLTreeDatabaseCatalogEntry should put some database level stats in" );
    }

    bool BLTreeDatabaseCatalogEntry::currentFilesCompatible(
        OperationContext* ctx ) const
    {
        error() << "BLTreeDatabaseCatalogEntry::currentFilesCompatible not done";
        return true;
    }

    void BLTreeDatabaseCatalogEntry::getCollectionNamespaces(
        std::list<std::string>* out ) const
    {
        _engine->getCollectionNamespaces( name(), out );
    }

    CollectionCatalogEntry* BLTreeDatabaseCatalogEntry::getCollectionCatalogEntry(
        OperationContext* ctx,
        const StringData& ns ) const
    {
        BLTreeEngine::Entry* entry = _engine->getEntry( ns );
        if ( !entry ) return NULL;
        return entry->collectionEntry.get();
    }

    RecordStore* BLTreeDatabaseCatalogEntry::getRecordStore(
        OperationContext* ctx,
        const StringData& ns )
    {
        BLTreeEngine::Entry* entry = _engine->getEntry( ns );
        if ( !entry ) return NULL;
        return entry->recordStore.get();
    }

    Status BLTreeDatabaseCatalogEntry::createCollection(
        OperationContext* ctx,
        const StringData& ns,
        const CollectionOptions& options,
        bool allocateDefaultSpace )
    {
        return _engine->createCollection( ctx, ns, options );
    }

    Status BLTreeDatabaseCatalogEntry::renameCollection(
        OperationContext* ctx,
        const StringData& fromNS,
        const StringData& toNS,
        bool stayTemp )
    {
        return Status( ErrorCodes::InternalError,
                       "BLTreeDatabaseCatalogEntry doesn't support rename yet" );
    }

    Status BLTreeDatabaseCatalogEntry::dropCollection(
        OperationContext* ctx,
        const StringData& ns )
    {
        return _engine->dropCollection( ctx, ns );
    }
}
