//@file bltree_engine.h

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

#include "mongo/base/status.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/storage/bltree/bltree.h"

#include <string>
#include <vector>

namespace mongo {

    class BLTreeEngine : public StorageEngine {

        MONGO_DISALLOW_COPYING( BLTreeEngine );

    public:
        typedef std::string               KeyString;
        typedef std::string               Value;
        typedef StringData                DBName;
        typedef std::vector<std::string>  DBNameList;

        BLTreeEngine( const std::string& path );
        virtual ~BLTreeEngine();

        virtual void           listDatabases   ( DBNameList* ) const;
        virtual int            flushAllFiles   ( bool sync );
        virtual RecoveryUnit*  newRecoveryUnit ( OperationContext* );
        virtual void           cleanShutdown   ( OperationContext* );
        virtual Status         closeDatabase   ( OperationContext*, const DBName& );
        virtual Status         dropDatabase    ( OperationContext*, const DBName& );
        virtual Status         repairDatabase  ( OperationContext*, const DBName&,
                                                   bool preserveClonedFilesOnFailure = false,
                                                   bool backupOriginalFiles = false );

        virtual DatabaseCatalogEntry*   getDatabaseCatalogEntry( OperationContext*, const DBName& );

        //
        // BLTree specific
        //
        Status get    ( const KeyString&, Value& );
        Status put    ( const KeyString&, const BSONObj& );
        Status remove ( const KeyString& );

        struct Entry {
            boost::scoped_ptr<BLTreeCollectionCatalogEntry> collectionEntry;
            boost::scoped_ptr<BLTreeRecordStore> recordStore;
        };

        Entry* getEntry( const StringData& ns );
        const Entry* getEntry( const StringData& ns ) const;

    private:
        Status _dropCollection_inlock( OperationContext*, const StringData& ns );

        boost::scoped_ptr<BLTree> _blt;
        typedef StringMap< boost::shared_ptr<Entry> > EntryMap;
        EntryMap _entryMap;

        mutable boost::mutex _dbCatalogMapMutex;
        mutable boost::mutex _entryMapMutex;

        std::string _path;

        //std::map< std::string, BLTree* > _idxMap;

    };

    struct BLTreeConfig {
        std::string _metaName;      // metadata index file name
        std::string _openMode;      // file open mode "ro" or "rw"
        uint _pageBits;             // lg( page size )
        uint _poolSize;             // number of segments
        uint _segBits;              // lg( segment size in pages )
        uint _hashSize;             // segment hash table size

   };

}
