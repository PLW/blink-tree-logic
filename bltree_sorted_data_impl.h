//@file bltree_sorted_data_impl.h

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

#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/storage/index_entry_comparison.h"

namespace bltree {
    class ColumnFamilyHandle;
    class DB;
}

namespace mongo {

    class BLTreeRecoveryUnit;

    class BLTreeSortedDataBuilderImpl : public SortedDataBuilderInterface {
    public:
        virtual Status addKey(const BSONObj& key, const DiskLoc& loc) = 0;
    };

    /**
     * BLTree implementation of the SortedDataInterface. Each index is stored as a single column
     * family. Each mapping from a BSONObj to a DiskLoc is stored as the key of a key-value pair
     * in the column family. Consequently, each value in the database is simply an empty string.
     * This is done because BLTreeDB only supports unique keys, and because BLTreeDB can take a custom
     * comparator to use when ordering keys. We use a custom comparator which orders keys based
     * first upon the BSONObj in the key, and uses the DiskLoc as a tiebreaker.
     */
    class BLTreeSortedDataImpl : public SortedDataInterface {

        MONGO_DISALLOW_COPYING( BLTreeSortedDataImpl );

    public:
        BLTreeSortedDataImpl( bltree::DB* db, bltree::ColumnFamilyHandle* cf, Ordering order );

        virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* ctx, bool dupsAllowed);

        virtual Status insert(OperationContext* ctx,
                              const BSONObj& key,
                              const DiskLoc& loc,
                              bool dupsAllowed);

        virtual bool unindex(OperationContext* ctx, const BSONObj& key, const DiskLoc& loc);
        virtual Status dupKeyCheck(OperationContext* ctx, const BSONObj& key, const DiskLoc& loc);
        virtual void fullValidate(OperationContext* ctx, long long* numKeysOut);
        virtual bool isEmpty(OperationContext* ctx);
        virtual Status touch(OperationContext* ctx) const;
        virtual Cursor* newCursor(OperationContext* ctx, int direction) const;
        virtual Status initAsEmpty(OperationContext* ctx);
        virtual long long getSpaceUsedBytes( OperationContext* ctx ) const;

        //bltree specific

        // ownership passes to caller. Bare because we need to pass the bare pointer to the
        // bltree::Options class
        static bltree::Comparator* newBLTreeComparator( const Ordering& order );

    private:
        typedef DiskLoc RecordId;

        BLTreeRecoveryUnit* _getRecoveryUnit( OperationContext* opCtx ) const;
        bltree::DB* _db; // not owned

        // Each index is stored as a single column family, so this stores the handle to the
        // relevant column family
        bltree::ColumnFamilyHandle* _columnFamily; // not owned

        // used to construct BLTreeCursors
        const Ordering _order;

        // Creates an error code message out of a key
        std::string dupKeyError(const BSONObj& key) const;
    };

} // namespace mongo
