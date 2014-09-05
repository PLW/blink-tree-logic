// bltree_sorted_data_impl_test.cpp

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

#include <memory>

#include <boost/shared_ptr.hpp>
#include <boost/filesystem/operations.hpp>

#include <bltreedb/comparator.h>
#include <bltreedb/db.h>
#include <bltreedb/options.h>
#include <bltreedb/slice.h>

#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/bltree/bltree_engine.h"
#include "mongo/db/storage/bltree/bltree_sorted_data_impl.h"
#include "mongo/db/storage/bltree/bltree_record_store.h"
#include "mongo/db/storage/bltree/bltree_recovery_unit.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"

using namespace mongo;

namespace mongo {

    class MyOperationContext : public OperationContextNoop {
    public:
        MyOperationContext( bltreedb::DB* db )
            : OperationContextNoop( new BLTreeRecoveryUnit( db, false ) ) {
        }
    };

    // to be used in testing
    static std::unique_ptr<bltreedb::Comparator> _bltreeComparator(
            BLTreeSortedDataImpl::newBLTreeComparator( Ordering::make( BSON( "a" << 1 ) ) ) );

    string _bltreeSortedDataTestDir = "mongo-bltree-test";

    bltreedb::DB* getDB( string path ) {
        boost::filesystem::remove_all( path );

        bltreedb::Options options = BLTreeEngine::dbOptions();

        // open DB
        bltreedb::DB* db;
        bltreedb::Status s = bltreedb::DB::Open(options, path, &db);
        ASSERT(s.ok());

        return db;
    }

    const Ordering dummyOrdering = Ordering::make( BSONObj() );

    TEST( BLTreeRecordStoreTest, BrainDead ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            BSONObj key = BSON( "" << 1 );
            DiskLoc loc( 5, 16 );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );
                    ASSERT( !sortedData.unindex( &opCtx, key, loc ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );
                    Status res = sortedData.insert( &opCtx, key, loc, true );
                    ASSERT_OK( res );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );
                    ASSERT( sortedData.unindex( &opCtx, key, loc ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );
                    sortedData.unindex( &opCtx, key, loc );
                    uow.commit();
                }
            }

        }
    }

    TEST( BLTreeRecordStoreTest, Locate1 ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            BSONObj key = BSON( "" << 1 );
            DiskLoc loc( 5, 16 );

            {

                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, 1 ) );
                ASSERT( !cursor->locate( key, loc ) );
            }

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );
                    Status res = sortedData.insert( &opCtx, key, loc, true );
                    ASSERT_OK( res );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, 1 ) );
                ASSERT( cursor->locate( key, loc ) );
                ASSERT_EQUALS( key, cursor->getKey() );
                ASSERT_EQUALS( loc, cursor->getDiskLoc() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, Locate2 ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, 1 ) );
                ASSERT( cursor->locate( BSON( "a" << 2 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );

                cursor->advance();
                ASSERT_EQUALS( BSON( "" << 3 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,3), cursor->getDiskLoc() );

                cursor->advance();
                ASSERT( cursor->isEOF() );
            }
        }
    }

    boost::shared_ptr<bltreedb::ColumnFamilyHandle> makeColumnFamily( bltreedb::DB* db ) {
        bltreedb::ColumnFamilyOptions options;
        options.comparator = _bltreeComparator.get();

        bltreedb::ColumnFamilyHandle* cfh;
        bltreedb::Status s = db->CreateColumnFamily( options, "simpleColumnFamily", &cfh );
        ASSERT( s.ok() );

        return boost::shared_ptr<bltreedb::ColumnFamilyHandle>( cfh );
    }

    TEST( BLTreeRecordStoreTest, LocateInexact ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            boost::shared_ptr<bltreedb::ColumnFamilyHandle> cfh = makeColumnFamily( db.get() );

            BLTreeSortedDataImpl sortedData( db.get(), cfh.get(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, 1 ) );
                ASSERT_FALSE( cursor->locate( BSON( "a" << 2 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 3 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,3), cursor->getDiskLoc() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, Snapshots ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );

                // get a cursor
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, 1 ) );

                // insert some more stuff
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }

                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );
                cursor->advance();

                // make sure that the cursor can't "see" anything added after it was created.
                ASSERT( cursor-> isEOF() );
                ASSERT_FALSE( cursor->locate( BSON( "" << 3 ), DiskLoc(1,3) ) );
                ASSERT( cursor->isEOF() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionSimple ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, 1 ) );
                ASSERT( cursor->locate( BSON( "a" << 1 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 1 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,1), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                // restore position
                cursor->restorePosition();
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 1 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,1), cursor->getDiskLoc() );

                // repeat, with a different value
                ASSERT( cursor->locate( BSON( "a" << 2 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                // restore position
                cursor->restorePosition();
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionEOF ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, 1 ) );
                ASSERT( cursor->locate( BSON( "a" << 1 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 1 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,1), cursor->getDiskLoc() );

                // advance to the end
                while ( !cursor->isEOF() ) {
                    cursor->advance();
                }

                ASSERT( cursor->isEOF() );

                // save the position
                cursor->savePosition();

                // restore position, make sure we're at the end
                cursor->restorePosition();
                ASSERT( cursor->isEOF()  );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionInsert ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, 1 ) );
                ASSERT( cursor->locate( BSON( "" << 3 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 3 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,3), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                {
                    MyOperationContext opCtx( db.get() );
                    {
                        WriteUnitOfWork uow( &opCtx );
                        ASSERT_OK(
                                sortedData.insert( &opCtx, BSON( "" << 4 ), DiskLoc(1,4), true ) );
                        uow.commit();
                    }
                }

                // restore position, make sure we don't see the newly inserted value
                cursor->restorePosition();
                ASSERT( !cursor->isEOF() );
                ASSERT_EQUALS( BSON( "" << 3 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,3), cursor->getDiskLoc() );

                cursor->advance();
                ASSERT( cursor->isEOF() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionDelete2 ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, 1 ) );
                ASSERT( cursor->locate( BSON( "" << 2 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                {
                    MyOperationContext opCtx( db.get() );
                    {
                        WriteUnitOfWork uow( &opCtx );
                        ASSERT( sortedData.unindex( &opCtx, BSON( "" << 1 ), DiskLoc(1,1) ) );
                        uow.commit();
                    }
                }

                // restore position
                cursor->restorePosition();
                ASSERT( !cursor->isEOF() );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionDelete3 ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, 1 ) );
                ASSERT( cursor->locate( BSON( "" << 2 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                {
                    MyOperationContext opCtx( db.get() );
                    {
                        WriteUnitOfWork uow( &opCtx );
                        ASSERT( sortedData.unindex( &opCtx, BSON( "" << 3 ), DiskLoc(1,3) ) );
                        uow.commit();
                    }
                }

                // restore position
                cursor->restorePosition();
                ASSERT( !cursor->isEOF() );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );

                // make sure that we can still see the unindexed data, since we're working on
                // a snapshot
                cursor->advance();
                ASSERT( !cursor->isEOF() );
                ASSERT_EQUALS( BSON( "" << 3 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,3), cursor->getDiskLoc() );

                cursor->advance();
                ASSERT( cursor->isEOF() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, Locate1Reverse ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            BSONObj key = BSON( "" << 1 );
            DiskLoc loc( 5, 16 );

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, -1 ) );
                ASSERT( !cursor->locate( key, loc ) );
            }

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );
                    Status res = sortedData.insert( &opCtx, key, loc, true );
                    ASSERT_OK( res );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, -1 ) );
                ASSERT( cursor->locate( key, loc ) );
                ASSERT_EQUALS( key, cursor->getKey() );
                ASSERT_EQUALS( loc, cursor->getDiskLoc() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, LocateInexactReverse ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            boost::shared_ptr<bltreedb::ColumnFamilyHandle> cfh = makeColumnFamily( db.get() );

            BLTreeSortedDataImpl sortedData( db.get(), cfh.get(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "a" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "a" << 3 ), DiskLoc(1,1), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, -1 ) );
                ASSERT_FALSE( cursor->locate( BSON( "a" << 2 ), DiskLoc(1,1) ) );
                ASSERT_FALSE( cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 1 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,1), cursor->getDiskLoc() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionReverseSimple ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, -1 ) );
                ASSERT( cursor->locate( BSON( "a" << 1 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 1 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,1), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                // restore position
                cursor->restorePosition();
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 1 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,1), cursor->getDiskLoc() );

                // repeat, with a different value
                ASSERT( cursor->locate( BSON( "a" << 2 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                // restore position
                cursor->restorePosition();
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionEOFReverse ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 4 ), DiskLoc(1,4), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx, -1 ) );
                ASSERT_FALSE( cursor->locate( BSON( "" << 2 ), DiskLoc(1,2) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 1 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,1), cursor->getDiskLoc() );

                // advance to the end
                while ( !cursor->isEOF() ) {
                    cursor->advance();
                }

                ASSERT( cursor->isEOF() );

                // save the position
                cursor->savePosition();

                // restore position, make sure we're at the end
                cursor->restorePosition();
                ASSERT( cursor->isEOF() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionInsertReverse ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx,
                                                                                      -1 ) );
                ASSERT( cursor->locate( BSON( "" << 3 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 3 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,3), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                {
                    MyOperationContext opCtx( db.get() );
                    {
                        WriteUnitOfWork uow( &opCtx );
                        ASSERT_OK(
                                sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                        uow.commit();
                    }
                }

                // restore position, make sure we don't see the newly inserted value
                cursor->restorePosition();
                ASSERT( !cursor->isEOF() );
                ASSERT_EQUALS( BSON( "" << 3 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,3), cursor->getDiskLoc() );

                cursor->advance();
                ASSERT( !cursor->isEOF() );
                ASSERT_EQUALS( BSON( "" << 1 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,1), cursor->getDiskLoc() );

                cursor->advance();
                ASSERT( cursor->isEOF() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionDelete1Reverse ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx,
                                                                                      -1 ) );
                ASSERT( cursor->locate( BSON( "" << 3 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 3 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,3), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                {
                    MyOperationContext opCtx( db.get() );
                    {
                        WriteUnitOfWork uow( &opCtx );
                        ASSERT( sortedData.unindex( &opCtx, BSON( "" << 3 ), DiskLoc(1,3) ) );
                        uow.commit();
                    }
                }

                // restore position, make sure we still see the deleted key and value, because
                // we're using a snapshot
                cursor->restorePosition();
                ASSERT( !cursor->isEOF() );
                ASSERT_EQUALS( BSON( "" << 3 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,3), cursor->getDiskLoc() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionDelete2Reverse ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx,
                                                                                      -1 ) );
                ASSERT( cursor->locate( BSON( "" << 2 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                {
                    MyOperationContext opCtx( db.get() );
                    {
                        WriteUnitOfWork uow( &opCtx );
                        ASSERT( sortedData.unindex( &opCtx, BSON( "" << 1 ), DiskLoc(1,1) ) );
                        uow.commit();
                    }
                }

                // restore position
                cursor->restorePosition();
                ASSERT( !cursor->isEOF() );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );
            }
        }
    }

    TEST( BLTreeRecordStoreTest, SaveAndRestorePositionDelete3Reverse ) {
        unittest::TempDir td( _bltreeSortedDataTestDir );
        scoped_ptr<bltreedb::DB> db( getDB( td.path() ) );

        {
            BLTreeSortedDataImpl sortedData( db.get(), db->DefaultColumnFamily(), dummyOrdering );

            {
                MyOperationContext opCtx( db.get() );
                {
                    WriteUnitOfWork uow( &opCtx );

                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 1 ), DiskLoc(1,1), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 2 ), DiskLoc(1,2), true ) );
                    ASSERT_OK( sortedData.insert( &opCtx, BSON( "" << 3 ), DiskLoc(1,3), true ) );
                    uow.commit();
                }
            }

            {
                MyOperationContext opCtx( db.get() );
                scoped_ptr<SortedDataInterface::Cursor> cursor( sortedData.newCursor( &opCtx,
                                                                                      -1 ) );
                ASSERT( cursor->locate( BSON( "" << 2 ), DiskLoc(0,0) ) );
                ASSERT( !cursor->isEOF()  );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );

                // save the position
                cursor->savePosition();

                {
                    MyOperationContext opCtx( db.get() );
                    {
                        WriteUnitOfWork uow( &opCtx );
                        ASSERT( sortedData.unindex( &opCtx, BSON( "" << 1 ), DiskLoc(1,1) ) );
                        uow.commit();
                    }
                }

                // restore position
                cursor->restorePosition();
                ASSERT( !cursor->isEOF() );
                ASSERT_EQUALS( BSON( "" << 2 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,2), cursor->getDiskLoc() );

                // make sure that we can still see the unindexed data, since we're working on
                // a snapshot
                cursor->advance();
                ASSERT( !cursor->isEOF() );
                ASSERT_EQUALS( BSON( "" << 1 ), cursor->getKey() );
                ASSERT_EQUALS( DiskLoc(1,1), cursor->getDiskLoc() );

                cursor->advance();
                ASSERT( cursor->isEOF() );
            }
        }
    }
}
