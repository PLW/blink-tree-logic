// bltree_record_store.cpp

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

#include "mongo/db/operation_context.h"
#include "mongo/db/storage/bltree/bltree_record_store.h"
#include "mongo/db/storage/bltree/bltree_recovery_unit.h"

#include <bltree/comparator.h>
#include <bltree/db.h>
#include <bltree/options.h>
#include <bltree/slice.h>

#include "mongo/util/log.h"

namespace mongo {

    BLTreeRecordStore::BLTreeRecordStore(
        const StringData& ns,
        bltree::DB* db, // not owned here
        bltree::ColumnFamilyHandle* columnFamily,
        bltree::ColumnFamilyHandle* metadataColumnFamily,
        bool isCapped,
        int64_t cappedMaxSize,
        int64_t cappedMaxDocs,
        CappedDocumentDeleteCallback* cappedDeleteCallback )
    :
        RecordStore( ns ),
        _db( db ),
        _columnFamily( columnFamily ),
        _metadataColumnFamily( metadataColumnFamily ),
        _isCapped( isCapped ),
        _cappedMaxSize( cappedMaxSize ),
        _cappedMaxDocs( cappedMaxDocs ),
        _cappedDeleteCallback( cappedDeleteCallback ),
        _dataSizeKey( ns.toString() + "-dataSize" ),
        _numRecordsKey( ns.toString() + "-numRecords" )
    {
        invariant( _db );
        invariant( _columnFamily );
        invariant( _metadataColumnFamily );
        invariant( _columnFamily != _metadataColumnFamily );

        if (_isCapped) {
            invariant(_cappedMaxSize > 0);
            invariant(_cappedMaxDocs == -1 || _cappedMaxDocs > 0);
        }
        else {
            invariant(_cappedMaxSize == -1);
            invariant(_cappedMaxDocs == -1);
        }

        // Get next id
        boost::scoped_ptr<bltree::Iterator> iter(
            db->NewIterator( _readOptions(), columnFamily ) );

        iter->SeekToLast();
        if (iter->Valid()) {
            bltree::Slice lastSlice = iter->key();
            DiskLoc lastLoc = _makeDiskLoc( lastSlice );
            _nextIdNum.store( lastLoc.getOfs() + ( uint64_t( lastLoc.a() ) << 32 ) + 1) ;
        }
        else {
            // Need to start at 1 so we are always higher than minDiskLoc
            _nextIdNum.store( 1 );
        }

        // load metadata
        std::string value;
        bool metadataPresent = true;
        if (!_db->Get( _readOptions(),
                       _metadataColumnFamily,
                       bltree::Slice( _numRecordsKey ),
                       &value ).ok()) {
            _numRecords = 0;
            metadataPresent = false;
        }
        else {
            memcpy( &_numRecords, value.data(), sizeof( _numRecords ));
        }

        if (!_db->Get( _readOptions(),
                       _metadataColumnFamily,
                       bltree::Slice( _dataSizeKey ),
                       &value ).ok()) {
            _dataSize = 0;
            invariant(!metadataPresent);
        }
        else {
            memcpy( &_dataSize, value.data(), sizeof( _dataSize ));
            invariant( _dataSize >= 0 );
        }
    }

    int64_t BLTreeRecordStore::storageSize(
        OperationContext* ctx,
        BSONObjBuilder* extraInfo,
        int infoLevel ) const
    {
        uint64_t storageSize;
        bltree::Range wholeRange( _makeKey( minDiskLoc ), _makeKey( maxDiskLoc ) );
        _db->GetApproximateSizes( _columnFamily, &wholeRange, 1, &storageSize);
        return static_cast<int64_t>( storageSize );
    }

    RecordData BLTreeRecordStore::dataFor(
        OperationContext* ctx,
        const DiskLoc& loc) const
    {
        std::string value;
        bltree::Status status = _db->Get( _readOptions(), _columnFamily, _makeKey( loc ), &value );
        if ( !status.ok() ) {
            log() << "bltree Get failed, blowing up: " << status.ToString();
            invariant( false );
        }
        boost::shared_array<char> data( new char[value.size()] );
        memcpy( data.get(), value.data(), value.size() );

        return RecordData( data.get(), value.size(), data );
    }

    void BLTreeRecordStore::deleteRecord(
        OperationContext* ctx,
        const DiskLoc& dl )
    {
        BLTreeRecoveryUnit* ru = _getRecoveryUnit( ctx );
        std::string oldValue;
        _db->Get( _readOptions( ctx ), _columnFamily, _makeKey( dl ), &oldValue );
        int oldLength = oldValue.size();
        ru->writeBatch()->Delete( _columnFamily, _makeKey( dl ) );
        _changeNumRecords(ctx, false);
        _increaseDataSize(ctx, -oldLength);
    }

    bool BLTreeRecordStore::cappedAndNeedDelete() const
    {
        if (!_isCapped) return false;
        if (_dataSize > _cappedMaxSize) return true;
        if ((_cappedMaxDocs != -1) && (_numRecords > _cappedMaxDocs)) return true;
        return false;
    }

    void BLTreeRecordStore::cappedDeleteAsNeeded(
        OperationContext* ctx)
    {
        if (!cappedAndNeedDelete()) return;

        // This persistent iterator is necessary since you can't read your own writes
        boost::scoped_ptr<bltree::Iterator> iter( _db->NewIterator( _readOptions( ctx ),
                                                                     _columnFamily ) );
        iter->SeekToFirst();

        // XXX TODO there is a bug here where if the size of the write batch exceeds the cap size
        // then iter will not be valid and it will crash. To fix this we need the ability to
        // query the write batch, and delete the oldest record in the write batch until the
        // size of the write batch is less than the cap

        // XXX PROBLEMS
        // 2 threads could delete the same document
        // multiple inserts using the same snapshot will delete the same document
        while ( cappedAndNeedDelete() && iter->Valid() ) {
            invariant(_numRecords > 0);
            bltree::Slice slice = iter->key();
            DiskLoc oldest = _makeDiskLoc( slice );
            if ( _cappedDeleteCallback ) {
                uassertStatusOK(_cappedDeleteCallback->aboutToDeleteCapped(ctx, oldest));
            }
            deleteRecord(ctx, oldest);
            iter->Next();
        }
    }

    StatusWith<DiskLoc> BLTreeRecordStore::insertRecord(
        OperationContext* ctx,
        const char* data,
        int len,
        bool enforceQuota )
    {
        if ( _isCapped && len > _cappedMaxSize ) {
            return StatusWith<DiskLoc>( ErrorCodes::BadValue,
                                       "object to insert exceeds cappedMaxSize" );
        }

        BLTreeRecoveryUnit* ru = _getRecoveryUnit( ctx );
        DiskLoc loc = _nextId();
        ru->writeBatch()->Put( _columnFamily, _makeKey( loc ), bltree::Slice( data, len ) );
        _changeNumRecords( ctx, true );
        _increaseDataSize( ctx, len );
        cappedDeleteAsNeeded(ctx);
        return StatusWith<DiskLoc>( loc );
    }

    StatusWith<DiskLoc> BLTreeRecordStore::insertRecord(
        OperationContext* ctx,
        const DocWriter* doc,
        bool enforceQuota )
    {
        const int len = doc->documentSize();
        boost::scoped_array<char> buf( new char[len] );
        doc->writeDocument( buf.get() );
        return insertRecord( ctx, buf.get(), len, enforceQuota );
    }

    StatusWith<DiskLoc> BLTreeRecordStore::updateRecord(
        OperationContext* ctx,
        const DiskLoc& loc,
        const char* data,
        int len,
        bool enforceQuota,
        UpdateMoveNotifier* notifier )
    {
        BLTreeRecoveryUnit* ru = _getRecoveryUnit( ctx );

        std::string old_value;
        // XXX Be sure to also first query the write batch once Facebook implements that
        bltree::Status status =
            _db->Get( _readOptions( ctx ), _columnFamily, _makeKey( loc ), &old_value );
        if ( !status.ok() ) {
            return StatusWith<DiskLoc>( ErrorCodes::InternalError, status.ToString() );
        }

        int old_length = old_value.size();
        ru->writeBatch()->Put( _columnFamily, _makeKey( loc ), bltree::Slice( data, len ) );
        _increaseDataSize(ctx, len - old_length);
        cappedDeleteAsNeeded(ctx);
        return StatusWith<DiskLoc>( loc );
    }

    Status BLTreeRecordStore::updateWithDamages(
        OperationContext* ctx,
        const DiskLoc& loc,
        const char* damangeSource,
        const mutablebson::DamageVector& damages )
    {
        BLTreeRecoveryUnit* ru = _getRecoveryUnit( ctx );

        bltree::Slice key = _makeKey( loc );

        // get original value
        std::string value;
        bltree::Status status;
        status = _db->Get( _readOptions( ctx ), _columnFamily, key, &value );

        if ( !status.ok() ) {
            if ( status.IsNotFound() )
                return Status( ErrorCodes::InternalError, "doc not found for in-place update" );

            log() << "bltree Get failed, blowing up: " << status.ToString();
            invariant( false );
        }

        // apply changes to our copy
        for( size_t i = 0; i < damages.size(); i++ ) {
            mutablebson::DamageEvent event = damages[i];
            const char* sourcePtr = damangeSource + event.sourceOffset;

            invariant( event.targetOffset + event.size < value.length() );
            value.replace( event.targetOffset, event.size, sourcePtr, event.size );
        }

        ru->writeBatch()->Put( _columnFamily, key, value );
        return Status::OK();
    }

    RecordIterator* BLTreeRecordStore::getIterator(
        OperationContext* ctx,
        const DiskLoc& start,
        bool tailable,
        const CollectionScanParams::Direction& dir) const
    {
        invariant( !tailable );
        return new Iterator( ctx, this, dir, start );
    }


    RecordIterator* BLTreeRecordStore::getIteratorForRepair(
        OperationContext* ctx ) const
    {
        return getIterator( ctx );
    }

    std::vector<RecordIterator*> BLTreeRecordStore::getManyIterators(
        OperationContext* ctx ) const
    {
        // AFB: any way to get the split point keys for the bottom layer of the lsm tree?
        std::vector<RecordIterator*> iterators;
        iterators.push_back( getIterator( ctx ) );
        return iterators;
    }

    Status BLTreeRecordStore::truncate(
        OperationContext* ctx )
    {
        // XXX once we have readable WriteBatch, also delete outstanding writes to
        // this collection in the WriteBatch
        //AFB add Clear(ColumnFamilyHandle*)
        boost::scoped_ptr<RecordIterator> iter( getIterator( ctx ) );
        while( !iter->isEOF() ) {
            DiskLoc loc = iter->getNext();
            deleteRecord( ctx, loc );
        }

        return Status::OK();
    }

    Status BLTreeRecordStore::compact(
        OperationContext* ctx,
        RecordStoreCompactAdaptor* adaptor,
        const CompactOptions* options,
        CompactStats* stats )
    {
        bltree::Status status = _db->CompactRange( _columnFamily, NULL, NULL );
        if ( status.ok() )
            return Status::OK();
        else
            return Status( ErrorCodes::InternalError, status.ToString() );
    }

    Status BLTreeRecordStore::validate(
        OperationContext* ctx,
        bool full,
        bool scanData,
        ValidateAdaptor* adaptor,
        ValidateResults* results,
        BSONObjBuilder* output ) const
    {
        // TODO validate that _numRecords and _dataSize are correct in scanData mode
        if ( scanData ) {
            bool invalidObject = false;
            size_t numRecords = 0;
            boost::scoped_ptr<RecordIterator> iter( getIterator( ctx ) );
            while( !iter->isEOF() ) {
                numRecords++;
                RecordData data = dataFor( ctx, iter->curr() );
                size_t dataSize;
                const Status status = adaptor->validate( data, &dataSize );
                if (!status.isOK()) {
                    results->valid = false;
                    if ( invalidObject ) {
                        results->errors.push_back("invalid object detected (see logs)");
                    }
                    invalidObject = true;
                    log() << "Invalid object detected in " << _ns << ": " << status.reason();
                }
                iter->getNext();
            }
            output->appendNumber("nrecords", numRecords);
        }
        else
            output->appendNumber("nrecords", _numRecords);

        return Status::OK();
    }

    void BLTreeRecordStore::appendCustomStats(
        OperationContext* ctx,  
        BSONObjBuilder* result,
        double scale ) const
    {
        string statsString;
        bool valid = _db->GetProperty( _columnFamily, "bltree.stats", &statsString );
        invariant( valid );
        result->append( "stats", statsString );
    }


    // AFB: is there a way to force column families to be cached in bltree?
    Status BLTreeRecordStore::touch(
        OperationContext* ctx,
        BSONObjBuilder* output ) const
    {
        return Status::OK();
    }

    Status BLTreeRecordStore::setCustomOption(
        OperationContext* ctx,
        const BSONElement& option,
        BSONObjBuilder* info )
    {
        string optionName = option.fieldName();
        if ( optionName == "usePowerOf2Sizes" ) {
            return Status::OK();
        }
        return Status( ErrorCodes::BadValue, "Invalid option: " + optionName );
    }

    namespace {
        class BLTreeCollectionComparator : public bltree::Comparator {
            public:
                BLTreeCollectionComparator() { }
                virtual ~BLTreeCollectionComparator() { }

                virtual int Compare( const bltree::Slice& a, const bltree::Slice& b ) const {
                    DiskLoc lhs = reinterpret_cast<const DiskLoc*>( a.data() )[0];
                    DiskLoc rhs = reinterpret_cast<const DiskLoc*>( b.data() )[0];
                    return lhs.compare( rhs );
                }

                virtual const char* Name() const {
                    return "mongodb.BLTreeCollectionComparator";
                }

                /**
                 * From the BLTreeDB comments: "an implementation of this method that does nothing is
                 * correct"
                 */
                virtual void FindShortestSeparator( std::string* start,
                        const bltree::Slice& limit) const { }

                /**
                 * From the BLTreeDB comments: "an implementation of this method that does nothing is
                 * correct.
                 */
                virtual void FindShortSuccessor(std::string* key) const { }
        };
    }

    bltree::Comparator* BLTreeRecordStore::newBLTreeCollectionComparator() {
        return new BLTreeCollectionComparator();
    }

    void BLTreeRecordStore::temp_cappedTruncateAfter(
        OperationContext* ctx,
        DiskLoc end,
        bool inclusive )
    {
        boost::scoped_ptr<RecordIterator> iter(
                getIterator( ctx, maxDiskLoc, false, CollectionScanParams::BACKWARD ) );

        while( !iter->isEOF() ) {
            WriteUnitOfWork wu( ctx );
            DiskLoc loc = iter->getNext();
            if ( loc < end || ( !inclusive && loc == end))
                return;

            deleteRecord( ctx, loc );
            wu.commit();
        }
    }

    void BLTreeRecordStore::dropRsMetaData(
        OperationContext* opCtx )
    {
        BLTreeRecoveryUnit* ru = _getRecoveryUnit( opCtx );
        boost::mutex::scoped_lock dataSizeLk( _dataSizeLock );
        ru->writeBatch()->Delete( _metadataColumnFamily, _dataSizeKey );
        boost::mutex::scoped_lock numRecordsLk( _numRecordsLock );
        ru->writeBatch()->Delete( _metadataColumnFamily, _numRecordsKey );
    }

    bltree::ReadOptions BLTreeRecordStore::_readOptions(
        OperationContext* opCtx ) const
    {
        bltree::ReadOptions options;
        if ( opCtx ) {
            options.snapshot = _getRecoveryUnit( opCtx )->snapshot();
        }
        return options;
    }

    DiskLoc BLTreeRecordStore::_nextId() {
        const uint64_t myId = _nextIdNum.fetchAndAdd(1);
        int a = myId >> 32;
        // This masks the lowest 4 bytes of myId
        int ofs = myId & 0x00000000FFFFFFFF;
        DiskLoc loc( a, ofs );
        return loc;
    }

    bltree::Slice BLTreeRecordStore::_makeKey(
        const DiskLoc& loc )
    {
        return bltree::Slice( reinterpret_cast<const char*>( &loc ), sizeof( loc ) );
    }

    BLTreeRecoveryUnit* BLTreeRecordStore::_getRecoveryUnit(
        OperationContext* opCtx )
    {
        return dynamic_cast<BLTreeRecoveryUnit*>( opCtx->recoveryUnit() );
    }

    DiskLoc BLTreeRecordStore::_makeDiskLoc(
        const bltree::Slice& slice )
    {
        return reinterpret_cast<const DiskLoc*>( slice.data() )[0];
    }

    // XXX make sure these work with rollbacks (I don't think they will)
    void BLTreeRecordStore::_changeNumRecords(
        OperationContext* ctx,
        bool insert )
    {
        boost::mutex::scoped_lock lk( _numRecordsLock );

        if ( insert ) _numRecords++; else _numRecords--;

        BLTreeRecoveryUnit* ru = _getRecoveryUnit( ctx );
        const char* nr_ptr = reinterpret_cast<char*>( &_numRecords );
        ru->writeBatch()->Put( _metadataColumnFamily,
                               bltree::Slice( _numRecordsKey ),
                               bltree::Slice( nr_ptr, sizeof(long long) ) );
    }


    void BLTreeRecordStore::_increaseDataSize(
        OperationContext* ctx,
        int amount )
    {
        boost::mutex::scoped_lock lk( _dataSizeLock );

        _dataSize += amount;
        BLTreeRecoveryUnit* ru = _getRecoveryUnit( ctx );
        const char* ds_ptr = reinterpret_cast<char*>( &_dataSize );

        ru->writeBatch()->Put( _metadataColumnFamily,
                               bltree::Slice( _dataSizeKey ),
                               bltree::Slice( ds_ptr, sizeof(long long) ) );
    }

    // --------

    BLTreeRecordStore::Iterator::Iterator(
        OperationContext* ctx,
        const BLTreeRecordStore* rs,
        const CollectionScanParams::Direction& dir,
        const DiskLoc& start )
    :
        _ctx( ctx ),
        _rs( rs ),
        _dir( dir ),
        _reseekKeyValid( false ),
        _iterator( _rs->_db->NewIterator( rs->_readOptions(), rs->_columnFamily ) )
    {
        if (start.isNull()) {
            if (_forward()) _iterator->SeekToFirst(); else _iterator->SeekToLast();
        else {
            _iterator->Seek( rs->_makeKey( start ) );

            if ( !_forward() && !_iterator->Valid() ) {
                _iterator->SeekToLast();
            }
            else if ( !_forward() && _iterator->Valid() &&
                      _makeDiskLoc( _iterator->key() ) != start ) {
                _iterator->Prev();
            }
        }

        _checkStatus();
    }

    void BLTreeRecordStore::Iterator::_checkStatus()
    {
        if ( !_iterator->status().ok() ) {
            log() << "BLTree Iterator Error: " << _iterator->status().ToString();
        }
        invariant( _iterator->status().ok() );
    }

    bool BLTreeRecordStore::Iterator::isEOF()
    {
        return !_iterator || !_iterator->Valid();
    }

    DiskLoc BLTreeRecordStore::Iterator::curr()
    {
        if ( !_iterator->Valid() ) return DiskLoc();
        bltree::Slice slice = _iterator->key();
        return _makeDiskLoc( slice );
    }

    DiskLoc BLTreeRecordStore::Iterator::getNext()
    {
        if ( !_iterator->Valid() ) return DiskLoc();
        DiskLoc toReturn = curr();
        if ( _forward() ) _iterator->Next(); else _iterator->Prev();
        return toReturn;
    }

    void BLTreeRecordStore::Iterator::invalidate(
        const DiskLoc& dl )
    {
        _iterator.reset( NULL );
    }

    void BLTreeRecordStore::Iterator::saveState()
    {
        if ( !_iterator ) return;
        if ( _iterator->Valid() ) {
            _reseekKey = _iterator->key().ToString();
            _reseekKeyValid = true;
        }
    }

    bool BLTreeRecordStore::Iterator::restoreState()
    {
        if ( !_reseekKeyValid ) {
          _iterator.reset( NULL );
          return true;
        }
        _iterator.reset( _rs->_db->NewIterator( _rs->_readOptions(), _rs->_columnFamily ) );
        _checkStatus();
        _iterator->Seek( _reseekKey );
        _checkStatus();
        _reseekKeyValid = false;
        return true;
    }

    RecordData BLTreeRecordStore::Iterator::dataFor(
        const DiskLoc& loc ) const
    {
        return _rs->dataFor( _ctx, loc );
    }

    bool BLTreeRecordStore::Iterator::_forward() const
    {
        return _dir == CollectionScanParams::FORWARD;
    }
}
