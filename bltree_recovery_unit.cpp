// bltree_recovery_unit.cpp

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

#include "mongo/db/storage/bltree/bltree_recovery_unit.h"
#include "mongo/util/log.h"

namespace mongo {

    BLTreeRecoveryUnit::BLTreeRecoveryUnit( BLTree* db, bool defaultCommit )
    : _db( db ), _defaultCommit( defaultCommit ), _depth( 0 ) {}

    BLTreeRecoveryUnit::~BLTreeRecoveryUnit() {
        if (_defaultCommit) commitUnitOfWork();
    }

    /**
    *  Recovery unit Semantics:
    *
    *  begin() and end() mark the begining and end of a unit of work. Each call
    *  to begin() must be matched with exactly one call to end(). commit() can be
    *  called any number of times between begin() and end() but must not be
    *  called outside.  When end() is called, all changes since the last commit()
    *  (if any) will be rolled back.
    *
    *  If UnitsOfWork nest, (ie) begin() is called twice before a call to end()
    *  the prior paragraph describes the behavior of the outermost UnitOfWork.
    *  Inner UnitsOfWork neither commit nor rollback on their own but rely on
    *  the outermost to do it.  If an inner UnitOfWork commits any changes, it
    *  is illegal for an outer unit to rollback. If an inner UnitOfWork
    *  rollsback any changes, it is illegal for an outer UnitOfWork to do
    *  anything other than rollback.
    *
    *  The goal is not to fully support nested transaction, instead we want to
    *  allow delaying commit on a unit if it is part of a larger atomic unit.
    *
    *  RecoveryUnit takes ownership of its changes. The commitUnitOfWork()
    *  method calls the commit() method of each registered change in order of
    *  registration. The endUnitOfWork() method calls the rollback() method
    *  of each registered Change in reverse order of registration. Either
    *  will unregister and delete the changes.
    *
    *  The registerChange() method may only be called when a WriteUnitOfWork
    *  is active, and may not be called during commit or rollback.
    */
    void BLTreeRecoveryUnit::registerChange( Change* change) {
        change->setDepth( _depth );
        _changev.push_back( change );
    }

    void BLTreeRecoveryUnit::beginUnitOfWork() {
        ++_depth;
    }

    void BLTreeRecoveryUnit::commitUnitOfWork() {
        std::vector< Change* >::const_iterator it;
        for (it = _changev.begin(); it!=_changev.end() ++it) {
            BLTreeChange change = dynamic_cast<BLTreeChange*>( *it );
            uassert( -1, NULL!=change );
            if (0==_depth) {
                uassert( -1, !change->rolledBack() );
                change->commit();
                delete change;
            }
            else if (_depth == change.getDepth()) {
                change->setCommit();
            }
        }
        changev.clear();
    }

    void BLTreeRecoveryUnit::endUnitOfWork() {
        --_depth;
        std::vector< BLTreeChange* >::const_iterator rit;
        for (it = _changev.rbegin(); rit!=_changev.rend() ++rit) {
            BLTreeChange change = dynamic_cast<BLTreeChange*>( *it );
            uassert( -1, NULL!=change );
            if (0==_depth) {
                uassert( -1, !change.committed() );
                change->rollback();
                delete change;
            }
            else if (_depth == change->getDepth()) {
                change->setRollback();
            }
        }
        changev.clear();
    }

    bool BLTreeRecoveryUnit::commitIfNeeded( bool force ) {
        commitUnitOfWork();
        return true;
    }

    bool BLTreeRecoveryUnit::awaitCommit() {
        return true;
    }

    void* BLTreeRecoveryUnit::writingPtr( void* data, size_t len) {
        return data;
    }

    void BLTreeRecoveryUnit::syncDataAndTruncateJournal() {
        return;
    }

}  // namespace mongo

