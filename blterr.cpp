//@file blterr.cpp
/*
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

#ifndef STANDALONE
#include "mongo/db/storage/bltree/blterr.h"
#else
#include "blterr.h"
#endif

namespace mongo {

    const char* bltstrerror( int err ) {
        switch (err) {
        case BLTERR_ok: return "ok";
        case BLTERR_notfound: return "not-found error";
        case BLTERR_struct: return "struct error";
        case BLTERR_ovflw: return "overflow error";
        case BLTERR_read: return "read error";
        case BLTERR_lock: return "lock error";
        case BLTERR_hash: return "hash error";
        case BLTERR_kill: return "kill error";
        case BLTERR_map: return "mmap error";
        case BLTERR_write: return "write error";
        case BLTERR_eof: return "eof error";
        default: return "!!internal problem: unrecognized error code";
        }
    }

}   // namespace mongo
