//@file buffer_mgr_test.cpp
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

#include "buffer_mgr.h"
#include "common.h"
#include "logger.h"

#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <string>
#include <string.h>
    
using namespace std;
using namespace mongo;

int main( int argc, char* argv[] ) {

    ostream* streamMain = new std::ostringstream();
    std::vector< pair< std::string, std::ostream* > > v;
    v.push_back( pair< std::string, std::ostream* >( "main", streamMain ) );
    Logger::init( v );

    const char* fname = argv[1];
    uint pageBits = strtoul( argv[2], NULL, 10 );
    uint pageSize = (1<<pageBits);

    cout << "fname = " << fname << endl;
    cout << "pageBits = " << pageBits << endl;
    cout << "pageSize = " << pageSize << endl;

    Page* page = (Page*) malloc( pageSize );

    int fd = open( fname, O_RDWR | O_CREAT, 0666 );

    uint pageId = 7;
    size_t nread = pread( fd, page, pageSize, pageId << pageBits );
    if (nread < pageSize) {
        __OSS__( "readPage(" << pageId << ") error: " << strerror(errno) );
        Logger::logError( "main", __ss__, __LOC__ );
        return 1;
    }
    cout << *page << endl;

    uint poolSize = 8192;
    uint segBits  = 5;
    uint hashSize = poolSize / 8;

    BufferMgr* mgr = BufferMgr::create( argv[1], BLT_rw, pageBits, poolSize, segBits, hashSize );

    mgr->latchAudit( "main" );

    const char* keys[] = {
        "g6tyz6qx0tlagmqfs5sj",
        "o70tl8tqihwgg04d",
        "74gjk2b0o8xmjh0h7j8nipw4b2",
        "hdf3b8p0kihawhdoq1edz3csb6e5o5",
        "vpu45tkwjs40urj76asqfl",
        "y252zlfdmudhrdstmq3srk2",
        "x2oe7fdw7p4dg8wta0g63eqvvke",
        "amnyuhisn1ulg44n4qm4g71pdyreov",
        "onps6x05ar51e1v6wrz5exetg2akwb",
        "aoit24zylxas12ty" };

    for (uint i=0; i<10; ++i) {
        PageSet* set = new PageSet();

        const char* key = keys[i];
        uint keylen = strlen( key );

        if (mgr->loadPage( set, (const uchar *)key, keylen, 0, LockRead, "main" )) {
            __OSS__( "return code: '" << mgr->decodeLastErr() << "' for key '" << key << '\'' );
            Logger::logError( "main", __ss__, __LOC__ );
         }

        int slot = mgr->findSlot( set, (const uchar *)key, keylen, "main" );
        delete set;
    }

}

