//@file bltindex_test.cpp
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

/*
*  This module contains derived code.   The original
*  copyright notice is as follows:
*
*    This work, including the source code, documentation
*    and related data, is placed into the public domain.
*  
*    The orginal author is Karl Malbrain (malbrain@cal.berkeley.edu)
*  
*    THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY
*    OF ANY KIND, NOT EVEN THE IMPLIED WARRANTY OF
*    MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE,
*    ASSUMES _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE
*    RESULTING FROM THE USE, MODIFICATION, OR
*    REDISTRIBUTION OF THIS SOFTWARE.
*/

#include "bltindex.h"
#include "common.h"
#include "buffer_mgr.h"
#include "logger.h"

#include <iostream>
#include <pthread.h>
#include <sstream>
#include <string>
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <vector>
    
using namespace std;
using namespace mongo;

#define __TRACE__    ' '<<__FILE__ <<':'<<__FUNCTION__<<':'<<__LINE__

double getCpuTime( int type ) {
    struct rusage used[1];
    struct timeval tv[1];

    switch (type) {
    case 0: {
        gettimeofday( tv, NULL );
        return (double)tv->tv_sec + (double)tv->tv_usec / 1000000;
    }
    case 1: {
        getrusage( RUSAGE_SELF, used );
        return (double)used->ru_utime.tv_sec + (double)used->ru_utime.tv_usec / 1000000;
    }
    case 2: {
        getrusage( RUSAGE_SELF, used );
        return (double)used->ru_stime.tv_sec + (double)used->ru_stime.tv_usec / 1000000;
    } }
    return 0;
}

typedef struct {
    char _type;
    char _idx;
    const char* _infile;
    BufferMgr* _mgr;
    const char* _thread;
} ThreadArg;

//
// thread callback
//
void* indexOp( void* rawArg ) {
    uchar key[256];
    PageSet set[1]; // set -> stack alloc

    ThreadArg* args = (ThreadArg *)rawArg;
    BufferMgr* mgr = args->_mgr;
    const char* thread = args->_thread;

    // per thread index accessor
    BLTIndex* blt = BLTIndex::create( mgr, thread );

    time_t tod[1];
    time( tod );

    __OSS__( "args->_type = " << args->_type );
    Logger::logDebug( thread, __ss__, __LOC__ );

    switch (args->_type | 0x20) {
    case 'a': {
        Logger::logInfo( thread, "\n[[ latch mgr audit ]]", __LOC__ );
        blt->latchAudit();
        break;
    }
    case 'w': {
        {
            __OSS__( "\n[[ indexing ]] " << args->_infile );
            Logger::logInfo( thread, __ss__, __LOC__ );
        }

        FILE* in = fopen( args->_infile, "rb" );
        if (in) {
            int len = 0;
            int docid = 0;
            int ch;

            while (ch = getc(in), ch != EOF) {
                if (ch == '\n') {
                    docid++;

                    if (blt->insertKey( key, len, 0, docid, *tod )) {
                        cout << __TRACE__ << "Error " << blt->getLastError() << ", docid " << docid << endl;
                        exit(0);
                    }
                    len = 0;
                 }
                 else if (len < 255) {
                    key[len++] = ch;
                }
            }
            {
                __OSS__( "finished " << args->_infile << " for " << docid << " keys" );
                Logger::logInfo( thread, __ss__, __LOC__ );
            }
        }
        break;
    }
    case 'd': {
        {
            __OSS__( "\n[[ started deleting keys ]] : " << args->_infile );
            Logger::logInfo( thread, __ss__, __LOC__ );
        }

        FILE* in = fopen( args->_infile, "rb" );
        if (in) {
            int len = 0;
            int line = 0;
            int ch;

            while (ch = getc(in), ch != EOF) {
                if (ch == '\n') {
                    line++;
                    if (blt->deleteKey( key, len, 0)) {
                        cout << __TRACE__ << "Error " << blt->getLastError() << ", Line " << line << endl;
                        exit(0);
                    }
                    (cout << "deleted '").write( (const char*)key, len ) << '\'' << endl;
                    len = 0;
                }
                else if (len < 255) {
                    key[len++] = ch;
                }
            }
            {
                __OSS__( "finished " << args->_infile << " for " << line << " keys" );
                Logger::logInfo( thread, __ss__, __LOC__ );
            }
        }
        break;
    }
    case 'f': {
        {
            __OSS__( "\n[[ finding keys for ]] " << args->_infile );
            Logger::logInfo( thread, __ss__, __LOC__ );
        }

        FILE* in = fopen( args->_infile, "rb" );
        if (in) {
            BLTERR err;
            uint found = 0;
            int line = 0;
            int len = 0;
            int ch;

            while (ch = getc(in), ch != EOF) {
                if (ch == '\n') {
                    ++line;
                    if (blt->findKey( key, len )) {
                        ++found;
                    }
                    else if ( (err = blt->getLastError()) ) {
                        cout << __TRACE__ << "Error " << err << " Syserr " << strerror(errno) << " Line " << line << endl;
                        exit(-1);
                    }
                    len = 0;
                }
                else if (len < 255) {
                    key[len++] = ch;
                }
            }
            {
                __OSS__( "finished " << args->_infile << " for " << line << " keys, found " << found );
                Logger::logInfo( thread, __ss__, __LOC__ );
            }
        }
        break;
    }
    case 's': {
        Logger::logInfo( thread, "\n[[ scanning ]]", __LOC__ );

        uint cnt = 0;
        PageNo pageNo = LEAF_page;
        PageNo next;

        do {
            if ((set->_pool = mgr->pinPool( pageNo, thread ))) {
                set->_page = mgr->page( set->_pool, pageNo, thread );
            }
            else {
                break;
            }

            Page* page = set->_page;

            set->_latch = mgr->getLatchMgr()->pinLatch( pageNo, thread );
            mgr->lockPage( LockRead, set->_latch, thread );
            next = Page::getPageNo( page->_right );
            cnt += page->_act;

            cout << "\npage id : " << pageNo << " -> " << next << '\n' << *(page) << endl;

            for (int slot = 0; slot++ < page->_cnt;) {
                if (next || slot < page->_cnt) {
                    if (!Page::slotptr(page, slot)->_dead) {
                        BLTKey* key = Page::keyptr(page, slot);
                        cout.write( (const char*)key->_key, key->_len ) << endl;
                    }
                }
            }

            mgr->unlockPage( LockRead, set->_latch, thread );
            mgr->getLatchMgr()->unpinLatch( set->_latch, thread );
            mgr->unpinPool( set->_pool, thread );
        } while ((pageNo = next));

        --cnt;    // don't count stop/sentinel key

        {
            __OSS__( " Total keys read " << cnt );
            Logger::logInfo( thread, __ss__, __LOC__ );
        }

        break;
    }
    case 'c': {
        Logger::logInfo( thread, "\n[[ counting ]]", __LOC__ );
        uint cnt = 0;
        PageNo pageNo = LEAF_page;
        PageNo next = mgr->getLatchMgr()->_nlatchPage + LATCH_page;

        while (pageNo < Page::getPageNo(mgr->getLatchMgr()->_alloc->_right)) {
            PageNo off = pageNo << mgr->getPageBits();
            pread( mgr->getFD(), blt->getFrame(), mgr->getPageSize(), off );
            if (!blt->getFrame()->_free && !blt->getFrame()->_level) cnt += blt->getFrame()->_act;
            if (pageNo > LEAF_page) next = pageNo + 1;
            pageNo = next;
        }
        
        cnt--;    // remove stopper key
        {
            __OSS__( "Total keys read " << cnt );
            Logger::logInfo( thread, __ss__, __LOC__ );
        }
        break;
    }
    default: {
        __OSS__( "Unrecognized command type: " << args->_type );
        Logger::logError( thread, __ss__, __LOC__ );
    }
    }

    blt->close();
    return NULL;
}

typedef struct timeval timer;

void usage( const char* arg0 ) {
    cout << "Usage: " << arg0  << "OPTIONS\n"
            "  -f dbname      - the name of the index file(s)\n"
            "  -c cmd         - one of: Audit, Write, Delete, Find, Scan, Count\n"
            "  -p PageBits    - page size in bits; default 16\n"
            "  -n PoolSize    - number of buffer pool mmapped page segments; default 8192\n"
            "  -s SegBits     - segment size in pages in bits; default 5\n"
            "  -k k_1,k_2,..  - list of source key files k_i, one per thread" << endl;
}

int main(int argc, char* argv[] ) {

    string dbname;          // index file name
    string cmd;             // command = { Audit|Write|Delete|Find|Scan|Count }
    uint pageBits = 16;     // (i.e.) 32KB per page
    uint poolSize = 8192;   // (i.e.) 8192 segments
    uint segBits = 5;       // (i.e.) 32 pages per segment

    vector<string> srcv;    // source files containing keys
    vector<string> cmdv;    // corresponding commands

    opterr = 0;
    char c;
    while ((c = getopt( argc, argv, "f:c:p:n:s:k:" )) != -1) {
        switch (c) {
        case 'f': { // -f dbName
            dbname = optarg;
            break;
        }
        case 'c': { // -c (Read|Write|Scan|Delete|Find)[,..]
            //cmd = optarg;
            char sep[] = ",";
            char* tok = strtok( optarg, sep );
            while (tok) {
                if (strspn( tok, " \t" ) == strlen(tok)) continue;
                cmdv.push_back( tok );
                tok = strtok( 0, sep );
            }
            break;
        }
        case 'p': { // -p pageBits
            pageBits = strtoul( optarg, NULL, 10 );
            break;
        }
        case 'n': { // -n poolSize
            poolSize = strtoul( optarg, NULL, 10 );
            break;
        }
        case 's': { // -s segBits
            segBits = strtoul( optarg, NULL, 10 );
            break;
        }
        case 'k': { // -k keyFile1,keyFile2,..
            char sep[] = ",";
            char* tok = strtok( optarg, sep );
            while (tok) {
                if (strspn( tok, " \t" ) == strlen(tok)) continue;
                srcv.push_back( tok );
                tok = strtok( 0, sep );
            }
            break;
        }
        case '?': {
            usage( argv[0] );
            return 1;
        }
        default: exit(-1);
        }
    }

    if (poolSize > 65536) {
        cout << __TRACE__ << "poolSize too large, defaulting to 65536" << endl;
        poolSize = 65536;
    }
    
    cout << __TRACE__ << "dbname = " << dbname << endl;
    cout << __TRACE__ << "cmd = " << cmd << endl;
    cout << __TRACE__ << "pageBits = " << pageBits << endl;
    cout << __TRACE__ << "poolSize = " << poolSize << endl;
    cout << __TRACE__ << "segBits = " << segBits << endl;
    cout << __TRACE__ << "cmd count = " << cmdv.size() << endl;
    cout << __TRACE__ << "src count = " << srcv.size() << endl;

    if (cmdv.size() != srcv.size()) {
        cout << __TRACE__ << "Need one command per source key file (per thread)" << endl;
        exit(-1);
    }

    for (uint i=0; i<srcv.size(); ++i) {
        cout << __TRACE__ << cmdv[i] << " -> " << srcv[i] << endl;
    } 

    double start = getCpuTime(0);
    int cnt = srcv.size();

    if (cnt > 100) {
        cout << __TRACE__ << "cmd count exceeds 100.  bailing." << endl;
        exit( -1 );
    }

    pthread_t threads[ cnt ];
    ThreadArg args[ cnt ];
    const char* threadNames[ 100 ] = {
      "main", "01", "02", "03", "04", "05", "06", "07", "08", "09",
        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
        "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
        "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
        "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
        "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
        "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
        "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
        "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
        "90", "91", "92", "93", "94", "95", "96", "97", "98", "99" };

    // initialize logger
    vector< pair< string, ostream* > > v;
    for (uint32_t i=0; i<=cnt; ++i) {
        v.push_back( pair< string, ostream* >( threadNames[ i ], new ostringstream() ) );
    }
    Logger::init( v );

    // allocate buffer pool manager
    BufferMgr* mgr = BufferMgr::create( dbname.c_str(), // index file name
                                        pageBits,       // page size in bits
                                        poolSize,       // number of segments
                                        segBits,        // segment size in pages in bits
                                        poolSize>>3 );  // hashSize

    if (!mgr) {
        cout << __TRACE__ << "buffer pool create failed.  bailing." << endl;
        exit(1);
    }

    // start threads
    cout << __TRACE__ << "starting:" << endl;
    for (uint i = 0; i < cnt; ++i) {
        cout << "    thread " << i << endl;
        args[i]._infile = srcv[i].c_str();
        args[i]._type = cmdv[i][0];   // (i.e.) A/W/D/F/S/C
        args[i]._mgr = mgr;
        args[i]._idx = i;
        args[i]._thread = threadNames[ i+1 ];
        int err = pthread_create( &threads[i], NULL, indexOp, &args[i] );
        if (err) cout << __TRACE__ << "Error creating thread: " << err << endl;
    }

    // wait for termination
    cout << __TRACE__ << "waiting for thread terminations" << endl;
    for (uint idx = 0; idx < cnt; ++idx) {
        pthread_join( threads[idx], NULL );
    }

    cout << "\n*** Thread 'main' log ***" << endl;
    const char* thread = "main";
    ostream* os = Logger::getStream( thread );
    assert( NULL != os );
    ostringstream* oss = dynamic_cast<ostringstream*>( os );
    assert( NULL != oss );
    cout << oss->str() << endl;

    for (uint idx = 0; idx < cnt; ++idx) {
        const char* thread = args[ idx ]._thread;
        cout << "\n*** Thread '" << thread << "' log ***" << endl;
        ostream* os = Logger::getStream( thread );
        assert( NULL != os );
        ostringstream* oss = dynamic_cast<ostringstream*>( os );
        assert( NULL != oss );
        cout << oss->str() << endl;

    }

    float elapsed = getCpuTime(0) - start;
    cout << " real " << (int)(elapsed/60) << "m " << elapsed - (int)(elapsed/60)*60 << 's' << endl;
    elapsed = getCpuTime(1);
    cout << " user " << (int)(elapsed/60) << "m " << elapsed - (int)(elapsed/60)*60 << 's' << endl;
    elapsed = getCpuTime(2);
    cout << " sys  " << (int)(elapsed/60) << "m " << elapsed - (int)(elapsed/60)*60 << 's' << endl;

    mgr->close( "main" );
}

