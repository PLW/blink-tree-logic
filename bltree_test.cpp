//@file bltree_test.cpp
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

#ifndef STANDALONE
#include "mongo/base/status.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/mmap_v1/bltree/bltree.h"
#include "mongo/db/storage/mmap_v1/bltree/bufmgr.h"
#include "mongo/db/storage/mmap_v1/bltree/common.h"
#include "mongo/db/storage/mmap_v1/bltree/logger.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/unittest/unittest.h"
#else
#include "common.h"
#include "bltree.h"
#include "bufmgr.h"
#include "logger.h"

#define Status int

#endif

#include <errno.h>
#include <iostream>
#include <pthread.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <vector>
    
using namespace std;

namespace mongo {

    class BLTreeTestDriver {
	public:

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

		void printRUsage() {
		    struct rusage used[1];
		    getrusage( RUSAGE_SELF, used );
		
		    cout
		        << "\nProcess resource usage:"
		        << "\nmaximum resident set size = " << used->ru_maxrss 
		        << "\nintegral shared memory size = " << used->ru_ixrss
		        << "\nintegral unshared data size = " << used->ru_idrss
		        << "\nintegral unshared stack size = " << used->ru_isrss
		        << "\npage reclaims (soft page faults) = " << used->ru_minflt
		        << "\npage faults (hard page faults) = " << used->ru_majflt
		        << "\nswaps = " << used->ru_nswap
		        << "\nblock input operations = " << used->ru_inblock
		        << "\nblock output operations = " << used->ru_oublock
		        << "\nIPC messages sent = " << used->ru_msgsnd
		        << "\nIPC messages received = " << used->ru_msgrcv
		        << "\nsignals received = " << used->ru_nsignals
		        << "\nvoluntary context switches = " << used->ru_nvcsw
		        << "\ninvoluntary context switches = " << used->ru_nivcsw << endl;
		
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
		static void* indexOp( void* rawArg ) {
		    uchar key[256];
		    PageSet set[1]; // set -> stack alloc
		
		    ThreadArg* args = (ThreadArg *)rawArg;
		    BufferMgr* mgr = args->_mgr;
		    const char* thread = args->_thread;
		
		    // per thread index accessor
		    BLTree* blt = BLTree::create( mgr, thread );
		
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
		            __OSS__( "\nINDEXING: " << args->_infile );
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
		                        __OSS__( "Error " << blt->getLastError() << ", docid " << docid );
		                        Logger::logError( thread, __ss__, __LOC__ );
		                        exit(0);
		                    }
		                    len = 0;
		
		                    if (0 == docid % 250000) {
		                        __OSS__( "thread " << thread << " inserted " << docid << " keys" );
		                        Logger::logInfo( "main", __ss__, __LOC__ );
		                    }
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
		            __OSS__( "\nDELETING KEYS: " << args->_infile );
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
		                        __OSS__( "Error " << blt->getLastError() << ", Line " << line );
		                        Logger::logError( thread, __ss__, __LOC__ );
		                        exit(0);
		                    }
		                    //(cout << "deleted '").write( (const char*)key, len ) << '\'' << endl;
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
		            __OSS__( "\nFINDING: " << args->_infile );
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
		                        __OSS__( "Error " << err << " Syserr " << strerror(errno) << " Line " << line );
		                        Logger::logError( thread, __ss__, __LOC__ );
		                        exit(-1);
		                    }
		                    len = 0;
		
		                    if (0 == line % 250000) {
		                        __OSS__( "thread " << thread << " found " << found << " of " << line << " keys" );
		                        Logger::logInfo( "main", __ss__, __LOC__ );
		                    }
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
		        Logger::logInfo( thread, "\nSCANNING", __LOC__ );
		
		        uint cnt = 0;
		        PageNo pageNo = LEAF_page;
		        PageNo next;
		        char buf[256];
		
		        do {
		            if ((set->_pool = mgr->pinPoolEntry( pageNo, thread ))) {
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
		
		            //cout << "\npage id : " << pageNo << " -> " << next << '\n' << *(page) << endl;
		
		            for (uint slot = 0; slot++ < page->_cnt;) {
		                if (next || slot < page->_cnt) {
		                    if (!Page::slotptr(page, slot)->_dead) {
		                        BLTKey* key = Page::keyptr(page, slot);
		                        strncpy( buf, (const char*)key->_key, key->_len );
		                        buf[ key->_len ] = 0;
		                        //Logger::logInfo( thread, buf, __LOC__ );
		                    }
		                }
		            }
		
		            mgr->unlockPage( LockRead, set->_latch, thread );
		            mgr->getLatchMgr()->unpinLatch( set->_latch, thread );
		            mgr->unpinPoolEntry( set->_pool, thread );
		        } while ((pageNo = next));
		
		        --cnt;    // don't count stop/sentinel key
		
		        {
		            __OSS__( " Total keys read " << cnt );
		            Logger::logInfo( thread, __ss__, __LOC__ );
		        }
		
		        break;
		    }
		    case 'c': {
		        Logger::logInfo( thread, "\nCOUNTING", __LOC__ );
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

		Status drive( const std::string& dbname,  // index file name
		    		  const std::vector<std::string>& cmdv,  // cmd list 
		    		  const std::vector<std::string>& srcv,  // source key file list
		              uint pageBits,     // (i.e.) 32KB per page
		              uint poolSize,     // (i.e.) 4096 segments -> 4GB
		              uint segBits )     // (i.e.) 32 pages per segment -> 1MB 
		{
		    if (poolSize > 65536) {
		        std::cout << "poolSize too large, defaulting to 65536" << std::endl;
		        poolSize = 65536;
		    }
		    
		    std::cout <<
		        " dbname = " << dbname <<
		        "\n pageBits = " << pageBits <<
		        "\n poolSize = " << poolSize <<
		        "\n segBits = "  << segBits << std::endl;
		
		    if (cmdv.size() != srcv.size()) {
#ifndef STANDALONE
		        return Status( ErrorCodes::InternalError,
                                "Need one command per source key file (per thread)." );
#else
                return -1;
#endif
		    }
		
		    for (uint i=0; i<srcv.size(); ++i) {
		        std::cout << " : " << cmdv[i] << " -> " << srcv[i] << std::endl;
		    } 

            if (cmdv.size() == 1 && cmdv[0]=="Clear") {
                if (dbname.size()) {
                    remove( dbname.c_str() );
                }
#ifndef STANDALONE
                return Status::OK();
#else
                return 0;
#endif
            }
		
		    double start = getCpuTime(0);
		    uint cnt = srcv.size();
		
		    if (cnt > 100) {
#ifndef STANDALONE
                return Status( ErrorCodes::InternalError, "Cmd count exceeds 100." );
#else
                return -1;
#endif
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
		    v.push_back( pair< string, ostream* >( threadNames[ 0 ], &std::cout ) );
		    for (uint i=1; i<=cnt; ++i) {
		        v.push_back( pair< string, ostream* >( threadNames[ i ], new ostringstream() ) );
		    }
		    Logger::init( v );

			// Record store
			
		
		    // allocate buffer pool manager
		    BufferMgr* mgr = BufferMgr::create( dbname.c_str(), // index file name
		                                        pageBits,       // page size in bits
		                                        poolSize,       // number of segments
		                                        segBits,        // segment size in pages in bits
		                                        poolSize );     // hashSize
		
		    if (!mgr) {
#ifndef STANDALONE
                return Status( ErrorCodes::InternalError, "Buffer pool create failed." );
#else
                return -1;
#endif
		    }

		    // start threads
		    std::cout << " Starting:" << std::endl;
		    for (uint i = 0; i < cnt; ++i) {
		        std::cout << "  thread " << i << std::endl;
		        args[i]._infile = srcv[i].c_str();
		        args[i]._type = cmdv[i][0];   // (i.e.) A/W/D/F/S/C
		        args[i]._mgr = mgr;
		        args[i]._idx = i;
		        args[i]._thread = threadNames[ i+1 ];
		        int err = pthread_create( &threads[i], NULL, BLTreeTestDriver::indexOp, &args[i] );
		        if (err) {
#ifndef STANDALONE
                    return Status( ErrorCodes::InternalError, "Error creating thread" );
#else
					return -1;
#endif
                }
		    }
		
		    // wait for termination
		    std::cout << " Waiting for thread terminations." << std::endl;
		    for (uint idx = 0; idx < cnt; ++idx) {
		        pthread_join( threads[idx], NULL );
		    }
		
		    for (uint idx = 0; idx < cnt; ++idx) {
		        const char* thread = args[ idx ]._thread;
		        std::cout << "\n*** Thread '" << thread << "' log ***" << std::endl;
		        ostream* os = Logger::getStream( thread );
				if (NULL!=os) {
		        	ostringstream* oss = dynamic_cast<ostringstream*>( os );
					if (NULL!=oss) {
		        		std::cout << oss->str() << std::endl;
					}
				}
		    }
		
		    float elapsed0 = getCpuTime(0) - start;
		    float elapsed1 = getCpuTime(1);
		    float elapsed2 = getCpuTime(2);

		    std::cout <<
                  " real "  << (int)(elapsed0/60) << "m " <<
                    elapsed0 - (int)(elapsed0/60)*60 << 's' <<
		        "\n user "  << (int)(elapsed1/60) << "m " <<
	                elapsed1 - (int)(elapsed1/60)*60 << 's' <<
		        "\n sys  "  << (int)(elapsed2/60) << "m " <<
	                elapsed2 - (int)(elapsed2/60)*60 << 's' << std::endl;
		
		    printRUsage();

		    mgr->close( "main" );

#ifndef STANDALONE
			return Status::OK();
#else
            return 0;
#endif

		}
	};

#ifndef STANDALONE

    //
    //  TESTS
    //

    TEST( BLTree, BasicWriteTest ) {

		BLTreeTestDriver driver;
        std::vector<std::string> cmdv;
        std::vector<std::string> srcv;

		/*
		// clear db
        cmdv.push_back( "Clear" );
        srcv.push_back( "" );
        ASSERT_OK( driver.drive( "testdb", cmdv, srcv, 15, 4096, 5 ) );

		// one insert thread
        cmdv.clear();
        cmdv.push_back( "Write" );
        srcv.clear();
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.0" );
        ASSERT_OK( driver.drive( "testdb", cmdv, srcv, 15, 4096, 5 ) );

		// clear db
        cmdv.clear();
        srcv.clear();
        cmdv.push_back( "Clear" );
        srcv.push_back( "" );
        ASSERT_OK( driver.drive( "testdb", cmdv, srcv, 15, 4096, 5 ) );

		// three insert threads
        cmdv.clear();
        cmdv.push_back( "Write" );
        cmdv.push_back( "Write" );
        cmdv.push_back( "Write" );
        srcv.clear();
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.0" );
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.1" );
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.2" );
        ASSERT_OK( driver.drive( "testdb", cmdv, srcv, 15, 4096, 5 ) );
		*/

		// five insert threads
        cmdv.clear();
        srcv.clear();

        cmdv.push_back( "Write" );	//[0]
        cmdv.push_back( "Write" );	//[1]
        cmdv.push_back( "Write" );	//[2]
        cmdv.push_back( "Write" );	//[3]
        cmdv.push_back( "Write" );	//[4]

        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.0" );
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.1" );
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.2" );
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.3" );
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.4" );

        ASSERT_OK( driver.drive( "testdb", cmdv, srcv, 15, 32768, 2 ) );

		// five find threads, two insert threads
		/*
        cmdv.clear();
        srcv.clear();

        cmdv.push_back( "Find" );	//[0]
        cmdv.push_back( "Find" );	//[1]
        cmdv.push_back( "Find" );	//[2]
        cmdv.push_back( "Find" );	//[3]
        cmdv.push_back( "Find" );	//[4]
        //cmdv.push_back( "Write" );	//[5]
        //cmdv.push_back( "Write" );	//[6]

        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.0" );
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.1" );
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.2" );
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.3" );
        srcv.push_back( "/home/paulpedersen/dev/bltree/keys.4" );
        //srcv.push_back( "/home/paulpedersen/dev/bltree/keys.5" );
        //srcv.push_back( "/home/paulpedersen/dev/bltree/keys.6" );

        ASSERT_OK( driver.drive( "testdb", cmdv, srcv, 15, 32768, 1 ) );
		*/

	}

#endif
	
}   // namespace mongo


#ifdef STANDALONE

void usage( const char* arg0 ) {
    cout << "Usage: " << arg0  << "OPTIONS\n"
            "  -f dbname      - the name of the index file(s)\n"
            "  -c cmd         - one of: Audit, Write, Delete, Find, Scan, Count\n"
            "  -p PageBits    - page size in bits; default 16\n"
            "  -n PoolEntrySize    - number of buffer pool mmapped page segments; default 8192\n"
            "  -s SegBits     - segment size in pages in bits; default 5\n"
            "  -k k_1,k_2,..  - list of source key files k_i, one per thread" << endl;
}

int main(int argc, char* argv[] ) {

    string dbname;          // index file name
    string cmd;             // command = { Audit|Write|Delete|Find|Scan|Count }
    uint pageBits = 16;     // (i.e.) 32KB per page
    uint poolSize = 8192;   // (i.e.) 8192 segments
    uint segBits = 5;       // (i.e.) 32 pages per segment

    mongo::BLTreeTestDriver driver;
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

    if (driver.drive( "testdb", cmdv, srcv, 15, 32768, 2 )) {
        cout << "driver returned error" << endl;
    }

}
#endif
