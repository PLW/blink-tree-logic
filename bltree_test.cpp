
#ifndef STANDALONE
#include "mongo/base/status.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/mmap_v1/bltree/common.h"
#include "mongo/db/storage/mmap_v1/bltree/blterr.h"
#include "mongo/db/storage/mmap_v1/bltree/bltkey.h"
#include "mongo/db/storage/mmap_v1/bltree/bltree.h"
#include "mongo/db/storage/mmap_v1/bltree/bltval.h"
#include "mongo/db/storage/mmap_v1/bltree/bufmgr.h"
#include "mongo/db/storage/mmap_v1/bltree/logger.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/unittest/unittest.h"
#else
#include "common.h"
#include "blterr.h"
#include "bltkey.h"
#include "bltree.h"
#include "bltval.h"
#include "bufmgr.h"
#include "logger.h"

#define Status int

#endif

#include <errno.h>
#include <fstream>
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
		    char type;
		    char idx;
		    char *infile;
		    BufMgr* mgr;
            const char* thread;
		} ThreadArg;

		//
		// thread callback
		//
		static void* indexOp( void* arg ) {
		    ThreadArg* args = (ThreadArg *)arg;

		    int line  = 0;
            int found = 0;
            int cnt   = 0;
            int len   = 0;

		    uid next;
            uid page_no = LEAF_page;   // start on first page of leaves
		    unsigned char key[256];
		    PageSet set[1];
		    BLTKey* ptr;
		    BLTVal* val;
		    BufMgr* mgr = args->mgr;
		    FILE* in;
		    int ch;
		
		    BLTree* bt = BLTree::create( mgr );
		
		    switch(args->type | 0x20) {
		    case 'a': {
		        fprintf( stderr, "started latch mgr audit\n" );
		        cnt = bt->latchaudit();
		        fprintf( stderr, "finished latch mgr audit, found %d keys\n", cnt );
		        break;
		    }
		    case 'p': {
		        fprintf( stderr, "started pennysort for %s\n", args->infile );
		        if ( (in = fopen( args->infile, "rb" )) ) {
		            while (ch = getc(in), ch != EOF) {
		                if (ch == '\n') {
		                    line++;
		                    if (bt->insertkey( key, 10, 0, key + 10, len - 10) ) {
		                        fprintf( stderr, "Error %d Line: %d\n", bt->err, line );
		                        exit( -1 );
		                    }
		                    len = 0;
		                }
		                else if (len < 255) {
		                    key[len++] = ch;
		                }
		            }
		        }
		        else {
		            fprintf( stderr, "error opening %s\n", args->infile );
		        }
		
		        fprintf( stderr, "finished %s for %d keys\n", args->infile, line );
		        break;
		    }
		    case 'w': {
		        fprintf( stderr, "started indexing for %s\n", args->infile );
		        if ( (in = fopen( args->infile, "rb" )) ) {
		            while (ch = getc(in), ch != EOF) {
		                if (ch == '\n') {
		                    line++;

		                    if (bt->insertkey( key, len, 0, key, len) ) {
		                        fprintf( stderr, "Error %d Line: %d\n", bt->err, line);
		                        exit( -1 );
		                    }
		                    len = 0;
		                }
		                else if (len < 255) {
		                    key[len++] = ch;
		                }
		            }
		        }
		        else {
		            fprintf( stderr, "error opening %s\n", args->infile );
		        }
		
		        fprintf(stderr, "finished %s for %d keys\n", args->infile, line);
		        break;
		    }
		    case 'd': {
		        fprintf( stderr, "started deleting keys for %s\n", args->infile );
		        if ( (in = fopen( args->infile, "rb" )) ) {
		            while (ch = getc(in), ch != EOF) {
		                if( ch == '\n' ) {
		                    line++;
		
		                    if (bt->deletekey( key, len, 0 )) {
		                        fprintf( stderr, "Error %d Line: %d\n", bt->err, line );
		                        exit( -1 );
		                    }
		                    len = 0;
		                }
		                else if (len < 255) {
		                    key[len++] = ch;
		                }
		            }
		        }
		        else {
		            fprintf( stderr, "error opening %s\n", args->infile );
		        }
		
		        fprintf(stderr, "finished %s for keys, %d \n", args->infile, line);
		        break;
		    }
		    case 'f': {
		        fprintf( stderr, "started finding keys for %s\n", args->infile );
		        if ( (in = fopen( args->infile, "rb" )) ) {
		            while( ch = getc(in), ch != EOF ) {
		                if (ch == '\n') {
		                    line++;
		    
		                    if (bt->findkey( key, len, NULL, 0 ) == 0) {
		                        found++;
		                    }
		                    else if (bt->err) {
		                        fprintf(stderr, "Error %d Syserr %d Line: %d\n", bt->err, errno, line);
		                         exit(0);
		                    }
		                    len = 0;
		                }
		                else if (len < 255) {
		                    key[len++] = ch;
		                }
		            }   // end while
		        }
		
		        fprintf( stderr, "finished %s for %d keys, found %d\n", args->infile, line, found );
		        break;
		    }
		    case 's': {
		        fprintf( stderr, "started scanning\n" );
		        do {
		            if ( (set->pool = bt->mgr->pinpool( page_no )) ) {
		                set->page = bt->mgr->page( set->pool, page_no );
		            }
		            else {
		                break;
		            }
		            set->latch = bt->mgr->pinlatch( page_no );
		            bt->mgr->lockpage( LockRead, set->latch );
		            next = BLTVal::getid( set->page->right );
		            cnt += set->page->act;
		
		            for (unsigned int slot = 0; slot++ < set->page->cnt; ) {
		                if (next || slot < set->page->cnt) {
		                    if (!slotptr(set->page, slot)->dead) {
		                        ptr = keyptr(set->page, slot);
		                        fwrite( ptr->key, ptr->len, 1, stdout );
		                        fputc( ' ', stdout );
		                        fputc( '-', stdout );
		                        fputc( '>', stdout );
		                        fputc( ' ', stdout );
		                        val = valptr( set->page, slot );
		                        fwrite( val->value, val->len, 1, stdout );
		                        fputc( '\n', stdout );
		                    }
		                }
		            }
		            bt->mgr->unlockpage( LockRead, set->latch );
		            bt->mgr->unpinlatch( set->latch );
		            bt->mgr->unpinpool( set->pool );
		        } while ( (page_no = next) );
		
		        cnt--;    // remove stopper key
		        fprintf(stderr, " Total keys read %d\n", cnt);
		        break;
		    }
		    case 'c':
		        //posix_fadvise( bt->mgr->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
		
		        fprintf(stderr, "started counting\n");
		        next = bt->mgr->latchmgr->nlatchpage + LATCH_page;
		        page_no = LEAF_page;
		
		        while (page_no < BLTVal::getid( bt->mgr->latchmgr->alloc->right )) {
		            uid off = page_no << bt->mgr->page_bits;
		            pread( bt->mgr->idx, bt->frame, bt->mgr->page_size, off );
		            if (!bt->frame->free && !bt->frame->lvl) {
		                cnt += bt->frame->act;
		            }
		            if (page_no > LEAF_page) {
		                next = page_no + 1;
		            }
		            page_no = next;
		        }
		        
		        cnt--;    // remove stopper key
		        fprintf( stderr, " Total keys read %d\n", cnt );
		        break;
		    }
		
		    bt->close();
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

            // allocate buffer pool manager
            char* name = (char *)dbname.c_str();
            BufMgr* mgr = BufMgr::create( name,         // index file name
                                          BT_rw,        // file open mode
                                          pageBits,     // page size in bits
                                          poolSize,     // number of segments
                                          segBits,      // segment size in pages in bits
                                          poolSize );   // hashSize
        
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
                args[i].infile = (char *)srcv[i].c_str();
                args[i].type = cmdv[i][0];   // (i.e.) A/W/D/F/S/C
                args[i].mgr = mgr;
                args[i].idx = i;
                args[i].thread = threadNames[ i+1 ];
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
                const char* thread = args[ idx ].thread;
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

            BufMgr::destroy( mgr );

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

        cmdv.push_back( "Write" );    //[0]
        cmdv.push_back( "Write" );    //[1]
        cmdv.push_back( "Write" );    //[2]
        cmdv.push_back( "Write" );    //[3]
        cmdv.push_back( "Write" );    //[4]

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

        cmdv.push_back( "Find" );    //[0]
        cmdv.push_back( "Find" );    //[1]
        cmdv.push_back( "Find" );    //[2]
        cmdv.push_back( "Find" );    //[3]
        cmdv.push_back( "Find" );    //[4]
        //cmdv.push_back( "Write" );    //[5]
        //cmdv.push_back( "Write" );    //[6]

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
    uint segBits  = 5;      // (i.e.) 32 pages per segment

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

    if (driver.drive( "testdb", cmdv, srcv, pageBits, poolSize, segBits )) {
        cout << "driver returned error" << endl;
    }

}
#endif

/*
int main( int argc, char* argv[] ) {

    int idx, cnt, len, slot, err;
    int segsize, bits = 16;
    double start, stop;
    unsigned int poolsize = 0;
    float elapsed;
    char key[1];

    pthread_t* threads;
    ThreadArg* args;
    mongo::BufMgr* mgr;
    mongo::BLTKey* ptr;
    mongo::BLTree*  bt;

    if (argc < 3) {
        fprintf( stderr, "Usage: %s idx_file Read/Write/Scan/Delete/Find [page_bits mapped_segments seg_bits line_numbers src_file1 src_file2 ... ]\n", argv[0] );
        fprintf( stderr, "  where page_bits is the page size in bits\n" );
        fprintf( stderr, "  mapped_segments is the number of mmap segments in buffer pool\n" );
        fprintf( stderr, "  seg_bits is the size of individual segments in buffer pool in pages in bits\n" );
        fprintf( stderr, "  src_file1 thru src_filen are files of keys separated by newline\n" );
        exit(0);
    }

    start = getCpuTime(0);

    if (argc > 3) {
        bits = atoi(argv[3]);
    }
    if (argc > 4) {
        poolsize = atoi(argv[4]);
    }
    if (!poolsize) {
        fprintf( stderr, "Warning: no mapped_pool\n" );
    }
    if (poolsize > 65535) {
        fprintf( stderr, "Warning: mapped_pool > 65535 segments\n" );
    }
    if (argc > 5) {
        segsize = atoi(argv[5]);
    }
    else {
        segsize = 4;     // 16 pages per mmap segment
    }

    printf( "allocate threads\n" );
    cnt = argc - 7;
    threads = (pthread_t *)malloc( cnt * sizeof(pthread_t) );
    args = (ThreadArg *)malloc( cnt * sizeof(ThreadArg) );

    printf( "create bufmgr\n" );
    mgr = mongo::BufMgr::create( (argv[1]), BT_rw, bits, poolsize, segsize, poolsize / 8 );

    if (!mgr) {
        fprintf(stderr, "Index Open Error %s\n", argv[1]);
        exit(-1);
    }

    // fire off threads
    printf( "fire off threads\n" );
    for (idx = 0; idx < cnt; idx++) {
        args[idx].infile = argv[idx + 7];
        args[idx].type = argv[2][0];
        args[idx].mgr = mgr;
        args[idx].idx = idx;
        if ( (err = pthread_create( threads + idx, NULL, index_file, args + idx)) ) {
            fprintf( stderr, "Error creating thread %d\n", err );
        }
    }

    // wait for termination
    printf( "wait for thread termination\n" );
    for (idx = 0; idx < cnt; idx++ ) {
        pthread_join( threads[idx], NULL );
    }

    printf( "print stats\n" );
    elapsed = getCpuTime(0) - start;
    fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
    elapsed = getCpuTime(1);
    fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
    elapsed = getCpuTime(2);
    fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);

    mongo::BufMgr::destroy( mgr );
    
}
*/
