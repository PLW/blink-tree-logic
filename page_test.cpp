//@file page_test.cpp
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

#include "page.h"

#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/types.h>
#include <unistd.h>
    
using namespace std;
using namespace mongo;

int main( int argc, char* argv[] ) {

    const char* fname = argv[1];
    uint32_t pageBits = strtoul( argv[2], NULL, 10 );
    uint32_t pageSize = (1 << pageBits);

    cout << "fname = " << fname << endl;
    cout << "pageBits = " << pageBits << endl;
    cout << "pageSize = " << pageSize << endl;

    Page* page = (Page*) malloc( pageSize );
    int fd = open( fname, O_RDWR | O_CREAT, 0666 );

    for (uint32_t docId=0; docId<10; ++docId) {
        size_t nread = pread( fd, page, pageSize, docId << pageBits );
        if (nread < pageSize) {
            cout << "readPage(" << docId <<") error: " << strerror(errno) << endl;
            return 1;
        }
        cout << *page << endl;
    }
    return 0;
}
