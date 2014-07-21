//@file latchmgr_test.cpp
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

#include "latchmgr.h"
#include "logger.h"

#include <iostream>
#include <sstream>
#include <utility>
#include <vector>
    
using namespace std;
using namespace mongo;

int main( int argc, char* argv[] ) {

    ostream* streamMain = new std::ostringstream();
    std::vector< pair< std::string, std::ostream* > > v;
    v.push_back( pair< std::string, std::ostream* >( "main", streamMain ) );
    Logger::init( v );

    SpinLatch latch;

    SpinLatch::spinReadLock( &latch, "main" );
    SpinLatch::spinReleaseRead( &latch, "main" );

    int i =  SpinLatch::spinTryWrite( &latch, "main" );
    cout << "i = " << i << endl;
    if (i) SpinLatch::spinReleaseWrite( &latch, "main" );

    SpinLatch::spinWriteLock( &latch, "main" );
    SpinLatch::spinReleaseWrite( &latch, "main" );

    std::ostringstream* logMain = dynamic_cast< std::ostringstream* >( Logger::getStream( "main" ) );
    cout << "log Main:" << endl << logMain->str() << endl;

}

