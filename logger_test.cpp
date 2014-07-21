//@file 
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

#include "common.h"
#include "logger.h"

#include <assert.h>
#include <string>
#include <iostream>
#include <sstream>
#include <utility>
#include <vector>
    
using namespace std;
using namespace mongo;

int main( int argc, char* argv[] ) {

    ostream* streamA = new ostringstream();
    ostream* streamB = new ostringstream();
    ostream* streamC = new ostringstream();

    vector< pair< string, ostream* > > v;
    v.push_back( pair< string, ostream* >( "a", streamA ) );
    v.push_back( pair< string, ostream* >( "b", streamB ) );
    v.push_back( pair< string, ostream* >( "c", streamC ) );

    Logger::init( v );
    Logger::logDebug( "a", "message 1", __LOC__ );
    Logger::logDebug( "b", "message 2", __LOC__ );
    Logger::logDebug( "b", "message 3", __LOC__ );
    Logger::logDebug( "c", "message 4", __LOC__ );
    Logger::logDebug( "a", "message 5", __LOC__ );
    Logger::logDebug( "c", "message 6", __LOC__ );
    Logger::logDebug( "b", "message 7", __LOC__ );
    Logger::logDebug( "a", "message 8", __LOC__ );
    Logger::logDebug( "c", "message 9", __LOC__ );

    ostringstream* logA = dynamic_cast< ostringstream* >( Logger::getStream( "a" ) );
    assert(logA != NULL);
    ostringstream* logB = dynamic_cast< ostringstream* >( Logger::getStream( "b" ) );
    assert(logB != NULL);
    ostringstream* logC = dynamic_cast< ostringstream* >( Logger::getStream( "c" ) );
    assert(logC != NULL);

    cout << "log A:" << endl << logA->str() << endl;
    cout << "log B:" << endl << logB->str() << endl;
    cout << "log C:" << endl << logC->str() << endl;

}

