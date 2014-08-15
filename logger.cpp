//@file logger.cpp
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
#include "mongo/db/storage/mmap_v1/bltree/logger.h"
#include "mongo/db/storage/mmap_v1/bltree/common.h"
#include "mongo/util/assert_util.h"
#else
#include "logger.h"
#include "common.h"
#include <assert.h>
#endif

#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace mongo {

    bool Logger::_init = false;
    std::map< std::string, std::ostream* > Logger::logv;

    void Logger::init( const std::vector< LabelStreamPair >& v ) {
        _init = true;
        std::vector< LabelStreamPair >::const_iterator it;
        for (it = v.begin(); it != v.end(); ++it) logv.insert( *it );
    }

    std::ostream* Logger::getStream( const std::string& label ) {
        std::map< std::string, std::ostream* >::iterator it = logv.find( label );
        return ( it == logv.end() ) ? NULL : it->second;
    }

    void Logger::logMsg( const std::string& label, const char* msg ) {
        if (NULL == msg) return;
        std::map< std::string, std::ostream* >::iterator it = logv.find( label );
        uassert( -1, "it == logv.end()", it != logv.end() );
        *(it->second) << label << ':' << msg << std::endl;
    }

    void Logger::logMsg( const std::string& label, const std::string& msg ) {
        std::map< std::string, std::ostream* >::iterator it = logv.find( label );
        uassert( -1, "it == logv.end()", it != logv.end() );
        *(it->second) << label << ':' << msg << std::endl;
    }

    void Logger::logInfo( const std::string& label, const char* msg,
                            const char* file, const char* func, uint32_t line ) {
        if (NULL == msg) return;
        std::map< std::string, std::ostream* >::iterator it = logv.find( label );
        uassert( -1, "it == logv.end()", it != logv.end() );
        *(it->second) << label << ':' << "Info [" << file<<':'<<func<<':'<<line << "]: " << msg << std::endl;
    }

    void Logger::logInfo( const std::string& label, const std::string& msg,
                            const char* file, const char* func, uint32_t line ) {
        Logger::logInfo( label, msg.c_str(), file, func, line );
    }

    void Logger::logDebug( const std::string& label, const char* msg,
                            const char* file, const char* func, uint32_t line ) {
        if (NULL == msg) return;
        std::map< std::string, std::ostream* >::iterator it = logv.find( label );
        uassert( -1, "it == logv.end()", it != logv.end() );
        *(it->second) << label << ':' << "Debug [" << file<<':'<<func<<':'<<line << "]: " << msg << std::endl;
    }

    void Logger::logDebug( const std::string& label, const std::string& msg,
                            const char* file, const char* func, uint32_t line ) {
        Logger::logDebug( label, msg.c_str(), file, func, line );
    }

    void Logger::logError( const std::string& label, const char* msg,
                            const char* file, const char* func, uint32_t line ) {
        if (NULL == msg) return;
        std::map< std::string, std::ostream* >::iterator it = logv.find( label );
        uassert( -1, "it == logv.end()", it != logv.end() );
        *(it->second) << label << ':' << "Error [" << file<<':'<<func<<':'<<line << "]: " << msg << std::endl;
    }

    void Logger::logError( const std::string& label, const std::string& msg,
                            const char* file, const char* func, uint32_t line ) {
        Logger::logError( label, msg.c_str(), file, func, line );
    }

}   // namespace mongo

