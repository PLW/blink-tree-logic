//@file bltval.h
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

#pragma once

#include "common.h"
#include <string>
#include <string.h>
    
namespace mongo {

    /**
    *  The Val structure occupies space at the upper end of each
    *  page.  It's a length byte followed by the value bytes.
    *  Proxy for a general BSON value.
    */
    class BLTVal {
    public:

        static PageNo getPageNo( const uchar* src ) {
            PageNo p = 0;
            for (int i=0; i<IdLength; ++i) {
                p <<= 8;
                p |= *src++;
            }
            return p;
        }

        static void putPageNo( uchar* dst, PageNo pageNo ) {
            PageNo p = pageNo;
            for (int i=IdLength-1; i>=0; --i) {
                dst[i] = (uchar)p;
                p >>= 8;
            }
        }

        std::string toString() const {
            char buf[ _len + 1 ];
            strncpy( buf, (const char *)_value, _len );
            buf[ _len ] = 0;
            return std::string( buf );
        }

        // data
        uchar _len;
        uchar _value[0];

    };

}   // namespace mongo
