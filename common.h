//@file common.h
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

#include <stdint.h>

namespace mongo {

    typedef uint64_t        PageNo;
    typedef uint64_t        DocId;
    typedef unsigned char   uchar;
    typedef uint16_t        ushort;
    typedef uint32_t        uint;
    typedef uint64_t        ulong;
    
    #define __LOC__         __FILE__,__FUNCTION__,__LINE__
    #define __OSS__(X)      std::ostringstream __oss__; __oss__ << X; std::string __ss__ = __oss__.str();

    // allow big files on 32-bit linux
#ifdef LINUX32
    #define _LARGEFILE64_SOURCE 
#endif

    // default mmap alignment
    #define MMAP_MIN_SIZE   4096

    // packed PageNo size
    #define IdLength        8                   // packed page number size

    // packed DocId size
    #define DocIdLength     8                   // packed docid size

    // page size parameters
    #define BLT_minbits     12                  // minimum page size in bits = lg(4K)
    #define BLT_maxbits     24                  // maximum page size in bits = lg(16M)
    #define BLT_minpage     (1<<BLT_minbits)    // minimum page size = 4K
    #define BLT_maxpage     (1<<BLT_maxbits)    // maximum page size = 16MB
    #define BLT_latchtableSize  1024            // number of latch manager slots (may need more)

    // file open modes
    #define BLT_ro          0x6f72              // read-only
    #define BLT_rw          0x7772              // read-write
    #define BLT_fl          0x6c66              // file-lock
   
    // depth min/max bounds
    #define MIN_level       2
    #define MAX_level       15
    
    // static pageno's
    #define ALLOC_page      0
    #define ROOT_page       1
    #define LEAF_page       2
    #define LATCH_page      3

    // 'pin' bits
    #define CLOCK_mask      0xe000
    #define CLOCK_unit      0x2000
    #define PIN_mask        0x07ff
    #define LVL_mask        0x1800
    #define LVL_shift       11

    // bit in pool->_pin
    #define CLOCK_bit       0x8000    

}   // namespace mongo

