//@file page.cpp
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

#include "page.h"
#include "blterr.h"

#include <errno.h>
#include <iostream>
#include <iomanip>
#include <memory.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
   
namespace mongo {
    /*
    *  Pages are managed as heaps:  key offsets and record-id's are allocated from
    *  the bottom, while the text of the keys are allocated from the top.  When
    *  the two areas meet, the page is full and it needs to be split.
    *
    *  When the root page fills, it is split in two and the tree height is raised
    *  by a new root at page one with two keys.
    *
    *  A key consists of a length byte, two bytes of index number (0 - 65534),
    *  and up to 253 bytes of key value.  Duplicate keys are discarded.
    *  Associated with each key is a 48 bit docid.
    *
    *  The bltindex pages are linked with right pointers to facilitate enumerators,
    *  and provide for concurrency.
    *
    *  Deleted keys are tombstoned with a dead bit until page cleanup. The fence key
    *  for a node is always present, even after deletion and cleanup.
    *
    *  Deleted leaf pages are reclaimed on a free list.
    *  The upper levels of the bltindex are fixed on creation.
    *
    *  Page slots use 1-based indexing.
    */

    // debugging output

    std::ostream& operator<<( std::ostream& os, const DiskLoc& loc ) {
        return os <<
            "DiskLoc[ fileno = " << std::setw(5) << loc.fileno <<
                     ", offset = " << std::setw(15) << loc.offset << ']';
    }

    std::ostream& operator<<( std::ostream& os, const Slot& slot ) {
        return os <<
            "Slot["
            " offset = " << (uint32_t)slot._off <<
            ", dead bit = " << (bool)slot._dead << ']';
    }

    std::ostream& operator<<( std::ostream& os, const Page& page ) {
        os <<
            "Page["
            "\n  key count = "   << page._cnt <<
            "\n  active key count = "   << page._act <<
            "\n  next key offset = "   << page._min <<
            "\n  page bit size = "  << (uint32_t)page._bits <<
            "\n  free bit = "  << (bool)page._free <<
            "\n  page level = "   << (uint32_t)page._level <<
            "\n  page being deleted = "  << (bool)page._kill <<
            "\n  dirty bit = " << (bool)page._dirty <<
            "\n]\n";

        for (uint slot = 1; slot <= page._cnt; ++slot) {
            Slot* slotPtr  = Page::slotptr( (Page*)&page, slot );
            BLTKey* keyPtr = Page::keyptr( (Page*)&page, slot );

            os << *slotPtr << " : "
                << std::string( (const char*)keyPtr->_key, keyPtr->_len )
                << std::endl;
        }

        return os << std::endl;
    }

}   // namespace mongo

