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
 * This is a derivative work.  The original 'C' source
 * code was put in the public domain by Karl Malbrain
 * (malbrain@cal.berkeley.edu.  The original copyright
 * notice is:
 *
 *     This work, including the source code, documentation
 *     and related data, is placed into the public domain.
 *
 *     The orginal author is Karl Malbrain.
 *
 *     THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY
 *     OF ANY KIND, NOT EVEN THE IMPLIED WARRANTY OF
 *     MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE,
 *     ASSUMES _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE
 *     RESULTING FROM THE USE, MODIFICATION, OR
 *     REDISTRIBUTION OF THIS SOFTWARE.
 *
 */

#ifndef STANDALONE
#include "mongo/db/storage/bltree/common.h"
#include "mongo/db/storage/bltree/page.h"
#else
#include "common.h"
#include "page.h"
#endif

namespace mongo {

    void BLTVal::putid( uchar* dest, uid id ) {
        int i = BtId;
        while( i-- ) {
            dest[i] = (uchar)id;
            id >>= 8;
        }
    }
    
    uid BLTVal::getid( uchar* src ) {
        uid id = 0;
        for (int i = 0; i < BtId; i++) {
            id <<= 8;
            id |= *src++; 
        }
        return id;
    }

	/**
    *  FUNCTION: findslot
    *
	*  find slot in page for given key at a given level
	*/
    int Page::findslot( Page* page, uchar *key, uint keylen ) {
	    uint diff;
        uint higher = page->cnt;
        uint low = 1;
        uint slot;
	    uint good = 0;
	
		// make stopper key an infinite fence value
		if (BLTVal::getid( page->right )) {
			higher++;
        }
		else {
			good++;
        }
	
		// low is the lowest candidate. loop ends when they meet.
		// higher is already tested as >= the passed key
		while ( (diff = higher - low) ) {
			slot = low + ( diff >> 1 );
			if (BLTKey::keycmp( keyptr(page, slot), key, keylen ) < 0) {
				low = slot + 1;
            }
			else {
				higher = slot;
                good++;
            }
		}
	
		//	return zero if key is on right link page
		return good ? higher : 0;
	}

}   // namespace mongo
