    /**
	*  The key structure occupies space at the upper end of
	*  each page.  It's a length byte followed by the key bytes.
	*/
	typedef struct BLTKey {
	    uchar _len;
	    uchar _key[1];
	} *BLTKeyRef;
	
