
#pragma once

namespace mongo {

    typedef enum {
        BTERR_ok = 0,
        BTERR_struct,
        BTERR_ovflw,
        BTERR_lock,
        BTERR_map,
        BTERR_wrt,
        BTERR_hash
    } BTERR;

}   // namespace mongo
