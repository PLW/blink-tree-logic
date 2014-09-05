
#include <stdint.h>
#include <stdlib.h>
#include <iostream>

using namespace std;

char alpha[] = {
    'a','b','c','d','e','f','g','h',
    'i','j','k','l','m','n','o','p',
    'q','r','s','t','u','v','w','x',
    'y','z','0','1','2','3','4','4',
    '5','6','7','8','9'
};

#define NON_EXACT   false
#define EXACT       true

string randomString( uint32_t m0, bool exact ) {
    uint32_t m = m0;
    if (m<8) m = 8;
    uint32_t n = (exact ? m : 4 + random() % (m-4));
    string result;
    for (uint32_t j=0; j<n; ++j) {
       result += alpha[ random() % 36 ];
    }
    return result;
}

int main( int argc, char* argv[] ) {
    uint32_t nkeys( 10000 );
    if (argc>1) {
        char* endptr;
        uint32_t n = strtoul( argv[1], &endptr, 10 );
        if (endptr != argv[1]) nkeys = n;
    }

    for (uint32_t i=0; i<nkeys; ++i) {
        string key = randomString( 12, EXACT );
        cout << key << '\t';
        cout <<
            "{ _id: \"" << key << "\","
               " a: \"" << randomString( 32, NON_EXACT ) << "\","
               " b: \"" << randomString( 32, NON_EXACT ) << "\","
               " c: \"" << randomString( 32, NON_EXACT ) << "\" }" << endl;
    }
}
