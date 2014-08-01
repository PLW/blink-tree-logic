
#include <stdlib.h>
#include <iostream>

using namespace std;

int main( int argc, char* argv[] ) {
    char alpha[] = {
        'a','b','c','d','e','f','g','h',
        'i','j','k','l','m','n','o','p',
        'q','r','s','t','u','v','w','x',
        'y','z','0','1','2','3','4','4',
        '5','6','7','8','9'
    };

    uint32_t nkeys( 10000 );
    if (argc>1) {
        char* endptr;
        uint32_t n = strtoul( argv[1], &endptr, 10 );
        if (endptr != argv[1]) nkeys = n;
    }

    for (uint32_t i=0; i<nkeys; ++i) {
        uint32_t n = 4+random()%28;
        string key;
        for (uint32_t j=0; j<n; ++j) {
            key += alpha[ random() % 36 ];
        }
        cout << key << endl;
    }
}
