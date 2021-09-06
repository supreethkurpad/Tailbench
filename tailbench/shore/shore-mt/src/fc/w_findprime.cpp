
#include "w_findprime.h"
#include <vector>

// lots of help from Wikipedia here!
w_base_t::int8_t w_findprime(w_base_t::int8_t min) 
{
    // the first 25 primes
    static char const prime_start[] = {
    // skip 2,3,5 because our mod60 test takes care of them for us
    /*2, 3, 5,*/ 7, 11, 13, 17, 19, 23, 29, 31, 37, 41,
    43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97
    };
    // x%60 isn't on this list, x is divisible by 2, 3 or 5. If it
    // *is* on the list it still might not be prime
    static char const sieve_start[] = {
    // same as the start list, but adds 1,49 and removes 3,5
    1, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 49, 53, 59
    };

    // use the starts to populate our data structures
    std::vector<w_base_t::int8_t> primes(prime_start, prime_start+sizeof(prime_start));
    char sieve[60];
    memset(sieve, 0, sizeof(sieve));

    for(w_base_t::uint8_t i=0; i < sizeof(sieve_start); i++)
    sieve[w_base_t::int8_t(sieve_start[i])] = 1;

    /* We aren't interested in 4000 digit numbers here, so a Sieve of
       Erastothenes will work just fine, especially since we're
       seeding it with the first 25 primes and avoiding the (many)
       numbers that divide by 2,3 or 5.
     */
    for(w_base_t::int8_t x=primes.back()+1; primes.back() < min; x++) {
    if(!sieve[x%60])
        continue; // divides by 2, 3 or 5

    bool prime = true;
    for(w_base_t::int8_t i=0; prime && primes[i]*primes[i] <= x; i++) 
        prime = (x%primes[i]) > 0;

    if(prime) 
        primes.push_back(x);
    }

    return primes.back();
}
