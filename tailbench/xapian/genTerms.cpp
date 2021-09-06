#include <xapian.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <fstream>

using namespace std;

void usage() {
    cout << "Usage: genterms -d db [ -f termsFile ]" << endl;
    exit(-1);
}

int main(int argc, char* argv[]) {

    if ((argc != 3) && (argc != 5)) usage();

    char* dbPath = NULL;
    string termsFileName = "terms.in";

    // Read command line options
    int c;
    string optString = "d:f:"; // d: db, t: terms file, n: numThreads
    while ((c = getopt(argc, argv, optString.c_str())) != -1) {
        switch (c) {
            case 'd':
                if(strcmp(optarg, "?") == 0) {
                    cerr << "Missing database" << endl;
                    usage();
                }
                dbPath = optarg;
                break;

            case 'f':
                if(strcmp(optarg, "?") == 0) {
                    cerr << "Missing terms file" << endl;
                    usage();
                    exit(-1);
                }
                termsFileName = optarg;
                break;

            default:
                cerr << "Unknown option: " << optopt << endl;
                exit(-1);

                break;
        }
    }

    // Open terms file
    ofstream termsFile(termsFileName); 
    if (termsFile.fail()) {
        cerr << "Can't open terms file " << termsFileName << endl;
        exit(-1);
    }

    Xapian::Database db;
    try {
        db.add_database(Xapian::Database(dbPath));
    }
    catch (const Xapian::Error& e) {
        cerr << "Error opening database: " << e.get_msg() << endl;
        usage();
    }

    Xapian::Enquire enquire(db);
    Xapian::Stem stemmer("english");
    Xapian::SimpleStopper stopper;
    const char* stopWords[] = { "a", "about", "an", "and", "are", "as", "at", "be",
        "by", "en", "for", "from", "how", "i", "in", "is", "it", "of", "on",
        "or", "that", "the", "this", "to", "was", "what", "when", "where",
        "which", "who", "why", "will", "with" };

    stopper = Xapian::SimpleStopper(stopWords, \
            stopWords + sizeof(stopWords) / sizeof(stopWords[0]));

    Xapian::QueryParser parser;
    parser.set_database(db);
    parser.set_default_op(Xapian::Query::OP_OR);
    parser.set_stemmer(stemmer);
    parser.set_stemming_strategy(Xapian::QueryParser::STEM_SOME);
    parser.set_stopper(&stopper);

    const unsigned llimit = 100;
    const unsigned ulimit = 1000;
    const unsigned int MSET_SIZE = ulimit + 1;
    Xapian::MSet mset;
    unsigned long count = 0;

    string lowercase = "abcdefghijklmnopqrstuvwxyz";
    for (Xapian::TermIterator it = db.allterms_begin(); it != db.allterms_end(); it++) {
        string term = *it;
        if ((term.find_first_of(lowercase) == 0) &&
            (term.find_first_not_of(lowercase) == string::npos)) {

            unsigned int flags = Xapian::QueryParser::FLAG_DEFAULT;
            Xapian::Query query = parser.parse_query(term, flags);
            enquire.set_query(query);
            mset = enquire.get_mset(0, MSET_SIZE);

            if (mset.size() >= llimit &&
                    mset.size() <= ulimit) {
                termsFile << *it << endl;
            }
        }
        ++count;
        if (count % 100000 == 0) cerr << "count = " << count << endl;
    }

    termsFile.close();

    return 0;
}
