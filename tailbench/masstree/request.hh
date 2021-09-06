
#define AGGR_FACTOR 256
#define AGGR_MASK ((AGGR_FACTOR) - 1)

#define KEY_SIZE 512
#define VAL_SIZE 512

enum ReqType { GET, PUT };
enum Status { SUCCESS, FAILURE };

struct Request {
    ReqType type;
    char key[KEY_SIZE];
    char val[VAL_SIZE];
};

struct Response {
    Status status;
};
