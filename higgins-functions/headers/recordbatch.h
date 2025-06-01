#include <stdint.h>
#include "arrow.h"

struct FFIRecordBatch
{
    int64_t n_columns;
    struct ArrowSchema **schema;
    struct ArrowArray **columns;
};
