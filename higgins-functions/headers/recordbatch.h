#include <stdint.h>
#include "arrow.h"

struct WasmRecordBatch
{
    struct ArrowSchema *schema;
    int64_t n_columns;
    struct ArrowArray **columns;
};
