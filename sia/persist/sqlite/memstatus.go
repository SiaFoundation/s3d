package sqlite

// go-sqlite3 does not expose sqlite3_config, and SQLITE_CONFIG_MEMSTATUS is 9
// in sqlite3.h. The call is only valid before the first connection opens,
// which init guarantees. Without the statistics the heap limit pragmas and
// sqlite3_memory_used become no ops for the whole process.

/*
extern int sqlite3_config(int, ...);
static int disableMemStatus(void) { return sqlite3_config(9, 0); }
*/
import "C"

import "fmt"

func init() {
	if rc := C.disableMemStatus(); rc != 0 {
		panic(fmt.Sprintf("failed to disable sqlite memory statistics: rc %d", rc))
	}
}
