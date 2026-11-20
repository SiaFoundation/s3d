package sqlite

// go-sqlite3 does not expose sqlite3_config, and SQLITE_CONFIG_MEMSTATUS is 9
// in sqlite3.h. The call is only valid before the first connection opens,
// which init guarantees.

/*
extern int sqlite3_config(int, ...);
static int disableMemStatus(void) { return sqlite3_config(9, 0); }
*/
import "C"

func init() {
	C.disableMemStatus()
}
