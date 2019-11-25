#ifndef DB_H
#define DB_H

#define IDX_LEN 8
#define DAT_LEN 64
typedef void* DBHANDLE;

DBHANDLE db_open();
void db_close(DBHANDLE _db);
int db_select(DBHANDLE _db, char* idx, char* buf);
int db_insert(DBHANDLE _db, char* idx, char* data);
int db_update(DBHANDLE _db, char* idx, char* data);
int db_delete(DBHANDLE _db, char* idx);

#endif