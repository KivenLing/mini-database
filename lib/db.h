#ifndef DB_H
#define DB_H

typedef void* DBHANDLE;

DBHANDLE db_open();
void db_close(DBHANDLE _db);
int select(DBHANDLE _db, char* idx);
int insert(DBHANDLE _db, char* idx, char* data);
int update(DBHANDLE _db, char* idx, char* data);
int delete(DBHANDLE _db, char* idx);

#endif