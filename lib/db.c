#include "apue.h"
#include "db.h"
#include "bplustree.h"
#include <fcntl.h>
#include <pthread.h>
#include <assert.h>

/* 基本符号 */
#define SEP ':'
#define SAPCE ' '
#define NEWLINE '\n'

#define DB_NAME "apue"
#define DB_NAME_MAX_LEN 128

/* 数据字段相关 */
/*
 * [0]     [7] | [ 8 ] | [9]                 [62] | [   63  ]
 * [   idx   ] | [SEP] | [         data         ] | [NEWLINE]
*/
#define IDX_LEN 8
#define DAT_LEN 64
#define MAX_IDX 99999999

/* 索引字段相关 */
/*
 * [0]     [7] | [ 8 ] | [9]                 [30] | [   31  ]
 * [   idx   ] | [SEP] | [         off          ] | [NEWLINE]
*/
#define IDX_DAT_LEN 32

#define ID_FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
#define IDX_FILE_FLAG (O_RDWR | O_CREAT)
#define DAT_FILE_FLAG (O_RDWR | O_CREAT | O_APPEND)


struct DB
{
    int datfd;
    int idxfd;
    off_t cur_off;
    char* name;
    struct bplus_tree* tree;
};

/*
 * 初始化DB结构体
 */
static struct DB* _db_alloc(int namelen)
{
    struct  DB *db = calloc(1, sizeof(struct DB));
    if (db = NULL)
    {
        err_dump("_db_alloc: calloc error");
    }
    db->datfd = db->idxfd = -1;
    db->cur_off = -1;
    if ((db->name = malloc(namelen + 1)) == NULL)
    {
        err_dump("_db_alloc: malloc error");
    }
    return db;
}

/*
 * 释放DB资源
 */
static void _db_free(struct DB* db)
{
    if (db)
    {
        if (db->idxfd >= 0)
        {
            close(db->idxfd);
        }
        if (db->datfd >= 0)
        {
            close(db->datfd);
        }
        free(db->name);
        if (db->tree)
        {
            bplus_tree_deinit(db->tree);
        }
        free(db);
    }
    
}

/*
 * 将buf[start, start + len - 1]字符复制
 */
static char* _buf_cpy(char* buf, int start, int len)
{
    /* 保证buf复制不越界 */
    assert(strlen(buf) > start + len);

    char* cpystr = (char*)malloc(len + 1);
    int i;
    for (i = 0; i < len; i++)
    {
        cpystr[i] = buf[start + i];
    }
    cpystr[i] = '\0';
    return cpystr;
}

/*
 * 将buf[start, start + len - 1]字符转换成数字
 */
int _buf_to_int(char* buf, int start, int len)
{
    char* dest_str = _buf_cpy(buf, start, len);
    int res = atoi(dest_str);
    free(dest_str);
    return res;
}

/*
 * 
 */

char* _int_to_buf()
{

}

/*
 * 将索引文件解析，转换成B+树
 * 一次读取4096字符进行解析
*/
static void _file_to_tree(struct DB* db)
{
    char buf[MAXLINE];
    int idx_in_block = -1;
    int block_num = 0;
    int kv_num; /* 索引KV在一块中的数量 */
    int readlen;
    while ((readlen = pread(db->idxfd, buf, MAXLINE, MAXLINE * block_num)) > 0)
    {
        kv_num = readlen / IDX_DAT_LEN;
        for (idx_in_block = 0; idx_in_block <= kv_num; idx_in_block++)
        {
            
        }
        
    }
    

}

/*
 * 打开数据库，返回数据库句柄
 * 1. 初始化DB结构体
 * 2. 打开数据文件和索引文件
 * 3. 通过索引文件建立B+树
 */
DBHANDLE db_open()
{
    struct DB* db;
    char dat_file[DB_NAME_MAX_LEN + 5];
    char idx_file[DB_NAME_MAX_LEN + 5];
    int namelen = strlen(DB_NAME);
    db = _db_alloc(namelen);
    strcpy(db->name, DB_NAME);
    strcpy(dat_file, db->name);
    strcat(dat_file, ".dat");
    strcpy(idx_file, db->name);
    strcat(idx_file, ".idx");
    db->datfd = open(dat_file, DAT_FILE_FLAG, ID_FILE_MODE);
    db->idxfd = open(idx_file, IDX_FILE_FLAG, ID_FILE_MODE);
    if (db->datfd < 0 || db->idxfd < 0)
    {
        _db_free(db);
        err_msg("db_open: file open error");
        return NULL;
    }
    db->cur_off = lseek(db->datfd, 0, SEEK_END);
    
}
