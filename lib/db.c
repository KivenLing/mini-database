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
    char *name;
    struct bplus_tree *tree;
};

/*
 * 初始化DB结构体
 */
static struct DB *_db_alloc(int namelen)
{
    struct DB *db = calloc(1, sizeof(struct DB));
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
    db->tree = bplus_tree_init(BPLUS_MAX_ORDER, BPLUS_MAX_ENTRIES);
    return db;
}

/*
 * 释放DB资源
 */
static void _db_free(struct DB *db)
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
static char *_buf_cpy(char *buf, int start, int len)
{
    /* 保证buf复制不越界 */
    assert(strlen(buf) > start + len);

    char *cpystr = (char *)malloc(len + 1);
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
int _buf_to_int(char *buf, int start, int len)
{
    char *dest_str = _buf_cpy(buf, start, len);
    int res = atoi(dest_str);
    free(dest_str);
    return res;
}

/*
 * 将整型转换成字符串，填充buf[start, start + len - 1]，填充fill字符
 */
static void _int_to_buf(char *buf, int num, int start, int len, char fill)
{
    assert(num >= 0);
    int i = -1;
    char stack[len + 1];
    int temp;
    do
    {
        i++;
        temp = num % 10;
        stack[i] = '0' + temp;
        num /= 10;
    } while (num > 0 && i <= len + 1);
    int j = 0;
    /* 填充数字字符 */
    while (i >= 0 && j < len)
    {
        buf[start + j] = stack[i];
        j++;
        i--;
    }
    /* 填充fill字符 */
    while (j < len)
    {
        buf[start + j] = fill;
        j++;
    }
}

/*
 * 将索引文件解析，转换成B+树
 * 一次读取4096字符进行解析
*/
static void _file_to_tree(struct DB *db)
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
            key_t key = _buf_to_int(buf, IDX_DAT_LEN * idx_in_block, IDX_LEN);
            /* 第 9 - 30 是记录off */
            int value = _buf_to_int(buf, IDX_DAT_LEN * idx_in_block + IDX_LEN + 1, IDX_DAT_LEN - IDX_LEN - 2);
            bplus_tree_put(db->tree, key, value);
        }
        block_num++;
    }
}

/*
 * 将叶子节点写入索引文件
 */
static int _tree_leaf_to_file(struct DB* db, struct bplus_leaf* leaf)
{
    if (leaf == NULL)
        return;
    int count = leaf->entries;
    char buf[IDX_DAT_LEN * count];
    for (int i = 0; i < count; i++)
    {
        int k = leaf->key[i];
        int v = leaf->data[i];
        _int_to_buf(buf, k, IDX_DAT_LEN * i, IDX_LEN, SAPCE);
        buf[IDX_DAT_LEN * i + IDX_LEN] = SEP;
        _int_to_buf(buf, IDX_DAT_LEN * i + IDX_LEN + 1, IDX_DAT_LEN - IDX_LEN - 2, SAPCE);
        buf[IDX_DAT_LEN * (i + 1) - 1] = NEWLINE;
    }
    if (write(db->idxfd, buf, IDX_DAT_LEN * count) < 0)
    {
        err_dump("_tree_leaf_to_file: write error");
        return -1;
    }
    return 1;
}

/*
 * 将B+树写入索引文件
 * 1. 遍历B+树，保存k, v
 * 2. 将k, v转换字符串，写入索引文件
 * 问题：这里用的方法效率较低, 原因是每次写入量小(主要)，单线程
 */
static void _tree_to_file(struct DB* db)
{
    struct bplus_leaf* first = first_leaf_node(db->tree);
    struct bplus_leaf* last = last_leaf_node(db->tree);
    while (first != last)
    {
        _tree_leaf_to_file(db, first);
        first = next_leaf_node(first);
    }
    _tree_leaf_to_file(db, last);
}

/*
 * 打开数据库，返回数据库句柄
 * 1. 初始化DB结构体
 * 2. 打开数据文件和索引文件
 * 3. 通过索引文件建立B+树
 */
DBHANDLE db_open()
{
    struct DB *db;
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
    _file_to_tree(db);
    return (DBHANDLE)db;
}

/*
 * 关闭数据库
 * 1. 将B+树写入索引文件
 * 2. 释放相关内存
 */
void db_close(DBHANDLE _db)
{
    struct DB *db = (struct DB *)_db;
    _tree_to_file(db);
    _db_free(db);
}
