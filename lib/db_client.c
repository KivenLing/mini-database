#include "apue.h"
#include "bplustree.h"
#include "db.h"
#include<fcntl.h>
#include<pthread.h>

#define SELECT "select"
#define UPDATE "update"
#define INSERT "insert"
#define DELETE "delete"

#define SEPSTR " \n"
#define NTHREAD 10
#define LINELEN 256
#define SQLARGC 2

#define COMMANDLEN 10
#define ARG1LEN 10
#define ARG2LEN 54
pthread_mutex_t pipe_mutex = PTHREAD_MUTEX_INITIALIZER;
/*
 * 我的想法是实现任务发布-实现的生产者-消费者模型
 */
typedef struct sql_command
{
    char command[COMMANDLEN];/* 查询命令 */
    char arg1[ARG1LEN];
    char arg2[ARG2LEN];
} sql_command;

DBHANDLE _db_server;

/*
 * 一个简单的sql语句解析
 * 默认一张表
 * 语法 command arg1 [arg2]
 * command 包含 select update insert delete
 * arg1 索引值 
 * arg2 数据值(不包含空格符 换行符)
 */
static sql_command* parser(char* sql)
{
    sql_command* sc = (sql_command*)malloc(sizeof(sql_command));
    char* token;
    char* temp;
    token = strtok_r(sql, SEPSTR, &temp);
    if (token == NULL)
    {
        free(sc);
        return NULL;
    }
    strncpy(sc->command, token, COMMANDLEN);
    token = strtok_r(NULL, SEPSTR, &temp);
    if (token == NULL)
    {
        free(sc);
        return NULL;
    }
    strncpy(sc->arg1, token, ARG1LEN);
    token = strtok_r(NULL, SEPSTR, &temp);
    if (token != NULL)
    {
        strncpy(sc->arg2, token, ARG2LEN);
    }
    // printf("token len = %ld\n", sizeof(token));
    // sc->command = (char*)malloc(sizeof(token) + 2);
    // strncpy(sc->command, token, sizeof(token));
    // printf("command len = %ld\n", sizeof(sc->command));
    // token = strtok_r(NULL, SEPSTR, &temp);
    // int i = 0;
    // while (token != NULL && i < SQLARGC)
    // {
    //     sc->args[i] = (char*)malloc(sizeof(token) + 2);
    //     strncpy(sc->args[i], token, sizeof(token));
    //     token = strtok_r(NULL, SEPSTR, &temp);
    //     i++;
    //     sc->args[i] = NULL;
    // }
    return sc;
}

// static void free_sql_com(sql_command* sql)
// {
//     if (sql == NULL)
//     {
//         return;
//     }
//     int i = 0;
//     while(sql->args[i] != NULL && i < SQLARGC)
//     {
//         printf("free arg[%d]\n", i);
//         free(sql->args[i]);
//         i++;
//     }
//     printf("free command\n");
//     printf("command = %s\n", sql->command);
//     free(sql->command);
//     printf("free sql\n");
//     free(sql);
// }

static void * sql_exec(void *arg)
{
    int n;
    sql_command * sql_com;
    int* pfd = (int*)arg;
    char sql[LINELEN];
    int res;
    for (;;)
    {
        pthread_mutex_lock(&pipe_mutex);
        n = read(pfd[0], sql, LINELEN);
        pthread_mutex_unlock(&pipe_mutex);
        if (n <= 0)
        {
            break;
        }
        printf("n = %d\n", n);
        printf("sql = %s\n", sql);
        sql_com = parser(sql);
        // printf("comand = %s\n", sql_com->command);
        // for (int i = 0; sql_com->args[i] != NULL && i < SQLARGC; i++)
        // {
        //     printf("arg[%d] = %s\n", i, sql_com->args[i]);
        // }
        // if (sql_com->args == NULL)
        // {
        //     printf("command error!\n");
        //     printf("usage: slelct|insert|delete|update key [data]\n");
        //     free(sql_com);
        //     continue;
        // }
        // if (strlen(sql_com->args[0]) > IDX_LEN) /* 检查key范围 */
        // {
        //     printf("key = %s too big\n", sql_com->args[0]);
        //     free(sql_com);
        //     continue;
        // }
        // if (strcmp(SELECT, sql_com->command) == 0)
        // {
        //     char* data = (char*)malloc(DAT_LEN + 1);
        //     data[DAT_LEN] = '\0';
        //     res = db_select(_db_server, sql_com->args[0], data);
        //     if (res < 0)
        //     {
        //         printf("no data for key = %s\n", sql_com->args[0]);
        //         free(sql_com);
        //         continue;
        //     }
        //     printf("key     :data\n");
        //     printf("%s",data);
        //     free(data);
        // }
        // else if (strcmp(INSERT, sql_com->command) == 0)
        // {
        //     if (sql_com->args[1] == NULL)
        //     {
        //         printf("usage: insert key data\n");
        //         free(sql_com);
        //         continue;
        //     }
        //     res = db_insert(_db_server, sql_com->args[0], sql_com->args[1]);
        //     if (res < 0)
        //     {
        //         printf("insert error");
        //         free(sql_com);
        //         continue;
        //     }
        //     printf("insert ok\n");
        // }
        // else if (strcmp(UPDATE, sql_com->command) == 0)
        // {
        //     if (sql_com->args[1] == NULL)
        //     {
        //         printf("usage: update key data\n");
        //         free(sql_com);
        //         continue;
        //     }
        //     res = db_update(_db_server, sql_com->args[0], sql_com->args[1]);
        //     if (res < 0)
        //     {
        //         printf("update error\n");
        //         free(sql_com);
        //         continue;
        //     }
        //     printf("update ok\n");
        // }
        // else if (strcmp(DELETE, sql_com->command) == 0)
        // {
        //     res = db_delete(_db_server, sql_com->args[0]);
        //     if (res < 0)
        //     {
        //         printf("no data for key = %s\n", sql_com->args[0]);
        //         free(sql_com);
        //         continue;
        //     }
        //     printf("update ok\n");
        // }
        // else
        // {
        //     printf("command error!\n");
        //     printf("usage: slelct|insert|delete|update key [data]\n");
        // }
        printf("free begin\n");
        free(sql_com);
        printf("free ok!\n");
    }
    pthread_exit((void *)0);
}

int db_client()
{
    //_db_server = db_open();
    int i, err;
    pthread_t wtid[NTHREAD];
    char buf[LINELEN];
    int pfd[2];
    if(pipe(pfd)<0)
	{
		err_dump("create pipe error!\n");
		return -1;
	}
    for (i = 0; i < NTHREAD; i++)
    {
        err = pthread_create(&wtid[i], NULL, sql_exec, pfd);
        if (err != 0)
        {
            printf("create the %dth thread error!\n",i);
			return -1;
        }
        
    }
    
    /* 从终端读取命令， 测试可以重定向到一个文件, 单生产者*/
    while (fgets(buf, LINELEN, stdin))
    {
        printf("buf = %s\n", buf);
        ssize_t wsz = write(pfd[1], buf, LINELEN);
        printf("wsz = %ld\n", wsz);
    }
    
    close(pfd[1]);
	for(i = 0; i < NTHREAD; i++)	
	{
		err = pthread_join(wtid[i], NULL);
		if(err != 0)
		{
			err_dump("join the %dth thread error!\n",i);
			return -1;
		}
	}
	close(pfd[0]);
    //db_close(_db_server);
    return 0;
}

int main(int argc, char const *argv[])
{
    db_client();
    return 0;
}
