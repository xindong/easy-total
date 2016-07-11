#ifndef BASE_XLIB_XDEFINE_H_
#define BASE_XLIB_XDEFINE_H_

#include <stdlib.h>
#include <string.h>
#include "xlib/xNewMem.h"

#pragma warning(disable:4099)  // LNK4099
#pragma warning(disable:4996)
#pragma warning(disable:4200)
#pragma warning(disable:4355)

typedef int INT;
typedef unsigned int UINT;

typedef char CHAR;
typedef char SBYTE;
typedef short SWORD;
typedef int SDWORD;
typedef long long SQWORD;

typedef unsigned char BYTE;
typedef unsigned short WORD;
typedef unsigned int DWORD;
typedef unsigned long long QWORD;

typedef float FLOAT;

#define SBYTE_MIN ((SBYTE)0x80)  // -128
#define SBYTE_MAX ((SBYTE)0x7f)  // 127
#define SWORD_MIN ((SWORD)0x8000)  // -32768
#define SWORD_MAX ((SWORD)0x7fff)  // 32767
#define SDWORD_MIN ((SDWORD)0x80000000)  // -2147483648
#define SDWORD_MAX ((SDWORD)0x7fffffff)  // 2147483647
#define SQWORD_MIN ((SQWORD)0x8000000000000000)	 // -9223372036854775808
#define SQWORD_MAX ((SQWORD)0x7fffffffffffffff)	 // 9223372036854775807

#define BYTE_MIN ((BYTE)0)
#define BYTE_MAX ((BYTE)0xff)  // 255
#define WORD_MIN ((WORD)0)
#define WORD_MAX ((WORD)0xffff)  // 65535
#define DWORD_MIN ((DWORD)0)
#define DWORD_MAX ((DWORD)0xffffffff)	 // 4294967295
#define QWORD_MIN ((QWORD)0)
#define QWORD_MAX ((QWORD)0xffffffffffffffff)	 // 18446744073709551615

#define ONE_THOUSAND	1000
#define ONE_MILLION		1000000
#define ONE_BILLION		1000000000

#define DAY_T 86400
#define WEEK_T (7 * 86400)

#define _CRT_SECURE_NO_WARNINGS

const DWORD LEN_8 = 8;
const DWORD LEN_16 = 16;
const DWORD LEN_32 = 32;
const DWORD LEN_40 = 40;
const DWORD LEN_64 = 64;
const DWORD LEN_128 = 128;
const DWORD LEN_256 = 256;
const DWORD LEN_512 = 512;

const DWORD MAX_NAMESIZE = 64;
const DWORD MAX_FILENAMESIZE = 255;

#define SHA1_LEN 40
#define SERIAL_LEN 36
#define DEFAULT_CONFIG_PATH "./Config/"

#define _IsUnused __attribute__ ((__unused__))

#define _TY(x) #x
#define _S(x) _TY(x)

#define MAX_SERVER_EVENT 256
#define MAX_DBDATA_SIZE (1<<24)
#define MAX_BINDATA_SIZE (1<<17)

#define USER_SOURCE_LEN 128
#define ACC_NAME_LEN 64
#define RAND_CODE_LEN 20  // 随机码长度
#define MD5_LEN 32  // md5码长度
#define CITIZEN_ID_LEN 18  // 公民身份证长度
#define CITIZEN_NAME_LEN 20  // 公民姓名长度

#endif  // BASE_XLIB_XDEFINE_H_

//EPOLL等待超时时间,单位:毫秒
#define EPOLL_WAIT_TIMEOUT 10
#define EPOLL_RW_WAIT_TIMEOUT 50
//主进程单帧运行时间,单位:微秒
#define SERVER_FRAME_TIME 5000
//SessionServer单帧运行时间,单位:微秒
#define SESSION_SERVER_FRAME_TIME 20000
//主进程单帧时间运行时间告警值,单位:微秒
#define WARN_SERVER_FRAME_TIME 100000
//listen线程单帧运行时间,单位：微秒
#define LISTEN_THREAD_FRAME_TIME 5000
//task线程单帧运行时间,单位:微秒
#define TASK_THREAD_FRAME_TIME 5000

