/*********************************************************************
 * Filename:   md5.h
 * Author:     Brad Conte (brad AT bradconte.com)
 * Copyright:
 * Disclaimer: This code is presented "as is" without any guarantees.
 * Details:    Defines the API for the corresponding MD5 implementation.
 *********************************************************************/

#ifndef MD5_H
#define MD5_H

/*************************** HEADER FILES ***************************/
#include <stddef.h>

/****************************** MACROS ******************************/
#define MD5_BLOCK_SIZE 16  // MD5 outputs a 16 byte digest

/**************************** DATA TYPES ****************************/

#ifndef _WIN32
typedef unsigned char BYTE;  // 8-bit byte
typedef unsigned int WORD;  // 32-bit word, change to "long" for 16-bit machines
#else
#include <Windows.h>
#endif

typedef struct
{
    BYTE data[64];
    WORD datalen;
    unsigned long long bitlen;
    WORD state[4];
} MD5_CTX;

/*********************** FUNCTION DECLARATIONS **********************/
void trantor_md5_init(MD5_CTX *ctx);
void trantor_md5_update(MD5_CTX *ctx, const BYTE data[], size_t len);
void trantor_md5_final(MD5_CTX *ctx, BYTE hash[]);

#endif  // MD5_H