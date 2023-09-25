/*********************************************************************
 * Filename:   sha256.h
 * Author:     Brad Conte (brad AT bradconte.com)
 * Copyright:
 * Disclaimer: This code is presented "as is" without any guarantees.
 * Details:    Defines the API for the corresponding SHA1 implementation.
 *********************************************************************/

#ifndef SHA256_H
#define SHA256_H

/*************************** HEADER FILES ***************************/
#ifndef _WIN32
#include <stddef.h>
#else
#include <Windows.h>
#endif

/****************************** MACROS ******************************/
#define SHA256_BLOCK_SIZE 32  // SHA256 outputs a 32 byte digest

/**************************** DATA TYPES ****************************/

#ifndef _WIN32
typedef unsigned char BYTE;  // 8-bit byte
typedef unsigned int WORD;  // 32-bit word, change to "long" for 16-bit machines
#endif

typedef struct
{
    BYTE data[64];
    WORD datalen;
    unsigned long long bitlen;
    WORD state[8];
} SHA256_CTX;

/*********************** FUNCTION DECLARATIONS **********************/
void trantor_sha256_init(SHA256_CTX *ctx);
void trantor_sha256_update(SHA256_CTX *ctx, const BYTE data[], size_t len);
void trantor_sha256_final(SHA256_CTX *ctx, BYTE hash[]);

#endif  // SHA256_H