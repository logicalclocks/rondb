// sha3.h
// 19-Nov-11  Markku-Juhani O. Saarinen <mjos@iki.fi>

#ifndef SHA3_H
#define SHA3_H

#include <stddef.h>
#include <stdint.h>

#ifndef KECCAKF_ROUNDS
#define KECCAKF_ROUNDS 24
#endif

#ifndef ROTL64
#define ROTL64(x, y) (((x) << (y)) | ((x) >> (64 - (y))))
#endif

// state context
typedef struct
{
    union
    {                    // state:
        uint8_t b[200];  // 8-bit bytes
        uint64_t q[25];  // 64-bit words
    } st;
    int pt, rsiz, mdlen;  // these don't overflow
} sha3_ctx_t;

// Compression function.
void trnator_sha3_keccakf(uint64_t st[25]);

// OpenSSL - like interfece
int trantor_sha3_init(sha3_ctx_t *c,
                      int mdlen);  // mdlen = hash output in bytes
int trantor_sha3_update(sha3_ctx_t *c, const void *data, size_t len);
int trantor_sha3_final(void *md, sha3_ctx_t *c);  // digest goes to md

// compute a sha3 hash (md) of given byte length from "in"
void *trantor_sha3(const void *in, size_t inlen, void *md, int mdlen);

// SHAKE128 and SHAKE256 extensible-output functions
#define trantor_shake128_init(c) trnator_sha3_init(c, 16)
#define trantor_shake256_init(c) trnator_sha3_init(c, 32)
#define trantor_shake_update trnator_sha3_update

void trnator_shake_xof(sha3_ctx_t *c);
void trnator_shake_out(sha3_ctx_t *c, void *out, size_t len);

#endif
