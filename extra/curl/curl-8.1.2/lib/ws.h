#ifndef HEADER_CURL_WS_H
#define HEADER_CURL_WS_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) Daniel Stenberg, <daniel@haxx.se>, et al.
 *
 * This software is licensed as described in the file COPYING, which
 * you should have received as part of this distribution. The terms
 * are also available at https://curl.se/docs/copyright.html.
 *
 * You may opt to use, copy, modify, merge, publish, distribute and/or sell
 * copies of the Software, and permit persons to whom the Software is
 * furnished to do so, under the terms of the COPYING file.
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
 * KIND, either express or implied.
 *
 * SPDX-License-Identifier: curl
 *
 ***************************************************************************/
#include "curl_setup.h"

#ifdef USE_WEBSOCKETS

#ifdef USE_HYPER
#define REQTYPE void
#else
#define REQTYPE struct dynbuf
#endif

/* a client-side WS frame decoder, parsing frame headers and
 * payload, keeping track of current position and stats */
enum ws_dec_state {
  WS_DEC_INIT,
  WS_DEC_HEAD,
  WS_DEC_PAYLOAD
};

<<<<<<<< HEAD:extra/curl/curl-7.88.1/lib/ws.h
/* part of 'struct HTTP', when used in the 'struct SingleRequest' in the
   Curl_easy struct */
struct websocket {
  bool contfragment; /* set TRUE if the previous fragment sent was not final */
  unsigned char mask[4]; /* 32 bit mask for this connection */
  struct Curl_easy *data; /* used for write callback handling */
  struct dynbuf buf;
  size_t usedbuf; /* number of leading bytes in 'buf' the most recent complete
                     websocket frame uses */
  struct curl_ws_frame frame; /* the struct used for frame state */
  size_t stillblen; /* number of bytes left in the buffer to deliver in
                         the next curl_ws_recv() call */
  const char *stillb; /* the stillblen pending bytes are here */
  curl_off_t sleft; /* outstanding number of payload bytes left to send */
========
struct ws_decoder {
  int frame_age;        /* zero */
  int frame_flags;      /* See the CURLWS_* defines */
  curl_off_t payload_offset;   /* the offset parsing is at */
  curl_off_t payload_len;
  unsigned char head[10];
  int head_len, head_total;
  enum ws_dec_state state;
};

/* a client-side WS frame encoder, generating frame headers and
 * converting payloads, tracking remaining data in current frame */
struct ws_encoder {
  curl_off_t payload_len;  /* payload length of current frame */
  curl_off_t payload_remain;  /* remaining payload of current */
>>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6:extra/curl/curl-8.1.2/lib/ws.h
  unsigned int xori; /* xor index */
  unsigned char mask[4]; /* 32 bit mask for this connection */
  unsigned char firstbyte; /* first byte of frame we encode */
  bool contfragment; /* set TRUE if the previous fragment sent was not final */
};

/* A websocket connection with en- and decoder that treat frames
 * and keep track of boundaries. */
struct websocket {
  struct Curl_easy *data; /* used for write callback handling */
  struct ws_decoder dec;  /* decode of we frames */
  struct ws_encoder enc;  /* decode of we frames */
  struct bufq recvbuf;    /* raw data from the server */
  struct bufq sendbuf;    /* raw data to be sent to the server */
  struct curl_ws_frame frame;  /* the current WS FRAME received */
};

<<<<<<<< HEAD:extra/curl/curl-7.88.1/lib/ws.h
struct ws_conn {
  struct dynbuf early; /* data already read when switching to ws */
};

========
>>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6:extra/curl/curl-8.1.2/lib/ws.h
CURLcode Curl_ws_request(struct Curl_easy *data, REQTYPE *req);
CURLcode Curl_ws_accept(struct Curl_easy *data, const char *mem, size_t len);
size_t Curl_ws_writecb(char *buffer, size_t size, size_t nitems, void *userp);
void Curl_ws_done(struct Curl_easy *data);
CURLcode Curl_ws_disconnect(struct Curl_easy *data,
                            struct connectdata *conn,
                            bool dead_connection);
#else
#define Curl_ws_request(x,y) CURLE_OK
#define Curl_ws_done(x) Curl_nop_stmt
#define Curl_ws_free(x) Curl_nop_stmt
#endif

#endif /* HEADER_CURL_WS_H */
