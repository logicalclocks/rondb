<<<<<<<< HEAD:extra/curl/curl-7.88.1/lib/idn.h
#ifndef HEADER_CURL_IDN_H
#define HEADER_CURL_IDN_H
========
#ifndef HEADER_CURL_H2_PROXY_H
#define HEADER_CURL_H2_PROXY_H
>>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6:extra/curl/curl-8.1.2/lib/cf-h2-proxy.h
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

#ifdef USE_WIN32_IDN
bool Curl_win32_idn_to_ascii(const char *in, char **out);
#endif /* USE_WIN32_IDN */
bool Curl_is_ASCII_name(const char *hostname);
CURLcode Curl_idnconvert_hostname(struct hostname *host);
#if defined(USE_LIBIDN2) || defined(USE_WIN32_IDN)
#define USE_IDN
void Curl_free_idnconverted_hostname(struct hostname *host);
char *Curl_idn_decode(const char *input);
#ifdef USE_LIBIDN2
#define Curl_idn_free(x) idn2_free(x)
#else
#define Curl_idn_free(x) free(x)
#endif

<<<<<<<< HEAD:extra/curl/curl-7.88.1/lib/idn.h
#else
#define Curl_free_idnconverted_hostname(x)
#define Curl_idn_decode(x) NULL
#endif
#endif /* HEADER_CURL_IDN_H */
========
#if defined(USE_NGHTTP2) && !defined(CURL_DISABLE_PROXY)

CURLcode Curl_cf_h2_proxy_insert_after(struct Curl_cfilter *cf,
                                       struct Curl_easy *data);

extern struct Curl_cftype Curl_cft_h2_proxy;


#endif /* defined(USE_NGHTTP2) && !defined(CURL_DISABLE_PROXY) */

#endif /* HEADER_CURL_H2_PROXY_H */
>>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6:extra/curl/curl-8.1.2/lib/cf-h2-proxy.h
