<<<<<<<< HEAD:extra/curl/curl-7.88.1/lib/vquic/curl_msh3.h
#ifndef HEADER_CURL_VQUIC_CURL_MSH3_H
#define HEADER_CURL_VQUIC_CURL_MSH3_H
========
#ifndef HEADER_CURL_CF_HAPROXY_H
#define HEADER_CURL_CF_HAPROXY_H
>>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6:extra/curl/curl-8.1.2/lib/cf-haproxy.h
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
#include "urldata.h"

#if !defined(CURL_DISABLE_PROXY)

CURLcode Curl_cf_haproxy_insert_after(struct Curl_cfilter *cf_at,
                                      struct Curl_easy *data);

<<<<<<<< HEAD:extra/curl/curl-7.88.1/lib/vquic/curl_msh3.h
void Curl_msh3_ver(char *p, size_t len);

CURLcode Curl_cf_msh3_create(struct Curl_cfilter **pcf,
                             struct Curl_easy *data,
                             struct connectdata *conn,
                             const struct Curl_addrinfo *ai);

bool Curl_conn_is_msh3(const struct Curl_easy *data,
                       const struct connectdata *conn,
                       int sockindex);
========
extern struct Curl_cftype Curl_cft_haproxy;
>>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6:extra/curl/curl-8.1.2/lib/cf-haproxy.h

#endif /* !CURL_DISABLE_PROXY */

<<<<<<<< HEAD:extra/curl/curl-7.88.1/lib/vquic/curl_msh3.h
#endif /* HEADER_CURL_VQUIC_CURL_MSH3_H */
========
#endif /* HEADER_CURL_CF_HAPROXY_H */
>>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6:extra/curl/curl-8.1.2/lib/cf-haproxy.h
