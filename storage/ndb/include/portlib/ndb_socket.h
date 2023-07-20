/*
   Copyright (c) 2008, 2023, Oracle and/or its affiliates.
   Copyright (c) 2022, 2023, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef NDB_SOCKET_H
#define NDB_SOCKET_H

<<<<<<< HEAD
#ifndef IN_IS_ADDR_UNSPECIFIED
#define IN_IS_ADDR_UNSPECIFIED(a) (((long int) (a)->s_addr) == 0x00000000)
#endif /* IN_IS_ADDR_UNSPECIFIED */

#ifdef _WIN32
#include <winsock2.h>
using socket_t = SOCKET;
#else
using socket_t = int;
#endif

struct ndb_socket_t {
  socket_t s;
};
=======
#include "portlib/ndb_sockaddr.h"
>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6

/* Include platform-specific inline functions */
#ifdef _WIN32
#include "ndb_socket_win32.h"
#else
#include "ndb_socket_posix.h"
#endif

// Default constructed ndb_socket_t is always invalid
static_assert(ndb_socket_t{}.s == INVALID_SOCKET);

/* Functions for creating and initializing ndb_socket_t */

static inline
void ndb_socket_create_from_native(ndb_socket_t & sock, socket_t s)
{
  sock.s = s;
}

static inline
void ndb_socket_create(ndb_socket_t &sock)
{
  sock.s = INVALID_SOCKET;
}

static inline
<<<<<<< HEAD
ndb_socket_t ndb_socket_create_return(ndb_socket_t & sock)
{
  sock.s = INVALID_SOCKET;
  return sock;
=======
ndb_socket_t ndb_socket_create(int af)
{
  return ndb_socket_t{socket(af, SOCK_STREAM, IPPROTO_TCP)};
>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6
}

static inline socket_t
ndb_socket_get_native(ndb_socket_t s)
{
  return s.s;
}

static inline
void ndb_socket_initialize(ndb_socket_t *s)
{
  s->s= INVALID_SOCKET;
}

static inline
void ndb_socket_invalidate(ndb_socket_t *s)
{
  s->s= INVALID_SOCKET;
}

static inline
int ndb_socket_valid(ndb_socket_t s)
{
  return (s.s != INVALID_SOCKET);
}

// Returns 0 on success, -1 on error
static inline
int ndb_getsockopt(ndb_socket_t s, int level, int optname, int *optval)
{
  socklen_t optlen = sizeof(int);
  int r = getsockopt(s.s, level, optname, (char*)optval, &optlen);
  return (r == -1) ? -1 : 0;
}

// Returns 0 on success, -1 on error
static inline
int ndb_setsockopt(ndb_socket_t s, int level, int optname, const int *optval)
{
  int r = setsockopt(s.s, level, optname, (const char*)optval, sizeof(int));
  return (r == -1) ? -1 : 0;
}

// Returns 0 on success, -1 on error
static inline
int ndb_socket_reuseaddr(ndb_socket_t s, int enable)
{
  const int on = enable;
  return ndb_setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on);
}

<<<<<<< HEAD
static inline
void ndb_socket_create_ipv4(ndb_socket_t& s, int type, int protocol)
{
  ndb_socket_create_from_native(s, socket(AF_INET, type, protocol));
  if (!ndb_socket_valid(s))
    ndb_socket_invalidate(&s);
}

/* Create an IPv6 socket as used by NDB for network communications,
   allowing mapped IPv4 addresses (socket option IPV6_V6ONLY is false).
*/
static inline
void ndb_socket_create_dual_stack(ndb_socket_t& s, int type, int protocol)
{
  ndb_socket_create_from_native(s, socket(AF_INET6, type, protocol));
  if (!ndb_socket_valid(s))
  {
    ndb_socket_invalidate(&s);
    return;
  }

  int on = 0;
  if (ndb_setsockopt(s, IPPROTO_IPV6, IPV6_V6ONLY, &on))
  {
    ndb_socket_close(s);
    ndb_socket_invalidate(&s);
  }
=======
// Returns 0 on success, -1 on error
static inline
int ndb_socket_dual_stack(ndb_socket_t s, int enable)
{
  int on = !enable;
  return ndb_setsockopt(s, IPPROTO_IPV6, IPV6_V6ONLY, &on);
>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6
}

/* Returns 0 on success, -1 on error
   Use ndb_socket_errno() to retrieve error
*/
static inline
int ndb_bind(ndb_socket_t s, const ndb_sockaddr *addr)
{
  int r = bind(s.s, addr->get_sockaddr(), addr->get_sockaddr_len());
  return r ? -1 : 0;
}

static inline
int ndb_bind_inet4(ndb_socket_t s, const struct sockaddr_in *addr)
{
  int r = bind(s.s, (const struct sockaddr*)addr, sizeof(struct sockaddr_in));
  return r ? -1 : 0;
}

/* Returns 0 on success, -1 on error
   Use ndb_socket_errno() to retrieve error
*/
static inline
int ndb_listen(ndb_socket_t s, int backlog)
{
  int r = listen(s.s, backlog);
  return r ? -1 : 0;
}

/* Returns 0 on success.
   Use ndb_socket_errno() to retrieve error
*/
static inline
ndb_socket_t ndb_accept(ndb_socket_t s, ndb_sockaddr *addr)
{
<<<<<<< HEAD
  ndb_socket_create_from_native(s, accept(s.s, addr, addrlen) );
  return s;
}

static inline
int ndb_connect_inet(ndb_socket_t s, const struct sockaddr_in *addr)
{
  return connect(s.s, (const struct sockaddr*)addr,
                 sizeof(struct sockaddr_in));
=======
  ndb_sockaddr::storage_type sa;
  socklen_t salen = sizeof(sa);
  socket_t sock = accept(s.s, &sa.common, &salen);
  if (sock != -1 && addr != nullptr)
  {
    *addr = ndb_sockaddr(&sa.common, salen);
  }
  return ndb_socket_create_from_native(sock);
>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6
}

/* Returns 0 on success.
   Use ndb_socket_errno() to retrieve error
*/
static inline
int ndb_connect(ndb_socket_t s, const ndb_sockaddr *addr)
{
  return connect(s.s, addr->get_sockaddr(), addr->get_sockaddr_len());
}

// Returns 0 on success, 1 on error
static inline
<<<<<<< HEAD
int ndb_getpeername(ndb_socket_t s, struct sockaddr *addr)
=======
int ndb_getpeername(ndb_socket_t s, ndb_sockaddr *addr)
>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6
{
  ndb_sockaddr::storage_type sa;
  socklen_t salen = sizeof(sa);
  if (getpeername(s.s, &sa.common, &salen) == -1)
    return 1;
  *addr = ndb_sockaddr(&sa.common, salen);
  return 0;
}

// Returns 0 on success, 1 on error
static inline
int ndb_getsockname(ndb_socket_t s, ndb_sockaddr *addr)
{
  ndb_sockaddr::storage_type sa;
  socklen_t salen = sizeof(sa);
  if (getsockname(s.s, &sa.common, &salen) == -1)
    return 1;
  *addr = ndb_sockaddr(&sa.common, salen);
  return 0;
}

// Returns 0 on success, 1 on error
static inline
int ndb_getsockname4(ndb_socket_t s, struct sockaddr_in *addr)
{
  socklen_t len = sizeof(struct sockaddr_in);
  if(getsockname(s.s, (struct sockaddr*) addr, &len))
    return 1;

  return 0;
}

// Returns 0 on success or ndb_socket_errno() on failure
static inline
<<<<<<< HEAD
int ndb_socket_connect_address(ndb_socket_t s, struct sockaddr_in6 *a)
{
  socklen_t addrlen= sizeof(*a);
  if(getpeername(s.s, (struct sockaddr*)a, &addrlen))
    return ndb_socket_errno();
=======
int ndb_socket_connect_address(ndb_socket_t s, ndb_sockaddr *a)
{
  if(ndb_getpeername(s, a) == -1) return ndb_socket_errno();

>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6
  return 0;
}

// Returns 0 on success, 1 on error
static inline
int ndb_socket_get_port(ndb_socket_t s, unsigned short *port)
{
<<<<<<< HEAD
  struct sockaddr_in6 servaddr;
  if(ndb_getsockname(s, &servaddr) == 1) return 1;
=======
  ndb_sockaddr servaddr;
  if(ndb_getsockname(s, &servaddr) < 0) return 1;
>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6

  *port= servaddr.get_port();
  return 0;
}

static inline
int ndb_socket_get_port4(ndb_socket_t s, unsigned short *port)
{
  struct sockaddr_in servaddr;
  socklen_t sock_len = sizeof(servaddr);
  if(getsockname(s.s, (struct sockaddr*)&servaddr, &sock_len) < 0) {
    return 1;
  }

  *port= ntohs(servaddr.sin_port);
  return 0;
}

static inline
void ndb_socket_close_with_reset(ndb_socket_t & sock, bool with_reset = false)
{
  if (with_reset)
  {
    // Force hard reset of the socket by turning on linger with timeout 0
    struct linger hard_reset = {1, 0};
    setsockopt(sock.s, SOL_SOCKET, SO_LINGER,
               (char*)&hard_reset, sizeof(hard_reset));
  }

  ndb_socket_close(sock);
}

/* Create a pair of connected sockets.
   Returns 0 on success.
*/
int ndb_socketpair(ndb_socket_t s[2]);

#endif
