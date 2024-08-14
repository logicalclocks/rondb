/*
   Copyright (c) 2004, 2024, Oracle and/or its affiliates.
   Copyright (c) 2022, 2023, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef SOCKET_CLIENT_HPP
#define SOCKET_CLIENT_HPP

#include "portlib/ndb_sockaddr.h"
#include "portlib/ndb_socket.h"
#include "util/NdbSocket.h"

class SocketAuthenticator;

class SocketClient {
  unsigned int m_connect_timeout_millisec;
  unsigned short m_last_used_port;
  SocketAuthenticator *m_auth;
  bool m_use_only_ipv4;
public:
  SocketClient(SocketAuthenticator *sa = nullptr);
  ~SocketClient();
  bool init(int af, bool use_only_ipv4);
  void set_connect_timeout(unsigned int timeout_millisec) {
    m_connect_timeout_millisec = timeout_millisec;
  }
  int bind(ndb_sockaddr local);
  NdbSocket connect(ndb_sockaddr server_addr);
  int authenticate(const NdbSocket &);

  ndb_socket_t m_sockfd;
};

#endif  // SOCKET_ClIENT_HPP
