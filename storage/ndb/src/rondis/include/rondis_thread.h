/*
 * Copyright (c) 2025, 2025, Hopsworks and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#include "server_thread.h"
#include "pink_conn.h"
#include "redis_conn.h"
#include "pink_thread.h"
#include "dispatch_thread.h"
#include "rondb.h"

class RondisHandle : public pink::ServerHandle
{
public:
    RondisHandle() : counter(0) {}
    int CreateWorkerSpecificData(void **data) const override;

private:
    mutable std::mutex mutex;
    mutable int counter;
};

class RondisConn : public pink::RedisConn
{
public:
    RondisConn(
        int fd,
        const std::string &ip_port,
        pink::Thread *thread,
        void *worker_specific_data);
    virtual ~RondisConn() override = default;

protected:
  int DealMessage(const pink::RedisCmdArgsType &argv, std::string *response) override;

private:
    int _worker_id;
};

class RondisConnFactory : public pink::ConnFactory
{
public:
    virtual std::shared_ptr<pink::PinkConn> NewPinkConn(
        int connfd,
        const std::string &ip_port,
        pink::Thread *thread,
        void *worker_specific_data,
        [[maybe_unused]]/*todo remove?*/ pink::PinkEpoll *pink_epoll = nullptr
    ) const override;
};
