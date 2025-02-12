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

#include "rondis_thread.h"

using namespace pink;

/*
    We define this so each connection knows from which worker thread it is
    running from. This enables us to to distribute Ndb objects across
    multiple worker threads.
*/
int RondisHandle::CreateWorkerSpecificData(void **data) const
{
    std::lock_guard<std::mutex> lock(mutex);
    *data = new int(counter++);
    return 0;
}

RondisConn::RondisConn(
    int fd,
    const std::string &ip_port,
    Thread *thread,
    void *worker_specific_data)
    : RedisConn(fd, ip_port, thread)
{
    int worker_id = *static_cast<int *>(worker_specific_data);
    _worker_id = worker_id;
}

int RondisConn::DealMessage(const RedisCmdArgsType &argv, std::string *response)
{
    /*    
        printf("Received Redis message: ");
        for (int i = 0; i < argv.size(); i++)
        {
            printf("%s ", argv[i].c_str());
        }
        printf("\n");
    */
    return rondb_redis_handler(argv, response, _worker_id);
}

std::shared_ptr<PinkConn> RondisConnFactory::NewPinkConn(
    int connfd,
    const std::string &ip_port,
    Thread *thread,
    void *worker_specific_data,
    [[maybe_unused]]/*todo remove?*/ PinkEpoll *pink_epoll
) const
{
    return std::make_shared<RondisConn>(connfd,
                                        ip_port,
                                        thread,
                                        worker_specific_data);
}

std::vector<Ndb *> ndb_objects;
std::map<std::string, std::string> db;
