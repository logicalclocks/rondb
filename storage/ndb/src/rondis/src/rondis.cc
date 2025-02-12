/*
   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.

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

#include <signal.h>
#include <atomic>
#include <mutex>

#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>
#include "common.h"
#include "rondis_thread.h"

using namespace pink;

static std::atomic<bool> running(false);

static void IntSigHandle(const int sig)
{
    printf("Catch Signal %d, cleanup...\n", sig);
    running.store(false);
    printf("server Exit");
}

static void SignalSetup()
{
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, &IntSigHandle);
    signal(SIGQUIT, &IntSigHandle);
    signal(SIGTERM, &IntSigHandle);
}

int main(int argc, char *argv[])
{
    int port = 6379;
    const char *connect_string = "localhost:13000";
    int worker_threads = 2;
    if (argc != 4)
    {
        printf("Not receiving 3 arguments, just using defaults\n");
    }
    else
    {
        port = atoi(argv[1]);
        connect_string = argv[2];
        worker_threads = atoi(argv[3]);
    }
    printf("Server will listen to %d and connect to MGMd at %s\n",
      port, connect_string);

    if (worker_threads < RONDIS_MAX_CONNECTIONS) {
        printf("Number of worker threads must be at least %d, otherwise"
               " we are wasting resources\n", RONDIS_MAX_CONNECTIONS);
        return -1;
    }

    ndb_objects.resize(worker_threads);

    if (setup_rondb(connect_string, worker_threads) != 0)
    {
        printf("Failed to setup RonDB environment\n");
        return -1;
    }
    SignalSetup();

    ConnFactory *conn_factory = new RondisConnFactory();

    RondisHandle *handle = new RondisHandle();

    ServerThread *my_thread = NewDispatchThread(
      port, worker_threads, conn_factory, 1000, 1000, handle);
    if (my_thread->StartThread() != 0)
    {
        printf("StartThread error happened!\n");
        rondb_end();
        return -1;
    }

    running.store(true);
    while (running.load())
    {
        sleep(1);
    }
    my_thread->StopThread();

    delete my_thread;
    delete handle;
    delete conn_factory;

    rondb_end();

    return 0;
}
