/*
 * Copyright (C) 2022 Hopsworks AB
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

#ifndef DATA_ACCESS_RONDB_SRC_NDB_OBJECT_POOL_HPP_
#define DATA_ACCESS_RONDB_SRC_NDB_OBJECT_POOL_HPP_

#include <NdbApi.hpp>
#include <list>
#include <mutex>
#include "rdrs-dal.h"

class NdbObjectPool {
 private:
  std::list<Ndb *> __ndb_objects;
  std::mutex __mutex;
  RonDB_Stats stats; 

  static NdbObjectPool *__instance; 
  NdbObjectPool() {
  }

 public:
  /**
   * Static method for initializing instance pool 
   *
   * @return ObjectPool instance.
   */
  static void InitPool();

  /**
   * Static method for accessing class instance.
   *
   * @return ObjectPool instance.
   */
  static NdbObjectPool *GetInstance();

  /**
   * Returns Ndb object
   *
   * New resource will be created if all the resources
   * were used at the time of the request.
   *
   * @return Status and Resource instance.
   */
  RS_Status GetNdbObject(Ndb_cluster_connection *ndb_connection, Ndb **ndb_object);

  /**
   * Return resource back to the pool.
   *
   * @param object Resource instance.
   * @return void
   */
  void ReturnResource(Ndb *object);

  /**
   * Get status
   *
   * @return RonDB_Stats
   */
  RonDB_Stats GetStats();

  /**
   * Purge. Delete all Ndb objects
   *
   */
  RS_Status Close();

};
#endif  // DATA_ACCESS_RONDB_SRC_NDB_OBJECT_POOL_HPP_
