/*
   Copyright (c) 2003, 2021, Oracle and/or its affiliates.
   Copyright (c) 2022, 2022, Hopsworks and/or its affiliates.

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

#ifndef COMMIT_REQ_H
#define COMMIT_REQ_H

#include "SignalData.hpp"

#define JAM_FILE_ID 548

class CommitReq {
  /**
   * Receiver(s)
   */
  friend class Dblqh;

  /**
   * Sender(s)
   */
  friend class Dbtc;      

  /**
   * For printing
   */
  friend bool printCOMMITREQ(FILE * output,
                             const Uint32 * theData,
                             Uint32 len,
                             Uint16 receiverBlockNo);

public:
  static constexpr Uint32 SignalLength = 8;

private:
  Uint32 reqPtr;
  Uint32 reqBlockref;
  Uint32 gci_hi;
  Uint32 transid1;
  Uint32 transid2;
  Uint32 old_blockref;
  Uint32 tcOprec;
  Uint32 gci_lo;
};

class Commit {
  /**
   * Receiver(s) and Sender(s)
   */
  friend class Dblqh;

  /**
   * Sender(s)
   */
  friend class Dbtc;      

  /**
   * For printing
   */
  friend bool printCOMMIT(FILE * output,
                          const Uint32 * theData,
                          Uint32 len,
                          Uint16 receiverBlockNo);

public:
  static constexpr Uint32 SignalLength = 5;

private:
  Uint32 tcConnectPtr;
  Uint32 gci_hi;
  Uint32 transid1;
  Uint32 transid2;
  Uint32 gci_lo;
};

class Committed {
  /**
   * Receiver(s)
   */
  friend class Dbtc;      

  /**
   * Sender(s)
   */
  friend class Dblqh;

  /**
   * For printing
   */
  friend bool printCOMMITTED(FILE * output,
                             const Uint32 * theData,
                             Uint32 len,
                             Uint16 receiverBlockNo);

public:
  static constexpr Uint32 SignalLength = 3;

private:
  Uint32 tcConnectPtr;
  Uint32 transid1;
  Uint32 transid2;
};

class CommitConf {
  /**
   * Receiver(s)
   */
  friend class Dbtc;      

  /**
   * Sender(s)
   */
  friend class Dblqh;

  /**
   * For printing
   */
  friend bool printCOMMITCONF(FILE * output,
                                const Uint32 * theData,
                                Uint32 len,
                                Uint16 receiverBlockNo);

public:
  static constexpr Uint32 SignalLength = 4;

private:
  Uint32 tcConnectPtr;
  Uint32 senderNodeId;
  Uint32 transid1;
  Uint32 transid2;
};

class CompleteReq {
  /**
   * Receiver(s)
   */
  friend class Dblqh;

  /**
   * Sender(s)
   */
  friend class Dbtc;      

  /**
   * For printing
   */
  friend bool printCOMPLETEREQ(FILE * output,
                               const Uint32 * theData,
                               Uint32 len,
                               Uint16 receiverBlockNo);

public:
  static constexpr Uint32 SignalLength = 6;

private:
  Uint32 reqPtr;
  Uint32 reqBlockref;
  Uint32 transid1;
  Uint32 transid2;
  Uint32 old_blockref;
  Uint32 tcOprec;
};

class Complete {
  /**
   * Receiver(s) and Sender(s)
   */
  friend class Dblqh;

  /**
   * Sender(s)
   */
  friend class Dbtc;      

  /**
   * For printing
   */
  friend bool printCOMPLETE(FILE * output,
                            const Uint32 * theData,
                            Uint32 len,
                            Uint16 receiverBlockNo);

public:
  static constexpr Uint32 SignalLength = 3;

private:
  Uint32 tcConnectPtr;
  Uint32 transid1;
  Uint32 transid2;
};

class Completed {
  /**
   * Receiver(s)
   */
  friend class Dbtc;      

  /**
   * Sender(s)
   */
  friend class Dblqh;

  /**
   * For printing
   */
  friend bool printCOMPLETED(FILE * output,
                             const Uint32 * theData,
                             Uint32 len,
                             Uint16 receiverBlockNo);

public:
  static constexpr Uint32 SignalLength = 3;

private:
  Uint32 tcConnectPtr;
  Uint32 transid1;
  Uint32 transid2;
};

class CompleteConf {
  /**
   * Receiver(s)
   */
  friend class Dbtc;      

  /**
   * Sender(s)
   */
  friend class Dblqh;

  /**
   * For printing
   */
  friend bool printCOMPLETECONF(FILE * output,
                                const Uint32 * theData,
                                Uint32 len,
                                Uint16 receiverBlockNo);

public:
  static constexpr Uint32 SignalLength = 4;

private:
  Uint32 tcConnectPtr;
  Uint32 senderNodeId;
  Uint32 transid1;
  Uint32 transid2;
};

#undef JAM_FILE_ID
#endif
