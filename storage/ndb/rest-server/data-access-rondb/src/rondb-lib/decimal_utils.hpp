/*
 Copyright 2010 Sun Microsystems, Inc.
 Use is subject to license terms.

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
/*
 * decimal_utils.hpp
 * This is copied form storage/ndb/include/util/decimal_utils.hpp 
 */


#ifndef DATA_ACCESS_RONDB_SRC_RONDB_LIB_DECIMAL_UTILS_HPP_
#define DATA_ACCESS_RONDB_SRC_RONDB_LIB_DECIMAL_UTILS_HPP_


/* return values (redeclared here if to be mapped to Java) */
#define E_DEC_OK        0
#define E_DEC_TRUNCATED 1
#define E_DEC_OVERFLOW  2
#define E_DEC_BAD_NUM   8
#define E_DEC_OOM       16
/* return values below here are unique to ndbjtie --
   not present in MySQL's decimal library */
#define E_DEC_BAD_PREC  32
#define E_DEC_BAD_SCALE 64

/*
 decimal_str2bin: Convert string directly to on-disk binary format.
 str  - string to convert
 str_len - length of string
 prec - precision of column
 scale - scale of column
 bin - buffer for binary representation
 bin_len - length of buffer

 NOTES
   Added so that NDB API programs can convert directly between  the stored
   binary format and a string representation without using decimal_t.

 RETURN VALUE
   E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW/E_DEC_OOM
*/
int decimal_str2bin(const char *str, int str_len, int prec, int scale, void *bin, int bin_len);

/*
 decimal_bin2str():  Convert directly from on-disk binary format to string
 bin  - value to convert
 bin_len - length to convert
 prec - precision of column
 scale - scale of column
 str - buffer for string representation
 str_len - length of buffer

 NOTES
   Added so that NDB API programs can convert directly between  the stored
   binary format and a string representation without using decimal_t.


 RETURN VALUE
    E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW/E_DEC_BAD_NUM/E_DEC_OOM
 */

/* Three MySQL defs duplicated here : */
static const int MaxMySQLDecimalPrecision = 65;
static const int MaxDecimalStrLen         = MaxMySQLDecimalPrecision + 3;

int decimal_bin2str(const void *bin, int bin_len, int prec, int scale, char *str, int str_len);

static int howManyBytesNeeded[] = {
    0,  1,  1,  2,  2,  3,  3,  4,  4,  4,  5,  5,  6,  6,  7,  7,  8,  8,  8,  9,  9,  10,
    10, 11, 11, 12, 12, 12, 13, 13, 14, 14, 15, 15, 16, 16, 16, 17, 17, 18, 18, 19, 19, 20,
    20, 20, 21, 21, 22, 22, 23, 23, 24, 24, 24, 25, 25, 26, 26, 27, 27, 28, 28, 28, 29, 29};

/** Get the number of bytes needed in memory to represent the decimal number.
 *
 * @param precision the precision of the number
 * @param scale the scale
 * @return the number of bytes needed for the binary representation of the number
 */

inline int getDecimalColumnSpace(int precision, int scale) {
  int howManyBytesNeededForIntegral = howManyBytesNeeded[precision - scale];
  int howManyBytesNeededForFraction = howManyBytesNeeded[scale];
  int result                        = howManyBytesNeededForIntegral + howManyBytesNeededForFraction;
  return result;
}

#endif  // DATA_ACCESS_RONDB_SRC_RONDB_LIB_DECIMAL_UTILS_HPP_
