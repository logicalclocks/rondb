/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

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

#include "Keywords.hpp"
#include "../../../../sql/lex.h"

// todo: Make sure this test gets run in the standard way
// todo: Maybe rename to testRonSQLKeywords-t

using std::cout;
using std::endl;

// A list of keywords implemented in some old versions of MySQL, but not the
// current version. These are still forbidden as unquoted identifiers in RonSQL.
static const char* mysql_deprecated[] =
{
  "ANALYSE",
  "DES_KEY_FILE",
  "GET_MASTER_PUBLIC_KEY", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_AUTO_POSITION", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_BIND", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_COMPRESSION_ALGORITHMS", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords. Note the S on the end, which is not present in the _SYM
  "MASTER_CONNECT_RETRY", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_DELAY", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_HEARTBEAT_PERIOD", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_HOST", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_LOG_FILE", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_LOG_POS", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_PASSWORD", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_PORT", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_PUBLIC_KEY_PATH", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_RETRY_COUNT", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_SERVER_ID", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_SSL", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_SSL_CA", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_SSL_CAPATH", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_SSL_CERT", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_SSL_CIPHER", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_SSL_CRL", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_SSL_CRLPATH", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_SSL_KEY", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_SSL_VERIFY_SERVER_CERT", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_TLS_CIPHERSUITES", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_TLS_VERSION", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_USER", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "MASTER_ZSTD_COMPRESSION_LEVEL", // Removed in 7cabca9bfb8 WL#15831: Step 1/4: Removing parser keywords
  "PARSE_GCOL_EXPR",
  "REDOFILE",
  "REMOTE",
  "SQL_CACHE",
};
static const Uint32 mysql_deprecated_len = ARRAY_LEN(mysql_deprecated);

// A list of operators found in lex.h
static const char* mysql_operators[] =
{
  "!=",
  "&&",
  "<",
  "<<",
  "<=",
  "<=>",
  "<>",
  "=",
  ">",
  ">=",
  ">>",
  "||",
};
static const Uint32 mysql_operators_len = ARRAY_LEN(mysql_operators);

void assert_list_sorted(const char* list_name, const char** list, Uint32 list_len)
{
  for (Uint32 i=0; i < list_len-1; i++)
  {
    if (strcmp(list[i], list[i+1]) >= 0)
    {
      cout << list_name << " is not sorted: " << list[i] << " >= " << list[i+1] << endl;
      abort();
    }
  }
}

bool string_exists_in_list(const char* word, const char** list, Uint32 list_len)
{
  for (Uint32 i=0; i < list_len; i++)
  {
    if (strcmp(word, list[i]) == 0)
    {
      return true;
    }
  }
  return false;
}

int
main()
{
  bool ok = true;
  // From Keywords.hpp, a list of keywords implemented in RonSQL
  const Uint32 ronsql_imple_len = number_of_keywords_implemented_in_ronsql;
  const char* ronsql_impl[ronsql_imple_len];
  for (Uint32 i=0; i < ronsql_imple_len; i++)
  {
    ronsql_impl[i] = keywords_implemented_in_ronsql[i].text;
  }
  // From Keywords.hpp, a list of reserved keywords in RonSQL. This includes all
  // keywords, both reserved and not, implemented in the current or some past
  // versions of MySQL. These are forbidden as unquoted identifiers in RonSQL.
  const Uint32 ronsql_reserved_len = number_of_keywords_defined_in_mysql;
  const char** ronsql_reserved = keywords_defined_in_mysql;
  // From ../../../../sql/lex.h, a list of keywords implemented in the current
  // version of MySQL.
  const Uint32 mysql_current_len = ARRAY_LEN(symbols);
  const char* mysql_current[mysql_current_len];
  for (Uint32 i=0; i < mysql_current_len; i++)
  {
    mysql_current[i] = symbols[i].name;
  }
  // Both lists in Keywords.hpp should be sorted.
  assert_list_sorted("ronsql_impl", ronsql_impl, ronsql_imple_len);
  assert_list_sorted("ronsql_reserved", ronsql_reserved, ronsql_reserved_len);
  // Exception lists above should be sorted.
  assert_list_sorted("mysql_deprecated", mysql_deprecated, mysql_deprecated_len);
  assert_list_sorted("mysql_operators", mysql_operators, mysql_operators_len);
  // Concatenate all five lists mentioned. There will be duplicates, that's ok.
  const Uint32 all_keywords_len =
    ronsql_imple_len +
    ronsql_reserved_len +
    mysql_current_len +
    mysql_deprecated_len +
    mysql_operators_len;
  const char* all_keywords[all_keywords_len];
  {
    Uint32 aidx = 0;
    for (Uint32 i=0; i < ronsql_imple_len; i++)
      all_keywords[aidx++] = ronsql_impl[i];
    for (Uint32 i=0; i < ronsql_reserved_len; i++)
      all_keywords[aidx++] = ronsql_reserved[i];
    for (Uint32 i=0; i < mysql_current_len; i++)
      all_keywords[aidx++] = mysql_current[i];
    for (Uint32 i=0; i < mysql_deprecated_len; i++)
      all_keywords[aidx++] = mysql_deprecated[i];
    for (Uint32 i=0; i < mysql_operators_len; i++)
      all_keywords[aidx++] = mysql_operators[i];
  }
  Uint32 actual_max_strlen_for_keyword_implemented_in_ronsql = 0;
  // Assertions about all keywords
  for (Uint32 i=0; i < all_keywords_len; i++)
  {
    const char* this_word = all_keywords[i];
    static const Uint32 p_ronsql_impl = 0x01; // Implemented in RonSQL
    static const Uint32 p_ronsql_resv = 0x02; // Reserved in RonSQL
    static const Uint32 p_mysql_curr = 0x04;  // Keywords implemented in current MySQL
    static const Uint32 p_mysql_depr = 0x08;  // Keywords in past but not current MySQL
    static const Uint32 p_mysql_oper = 0x10;  // Operators in current MySQL
    static const Uint32 p_AZ_ = 0x20;         // Contains at least one character A-Z or underscore
    static const Uint32 p_09 = 0x40;          // Contains at least one digit
    static const Uint32 p_opchars = 0x80;     // Contains at least one character !&<=>|
    static const Uint32 p_other = 0x100;      // Contains at least one other character
    Uint32 flags = 0;
    if (string_exists_in_list(this_word, ronsql_impl, ronsql_imple_len))
      flags |= p_ronsql_impl;
    if (string_exists_in_list(this_word, ronsql_reserved, ronsql_reserved_len))
      flags |= p_ronsql_resv;
    if (string_exists_in_list(this_word, mysql_current, mysql_current_len))
      flags |= p_mysql_curr;
    if (string_exists_in_list(this_word, mysql_deprecated, mysql_deprecated_len))
      flags |= p_mysql_depr;
    if (string_exists_in_list(this_word, mysql_operators, mysql_operators_len))
      flags |= p_mysql_oper;
    Uint32 word_len = strlen(this_word);
    for (Uint32 j=0; j < word_len; j++)
    {
      char c = this_word[j];
      if (('A' <= c && c <= 'Z') || c == '_')
        flags |= p_AZ_;
      else if ('0' <= c && c <= '9')
        flags |= p_09;
      else if (
               c == '!' ||
               c == '&' ||
               c == '<' ||
               c == '=' ||
               c == '>' ||
               c == '|')
        flags |= p_opchars;
      else
        flags |= p_other;
    }
    switch (flags)    {
    case p_ronsql_impl | p_ronsql_resv | p_mysql_curr | p_AZ_:
      /*
       * Implemented keywords should appear in reserved list, be supported in
       * current MySQL, consist of capital A-Z and underscore (WARNING: The
       * lexer does not currently support digits.). They should also have an
       * acceptable length:
       */
      if (!(1 <= word_len && word_len <= max_strlen_for_keyword_implemented_in_ronsql))
      {
        ok = false;
        cout << "Problem with keyword: " << this_word << endl;
        cout << "  Length of keyword is " << word_len << " which is not in the range 1.."
             << max_strlen_for_keyword_implemented_in_ronsql << endl;
      }
      actual_max_strlen_for_keyword_implemented_in_ronsql =
        std::max(actual_max_strlen_for_keyword_implemented_in_ronsql,
                 word_len);
      break;
    case p_ronsql_resv | p_mysql_curr | p_AZ_:
    case p_ronsql_resv | p_mysql_curr | p_AZ_ | p_09:
      break; // Reserved by RonSQL due to implemented in MySQL
    case p_ronsql_resv | p_mysql_depr | p_AZ_:
      break; // Reserved by RonSQL due to deprecated in MySQL
    case p_mysql_curr | p_mysql_oper | p_opchars:
      break; // Operators in MySQL happen to appear in the symbol list
    default:
      ok = false;
      cout << "Problem with keyword: " << this_word << endl;
      cout << "  flags:"
           << (flags & p_ronsql_impl ? " | p_ronsql_impl" : "")
           << (flags & p_ronsql_resv ? " | p_ronsql_resv" : "")
           << (flags & p_mysql_curr ? " | p_mysql_curr" : "")
           << (flags & p_mysql_depr ? " | p_mysql_depr" : "")
           << (flags & p_mysql_oper ? " | p_mysql_oper" : "")
           << (flags & p_AZ_ ? " | p_AZ_" : "")
           << (flags & p_09 ? " | p_09" : "")
           << (flags & p_opchars ? " | p_opchars" : "")
           << (flags & p_other ? " | p_other" : "")
           << endl;
      break;
    }
  }
  if (actual_max_strlen_for_keyword_implemented_in_ronsql
      != max_strlen_for_keyword_implemented_in_ronsql)
  {
    ok = false;
    cout << "Actual maximum implemented word length is "
         << actual_max_strlen_for_keyword_implemented_in_ronsql << endl;
  }
  if (!ok)
  {
    cout << "FAIL" << endl;
    return 1;
  }
  cout << "OK" << endl;
}
