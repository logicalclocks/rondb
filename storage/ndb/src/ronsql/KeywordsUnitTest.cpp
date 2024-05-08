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
  "MASTER_SERVER_ID",
  "PARSE_GCOL_EXPR",
  "REDOFILE",
  "REMOTE",
  "SQL_CACHE",
};
static const int mysql_deprecated_len = sizeof(mysql_deprecated) / sizeof(mysql_deprecated[0]);

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
static const int mysql_operators_len = sizeof(mysql_operators) / sizeof(mysql_operators[0]);

void assert_list_sorted(const char* list_name, const char** list, int list_len)
{
  for (int i=0; i < list_len-1; i++)
  {
    if (strcmp(list[i], list[i+1]) >= 0)
    {
      cout << list_name << " is not sorted: " << list[i] << " >= " << list[i+1] << endl;
      abort();
    }
  }
}

bool string_exists_in_list(const char* word, const char** list, int list_len)
{
  for (int i=0; i < list_len; i++)
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
  const int ronsql_imple_len = number_of_keywords_implemented_in_ronsql;
  const char* ronsql_impl[ronsql_imple_len];
  for (int i=0; i < ronsql_imple_len; i++)
  {
    ronsql_impl[i] = keywords_implemented_in_ronsql[i].text;
  }
  // From Keywords.hpp, a list of reserved keywords in RonSQL. This includes all
  // keywords, both reserved and not, implemented in the current or some past
  // versions of MySQL. These are forbidden as unquoted identifiers in RonSQL.
  const int mysql_reserved_len = number_of_keywords_defined_in_mysql;
  const char** mysql_reserved = keywords_defined_in_mysql;
  // From ../../../../sql/lex.h, a list of keywords implemented in the current
  // version of MySQL.
  const int mysql_current_len = sizeof(symbols) / sizeof(symbols[0]);
  const char* mysql_current[mysql_current_len];
  for (int i=0; i < mysql_current_len; i++)
  {
    mysql_current[i] = symbols[i].name;
  }
  // Both lists in Keywords.hpp should be sorted.
  assert_list_sorted("ronsql_impl", ronsql_impl, ronsql_imple_len);
  assert_list_sorted("mysql_reserved", mysql_reserved, mysql_reserved_len);
  // Exception lists above should be sorted.
  assert_list_sorted("mysql_deprecated", mysql_deprecated, mysql_deprecated_len);
  assert_list_sorted("mysql_operators", mysql_operators, mysql_operators_len);
  // Concatenate all five lists mentioned. There will be duplicates, that's ok.
  const int all_keywords_len =
    ronsql_imple_len +
    mysql_reserved_len +
    mysql_current_len +
    mysql_deprecated_len +
    mysql_operators_len;
  const char* all_keywords[all_keywords_len];
  {
    int aidx = 0;
    for (int i=0; i < ronsql_imple_len; i++)
      all_keywords[aidx++] = ronsql_impl[i];
    for (int i=0; i < mysql_reserved_len; i++)
      all_keywords[aidx++] = mysql_reserved[i];
    for (int i=0; i < mysql_current_len; i++)
      all_keywords[aidx++] = mysql_current[i];
    for (int i=0; i < mysql_deprecated_len; i++)
      all_keywords[aidx++] = mysql_deprecated[i];
    for (int i=0; i < mysql_operators_len; i++)
      all_keywords[aidx++] = mysql_operators[i];
  }
  // Assertions about all keywords
  for (int i=0; i < all_keywords_len; i++)
  {
    const char* this_word = all_keywords[i];
    static const int p_ronsql_impl = 0x01;
    static const int p_mysql_resv = 0x02;
    static const int p_mysql_curr = 0x04;
    static const int p_mysql_depr = 0x08;
    static const int p_mysql_oper = 0x10;
    static const int p_AZ09_ = 0x20;
    static const int p_opchars = 0x40;
    static const int p_other = 0x80;
    int flags = 0;
    if (string_exists_in_list(this_word, ronsql_impl, ronsql_imple_len))
      flags |= p_ronsql_impl;
    if (string_exists_in_list(this_word, mysql_reserved, mysql_reserved_len))
      flags |= p_mysql_resv;
    if (string_exists_in_list(this_word, mysql_current, mysql_current_len))
      flags |= p_mysql_curr;
    if (string_exists_in_list(this_word, mysql_deprecated, mysql_deprecated_len))
      flags |= p_mysql_depr;
    if (string_exists_in_list(this_word, mysql_operators, mysql_operators_len))
      flags |= p_mysql_oper;
    unsigned int word_len = strlen(this_word);
    for (unsigned int j=0; j < word_len; j++)
    {
      char c = this_word[j];
      if (('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '_')
        flags |= p_AZ09_;
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
    case p_ronsql_impl | p_mysql_resv | p_mysql_curr | p_AZ09_:
      // Implemented keywords should appear in reserved list, be supported in
      // current MySQL, consist of capital A-Z, digits 0-9 and underscore.
      // They should also have an acceptable length:
      if (!(1 <= word_len && word_len <= max_strlen_for_keyword_implemented_in_ronsql))
      {
        ok = false;
        cout << "Problem with keyword: " << this_word << endl;
        cout << "  Length of keyword is " << word_len << " which is not in the range 1.."
             << max_strlen_for_keyword_implemented_in_ronsql << endl;
      }
      break;
    case p_mysql_resv | p_mysql_curr | p_AZ09_:
      break; // Reserved by RonSQL due to implemented in MySQL
    case p_mysql_resv | p_mysql_depr | p_AZ09_:
      break; // Reserved by RonSQL due to deprecated in MySQL
    case p_mysql_curr | p_mysql_oper | p_opchars:
      break; // Operators in MySQL happen to appear in the symbol list
    default:
      ok = false;
      cout << "Problem with keyword: " << this_word << endl;
      cout << "  flags:"
           << (flags & p_ronsql_impl ? " | p_ronsql_impl" : "")
           << (flags & p_mysql_resv ? " | p_mysql_resv" : "")
           << (flags & p_mysql_curr ? " | p_mysql_curr" : "")
           << (flags & p_mysql_depr ? " | p_mysql_depr" : "")
           << (flags & p_mysql_oper ? " | p_mysql_oper" : "")
           << (flags & p_AZ09_ ? " | p_AZ09_" : "")
           << (flags & p_opchars ? " | p_opchars" : "")
           << (flags & p_other ? " | p_other" : "")
           << endl;
      break;
    }
  }
  if (!ok)
  {
    cout << "FAIL" << endl;
    return 1;
  }
  cout << "OK" << endl;
}
