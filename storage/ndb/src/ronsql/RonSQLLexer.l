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

/*
 * RonSQLLexer.l is the lexer file. The "fast lexical analyser generator",
 * `flex', will generate RonSQLLexer.l.hpp and RonSQLLexer.l.cpp from this
 * file.
 */

%option noyywrap
%option warn
%option nodefault
%option reentrant
%option bison-bridge
%option bison-locations
%option extra-type="RonSQLPreparer::Context*"
%option prefix="rsqlp_"
%option case-insensitive
%option batch
/* todo experiment with optimization options
 * Probably use %option fast
 * but also try %option full
 * See https://www.cs.virginia.edu/~cr4bd/flex-manual/Options-for-Scanner-Speed-and-Size.html
 */
%option perf-report

/* Suppress built-in allocator */
%option noyyalloc
%option noyyfree
%option noyyrealloc

/* Suppress functions we don't use */
%option noinput
%option nounput
%option noyy_pop_state
%option noyy_push_state
%option noyy_top_state
%option noyyget_debug
%option noyyget_in
%option noyyget_lineno
%option noyyget_lloc
%option noyyget_lval
%option noyyget_out
%option noyyset_debug
%option noyyset_in
%option noyyset_lineno
%option noyyset_lloc
%option noyyset_lval
%option noyyset_out

/*
 * This section will go into both RonSQLLexer.l.hpp and RonSQLLexer.l.cpp,
 * near the top.
 */
%top{
#include "RonSQLPreparer.hpp"
#define YYSTYPE RSQLP_STYPE
#define YYLTYPE RSQLP_LTYPE
#define yy_accept rsqlp__accept
#define yy_base rsqlp__base
#define yy_chk rsqlp__chk
#define yy_def rsqlp__def
#define yy_ec rsqlp__ec
#define yy_meta rsqlp__meta
#define yy_nxt rsqlp__nxt
}

/*
 * This section will go into RonSQLLexer.l.cpp only, somewhere in the middle.
 */
%{
#include <stdio.h>
#include "RonSQLParser.y.hpp"
#include "Keywords.hpp"
#define YY_DECL TokenKind yylex(RSQLP_STYPE* yylval_param, RSQLP_LTYPE* yylloc_param, yyscan_t yyscanner)
#define rsqlp_leng     ({ assert(yyleng >= 0); size_t(yyleng); })
#define lloc_this_rule() do \
  { \
    yylloc->begin = yytext; \
    yylloc->end = yytext + rsqlp_leng; \
  } while (0)
#define keyword(token) do { \
  lloc_this_rule(); \
  return T_ ## token; \
} while (0)
#define return_err(ERR_TOKEN) do { \
  context->set_err_state( \
    RonSQLPreparer::ErrState::LEX_ ## ERR_TOKEN, \
    yylloc->begin, yylloc->end - yylloc->begin); \
  return T_ERR; \
} while (0)
#define return_err_for_this_rule(ERR_TOKEN) do { \
  lloc_this_rule(); \
  return_err(ERR_TOKEN); \
} while (0)
#define context yyextra
%}

/*
 * Start conditions other than INITAL. These are used to provide some (very
 * limited) context awareness in the lexer.
 */
%x QUOTED_IDENTIFIER
%x SINGLE_QUOTED_STRING

/* U_PARTIAL and U_OVERLONG are only used to detect UTF-8 decoding errors. */
U_2_PAR_1    (?# 110xxxxx                                             )[\300-\337]
U_3_PAR_1    (?# 1110xxxx                                             )[\340-\357]
U_3_PAR_2    (?# 1110xxxx 10xxxxxx                                    )[\340-\357][\200-\277]
U_4_PAR_1    (?# 11110xxx                                             )[\360-\367]
U_4_PAR_2    (?# 11110xxx 10xxxxxx                                    )[\360-\367][\200-\277]
U_4_PAR_3    (?# 11110xxx 10xxxxxx 10xxxxxx                           )[\360-\367][\200-\277][\200-\277]
U_PARTIAL    (?# An incomplete UTF-8 multi-byte sequence              ){U_2_PAR_1}|{U_3_PAR_1}|{U_3_PAR_2}|{U_4_PAR_1}|{U_4_PAR_2}|{U_4_PAR_3}
U_2_OVERLONG (?# 1100000x 10xxxxxx                                    )[\300\301][\200-\277]
U_3_OVERLONG (?# 11100000 100xxxxx 10xxxxxx                           )\340[\200-\237][\200-\277]
U_4_OVERLONG (?# 11110000 1000xxxx 10xxxxxx 10xxxxxx                  )\360[\200-\217][\200-\277]{2}
U_OVERLONG   (?# An overlong but otherwise correct UTF-8 character    ){U_2_OVERLONG}|{U_3_OVERLONG}|{U_4_OVERLONG}

/* U_EXTBMP and U_NONBMP are used to match correct UTF-8 characters. It's
 * tempting to match a simpler superset and use the error definitions above in a
 * higher-priority rule, but then it becomes problematic to use these
 * definitions in rules that match more than one character such as
 * {UNQUOTED_IDENTIFIER_CHARACTER}+ so the below definitions need to be correct
 * as they are, standalone. The difference operator {-} would have come in handy
 * but it unfortunately only works with character sets, not regular expressions.
 * Therefore, these definitions are a little involved.
 */
          // U+0080--U+07ff: 110NNNNx 10xxxxxx where NNNN != 0000
U_2          [\302-\337][\200-\277]
          // U+0800--U+d7ff: 1110NNNN 10Nxxxxx 10xxxxxx where 00001 <= NNNNN < 11011
U_3_LOW      (\340[\240-\277]|[\341-\354][\200-\277]|\355[\200-\237])[\200-\277]
          // U+d800--U+dfff: 11101101 101xxxxx 10xxxxxx
U_SURROGATE  \355[\240-\277][\200-\277]
          // U+e000--U+ffff: 1110111x 10xxxxxx 10xxxxxx
U_3_HIGH     [\356-\357][\200-\277]{2}
          // U+0080--U+d7ff, U+e000--U+ffff
U_EXTBMP     {U_2}|{U_3_LOW}|{U_3_HIGH}
          // U+010000--U+10ffff: 11110NNN 10NNxxxx 10xxxxxx 10xxxxxx where 00001 <= NNNNN <= 10000
U_4          (\360[\220-\277]|[\361-\363][\200-\277]|\364[\200-\217])[\200-\277]{2}
U_NONBMP     {U_4}

/* U_TOOHIGH and U_ILLEG_BYTE are only used to match UTF-8 decoding errors. */
          // U+110000--U+1fffff: 111101NN 10NNxxxx 10xxxxxx 10xxxxxx where NNNN!=0000
U_TOOHIGH    (\364[\220-\277]|[\365-\367][\200-\277])[\200-\277]{2}
          // 11111xxx
U_ILLEG_BYTE [\370-\377]

/*
 * Permitted characters in unquoted identifiers:
 * - ASCII: [0-9,a-z,A-Z$_] (basic Latin letters, digits 0-9, dollar,
 *   underscore)
 * - Extended: U+0080 .. U+FFFF
 * Permitted characters in quoted identifiers include the full Unicode Basic
 * Multilingual Plane (BMP), except U+0000:
 * - ASCII: U+0001 .. U+007F
 * - Extended: U+0080 .. U+FFFF
 * ASCII NUL (U+0000) and supplementary characters (U+10000 and higher) are not
 * permitted in quoted or unquoted identifiers.
 * See https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
 */

UNQUOTED_IDENTIFIER_CHARACTER                                          [$0-9A-Z_a-z]|{U_EXTBMP}
QUOTED_IDENTIFIER_CHARACTER_EXCEPT_BACKTICK                            [^\0`\200-\377]|{U_EXTBMP}
SINGLE_QUOTED_STRING_CHARACTER_EXCEPT_APOSTROPHE_AND_BACKSLASH         [^\0\047\134\200-\377]|{U_EXTBMP}|{U_NONBMP}

/*
 * Double percent sign marking the begining (and end) of the lexical rules
 * section
 */
%%

 /* Top section for rules to catch errors */

<*>\0 {
  return_err_for_this_rule(NUL);
}

<*>{U_ILLEG_BYTE} {
  /*
   * Matches bytes 0xf8-0xff which are illegal in UTF-8. Technically, the same
   * applies to 0xc0-0xc1, but any presence of those bytes will be picked up by
   * U_OVERLONG or another error rule, potentially leading to slightly more
   * informative error messages.
   */
  return_err_for_this_rule(U_ILLEGAL_BYTE);
}

<*>{U_PARTIAL} {
  /*
   * A leading byte for a UTF-8 multi-byte sequence followed by too few
   * continuation bytes. Since this rule matches only sequences that are shorter
   * than correct sequences, it has lower priority and will therefore only match
   * a partial multi-byte sequence followed by an unexpected byte or <<EOF>>.
   */
   return_err_for_this_rule(U_ENC_ERR);
}

<*>[\200-\277] {
  // Rogue UTF-8 continuation byte
  return_err_for_this_rule(U_ENC_ERR);
}

<*>{U_OVERLONG} {
  return_err_for_this_rule(U_OVERLONG);
}

<*>{U_TOOHIGH} {
  return_err_for_this_rule(U_TOOHIGH);
}

<*>{U_SURROGATE} {
  return_err_for_this_rule(U_SURROGATE);
}

<INITIAL,QUOTED_IDENTIFIER>{U_NONBMP} {
  return_err_for_this_rule(NONBMP_IDENTIFIER);
}

 /* Section for functionality (happy path) rules */

[ \t\r\n] ; // ignore whitespace

[0-9]+\.[0-9]+ {
  char hold_char = yytext[rsqlp_leng];
  yytext[rsqlp_leng] = char(0);
  yylval->fval = atof(yytext);
  yytext[rsqlp_leng] = hold_char;
  lloc_this_rule();
  return T_FLOAT;
}

[0-9]+ {
  if (rsqlp_leng > 19) {
    return_err_for_this_rule(LITERAL_INTEGER_TOO_BIG);
  }
  if (rsqlp_leng == 19) {
    constexpr const char* maxInt64Str = "9223372036854775807";
    for (Uint32 i = 0; i < 19; i++) {
      if (yytext[i] > maxInt64Str[i]) {
        return_err_for_this_rule(LITERAL_INTEGER_TOO_BIG);
      }
    }
  }
  char hold_char = yytext[rsqlp_leng];
  yytext[rsqlp_leng] = char(0);
  yylval->bival = atoll(yytext); // todo Is this portable?
  yytext[rsqlp_leng] = hold_char;
  lloc_this_rule();
  return T_INT;
}

 /* Punctuation, operators, and keywords with characters other than [a-z_] */
"!"       keyword(EXCLAMATION);
"%"       keyword(MODULO);
"&"       keyword(BITWISE_AND);
"&&"      keyword(AND);
"("       keyword(LEFT);
")"       keyword(RIGHT);
"*"       keyword(MULTIPLY);
"+"       keyword(PLUS);
","       keyword(COMMA);
"-"       keyword(MINUS);
"/"       keyword(SLASH);
";"       keyword(SEMICOLON);
"<"       keyword(LT);
"<<"      keyword(BITSHIFT_LEFT);
"<="      keyword(LE);
"<>"|"!=" keyword(NOT_EQUALS);
"="       keyword(EQUALS);
">"       keyword(GT);
">="      keyword(GE);
">>"      keyword(BITSHIFT_RIGHT);
"^"       keyword(BITWISE_XOR);
"|"       keyword(BITWISE_OR);
"||"      keyword(OR);

[a-z_]{1,18} {
  // The max keyword length needs to be hard-coded in the rule regexp above.
  static_assert(18 == max_strlen_for_keyword_implemented_in_ronsql);
  assert(rsqlp_leng <= max_strlen_for_keyword_implemented_in_ronsql);
  lloc_this_rule();
  // This rule matches all keywords that use [a-z_] characters and some unquoted identifiers.
  //
  // Create an upper-case c-string
  char cword[max_strlen_for_keyword_implemented_in_ronsql + 1];
  memcpy(cword, yytext, rsqlp_leng);
  cword[rsqlp_leng] = char(0); // The difference between char(0) and '\0' is that
                           // the former escapes post-editing, see
                           // build_lexer.sh.
  for (Uint32 i = 0; i < rsqlp_leng; i++)
  {
    cword[i] &= 0xdf; // This works for uppercasing [A-Za-z_] but not digits. So
                      // far, no implemented keyword has digits.
  }
  // Binary search for cword in keywords_implemented_in_ronsql list, returning
  // value if found
  // Variables low, high and mid need to be signed since `high = mid - 1` below can result in a negative value.
  Int32 low = 0;
  Int32 high = (Int32)number_of_keywords_implemented_in_ronsql - 1;
  while (low <= high)
  {
    Int32 mid = (low + high) / 2;
    int cmp = strcmp(cword, keywords_implemented_in_ronsql[mid].text);
    if (cmp < 0)
    {
      high = mid - 1;
    }
    else if (cmp > 0)
    {
      low = mid + 1;
    }
    else
    {
      return keywords_implemented_in_ronsql[mid].value;
    }
  }
  /*
   * The keywords_defined_in_mysql list contains all keywords in MySQL 8.0 and
   * 5.7. At this point we know that cword is not a keyword we implement, so if
   * we find it in the list, we issue a LEX_UNIMPLEMENTED_KEYWORD error.
   *
   * The reason we want to throw an error at the mention of an unimplemented
   * keyword, is that we might want to implement it in the future, at which
   * point such keyword might not be valid as an unquoted identifier. For the
   * sake of backward compatibility, we'd like such identifiers to be invalid
   * even before the keyword is implemented. Note that MySQL differs between
   * reserved and non-reserved keywords, where only reserved keywords are
   * invalid as unquoted identifiers. For the sake of simplicity, we make no
   * such distinction for RonSQL, and instead disallow the use of all
   * keywords - both reserved and not, both implemented and not - as unquoted
   * identifiers.
   */
  low = 0;
  high = number_of_keywords_defined_in_mysql - 1;
  while (low <= high)
  {
    Uint32 mid = (low + high) / 2;
    int cmp = strcmp(cword, keywords_defined_in_mysql[mid]);
    if (cmp < 0)
    {
      high = mid - 1;
    }
    else if (cmp > 0)
    {
      low = mid + 1;
    }
    else
    {
      return_err(UNIMPLEMENTED_KEYWORD);
    }
  }
  // cword is not a keyword, therefore it is an unquoted identifier.
  // todo: Decide whether to disallow unquoted identifiers altogether.
  yylval->str = LexString{yytext, rsqlp_leng};
  if (rsqlp_leng > 64)
  {
    // return_err(TOO_LONG_IDENTIFIER);
    // However, this should not happen since the rule only matches up to 18
    // bytes.
    abort();
  }
  return T_IDENTIFIER;
}

{UNQUOTED_IDENTIFIER_CHARACTER}+ {
 /*
  * This regex will match not only identifiers, but also keywords. Terminating
  * or delimiting tokens by whitespace is not part of the lexer rules, so one
  * could believe that something like "selectblabla" could be interpreted as the
  * SELECT keyword followed by an identifier "blabla". This ambiguity is
  * resolved since flex prioritizes the rule with the longest match, so in this
  * case the whole of "selectblabla" will be interpreted as an identifier. Now,
  * the keyword "select" by itself also matches this rule. In the case of two
  * rules matching with equal length, flex selects the one mentioned first. So,
  * since the keywords are defined first, they take precedence.
  */
  // todo Do we need to check that it is not a reserved keyword?
  yylval->str = LexString{yytext, rsqlp_leng};
  lloc_this_rule();
  if (rsqlp_leng > 64)
  {
    return_err(TOO_LONG_IDENTIFIER);
  }
  return T_IDENTIFIER;
}

` {
  BEGIN(QUOTED_IDENTIFIER);
  yylval->str = LexString{ NULL, 0};
  yylloc->begin = yytext;
}
<QUOTED_IDENTIFIER>{QUOTED_IDENTIFIER_CHARACTER_EXCEPT_BACKTICK}+ {
  yylval->str = yylval->str.concat(LexString{ yytext, rsqlp_leng },
                                   context->get_allocator());
}
<QUOTED_IDENTIFIER>`` {
  yylval->str = yylval->str.concat(LexString{ yytext, 1 },
                                   context->get_allocator());
}
<QUOTED_IDENTIFIER>` {
  yylloc->end = yytext + rsqlp_leng;
  BEGIN(INITIAL);
  if (yylval->str.len > 64)
  {
    return_err(TOO_LONG_IDENTIFIER);
  }
  return T_IDENTIFIER;
}
<QUOTED_IDENTIFIER><<EOF>> {
  yylloc->end = yytext;
  return_err(UNEXPECTED_EOI_IN_QUOTED_IDENTIFIER);
}

 /*
  * Single-quoted strings implemented according to
  * https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
  *
  * \047 is apostrophe ('), \134 is backslash (\)
  */
\047 {
  BEGIN(SINGLE_QUOTED_STRING);
  yylval->str = LexString{ NULL, 0};
  yylloc->begin = yytext;
}
<SINGLE_QUOTED_STRING>{SINGLE_QUOTED_STRING_CHARACTER_EXCEPT_APOSTROPHE_AND_BACKSLASH}+ {
  yylval->str = yylval->str.concat(LexString{ yytext, rsqlp_leng },
                                   context->get_allocator());
}
<SINGLE_QUOTED_STRING>\047\047 {
  yylval->str = yylval->str.concat(LexString{ yytext, 1 },
                                   context->get_allocator());
}
<SINGLE_QUOTED_STRING>\047 {
  yylloc->end = yytext + rsqlp_leng;
  BEGIN(INITIAL);
  return T_STRING;
}
<SINGLE_QUOTED_STRING>\134. {
  char escchar = yytext[1];
  LexString result = { NULL, 0};
  switch (escchar)
  {
  case '0':
    result = { "\0", 1 };
    break;
  case '\'':
    result = { "\'", 1 };
     break;
  case '"':
    result = { "\"", 1 };
    break;
  case 'b':
    result = { "\b", 1 };
    break;
  case 'n':
    result = { "\n", 1 };
    break;
  case 'r':
    result = { "\r", 1 };
    break;
  case 't':
    result = { "\t", 1 };
    break;
  case 'Z':
    result = { "\032", 1 };
    break;
  case '\\':
    result = { "\\", 1 };
    break;
  case '%':
    result = { "\\%", 2 };
    break;
  case '_':
    result = { "\\_", 2 };
    break;
  default:
    result = { &yytext[1], 1 };
    break;
  }
  yylval->str = yylval->str.concat(result, context->get_allocator());
}
<SINGLE_QUOTED_STRING>\134 {
  return_err_for_this_rule(INCOMPLETE_ESCAPE_SEQUENCE_IN_SINGLE_QUOTED_STRING);
}
<SINGLE_QUOTED_STRING><<EOF>> {
  yylloc->end = yytext;
  return_err(UNEXPECTED_EOI_IN_SINGLE_QUOTED_STRING);
}
<SINGLE_QUOTED_STRING>\047[ \t\r\n]+\047 {
  // Quoted strings placed next to each other are concatenated to a single string.
}

 /* Bottom section for rules to catch errors */

[\001-\010\013\014\016-\037\042\043\056\072\077-\100\133-\135\137\173\175-\177] {
  /*
   * We have no default rule. Instead, this rule is carefully maintained to
   * catch anything that other rules won't. This is mostly for documentation
   * purposes. If this rule catches too little, flex will warn and our build
   * script (build_lexer.sh) will treat that warning as an error. If it catches
   * too much, the fact that it is placed last and only matches one byte will
   * put it strictly lowest in priority, so flex will automatically shrink the
   * scope appropriately.
   */
  context->set_err_state(
    RonSQLPreparer::ErrState::LEX_ILLEGAL_TOKEN,
    yytext, 1);
  return T_ERR;
}

%%

union rsqlp_alloc_align
{
  struct yy_buffer_state ignore1;
  char ignore2;
  yyguts_t ignore3;
};

void*
rsqlp_alloc(size_t bytes, void * scanner)
{
  return rsqlp_get_extra(scanner)->get_allocator()->
    alloc_bytes(bytes, alignof(union rsqlp_alloc_align));
}

void*
rsqlp_realloc(void * ptr, size_t bytes, void * scanner)
{
  (void)ptr;
  (void)bytes;
  (void)scanner;
  // realloc should not be needed in the lexer.
  abort();
  // If it is needed, we need to keep track of allocation size.
}

void
rsqlp_free(void * ptr, void * scanner)
{
    (void)ptr;
    (void)scanner;
    // Do nothing. ArenaAllocator will free everything at end of its lifetime.
}
