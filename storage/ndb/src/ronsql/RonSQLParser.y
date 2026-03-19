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
 * RonSQLParser.y is the parser file. The "GNU Project parser generator",
 * `bison', will generate RonSQLParser.y.hpp and RonSQLParser.y.cpp from this
 * file.
 */

%define api.pure full
%parse-param {yyscan_t scanner}
%lex-param {yyscan_t scanner}
%define api.prefix {rsqlp_}
%locations
%define api.location.type {LexLocation}

/* This section will go into RonSQLParser.y.hpp, near the top. */
%code requires
{
typedef void * yyscan_t;
#include "LexString.hpp"
#include "RonSQLPreparer.hpp"
#include "AggregationAPICompiler.hpp"

// Let bison use RonSQLPreparer's arena allocator
#define YYMALLOC(SIZE) context->get_allocator()->alloc_bytes(SIZE, alignof(union yyalloc))
#define YYFREE(PTR) void()

// Let bison know it's okay to move the stack using memcpy. This could otherwise
// lead to OOM errors.
#define RSQLP_LTYPE_IS_TRIVIAL 1
#define RSQLP_STYPE_IS_TRIVIAL 1

// #define YYMAXDEPTH 10000 is the default.

// Redefine default location propagation
# define YYLLOC_DEFAULT(Cur, Rhs, N) \
do \
  if (N) \
    { \
      (Cur).begin = YYRHSLOC(Rhs, 1).begin; \
      (Cur).end = YYRHSLOC(Rhs, N).end; \
    } \
  else \
    { \
      (Cur).begin = (Cur).end = YYRHSLOC(Rhs, 0).end; \
    } \
while (0)

#define yycheck rsqlp_check
#define yydefact rsqlp_defact
#define yydefgoto rsqlp_defgoto
#define yypact rsqlp_pact
#define yypgoto rsqlp_pgoto
#define yyr1 rsqlp_r1
#define yyr2 rsqlp_r2
#define yystos rsqlp_stos
#define yytable rsqlp_table
#define yytranslate rsqlp_translate
}

/* This section will go into RonSQLParser.y.cpp, near the top. */
%code top
{
#include <stdio.h>
#include <stdlib.h>
#include "RonSQLParser.y.hpp"
#include "RonSQLLexer.l.hpp"
extern void rsqlp_error(RSQLP_LTYPE* yylloc, yyscan_t yyscanner, const char* s);
#define context (rsqlp_get_extra(scanner))
/* Unfortunately initptr got a little complex. It just means that we assume THIS
 * to be of type SOMETYPE*&, allocate sizeof(SOMETYPE) new memory and set THIS
 * to the address of the new allocation. In other words, it should be equivalent
 * to:
 * THIS = context->get_allocator()->alloc_exc<SOMETYPE>(1);
 */
#define initptr(THIS) do \
  { \
    THIS = context->get_allocator()->alloc_exc<std::remove_pointer<std::remove_reference<decltype(THIS)>::type>::type>(1); \
  } while (0)
#define init_aggfun(RES,LOC,FUN,ARG) do \
  { \
    initptr(RES); \
    RES->type = Outputs::Type::AGGREGATE; \
    RES->aggregate.fun = FUN; \
    RES->aggregate.arg = ARG; \
    RES->output_name = LexString{(LOC).begin, size_t((LOC).end - (LOC).begin)}; \
    RES->next = NULL; \
  } while (0)
#define init_cond(RES,LEFT,OP,RIGHT) do \
  { \
    initptr(RES); \
    RES->args.left = LEFT; \
    RES->op = OP; \
    RES->args.right = RIGHT; \
  } while (0)
}

/* This defines the datatype for an AST node. This includes lexer tokens. */
%union
{
  TokenKind tokenkindval;
  Int64 bival;
  struct
  {
    double dbl;
    LexString ls;
  } fpval;
  bool bval;
  LexString str;
  LexCString str_c;
  struct
  {
    Outputs* head;
    Outputs* tail;
  } outputs_linked_list;
  Outputs* output;
  struct GroupbyColumns* groupby_cols;
  struct OrderbyColumns* orderby_cols;
  struct ConditionalExpression* conditional_expression;
  AggregationAPICompiler::Expr* arith_expr;
  struct TableRef* table_ref;
  struct JoinClause* join_clause;
  struct JoinCondition* join_condition;
  struct {
    JoinClause* head;
    JoinClause* tail;
  } join_list;
  struct SelectStatement* subquery_stmt;
}

%token<bival> T_INT
%token<fpval> T_FLOAT
%token T_COUNT T_MAX T_MIN T_SUM T_AVG T_LEFT T_RIGHT
%token T_EXISTS T_EXPLAIN T_SELECT T_FROM T_JOIN T_ON T_GROUP T_BY T_HAVING T_ORDER T_ASC T_DESC T_AS T_WHERE T_LIMIT
%token T_SEMICOLON
%token T_OR T_XOR T_AND T_NOT T_EQUALS T_GE T_GT T_LE T_LT T_NOT_EQUALS T_IS T_NULL T_LIKE T_IN T_BITWISE_OR T_BITWISE_AND T_BITSHIFT_LEFT T_BITSHIFT_RIGHT T_PLUS T_MINUS T_MULTIPLY T_SLASH T_DIV T_MODULO T_BITWISE_XOR T_EXCLAMATION T_DOT
%token T_INTERVAL T_DATE_ADD T_DATE_SUB T_EXTRACT T_MICROSECOND T_SECOND T_MINUTE T_HOUR T_DAY T_WEEK T_MONTH T_QUARTER T_YEAR T_SECOND_MICROSECOND T_MINUTE_MICROSECOND T_MINUTE_SECOND T_HOUR_MICROSECOND T_HOUR_SECOND T_HOUR_MINUTE T_DAY_MICROSECOND T_DAY_SECOND T_DAY_MINUTE T_DAY_HOUR T_YEAR_MONTH
%token T_CASE T_WHEN T_THEN T_ELSE T_END
%token T_KW_LEFT T_KW_OUTER

// RonSQLPreparer.cpp needs some values that are inequal to all tokens. They
// need to be declared here but aren't used in the lexer or parser.
%token I_MYSQL_TIME
%token I_IN_SUBQUERY I_SUBQUERY I_CORR_SCALAR

/*
 * MySQL operator presedence, strongest binding first:
 * See https://dev.mysql.com/doc/refman/8.0/en/operator-precedence.html
 *   INTERVAL
 *   BINARY, COLLATE
 *   !
 *   - (unary minus), ~ (unary bit inversion)
 *   ^
 *   *, /, DIV, %, MOD
 *   -, +
 *   <<, >>
 *   &
 *   |
 *   = (comparison), <=>, >=, >, <=, <, <>, !=, IS, LIKE, REGEXP, IN, MEMBER OF
 *   BETWEEN, CASE, WHEN, THEN, ELSE
 *   NOT
 *   AND, &&
 *   XOR
 *   OR, ||
 *   = (assignment), :=
 */

 /* Presedence of implemented operators, strongest binding last */
%left T_OR
%left T_XOR
%left T_AND
%precedence T_NOT
%left T_EQUALS T_GE T_GT T_LE T_LT T_NOT_EQUALS T_IS T_LIKE T_IN
%left T_BITWISE_OR
%left T_BITWISE_AND
%left T_BITSHIFT_LEFT T_BITSHIFT_RIGHT
%left T_PLUS T_MINUS
%left T_MULTIPLY T_SLASH T_DIV T_MODULO
%left T_BITWISE_XOR
%precedence T_EXCLAMATION
 // %left T_INTERVAL per spec, but bison claims it's useless.


%token T_ERR

%token<str> T_IDENTIFIER T_STRING
%token T_COMMA

%type<bval> explain_opt
%type<str> identifier
%type<str_c> identifier_c
%type<groupby_cols> groupby_opt groupby_cols groupby_col
%type<orderby_cols> orderby_opt orderby orderby_cols orderby_col
%type<output> output nonaliased_output
%type<outputs_linked_list> outputlist
%type<tokenkindval> aggfun interval_type
%type<arith_expr> arith_expr
%type<conditional_expression> where_opt cond_expr having_opt in_list
%type<bival> limit_opt
%type<table_ref> table_ref
%type<join_clause> join_clause
%type<join_condition> join_condition join_condition_list
%type<join_list> join_list
%type<subquery_stmt> subquery

%start selectstatement

%%

selectstatement:
  explain_opt T_SELECT outputlist T_FROM table_ref join_list where_opt groupby_opt having_opt orderby_opt limit_opt T_SEMICOLON
  {
    context->ast_root.do_explain = $1;
    context->ast_root.outputs = $3.head;
    context->ast_root.root_table = $5;
    context->ast_root.table = $5->name;
    if ($6.head != NULL) {
      context->ast_root.joins = $6.head;
    }
    context->ast_root.where_expression = $7;
    context->ast_root.groupby_columns = $8;
    context->ast_root.having_expression = $9;
    context->ast_root.orderby_columns = $10;
    context->ast_root.limit = $11;
    /*
     * These asserts make sure the definition of TokenKind matches both the
     * yychar variable in RonSQLzparser.y.cpp:rsqlp_parse() and the underlying
     * type of enum rsqlp_tokentype in RonSQLzparser.y.hpp.
     *
     * These asserts needs to end up inside rsqlp_parse() in the generated
     * RonSQLParser.y.cpp in order to access the definition of yychar. Putting
     * them inside any rule definition will do that.
     */
    static_assert(std::is_same<TokenKind, std::underlying_type_t<rsqlp_token_kind_t>>::value,
                  "Problem with TokenKind or rsqlp_token_kind_t definition");
    static_assert(std::is_same<TokenKind, decltype(yychar)>::value,
                  "Problem with TokenKind or yychar definition");
  // Suppress a compiler warning about unused yynerrs. It is unused, but we
  // can't easily remove it since it's declared in generated code.
  (void)yynerrs;
  }

explain_opt:
  %empty                                { $$ = false; }
| T_EXPLAIN                             { $$ = true; }

/* The outputlist rule is left-recursive in order to save both memory and cpu
 * cycles. Naively, this would produce a linked list in reverse order, so we use
 * a struct{head,tail} in order to produce a linked list that can be consumed in
 * the same order that it is constructed; left-to-right.
 */
outputlist:
  output                                { $$.head = $1; $$.tail = $1; }
| outputlist T_COMMA output             { $$.head = $1.head; $$.tail = $3; $1.tail->next = $3; }

output:
  nonaliased_output
  {
    if ($1->output_name.len > 64)
    {
      context->set_err_state(
        RonSQLPreparer::ErrState::TOO_LONG_UNALIASED_OUTPUT,
        (@$).begin,
        (@$).end - (@$).begin
      );
      YYERROR;
    }
    $$ = $1;
  }
| nonaliased_output T_AS identifier     { $$ = $1; $$->output_name = $3; }

nonaliased_output:
  identifier_c                          {
                                          initptr($$);
                                          $$->type = Outputs::Type::COLUMN;
                                          $$->column.col_idx = context->column_name_to_idx($1);
                                          $$->output_name = $1;
                                          $$->next = NULL;
                                        }
| identifier_c T_DOT identifier_c      {
                                          initptr($$);
                                          $$->type = Outputs::Type::COLUMN;
                                          $$->column.col_idx = context->qualified_column_name_to_idx($1, $3);
                                          $$->output_name = $3;
                                          $$->next = NULL;
                                        }
| aggfun T_LEFT arith_expr T_RIGHT      { init_aggfun($$, @$, $1, $3); }
| T_COUNT T_LEFT arith_expr T_RIGHT     {
                                          // This needs to be a separate rule from the "aggfun..."
                                          // rule above in order to avoid a shift/reduce conflict
                                          // with the COUNT(*) rule below.
                                          init_aggfun($$, @$, T_COUNT, $3);
                                        }
| T_COUNT T_LEFT T_MULTIPLY T_RIGHT     {
                                          // COUNT(*) is implemented as COUNT(1).
                                          init_aggfun($$, @$,
                                                      T_COUNT,
                                                      context->get_agg()->ConstantInteger(1));
                                        }
| T_AVG T_LEFT arith_expr T_RIGHT       { initptr($$);
                                          $$->type = Outputs::Type::AVG;
                                          $$->avg.arg = $3;
                                          $$->output_name = LexString{(@$).begin, size_t((@$).end - (@$).begin)};
                                          $$->next = NULL;
                                        }

/* T_COUNT not included here, in order to implement COUNT(*) */
aggfun:
  T_MAX                                 { $$ = T_MAX; }
| T_MIN                                 { $$ = T_MIN; }
| T_SUM                                 { $$ = T_SUM; }

arith_expr:
  identifier_c                          { $$ = context->get_agg()->Load(context->column_name_to_idx($1)); }
| identifier_c T_DOT identifier_c      { $$ = context->get_agg()->Load(context->qualified_column_name_to_idx($1, $3)); }
| T_INT                                 { $$ = context->get_agg()->ConstantInteger($1); }
| T_MINUS arith_expr                    { $$ = context->get_agg()->Minus(context->get_agg()->ConstantInteger(0), $2); }
| T_LEFT arith_expr T_RIGHT             { $$ = $2; }
| arith_expr T_PLUS arith_expr          { $$ = context->get_agg()->Add($1, $3); }
| arith_expr T_MINUS arith_expr         { $$ = context->get_agg()->Minus($1, $3); }
| arith_expr T_MULTIPLY arith_expr      { $$ = context->get_agg()->Mul($1, $3); }
| arith_expr T_SLASH arith_expr         { $$ = context->get_agg()->Div($1, $3); }
| arith_expr T_DIV arith_expr           { $$ = context->get_agg()->DivInt($1, $3); }
| arith_expr T_MODULO arith_expr        { $$ = context->get_agg()->Rem($1, $3); }
| T_CASE T_WHEN cond_expr T_THEN arith_expr T_ELSE arith_expr T_END
                                        { $$ = context->get_agg()->CaseExpr($3, $5, $7); }

identifier:
  T_IDENTIFIER                          { $$ = $1; }
identifier_c:
  identifier                            { $$ = $1.to_LexCString(context->get_allocator()); }

table_ref:
  identifier_c
  {
    initptr($$);
    $$->database = LexCString{NULL, 0};
    $$->name = $1;
    $$->alias = $1;
  }
| identifier_c T_DOT identifier_c
  {
    initptr($$);
    $$->database = $1;
    $$->name = $3;
    $$->alias = $3;
  }
| identifier_c T_AS identifier_c
  {
    initptr($$);
    $$->database = LexCString{NULL, 0};
    $$->name = $1;
    $$->alias = $3;
  }
| identifier_c T_DOT identifier_c T_AS identifier_c
  {
    initptr($$);
    $$->database = $1;
    $$->name = $3;
    $$->alias = $5;
  }

join_list:
  %empty                                { $$.head = NULL; $$.tail = NULL; }
| join_list join_clause                 { if ($1.head == NULL) {
                                            $$.head = $2; $$.tail = $2;
                                          } else {
                                            $$.head = $1.head; $$.tail = $2;
                                            $1.tail->next = $2;
                                          }
                                        }

join_clause:
  T_JOIN table_ref T_ON join_condition_list
  {
    initptr($$);
    $$->join_type = JoinClause::INNER_JOIN;
    $$->table = *$2;
    $$->conditions = $4;
    $$->next = NULL;
  }
| T_KW_LEFT T_JOIN table_ref T_ON join_condition_list
  {
    initptr($$);
    $$->join_type = JoinClause::LEFT_OUTER_JOIN;
    $$->table = *$3;
    $$->conditions = $5;
    $$->next = NULL;
  }
| T_KW_LEFT T_KW_OUTER T_JOIN table_ref T_ON join_condition_list
  {
    initptr($$);
    $$->join_type = JoinClause::LEFT_OUTER_JOIN;
    $$->table = *$4;
    $$->conditions = $6;
    $$->next = NULL;
  }

join_condition_list:
  join_condition
  {
    $$ = $1;
  }
| join_condition_list T_AND join_condition
  {
    /* Append $3 to end of list starting at $1 */
    struct JoinCondition *tail = $1;
    while (tail->next != NULL) tail = tail->next;
    tail->next = $3;
    $$ = $1;
  }

join_condition:
  identifier_c T_DOT identifier_c T_EQUALS identifier_c T_DOT identifier_c
  {
    initptr($$);
    $$->child_table = $1;
    $$->child_column = $3;
    $$->parent_table = $5;
    $$->parent_column = $7;
    $$->next = NULL;
  }

where_opt:
  %empty                                { $$ = NULL; }
| T_WHERE cond_expr                     { $$ = $2; }

cond_expr:
  identifier_c                          { initptr($$); $$->op = T_IDENTIFIER; $$->col_idx = context->column_name_to_idx($1); }
| identifier_c T_DOT identifier_c      { initptr($$); $$->op = T_IDENTIFIER; $$->col_idx = context->qualified_column_name_to_idx($1, $3); }
| T_STRING                              { initptr($$); $$->op = T_STRING; $$->string = $1; }
| T_INT                                 { initptr($$); $$->op = T_INT; $$->constant_integer = $1; }
| T_FLOAT                               { initptr($$); $$->op = T_FLOAT; $$->constant_float.dbl = $1.dbl; $$->constant_float.ls = $1.ls; }
| T_MINUS cond_expr                     { init_cond($$, NULL, T_MINUS, $2); }
| T_LEFT cond_expr T_RIGHT              { $$ = $2; }
| cond_expr T_OR cond_expr              { init_cond($$, $1, T_OR, $3); }
| cond_expr T_XOR cond_expr             { init_cond($$, $1, T_XOR, $3); }
| cond_expr T_AND cond_expr             { init_cond($$, $1, T_AND, $3); }
| T_NOT cond_expr                       { init_cond($$, $2, T_NOT, NULL); }
| cond_expr T_EQUALS cond_expr          { init_cond($$, $1, T_EQUALS, $3); }
| cond_expr T_GE cond_expr              { init_cond($$, $1, T_GE, $3); }
| cond_expr T_GT cond_expr              { init_cond($$, $1, T_GT, $3); }
| cond_expr T_LE cond_expr              { init_cond($$, $1, T_LE, $3); }
| cond_expr T_LT cond_expr              { init_cond($$, $1, T_LT, $3); }
| cond_expr T_NOT_EQUALS cond_expr      { init_cond($$, $1, T_NOT_EQUALS, $3); }
| cond_expr T_LIKE cond_expr            { init_cond($$, $1, T_LIKE, $3); }
| cond_expr T_IN T_LEFT in_list T_RIGHT {
    /* Expand col IN (a, b, c) to col = a OR col = b OR col = c */
    ConditionalExpression* item = $4;
    $$ = NULL;
    while (item != NULL) {
      ConditionalExpression* next = item->args.right;
      ConditionalExpression* eq;
      init_cond(eq, $1, T_EQUALS, item->args.left);
      if ($$ == NULL) {
        $$ = eq;
      } else {
        ConditionalExpression* or_node;
        init_cond(or_node, $$, T_OR, eq);
        $$ = or_node;
      }
      item = next;
    }
  }
| cond_expr T_IS T_NULL                 { initptr($$); $$->op = T_IS; $$->is.arg = $1; $$->is.null = true; }
| cond_expr T_IS T_NOT T_NULL           { initptr($$); $$->op = T_IS; $$->is.arg = $1; $$->is.null = false; }
| cond_expr T_BITWISE_OR cond_expr      { init_cond($$, $1, T_BITWISE_OR, $3); }
| cond_expr T_BITWISE_AND cond_expr     { init_cond($$, $1, T_BITWISE_AND, $3); }
| cond_expr T_BITSHIFT_LEFT cond_expr   { init_cond($$, $1, T_BITSHIFT_LEFT, $3); }
| cond_expr T_BITSHIFT_RIGHT cond_expr  { init_cond($$, $1, T_BITSHIFT_RIGHT, $3); }
| cond_expr T_PLUS cond_expr            { init_cond($$, $1, T_PLUS, $3); }
| cond_expr T_MINUS cond_expr           { init_cond($$, $1, T_MINUS, $3); }
| cond_expr T_MULTIPLY cond_expr        { init_cond($$, $1, T_MULTIPLY, $3); }
| cond_expr T_SLASH cond_expr           { init_cond($$, $1, T_SLASH, $3); }
| cond_expr T_DIV cond_expr             { init_cond($$, $1, T_DIV, $3); }
| cond_expr T_MODULO cond_expr          { init_cond($$, $1, T_MODULO, $3); }
| cond_expr T_BITWISE_XOR cond_expr     { init_cond($$, $1, T_BITWISE_XOR, $3); }
| T_EXCLAMATION cond_expr               { init_cond($$, $2, T_EXCLAMATION, NULL); }
| T_INTERVAL cond_expr interval_type    { initptr($$); $$->op = T_INTERVAL; $$->interval.arg = $2; $$->interval.interval_type = $3; }
| T_DATE_ADD T_LEFT cond_expr T_COMMA cond_expr T_RIGHT     { init_cond($$, $3, T_DATE_ADD, $5); }
| T_DATE_SUB T_LEFT cond_expr T_COMMA cond_expr T_RIGHT     { init_cond($$, $3, T_DATE_SUB, $5); }
| T_EXTRACT T_LEFT interval_type T_FROM cond_expr T_RIGHT   { initptr($$); $$->op = T_EXTRACT; $$->extract.interval_type = $3; $$->extract.arg = $5; }
| aggfun T_LEFT arith_expr T_RIGHT    {
                                          initptr($$);
                                          $$->op = $1;
                                          switch($1) {
                                            case T_SUM: $$->having_agg.agg_index = context->get_agg()->Sum($3); break;
                                            case T_MIN: $$->having_agg.agg_index = context->get_agg()->Min($3); break;
                                            case T_MAX: $$->having_agg.agg_index = context->get_agg()->Max($3); break;
                                            default: abort();
                                          }
                                        }
| T_COUNT T_LEFT arith_expr T_RIGHT   {
                                          initptr($$);
                                          $$->op = T_COUNT;
                                          $$->having_agg.agg_index = context->get_agg()->Count($3);
                                        }
| T_COUNT T_LEFT T_MULTIPLY T_RIGHT   {
                                          initptr($$);
                                          $$->op = T_COUNT;
                                          $$->having_agg.agg_index = context->get_agg()->Count(context->get_agg()->ConstantInteger(1));
                                        }
| T_AVG T_LEFT arith_expr T_RIGHT     {
                                          initptr($$);
                                          $$->op = T_AVG;
                                          $$->having_agg.agg_index = context->get_agg()->Sum($3);
                                          $$->having_agg.agg_index2 = context->get_agg()->Count($3);
                                        }
| T_EXISTS T_LEFT subquery T_RIGHT    {
                                          initptr($$);
                                          $$->op = T_EXISTS;
                                          $$->subquery.stmt = $3;
                                        }
| cond_expr T_IN T_LEFT subquery T_RIGHT
                                        {
                                          initptr($$);
                                          $$->op = I_IN_SUBQUERY;
                                          $$->in_subquery.expr = $1;
                                          $$->in_subquery.stmt = $4;
                                        }
| T_LEFT subquery T_RIGHT             {
                                          initptr($$);
                                          $$->op = I_SUBQUERY;
                                          $$->subquery.stmt = $2;
                                        }

subquery:
  { context->enter_subquery(); }
  T_SELECT outputlist T_FROM table_ref join_list where_opt
  groupby_opt having_opt orderby_opt limit_opt
  {
    context->leave_subquery();
    $$ = context->get_allocator()->alloc_exc<SelectStatement>(1);
    $$->do_explain = false;
    $$->outputs = $3.head;
    $$->root_table = $5;
    $$->table = $5->name;
    $$->joins = $6.head;
    $$->where_expression = $7;
    $$->groupby_columns = $8;
    $$->having_expression = $9;
    $$->orderby_columns = $10;
    $$->limit = $11;
    $$->sql_begin = (@2).begin;
    $$->sql_end = (@11).end;
  }

interval_type:
  T_MICROSECOND                         { $$ = T_MICROSECOND; }
| T_SECOND                              { $$ = T_SECOND; }
| T_MINUTE                              { $$ = T_MINUTE; }
| T_HOUR                                { $$ = T_HOUR; }
| T_DAY                                 { $$ = T_DAY; }
| T_WEEK                                { $$ = T_WEEK; }
| T_MONTH                               { $$ = T_MONTH; }
| T_QUARTER                             { $$ = T_QUARTER; }
| T_YEAR                                { $$ = T_YEAR; }
| T_SECOND_MICROSECOND                  { $$ = T_SECOND_MICROSECOND; }
| T_MINUTE_MICROSECOND                  { $$ = T_MINUTE_MICROSECOND; }
| T_MINUTE_SECOND                       { $$ = T_MINUTE_SECOND; }
| T_HOUR_MICROSECOND                    { $$ = T_HOUR_MICROSECOND; }
| T_HOUR_SECOND                         { $$ = T_HOUR_SECOND; }
| T_HOUR_MINUTE                         { $$ = T_HOUR_MINUTE; }
| T_DAY_MICROSECOND                     { $$ = T_DAY_MICROSECOND; }
| T_DAY_SECOND                          { $$ = T_DAY_SECOND; }
| T_DAY_MINUTE                          { $$ = T_DAY_MINUTE; }
| T_DAY_HOUR                            { $$ = T_DAY_HOUR; }
| T_YEAR_MONTH                          { $$ = T_YEAR_MONTH; }

in_list:
  cond_expr                             { init_cond($$, $1, T_COMMA, NULL); }
| in_list T_COMMA cond_expr            { init_cond($$, $3, T_COMMA, $1); }

having_opt:
  %empty                                { $$ = NULL; }
| T_HAVING cond_expr                    { $$ = $2; }

groupby_opt:
  %empty                                { $$ = NULL; }
| T_GROUP T_BY groupby_cols             { $$ = $3; }

groupby_cols:
  groupby_col                           { $$ = $1; }
| groupby_col T_COMMA groupby_cols      { $$ = $1; $$->next = $3; }

groupby_col:
  identifier_c                          { initptr($$); $$->col_idx = context->column_name_to_idx($1); $$->next = NULL; }
| identifier_c T_DOT identifier_c      { initptr($$); $$->col_idx = context->qualified_column_name_to_idx($1, $3); $$->next = NULL; }

orderby_opt:
  %empty                                { $$ = NULL; }
| orderby                               { $$ = $1; }

orderby:
  T_ORDER T_BY orderby_cols             { $$ = $3; }

orderby_cols:
  orderby_col                           { $$ = $1; }
| orderby_col T_COMMA orderby_cols      { $$ = $1; $$->next = $3; }

orderby_col:
  identifier_c                          { initptr($$); $$->col_idx = context->column_name_to_idx($1); $$->ascending = true; $$->next = NULL; }
| identifier_c T_ASC                    { initptr($$); $$->col_idx = context->column_name_to_idx($1); $$->ascending = true; $$->next = NULL; }
| identifier_c T_DESC                   { initptr($$); $$->col_idx = context->column_name_to_idx($1); $$->ascending = false; $$->next = NULL; }
| identifier_c T_DOT identifier_c      { initptr($$); $$->col_idx = context->qualified_column_name_to_idx($1, $3); $$->ascending = true; $$->next = NULL; }
| identifier_c T_DOT identifier_c T_ASC  { initptr($$); $$->col_idx = context->qualified_column_name_to_idx($1, $3); $$->ascending = true; $$->next = NULL; }
| identifier_c T_DOT identifier_c T_DESC { initptr($$); $$->col_idx = context->qualified_column_name_to_idx($1, $3); $$->ascending = false; $$->next = NULL; }

limit_opt:
  %empty                                { $$ = -1; }
| T_LIMIT T_INT                         { $$ = $2; }

%%

void rsqlp_error(RSQLP_LTYPE* yylloc, yyscan_t scanner, const char *s)
{
  (void)s;
  // Get the location of the last lexer match. This is not the same as the last
  // token, but lex_end should match with the end of the last token.
  char* lex_begin = rsqlp_get_text(scanner);
  int len_int = rsqlp_get_leng(scanner); /* datatype matches int yyget_leng
                                          * (yyscan_t yyscanner) in
                                          * RonDBSQLLexer.l.cpp generated by
                                          * build_lexer.sh
                                          */
  assert(len_int >= 0);
  size_t lex_len = size_t(len_int);
  char* lex_end = lex_begin + lex_len;
  // Get the location of the error as computed by bison. This is generally
  // correct, but in the case of unexpected end of input, bison claims that the
  // last token is problematic even though it's not.
  char* loc_begin = yylloc->begin;
  char* loc_end = yylloc->end;
  size_t loc_len = loc_end - loc_begin;
  // If lexer has read no tokens after the error location, then use the error
  // location provided by bison.
  if (loc_end == lex_end)
  {
    context->set_err_state(
      RonSQLPreparer::ErrState::PARSER_ERROR,
      loc_begin,
      loc_len);
    return;
  }
  // If lexer has attempted to read something after the error location, then use
  // the last lexer match as error location. This should only happen on
  // unexpected end of input.
  if (loc_end <= lex_begin)
  {
    assert(lex_len == 1);
    context->set_err_state(
      RonSQLPreparer::ErrState::PARSER_ERROR,
      lex_begin,
      lex_len);
    return;
  }
  // Those two cases should be the only possibilities.
  abort();
}

// See comment near `#ifndef YYMAXDEPTH` in RonSQLParser.y.cpp
static_assert(((unsigned long int)(YYSTACK_ALLOC_MAXIMUM)) >=
              ((unsigned long int)(YYSTACK_BYTES(((unsigned long int)(YYMAXDEPTH))))),
              "YYMAXDEPTH too large");
