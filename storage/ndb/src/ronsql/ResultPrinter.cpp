/*
   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.

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

#include <algorithm>
#include <limits>
#include <cmath>
#include <iomanip>
#include <string>
#include "m_string.h"
#include "ResultPrinter.hpp"
#include "ResultPrinterInternal.hpp"
#include "RonSQLParser.y.hpp"
#include "define_formatter.hpp"
#include "RonSQLPreparer.hpp"
#include "mysql/strings/dtoa.h"
#include <sql_string.h>
#include "decimal.h"
#include <decimal_utils.hpp>
#include "my_time.h"
#include "mysql/strings/m_ctype.h"
#include "my_byteorder.h"

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_RONSQLPRINTER 1
#endif

#ifdef DEBUG_RONSQLPRINTER
#define DEB_TRACE() do { \
  printf("RonSQLPrinter.cpp:%d\n", __LINE__); \
  fflush(stdout); \
} while (0)
#else
#define DEB_TRACE() do { } while (0)
#endif

using std::endl;
using std::max;

/*
 * Lock-free UTC epoch-seconds -> broken-down MYSQL_TIME, for displaying
 * TIMESTAMP (Timestamp2) results.  glibc gmtime_r()/gmtime() funnel through
 * __tz_convert(), which takes the process-global tzset_lock on every call even
 * for UTC, serializing concurrent RDRS request threads.  This uses only the
 * in-tree calendar arithmetic (get_date_from_daynr, mysys/my_time.cc), so it
 * touches no glibc lock.  Field-for-field equivalent to MySQL's
 * sec_to_TIME(out, t, 0) (the my_tz_OFFSET0 path); mirrors the kernel's
 * ttl_utc_sec_to_TIME from commit 808bb79ce23 (Avoid glibc tzset_lock on TTL
 * hot paths).  Leaves second_part to the caller (fractional seconds).
 */
static inline void ronsql_utc_sec_to_TIME(time_t t, MYSQL_TIME *out)
{
  /* calc_daynr(1970, 1, 1) == 719528 (days from year 0 to the Unix epoch). */
  const int64_t EPOCH_DAYNR = 719528;
  int64_t days = (int64_t)t / 86400;
  int32_t secs = (int32_t)((int64_t)t % 86400);
  if (secs < 0) { /* t < 0: normalize into [0, 86400) */
    secs += 86400;
    days -= 1;
  }
  unsigned int year, month, day;
  get_date_from_daynr(days + EPOCH_DAYNR, &year, &month, &day);
  out->neg = false;
  out->second_part = 0;
  out->year = year;
  out->month = month;
  out->day = day;
  out->hour = secs / 3600;
  out->minute = (secs % 3600) / 60;
  out->second = secs % 60;
  out->time_zone_displacement = 0;
  out->time_type = MYSQL_TIMESTAMP_DATETIME;
}

/*
 * TIMESTAMP my_timeval -> MYSQL_TIME, including the zero-value rule:
 * MySQL reserves tv_sec == 0 for the zero timestamp 0000-00-00 00:00:00
 * (Field_timestampf::get_date_internal_at, sql/field.cc) — the valid
 * range starts at 1970-01-01 00:00:01.  Routing tv_sec == 0 through the
 * epoch converter would wrongly print 1970-01-01 00:00:00.
 */
static inline void ronsql_timestamp_tv_to_TIME(const my_timeval &tv,
                                               MYSQL_TIME *out)
{
  if (tv.m_tv_sec == 0) {
    memset(out, 0, sizeof(*out));
    out->time_type = MYSQL_TIMESTAMP_DATETIME;
    return;
  }
  ronsql_utc_sec_to_TIME((time_t)tv.m_tv_sec, out);
  out->second_part = (unsigned long)tv.m_tv_usec;
}

#define feature_not_implemented(description) \
  throw RonSQLPermanentError(RonSQLErrorClass::UNSUPPORTED, \
                             "RonSQL feature not implemented: " description)
#define bug(x) throw RonSQLPermanentError(x " Please report a bug.")

DEFINE_FORMATTER(quoted_identifier, LexCString, {
  os.put('`');
  for (Uint32 i = 0; i < value.len; i++)
  {
    char ch = value.str[i];
    if (ch == '`')
      os.write("``", 2);
    else
      os.put(ch);
  }
  os.put('`');
})

// T-16: defined below in ronsql_printer_internal (ResultPrinterInternal.hpp),
// which ResultPrinterUnitTest calls.
using ronsql_printer_internal::compare_having_typed;
using ronsql_printer_internal::decode_temporal_packed;
using ronsql_printer_internal::double_to_fixed;
using ronsql_printer_internal::format_fixed_scale;
using ronsql_printer_internal::having_arithmetic;
using ronsql_printer_internal::having_negate;
using ronsql_printer_internal::print_base64;
using ronsql_printer_internal::print_binary_tsv;
using ronsql_printer_internal::print_json_ascii;
using ronsql_printer_internal::print_string;
using ronsql_printer_internal::print_temporal_packed;
static double convert_result_to_double(NdbAggregator::Result result);

// require or investigate schema version
static inline void
require_sch(bool condition, const char* msg)
{
  if (likely(condition)) return;
  throw RonSQLMaybeStaleSchema(msg);
}

// The value bytes of a non-NULL BINARY / VARBINARY / LONGVARBINARY
// NdbRecAttr (or its PassthroughValue copy).  BINARY(n) is its full
// padded length, as MySQL returns it.
template <class Attr>
static void
binary_value_bytes(const Attr* attr,
                   const unsigned char** bytes,
                   size_t* len)
{
  const NdbDictionary::Column* col = attr->getColumn();
  require_sch(col != nullptr, "NULL column on BINARY NdbRecAttr");
  const unsigned char* data =
      pointer_cast<const unsigned char*>(attr->aRef());
  switch (attr->getType()) {
  case NdbDictionary::Column::Binary:
    *bytes = data;
    *len = (size_t)col->getSizeInBytes();
    break;
  case NdbDictionary::Column::Varbinary:
    *bytes = &data[1];
    *len = (size_t)data[0];
    break;
  default:
    assert(attr->getType() == NdbDictionary::Column::Longvarbinary);
    *bytes = &data[2];
    *len = (size_t)data[0] | ((size_t)data[1] << 8);
    break;
  }
}

ResultPrinter::ResultPrinter(ArenaMalloc* amalloc,
                             struct SelectStatement* query,
                             DynamicArray<LexCString>* column_names,
                             const ColumnMetadata* column_metadata,
                             RonSQLExecParams::OutputFormat output_format,
                             std::basic_ostream<char>* err):
  m_amalloc(amalloc),
  m_query(query),
  m_column_names(column_names),
  m_column_metadata(column_metadata),
  m_output_format(output_format),
  m_err(err),
  m_program(amalloc),
  m_groupby_cols(amalloc),
  m_outputs(amalloc),
  m_col_idx_groupby_map(amalloc),
  m_orderby_specs(amalloc),
  m_has_orderby(query->orderby_columns != NULL ||
                query->having_expression != NULL)
{
  assert(amalloc != NULL);
  assert(query != NULL);
  assert(column_names != NULL);
  switch (output_format)
  {
  case RonSQLExecParams::OutputFormat::JSON:
    break;
  case RonSQLExecParams::OutputFormat::JSON_ASCII:
    break;
  case RonSQLExecParams::OutputFormat::TEXT:
    break;
  case RonSQLExecParams::OutputFormat::TEXT_NOHEADER:
    break;
  default:
    ronsql_bug("The result printer got an unknown output format.");
  }
  assert(err != NULL);
  compile();
  optimize();
}

ResultPrinter::ResultPrinter(ArenaMalloc* amalloc,
                             struct SelectStatement* query,
                             DynamicArray<LexCString>* column_names,
                             const ColumnMetadata* column_metadata,
                             RonSQLExecParams::OutputFormat output_format,
                             std::basic_ostream<char>* err,
                             bool /*passthrough_marker*/):
  m_amalloc(amalloc),
  m_query(query),
  m_column_names(column_names),
  m_column_metadata(column_metadata),
  m_output_format(output_format),
  m_err(err),
  m_program(amalloc),
  m_groupby_cols(amalloc),
  m_outputs(amalloc),
  m_col_idx_groupby_map(amalloc),
  m_orderby_specs(amalloc),
  m_has_orderby(false)
{
  assert(amalloc != NULL);
  assert(query != NULL);
  assert(column_names != NULL);
  assert(err != NULL);
  setup_output_format();
}

void
ResultPrinter::validate_orderby_columns()
{
  std::basic_ostream<char>& err = *m_err;
  DynamicArray<LexCString>& column_names = *m_column_names;
  struct OrderbyColumns* ob = m_query->orderby_columns;
  while (ob != NULL)
  {
    if (ob->kind == OrderbyColumns::Kind::OUTPUT_REF)
    {
      // Alias reference — find the output and determine its type
      Uint32 output_idx = ob->output_idx;
      Outputs* out = m_query->outputs;
      for (Uint32 i = 0; i < output_idx && out != NULL; i++)
        out = out->next;
      assert(out != NULL);

      if (out->type == Outputs::Type::COLUMN)
      {
        // Column alias — resolve to GROUP BY index like a regular column
        Uint32 col_idx = out->column.col_idx;
        bool found = false;
        for (Uint32 i = 0; i < m_groupby_cols.size(); i++)
        {
          if (m_groupby_cols[i] == col_idx)
          {
            while (m_col_idx_groupby_map.size() < col_idx + 1)
              m_col_idx_groupby_map.push(0);
            m_col_idx_groupby_map[col_idx] = i;
            CHARSET_INFO* charset = NULL;
            if (m_column_metadata != NULL &&
                m_column_metadata[col_idx].has_metadata)
              charset = m_column_metadata[col_idx].charset;
            OrderbySpec spec;
            spec.kind = OrderbySpec::Kind::GROUPBY_COL;
            spec.groupby_idx = i;
            spec.ascending = ob->ascending;
            spec.charset = charset;
            spec.decimal_scale = double_sort_scale(col_idx);
            m_orderby_specs.push(spec);
            found = true;
            break;
          }
        }
        if (!found)
        {
          err << "Syntax error: ORDER BY alias refers to column "
              << quoted_identifier(column_names[out->column.col_idx])
              << " which is not in the GROUP BY clause." << endl;
          throw RonSQLPermanentError(
              "ORDER BY column not in GROUP BY clause.");
        }
      }
      else if (out->type == Outputs::Type::AVG)
      {
        // AVG alias — sort by SUM / COUNT, not by either slot alone
        OrderbySpec spec;
        spec.kind = OrderbySpec::Kind::AVG;
        spec.avg_key_idx = m_num_avg_keys++;
        spec.ascending = ob->ascending;
        spec.charset = NULL;
        spec.avg_sum_idx = out->avg.agg_index_sum;
        spec.avg_count_idx = out->avg.agg_index_count;
        spec.avg_scale = avg_arg_scale(out);
        spec.decimal_scale = 0;
        m_orderby_specs.push(spec);
      }
      else if (out->type == Outputs::Type::AGGREGATE ||
               out->type == Outputs::Type::SUBQUERY_AGG)
      {
        // Aggregate alias — sort by aggregate result
        Uint32 agg_idx = (out->type == Outputs::Type::SUBQUERY_AGG)
                             ? out->subquery_agg.agg_index
                             : out->aggregate.agg_index;
        OrderbySpec spec;
        spec.kind = OrderbySpec::Kind::AGGREGATE;
        spec.agg_result_idx = agg_idx;
        spec.ascending = ob->ascending;
        spec.charset = out->type == Outputs::Type::AGGREGATE
            ? aggregate_arg_charset(out)
            : NULL;
        // O-9: a SUM, MIN or MAX over a DECIMAL column is a DOUBLE here.
        spec.decimal_scale = 0;
        if (out->type == Outputs::Type::AGGREGATE &&
            out->aggregate.fun != T_COUNT)
        {
          const int scale = exact_arg_scale(out->aggregate.arg);
          if (scale > 0)
            spec.decimal_scale = scale;
        }
        m_orderby_specs.push(spec);
      }
    }
    else
    {
      // TABLE_COLUMN — existing logic
      Uint32 col_idx = ob->col_idx;
      bool found = false;
      for (Uint32 i = 0; i < m_groupby_cols.size(); i++)
      {
        if (m_groupby_cols[i] == col_idx)
        {
          while (m_col_idx_groupby_map.size() < col_idx + 1)
            m_col_idx_groupby_map.push(0);
          m_col_idx_groupby_map[col_idx] = i;
          CHARSET_INFO* charset = NULL;
          if (m_column_metadata != NULL &&
              m_column_metadata[col_idx].has_metadata)
            charset = m_column_metadata[col_idx].charset;
          OrderbySpec spec;
          spec.kind = OrderbySpec::Kind::GROUPBY_COL;
          spec.groupby_idx = i;
          spec.ascending = ob->ascending;
          spec.charset = charset;
          spec.decimal_scale = double_sort_scale(col_idx);
          m_orderby_specs.push(spec);
          found = true;
          break;
        }
      }
      if (!found)
      {
        assert(m_column_names->size() > col_idx);
        err << "Syntax error: ORDER BY refers to column "
            << quoted_identifier(column_names[col_idx])
            << " which is not in the GROUP BY clause." << endl;
        throw RonSQLPermanentError("ORDER BY column not in GROUP BY clause.");
      }
    }
    ob = ob->next;
  }
}

void
ResultPrinter::compile()
{
  std::basic_ostream<char>& err = *m_err;
  DynamicArray<LexCString>& column_names = *m_column_names;
  // Populate m_groupby_columns, an array of the column idxs listed in GROUP BY.
  {
    struct GroupbyColumns* g = m_query->groupby_columns;
    while(g != NULL)
    {
      m_groupby_cols.push(g->col_idx);
      g = g->next;
    }
  }
  // Populate and validate m_outputs, an array of the SELECT expressions.
  // Calculate number_of_aggregates.
  // Populate m_col_idx_groupby_map.
  Uint32 number_of_aggregates = 0;
  {
    struct Outputs* o = m_query->outputs;
    while(o != NULL)
    {
      m_outputs.push(o);
      switch (o->type)
      {
      case Outputs::Type::COLUMN:
        for (Uint32 i = 0; ; i++)
        {
          // Validate that the column appears in the GROUP BY clause
          Uint32 col_idx = o->column.col_idx;
          if (i >= m_groupby_cols.size())
          {
            assert(m_column_names->size() > col_idx);
            err << "Syntax error: SELECT expression refers to ungrouped column "
                << quoted_identifier(column_names[col_idx])
                << " outside of aggregate function." << endl
                << "You can either add this column to the GROUP BY clause, "
                << "or use it within an aggregate function e.g. Sum("
                << quoted_identifier(column_names[col_idx])
                << ")." << endl;
            throw RonSQLPermanentError("Ungrouped column in non-aggregated SELECT expression.");
            // todo Test for aggregates without groups and groups without aggregates.
          }
          if (m_groupby_cols[i] == col_idx)
          {
            while (m_col_idx_groupby_map.size() < col_idx + 1)
            {
              m_col_idx_groupby_map.push(0);
            }
            m_col_idx_groupby_map[col_idx] = i;
            break;
          }
        }
        break;
      case Outputs::Type::AGGREGATE:
        number_of_aggregates =
          max(number_of_aggregates, o->aggregate.agg_index + 1);
        break;
      case Outputs::Type::AVG:
        number_of_aggregates =
          max(number_of_aggregates, o->avg.agg_index_sum + 1);
        number_of_aggregates =
          max(number_of_aggregates, o->avg.agg_index_count + 1);
        break;
      default:
        ronsql_bug("The result printer got a SELECT output that is not a "
                   "column, an aggregate or an AVG.");
      }
      o = o->next;
    }
  }
  // Account for aggregates referenced only in HAVING (not in SELECT)
  if (m_query->having_expression != NULL)
  {
    scan_having_max_agg(m_query->having_expression, number_of_aggregates);
  }
  m_num_groupby_cols = m_groupby_cols.size();
  // Include hidden sentinel slot (if present) so it gets fetched from results
  if (m_query->sentinel_agg_slot >= 0 &&
      (Uint32)m_query->sentinel_agg_slot >= number_of_aggregates) {
    number_of_aggregates = (Uint32)m_query->sentinel_agg_slot + 1;
  }
  m_num_aggregates = number_of_aggregates;
  validate_orderby_columns();
  // Allocate registers. Even if some of them won't be used in an optimized
  // program, the memory waste is minimal.
  m_regs_g = m_amalloc->alloc_exc<NdbAggregator::Column>(m_groupby_cols.size());
  m_regs_a = m_amalloc->alloc_exc<NdbAggregator::Result>(number_of_aggregates);
  // Create a correct but non-optimized program
  for (Uint32 i = 0; i < m_groupby_cols.size(); i++)
  {
    Cmd cmd;
    cmd.type = Cmd::Type::STORE_GROUP_BY_COLUMN;
    cmd.store_group_by_column.group_by_idx = i;
    cmd.store_group_by_column.reg_g = i;
    m_program.push(cmd);
  }
  {
    Cmd cmd;
    cmd.type = Cmd::Type::END_OF_GROUP_BY_COLUMNS;
    m_program.push(cmd);
  }
  for (Uint32 i = 0; i < number_of_aggregates; i++)
  {
    Cmd cmd;
    cmd.type = Cmd::Type::STORE_AGGREGATE;
    cmd.store_aggregate.agg_index = i;
    cmd.store_aggregate.reg_a = i;
    m_program.push(cmd);
  }
  {
    Cmd cmd;
    cmd.type = Cmd::Type::END_OF_AGGREGATES;
    m_program.push(cmd);
  }
  m_print_start_idx = m_program.size();
  setup_output_format();
  for (Uint32 i = 0; i < m_outputs.size(); i++)
  {
    {
      Cmd cmd;
      cmd.type = Cmd::Type::PRINT_STR;
      bool is_first = i == 0;
      if (m_json_output)
      {
        cmd.print_str.content = LexString{ is_first ? "{" : ",", 1 };
        m_program.push(cmd);
      }
      else if (m_tsv_output && !is_first)
      {
        cmd.print_str.content = LexString{ "\t", 1 };
        m_program.push(cmd);
      }
      else if (m_tsv_output && is_first)
      {
        // The first column is not preceded by a tab.
      }
      else
      {
        ronsql_bug("The result printer has neither JSON nor TSV output.");
      }
    }
    Outputs* o = m_outputs[i];
    if (m_json_output)
    {
      Cmd cmd;
      cmd.type = Cmd::Type::PRINT_STR_JSON;
      cmd.print_str.content = o->output_name;
      m_program.push(cmd);
    }
    if (m_json_output)
    {
      Cmd cmd;
      cmd.type = Cmd::Type::PRINT_STR;
      cmd.print_str.content = LexString{ ":", 1 };
      m_program.push(cmd);
    }
    switch (o->type) {
      case Outputs::Type::COLUMN:
      {
        // todo indent case, move break inside braces. (This todo from review 2024-08-22 with MR)
        CHARSET_INFO* charset;
        int precision;
        int scale;
        if (m_column_metadata != NULL &&
            m_column_metadata[o->column.col_idx].has_metadata) {
          const ColumnMetadata& meta =
              m_column_metadata[o->column.col_idx];
          charset = meta.charset;
          precision = meta.precision;
          scale = meta.scale;
        } else {
          // During EXPLAIN SELECT without access to ndb we still need to
          // compile, but these values won't be used.
          charset = NULL;
          precision = 0;
          scale = 0;
        }
        ronsql_require_bug(precision <= 65,
                           "A GROUP BY output has a precision above 65.");
        ronsql_require_bug(scale <= 30,
                           "A GROUP BY output has a scale above 30.");
        ronsql_require_bug(scale <= precision,
                           "A GROUP BY output has a scale above its "
                           "precision.");
        Cmd cmd;
        cmd.type = Cmd::Type::PRINT_GROUP_BY_COLUMN;
        cmd.print_group_by_column.reg_g = m_col_idx_groupby_map[o->column.col_idx];
        cmd.print_group_by_column.charset = charset;
        cmd.print_group_by_column.precision = precision;
        cmd.print_group_by_column.scale = scale;
        cmd.print_group_by_column.float_display =
            m_column_metadata != NULL &&
            m_column_metadata[o->column.col_idx].float_display;
        // O-3: a CTE MIN / MAX of a temporal column is a Bigunsigned key
        // holding the packed value, decoded as print_passthrough_value
        // and print_aggregate_result do.
        cmd.print_group_by_column.temporal = TemporalDisplay::NONE;
        cmd.print_group_by_column.temporal_fsp = 0;
        if (m_column_metadata != NULL &&
            m_column_metadata[o->column.col_idx].has_metadata) {
          cmd.print_group_by_column.temporal =
              m_column_metadata[o->column.col_idx].temporal;
          cmd.print_group_by_column.temporal_fsp =
              m_column_metadata[o->column.col_idx].temporal_fsp;
        }
        m_program.push(cmd);
        break;
      }
      case Outputs::Type::AGGREGATE:
      {
        Cmd cmd;
        cmd.type = Cmd::Type::PRINT_AGGREGATE;
        cmd.print_aggregate.reg_a = o->aggregate.agg_index;
        cmd.print_aggregate.charset = aggregate_arg_charset(o);
        cmd.print_aggregate.float_display = aggregate_uses_float_display(o);
        {
          // D15: a SUM, MIN or MAX over a DECIMAL (precision > 0) prints
          // at the DECIMAL's scale, as mysqld does.  O-7: also above
          // precision 15; print_fixed_scale() rounds the double's
          // shortest digits.  Before, such a value dropped its scale
          // (10.50 printed 10.5, 0.00 printed 0).
          int sc = aggregate_arg_scale(o);
          int pr = aggregate_arg_precision(o);
          cmd.print_aggregate.scale = (pr > 0) ? sc : 0;
        }
        // D17 + temporal: MIN/MAX over a temporal column → decode the
        // Bigunsigned packed value back to its text form.
        {
          int fsp = 0;
          cmd.print_aggregate.temporal = aggregate_arg_temporal(o, fsp);
          cmd.print_aggregate.temporal_fsp = fsp;
        }
        m_program.push(cmd);
        break;
      }
      case Outputs::Type::AVG:
      {
        Cmd cmd;
        cmd.type = Cmd::Type::PRINT_AVG;
        cmd.print_avg.reg_a_sum = o->avg.agg_index_sum;
        cmd.print_avg.reg_a_count = o->avg.agg_index_count;
        cmd.print_avg.scale = avg_arg_scale(o);
        m_program.push(cmd);
        break;
      }
    default:
      ronsql_bug("The result printer got a SELECT output that is not a "
                 "column, an aggregate or an AVG.");
    }
  }
  if (m_json_output)
  {
    Cmd cmd;
    cmd.type = Cmd::Type::PRINT_STR;
    cmd.print_str.content = LexString{ "}\n", 2 };
    m_program.push(cmd);
  }
  else if (m_tsv_output)
  {
    Cmd cmd;
    cmd.type = Cmd::Type::PRINT_STR;
    cmd.print_str.content = LexString{ "\n", 1 };
    m_program.push(cmd);
  }
  else
  {
    ronsql_bug("The result printer has neither JSON nor TSV output.");
  }
}

void
ResultPrinter::optimize()
{
  // todo
}

DEFINE_FORMATTER(d2, uint, {
  if (value < 10) os << '0';
  os << value;
})

// RONSQL-45: a DATE year is printed with four digits, as in MySQL
// (0999-01-01, and 0000-00-00 for the zero date).
DEFINE_FORMATTER(d4, uint, {
  if (value < 1000) os << '0';
  if (value < 100) os << '0';
  if (value < 10) os << '0';
  os << value;
})

// Run a range of program commands. When executing store commands
// (from..m_print_start_idx), record must be non-NULL. When executing
// print commands (m_print_start_idx..end), out must be non-NULL.
void
ResultPrinter::run_program(Uint32 from, Uint32 to,
                           NdbAggregator::ResultRecord* record,
                           std::ostream*)
{
  for (Uint32 cmd_index = from; cmd_index < to; cmd_index++)
  {
    Cmd& cmd = m_program[cmd_index];
    switch (cmd.type)
    {
    case Cmd::Type::STORE_GROUP_BY_COLUMN:
      {
        assert(record != NULL);
        NdbAggregator::Column column = record->FetchGroupbyColumn();
        if (column.end())
        {
          bug("Got record with fewer GROUP BY columns than expected.");
        }
        m_regs_g[cmd.store_group_by_column.reg_g] = column;
      }
      break;
    case Cmd::Type::END_OF_GROUP_BY_COLUMNS:
      {
        assert(record != NULL);
        NdbAggregator::Column column = record->FetchGroupbyColumn();
        if (!column.end())
        {
          bug("Got record with more GROUP BY columns than expected.");
        }
      }
      break;
    case Cmd::Type::STORE_AGGREGATE:
      {
        assert(record != NULL);
        NdbAggregator::Result result = record->FetchAggregationResult();
        if (result.end())
        {
          bug("Got record with fewer aggregates than expected.");
        }
        m_regs_a[cmd.store_aggregate.reg_a] = result;
      }
      break;
    case Cmd::Type::END_OF_AGGREGATES:
      {
        assert(record != NULL);
        NdbAggregator::Result result = record->FetchAggregationResult();
        if (!result.end())
        {
          bug("Got record with more aggregates than expected.");
        }
      }
      break;
    case Cmd::Type::PRINT_GROUP_BY_COLUMN:
    case Cmd::Type::PRINT_AGGREGATE:
    case Cmd::Type::PRINT_AVG:
    case Cmd::Type::PRINT_STR:
    case Cmd::Type::PRINT_STR_JSON:
      // Delegate to print_record logic via the full program path.
      // These are handled by the existing print_record switch cases,
      // but here we just forward to the appropriate printing code.
      // For simplicity, re-use the inline print_record logic by
      // handling these in print_stored_record instead.
      assert(false && "Print commands should not be run via run_program");
      break;
    default:
      ronsql_bug("The result printer program has a command of unknown type.");
    }
  }
}

ResultPrinter::StoredRow
ResultPrinter::store_record(NdbAggregator::ResultRecord& record)
{
  // Run store phase to populate m_regs_g and m_regs_a
  run_program(0, m_print_start_idx, &record, NULL);
  // Copy registers into arena-allocated arrays
  StoredRow row;
  row.cols = m_amalloc->alloc_exc<NdbAggregator::Column>(m_num_groupby_cols);
  row.results = m_amalloc->alloc_exc<NdbAggregator::Result>(m_num_aggregates);
  memcpy(row.cols, m_regs_g,
         m_num_groupby_cols * sizeof(NdbAggregator::Column));
  memcpy(row.results, m_regs_a,
         m_num_aggregates * sizeof(NdbAggregator::Result));
  row.avg_keys = NULL;
  return row;
}

void
ResultPrinter::print_stored_record(StoredRow& row, std::ostream& out)
{
  // Restore registers from stored row
  memcpy(m_regs_g, row.cols,
         m_num_groupby_cols * sizeof(NdbAggregator::Column));
  memcpy(m_regs_a, row.results,
         m_num_aggregates * sizeof(NdbAggregator::Result));
  print_registers(out);
}

void
ResultPrinter::print_registers(std::ostream& out)
{
  // Run print phase of the program
  for (Uint32 cmd_index = m_print_start_idx;
       cmd_index < m_program.size(); cmd_index++)
  {
    Cmd& cmd = m_program[cmd_index];
    switch (cmd.type)
    {
    case Cmd::Type::PRINT_GROUP_BY_COLUMN:
      {
        NdbAggregator::Column column = m_regs_g[cmd.print_group_by_column.reg_g];
        if(column.is_null())
        {
          out << m_null_representation;
          break;
        }
        switch (column.type())
        {
        case NdbDictionary::Column::Type::Undefined:
          feature_not_implemented("Print GROUP BY column of type Undefined (NULL)");
        case NdbDictionary::Column::Type::Tinyint:
          out << Int32(column.data_int8());
          break;
        case NdbDictionary::Column::Type::Tinyunsigned:
          out << Uint32(column.data_uint8());
          break;
        case NdbDictionary::Column::Type::Smallint:
          out << column.data_int16();
          break;
        case NdbDictionary::Column::Type::Smallunsigned:
          out << column.data_uint16();
          break;
        case NdbDictionary::Column::Type::Mediumint:
          out << column.data_medium();
          break;
        case NdbDictionary::Column::Type::Mediumunsigned:
          out << column.data_umedium();
          break;
        case NdbDictionary::Column::Type::Int:
          out << column.data_int32();
          break;
        case NdbDictionary::Column::Type::Unsigned:
          out << column.data_uint32();
          break;
        case NdbDictionary::Column::Type::Bigint:
          out << column.data_int64();
          break;
        case NdbDictionary::Column::Type::Bigunsigned:
          // O-3: a packed CTE MIN / MAX temporal key.
          if (cmd.print_group_by_column.temporal != TemporalDisplay::NONE)
            print_temporal_packed(out, column.data_uint64(),
                                  cmd.print_group_by_column.temporal,
                                  cmd.print_group_by_column.temporal_fsp,
                                  m_quote);
          else
            out << column.data_uint64();
          break;
        case NdbDictionary::Column::Type::Float:
          print_float_or_double(out, column.data_float(), true);
          break;
        case NdbDictionary::Column::Type::Double:
          // cte_avg_plan.md V4 (C8): a Double GROUP BY column carrying
          // display scale metadata (an AVG or DECIMAL-widened CTE
          // output) prints with fixed scale like MySQL, whatever its
          // precision (O-7), like print_passthrough_value.
          // It is a DECIMAL in MySQL: rounded HALF_UP, never "-0.00"
          // (print_fixed_scale, O-5/O-6).
          {
            int sc = cmd.print_group_by_column.scale;
            int pr = cmd.print_group_by_column.precision;
            if (sc > 0 && pr > 0) {
              print_fixed_scale(out, column.data_double(), sc);
            } else {
              print_float_or_double(out, column.data_double(),
                                    cmd.print_group_by_column.float_display);
            }
          }
          break;
        case NdbDictionary::Column::Type::Olddecimal:
          feature_not_implemented("Print GROUP BY column of type Olddecimal");
        case NdbDictionary::Column::Type::Olddecimalunsigned:
          feature_not_implemented("Print GROUP BY column of type Olddecimalunsigned");
        case NdbDictionary::Column::Type::Decimal:
          [[fallthrough]];
        case NdbDictionary::Column::Type::Decimalunsigned:
          {
            int precision = cmd.print_group_by_column.precision;
            int scale = cmd.print_group_by_column.scale;
            constexpr int DECIMAL_MAX_STR_LEN_IN_BYTES = 68;
            char decStr[DECIMAL_MAX_STR_LEN_IN_BYTES];
            // O-14: on an error decimal_bin2str() leaves decStr unwritten.
            const int dec_ret =
                decimal_bin2str((const void*)column.data(),
                                column.byte_size(),
                                precision,
                                scale,
                                decStr,
                                DECIMAL_MAX_STR_LEN_IN_BYTES);
            ronsql_require_bug(dec_ret == E_DEC_OK,
                               "A DECIMAL GROUP BY value could not be "
                               "converted to text.");
            out << decStr;
            break;
          }
        case NdbDictionary::Column::Type::Char:
          {
            CHARSET_INFO* charset = cmd.print_group_by_column.charset;
            require_sch(charset != nullptr, "Could not find charset for CHAR column");
            LexString content = LexString{ column.data(), column.byte_size() };
            if (m_json_output) {
              out << '"';
              print_string(out, content, charset, true, m_utf8_output, true);
              out << '"';
            }
            else if (m_tsv_output) {
              print_string(out, content, charset, false, true, true);
            }
            else {
              ronsql_bug("The result printer has neither JSON nor TSV output.");
            }
            break;
          }
        case NdbDictionary::Column::Type::Varchar:
        case NdbDictionary::Column::Type::Longvarchar:
          {
            CHARSET_INFO* charset = cmd.print_group_by_column.charset;
            require_sch(charset != nullptr, "Could not find charset for VARCHAR column");
            LexString content;
            if (column.type() == NdbDictionary::Column::Type::Varchar) {
              content = LexString{ &column.data()[1],
                                   (size_t)(unsigned char)column.data()[0] };
            } else {
              content = LexString{ &column.data()[2],
                                   (size_t)(unsigned char)column.data()[0] |
                                   (((size_t)(unsigned char)column.data()[1]) << 8) };
            }
            out << m_quote;
            print_string(out,
                         content,
                         charset,
                         m_json_output,
                         m_utf8_output || m_tsv_output,
                         false);
            out << m_quote;
            break;
          }
        case NdbDictionary::Column::Type::Binary:
        case NdbDictionary::Column::Type::Varbinary:
        case NdbDictionary::Column::Type::Longvarbinary:
          // O-13: as in a pass-through result.
          print_groupby_as_passthrough(out, column,
                                       cmd.print_group_by_column.precision);
          break;
        case NdbDictionary::Column::Type::Datetime:
          feature_not_implemented("Print GROUP BY column of type Datetime");
        case NdbDictionary::Column::Type::Date:
          {
            Uint32 date = column.data_uint32();
            Uint32 year = date >> 9;
            Uint32 month = (date >> 5) & 0xf;
            Uint32 day = date & 0x1f;
            out << m_quote << d4(year) << "-" << d2(month) << "-" << d2(day) << m_quote;
            break;
          }
        case NdbDictionary::Column::Type::Blob:
          feature_not_implemented("Print GROUP BY column of type Blob");
        case NdbDictionary::Column::Type::Text:
          feature_not_implemented("Print GROUP BY column of type Text");
        case NdbDictionary::Column::Type::Bit:
          feature_not_implemented("Print GROUP BY column of type Bit");
        case NdbDictionary::Column::Type::Time:
          feature_not_implemented("Print GROUP BY column of type Time");
        case NdbDictionary::Column::Type::Year:
          // O-13: as in a pass-through result.
          print_groupby_as_passthrough(out, column, 0);
          break;
        case NdbDictionary::Column::Type::Timestamp:
          feature_not_implemented("Print GROUP BY column of type Timestamp");
        case NdbDictionary::Column::Type::Time2:
          // O-13: as in a pass-through result.
          print_groupby_as_passthrough(out, column,
                                       cmd.print_group_by_column.precision);
          break;
        case NdbDictionary::Column::Type::Datetime2:
          {
            int precision = cmd.print_group_by_column.precision;
            longlong numericDate = my_datetime_packed_from_binary(
              (const unsigned char*)column.data(),
              (unsigned int)precision);
            MYSQL_TIME lTime;
            TIME_from_longlong_datetime_packed(&lTime, numericDate);
            char to[MAX_DATE_STRING_REP_LENGTH];
            my_TIME_to_str(lTime, to, precision);
            out << m_quote << to << m_quote;
            break;
          }
        case NdbDictionary::Column::Type::Timestamp2:
          {
            int precision = cmd.print_group_by_column.precision;
            my_timeval myTV{};
            my_timestamp_from_binary(&myTV,
                                     (const unsigned char *)column.data(),
                                     (unsigned int) precision);
            MYSQL_TIME lTime;
            ronsql_timestamp_tv_to_TIME(myTV, &lTime);
            char to[MAX_DATE_STRING_REP_LENGTH];
            my_TIME_to_str(lTime, to, precision);
            out << m_quote << to << m_quote;
            break;
          }
        default:
          bug("Unexpected data type when printing GROUP BY column.");
        }
      }
      break;
    case Cmd::Type::PRINT_AGGREGATE:
      {
        NdbAggregator::Result result = m_regs_a[cmd.print_aggregate.reg_a];
        print_aggregate_result(out, result, cmd.print_aggregate.charset,
                               cmd.print_aggregate.scale,
                               cmd.print_aggregate.float_display,
                               cmd.print_aggregate.temporal,
                               cmd.print_aggregate.temporal_fsp);
      }
      break;
    case Cmd::Type::PRINT_AVG:
      {
        print_avg_result(out, m_regs_a[cmd.print_avg.reg_a_sum],
                         m_regs_a[cmd.print_avg.reg_a_count],
                         cmd.print_avg.scale);
      }
      break;
    case Cmd::Type::PRINT_STR:
      out.write(cmd.print_str.content.str, cmd.print_str.content.len);
      break;
    case Cmd::Type::PRINT_STR_JSON:
      if (m_json_output) {
        out << '"';
        print_string(out,
                     cmd.print_str.content,
                     &my_charset_utf8mb4_bin,
                     true,
                     m_utf8_output,
                     false);
        out << '"';
      } else {
        print_string(out,
                     cmd.print_str.content,
                     &my_charset_utf8mb4_bin,
                     false,
                     true,
                     false);
      }
      break;
    default:
      ronsql_bug("The result printer program has a command of unknown type.");
    }
  }
}

int
ResultPrinter::compare_rows(StoredRow& a, StoredRow& b)
{
  for (Uint32 i = 0; i < m_orderby_specs.size(); i++)
  {
    OrderbySpec& spec = m_orderby_specs[i];
    if (spec.kind == OrderbySpec::Kind::AGGREGATE)
    {
      NdbAggregator::Result& res_a = a.results[spec.agg_result_idx];
      NdbAggregator::Result& res_b = b.results[spec.agg_result_idx];
      // NULL handling
      if (res_a.is_null() && res_b.is_null()) continue;
      if (res_a.is_null()) return spec.ascending ? -1 : 1;
      if (res_b.is_null()) return spec.ascending ? 1 : -1;
      int cmp = 0;
      switch (res_a.type())
      {
      case NdbDictionary::Column::Bigint:
        cmp = (res_a.data_int64() < res_b.data_int64()) ? -1 :
              (res_a.data_int64() > res_b.data_int64()) ? 1 : 0;
        break;
      case NdbDictionary::Column::Bigunsigned:
        cmp = (res_a.data_uint64() < res_b.data_uint64()) ? -1 :
              (res_a.data_uint64() > res_b.data_uint64()) ? 1 : 0;
        break;
      case NdbDictionary::Column::Double:
        if (spec.decimal_scale > 0)
        {
          // O-9: as the DECIMAL it is in MySQL, so that sums that print
          // alike tie and the next ORDER BY key decides.
          cmp = ronsql_compare_at_scale(res_a.data_double(),
                                        res_b.data_double(),
                                        spec.decimal_scale);
          break;
        }
        cmp = (res_a.data_double() < res_b.data_double()) ? -1 :
              (res_a.data_double() > res_b.data_double()) ? 1 : 0;
        break;
      case NdbDictionary::Column::Char:
      case NdbDictionary::Column::Varchar:
      case NdbDictionary::Column::Longvarchar:
        {
          require_sch(spec.charset != nullptr,
                      "Could not find charset for string aggregation result");
          Uint32 len_a = 0;
          Uint32 len_b = 0;
          const char* data_a = res_a.data_str(&len_a);
          const char* data_b = res_b.data_str(&len_b);
          Uint32 prefix = res_a.type() == NdbDictionary::Column::Char ? 0 :
                          res_a.type() == NdbDictionary::Column::Varchar ? 1 :
                          2;
          if (prefix != 0)
          {
            // The length-prefixed keys go to two scratch buffers in the
            // arena that every comparison of the sort reuses.
            const Uint32 need = prefix + (len_a > len_b ? len_a : len_b);
            if (need > m_cmp_buf_len)
            {
              Uint32 cap = (m_cmp_buf_len == 0) ? 64 : m_cmp_buf_len;
              while (cap < need) cap *= 2;
              m_cmp_buf_a = m_amalloc->alloc_exc<char>(cap);
              m_cmp_buf_b = m_amalloc->alloc_exc<char>(cap);
              m_cmp_buf_len = cap;
            }
            char* raw_a = m_cmp_buf_a;
            char* raw_b = m_cmp_buf_b;
            if (prefix == 1)
            {
              raw_a[0] = static_cast<char>(len_a);
              raw_b[0] = static_cast<char>(len_b);
            }
            else
            {
              raw_a[0] = static_cast<char>(len_a & 0xff);
              raw_a[1] = static_cast<char>((len_a >> 8) & 0xff);
              raw_b[0] = static_cast<char>(len_b & 0xff);
              raw_b[1] = static_cast<char>((len_b >> 8) & 0xff);
            }
            memcpy(&raw_a[prefix], data_a, len_a);
            memcpy(&raw_b[prefix], data_b, len_b);
            data_a = raw_a;
            data_b = raw_b;
          }
          // A CASE's arms can be CHAR columns of different sizes
          NdbSqlUtil::Cmp* const cmp_fn = NdbSqlUtil::getStringResultCmp(
              static_cast<Uint32>(res_a.type()));
          cmp = (*cmp_fn)(spec.charset,
                          data_a, prefix + len_a,
                          data_b, prefix + len_b);
        }
        break;
      default:
        // Fallback: compare as int64
        cmp = (res_a.data_int64() < res_b.data_int64()) ? -1 :
              (res_a.data_int64() > res_b.data_int64()) ? 1 : 0;
        break;
      }
      if (cmp != 0)
        return spec.ascending ? cmp : -cmp;
    }
    else if (spec.kind == OrderbySpec::Kind::AVG)
    {
      const double key_a = a.avg_keys[spec.avg_key_idx];
      const double key_b = b.avg_keys[spec.avg_key_idx];
      // NaN is a NULL average
      const bool null_a = std::isnan(key_a);
      const bool null_b = std::isnan(key_b);
      if (null_a && null_b) continue;
      if (null_a) return spec.ascending ? -1 : 1;
      if (null_b) return spec.ascending ? 1 : -1;
      const int cmp = (key_a < key_b) ? -1 : (key_a > key_b) ? 1 : 0;
      if (cmp != 0)
        return spec.ascending ? cmp : -cmp;
    }
    else
    {
      NdbAggregator::Column& col_a = a.cols[spec.groupby_idx];
      NdbAggregator::Column& col_b = b.cols[spec.groupby_idx];
      // NULL handling: NULLs sort first (smallest) in ASC, matching MySQL
      if (col_a.is_null() && col_b.is_null()) continue;
      if (col_a.is_null()) return spec.ascending ? -1 : 1;
      if (col_b.is_null()) return spec.ascending ? 1 : -1;
      if (spec.decimal_scale > 0 &&
          col_a.type() == NdbDictionary::Column::Double)
      {
        // O-9: a DECIMAL-derived CTE output, as in the AGGREGATE arm.
        const int cmp = ronsql_compare_at_scale(col_a.data_double(),
                                                col_b.data_double(),
                                                spec.decimal_scale);
        if (cmp != 0)
          return spec.ascending ? cmp : -cmp;
        continue;
      }
      switch (col_a.type())
      {
      case NdbDictionary::Column::Char:
      case NdbDictionary::Column::Varchar:
      case NdbDictionary::Column::Longvarchar:
        // As in the AGGREGATE arm: the string comparators dereference
        // the charset (O-1).
        require_sch(spec.charset != nullptr,
                    "Could not find charset for string GROUP BY column");
        break;
      default:
        break;
      }
      const NdbSqlUtil::Type& sqlType =
          NdbSqlUtil::getType(static_cast<Uint32>(col_a.type()));
      int cmp = (*sqlType.m_cmp)(spec.charset,
                                 col_a.data(), col_a.byte_size(),
                                 col_b.data(), col_b.byte_size());
      if (cmp != 0)
      {
        return spec.ascending ? cmp : -cmp;
      }
    }
  }
  return 0;
}

// The AVG sort keys of a row, computed once instead of in every
// comparison.
void
ResultPrinter::set_avg_keys(StoredRow& row)
{
  row.avg_keys = m_amalloc->alloc_exc<double>(m_num_avg_keys);
  for (Uint32 i = 0; i < m_orderby_specs.size(); i++)
  {
    const OrderbySpec& spec = m_orderby_specs[i];
    if (spec.kind != OrderbySpec::Kind::AVG) continue;
    row.avg_keys[spec.avg_key_idx] =
        avg_sort_key(row.results[spec.avg_sum_idx],
                     row.results[spec.avg_count_idx],
                     spec.avg_scale);
  }
}

// The value print_avg_result() prints, or NaN where it prints NULL.  MySQL
// sorts an AVG by its value at the result scale, so averages that print
// the same compare equal and the next ORDER BY key breaks the tie.
double
ResultPrinter::avg_sort_key(NdbAggregator::Result sum,
                            NdbAggregator::Result count,
                            int scale)
{
  if (sum.is_null())
    return std::numeric_limits<double>::quiet_NaN();
  const double result =
      convert_result_to_double(sum) / convert_result_to_double(count);
  if (scale < 0)
    return result;
  ronsql_require_bug(scale <= 30, "The result printer got an AVG scale above 30.");
  char buffer[FLOATING_POINT_BUFFER];
  const size_t len = format_fixed_scale(result, scale, buffer);
  if (len == 0)
    return std::numeric_limits<double>::quiet_NaN();
  const char* end = buffer + len;
  int conv_error;
  return my_strtod(buffer, &end, &conv_error);
}

void
ResultPrinter::print_result_ordered(NdbAggregator* aggregator,
                                    std::basic_ostream<char>* out_stream)
{
  DEB_TRACE();
  assert(out_stream != NULL);
  std::ostream& out = *out_stream;
  // Buffer all rows
  DynamicArray<StoredRow> rows(m_amalloc);
  for (NdbAggregator::ResultRecord record = aggregator->FetchResultRecord();
       !record.end();
       record = aggregator->FetchResultRecord())
  {
    DEB_TRACE();
    StoredRow row = store_record(record);
    if (!group_passes()) continue;
    if (m_num_avg_keys > 0)
      set_avg_keys(row);
    rows.push(row);
  }
  Uint32 num_rows = rows.size();
  if (num_rows == 0)
  {
    if (m_json_output)
    {
      out << "[]\n";
    }
    return;
  }
  // Copy to a plain array for std::sort (DynamicArray lacks random-access
  // iterators)
  StoredRow* sort_array =
      m_amalloc->alloc_exc<StoredRow>(num_rows);
  for (Uint32 i = 0; i < num_rows; i++)
  {
    sort_array[i] = rows[i];
  }
  // Sort using ORDER BY comparator; under a LIMIT only the rows printed
  // (O-16: before, every group was sorted).
  const Uint32 print_count = ronsql_sort_top_n(
      sort_array, num_rows, m_query->limit,
      [this](StoredRow& a, StoredRow& b) -> bool {
        return compare_rows(a, b) < 0;
      });
  // Print results
  if (m_json_output)
  {
    DEB_TRACE();
    out << '[';
    for (Uint32 i = 0; i < print_count; i++)
    {
      if (i > 0) out << ',';
      print_stored_record(sort_array[i], out);
    }
    out << "]\n";
  }
  else if (m_tsv_output)
  {
    DEB_TRACE();
    // The mysql client prints nothing at all for an empty result set —
    // including LIMIT 0 — so suppress the header when no row will be
    // printed (matches the num_rows == 0 early return above).
    if (print_count > 0)
    {
      print_tsv_header(out);
    }
    for (Uint32 i = 0; i < print_count; i++)
    {
      print_stored_record(sort_array[i], out);
    }
  }
  else
  {
    DEB_TRACE();
    ronsql_bug("The result printer has neither JSON nor TSV output.");
  }
}

bool
ResultPrinter::group_passes()
{
  // Cross-table filter semantics: suppress groups where no rows passed
  // the filter (sentinel COUNT == 0). The kernel creates a group before
  // the filter stops the row, so such a group exists only because of
  // rows the WHERE rejected. Without GROUP BY the one row is the
  // aggregate over the whole WHERE result, which SQL returns also when
  // no row passed (COUNT 0, the others NULL), so it is never
  // suppressed (F-6).
  if (m_query->sentinel_agg_slot >= 0 && m_num_groupby_cols > 0 &&
      m_regs_a[m_query->sentinel_agg_slot].data_int64() == 0)
  {
    return false;
  }
  return m_query->having_expression == NULL ||
         evaluate_having(m_query->having_expression) == HAVING_TRUE;
}

// The TSV header line: the output names, unless the format has none.
void
ResultPrinter::print_tsv_header(std::ostream& out)
{
  if (!m_tsv_headers) return;
  bool first_column = true;
  for (Uint32 i = 0; i < m_outputs.size(); i++)
  {
    Outputs* o = m_outputs[i];
    if (first_column) first_column = false; else out << '\t';
    out << o->output_name;
  }
  out << '\n';
}

/*
 * O-16: a HAVING, or the sentinel of a cross-table filter, without an
 * ORDER BY.  Each group is decided alone, so it is loaded into the
 * registers, tested and printed at once; before, every group was stored
 * and sorted with no key first, and printed after the last.  A LIMIT
 * stops the fetch as on the streaming path.  The framing is the one of
 * print_result_ordered: JSON "[]" for no row, and in TSV the header only
 * before a first row.
 */
void
ResultPrinter::print_result_filtered(NdbAggregator* aggregator,
                                     std::basic_ostream<char>* out_stream)
{
  DEB_TRACE();
  assert(out_stream != NULL);
  std::ostream& out = *out_stream;
  if (!m_json_output && !m_tsv_output)
  {
    ronsql_bug("The result printer has neither JSON nor TSV output.");
  }
  if (m_json_output)
  {
    out << '[';
  }
  Int64 row_count = 0;
  for (NdbAggregator::ResultRecord record = aggregator->FetchResultRecord();
       !record.end();
       record = aggregator->FetchResultRecord())
  {
    DEB_TRACE();
    if (m_query->limit >= 0 && row_count >= m_query->limit)
    {
      break;
    }
    run_program(0, m_print_start_idx, &record, NULL);
    if (!group_passes()) continue;
    if (m_json_output)
    {
      if (row_count > 0) out << ',';
    }
    else if (row_count == 0)
    {
      print_tsv_header(out);
    }
    print_registers(out);
    row_count++;
  }
  if (m_json_output)
  {
    out << "]\n";
  }
}

void
ResultPrinter::setup_output_format()
{
  switch (m_output_format)
  {
  case RonSQLExecParams::OutputFormat::TEXT:
    m_json_output = false;
    m_utf8_output = true;
    m_tsv_output = true;
    m_tsv_headers = true;
    m_quote = "";
    m_null_representation = LexString{"NULL", 4};
    break;
  case RonSQLExecParams::OutputFormat::TEXT_NOHEADER:
    m_json_output = false;
    m_utf8_output = true;
    m_tsv_output = true;
    m_tsv_headers = false;
    m_quote = "";
    m_null_representation = LexString{"NULL", 4};
    break;
  case RonSQLExecParams::OutputFormat::JSON:
    m_json_output = true;
    m_utf8_output = true;
    m_tsv_output = false;
    m_tsv_headers = false;
    m_quote = "\"";
    m_null_representation = LexString{"null", 4};
    break;
  case RonSQLExecParams::OutputFormat::JSON_ASCII:
    m_json_output = true;
    m_utf8_output = false;
    m_tsv_output = false;
    m_tsv_headers = false;
    m_quote = "\"";
    m_null_representation = LexString{"null", 4};
    break;
  default:
    ronsql_bug("The result printer got an unknown output format.");
  }
}

// Phase 0b: decode a packed temporal value into its MySQL text form.
// The packed Uint64 is the kernel's Bigunsigned MIN/MAX representation
// — the column's native bytes (little-endian for DATE/YEAR, big-endian
// for DATETIME2/TIME2/TIMESTAMP2) loaded into a register — which is
// also what a raw column re-packs to (see print_passthrough_value).
// `quote` wraps the text: the printer's m_quote ("" for TSV, "\"" for
// JSON), for pass-through columns and aggregate results alike.
void
ronsql_printer_internal::print_temporal_packed(
    std::ostream& out, Uint64 w, ResultPrinter::TemporalDisplay temporal,
    int fsp, const char* quote)
{
  using TemporalDisplay = ResultPrinter::TemporalDisplay;
  char buf[64];
  buf[0] = '\0';
  switch (temporal)
  {
  case TemporalDisplay::DATE:
  {
    // w = (year<<9)|(month<<5)|day; w==0 → 0000-00-00 (MySQL zero date).
    Uint32 day = (Uint32)(w & 31);
    Uint32 month = (Uint32)((w >> 5) & 15);
    Uint32 year = (Uint32)(w >> 9);
    snprintf(buf, sizeof(buf), "%04u-%02u-%02u", year, month, day);
    break;
  }
  case TemporalDisplay::YEAR:
  {
    // 1-byte YEAR: 0 → 0000, else stored value + 1900.
    if (w == 0)
      snprintf(buf, sizeof(buf), "0000");
    else
      snprintf(buf, sizeof(buf), "%04u", (Uint32)(w + 1900));
    break;
  }
  case TemporalDisplay::DATETIME2:
  {
    // NDB stores DATETIME2 in MySQL's big-endian packed binary (proven by
    // the encode path's my_datetime_packed_to_binary).  Reconstruct the
    // n = 5+flen on-disk bytes from the big-endian value the kernel loaded,
    // then decode with MySQL's own codec so the string matches exactly.
    Uint32 flen = (1u + (Uint32)fsp) / 2u;
    Uint32 n = 5u + flen;
    Uint8 bytes[8];
    for (Uint32 i = 0; i < n; i++)
      bytes[i] = (Uint8)((w >> (8u * (n - 1u - i))) & 0xFFu);
    Int64 packed = my_datetime_packed_from_binary(bytes, fsp);
    MYSQL_TIME mt;
    TIME_from_longlong_datetime_packed(&mt, packed);
    my_TIME_to_str(mt, buf, fsp);
    break;
  }
  case TemporalDisplay::TIME2:
  {
    Uint32 flen = (1u + (Uint32)fsp) / 2u;
    Uint32 n = 3u + flen;
    Uint8 bytes[8];
    for (Uint32 i = 0; i < n; i++)
      bytes[i] = (Uint8)((w >> (8u * (n - 1u - i))) & 0xFFu);
    Int64 packed = my_time_packed_from_binary(bytes, fsp);
    MYSQL_TIME mt;
    TIME_from_longlong_time_packed(&mt, packed);
    my_TIME_to_str(mt, buf, fsp);
    break;
  }
  case TemporalDisplay::TIMESTAMP2:
  {
    // TIMESTAMP2 stores the UTC epoch (4+flen bytes, big-endian); decode the
    // reconstructed bytes to a my_timeval and break the epoch down in UTC
    // (RonSQL/RDRS treat the server as UTC; the mysql baseline runs with
    // time_zone='+00:00').  Uses the lock-free ronsql_utc_sec_to_TIME, not
    // gmtime_r.
    Uint32 flen = (1u + (Uint32)fsp) / 2u;
    Uint32 n = 4u + flen;
    Uint8 bytes[8];
    for (Uint32 i = 0; i < n; i++)
      bytes[i] = (Uint8)((w >> (8u * (n - 1u - i))) & 0xFFu);
    my_timeval tv{};
    my_timestamp_from_binary(&tv, bytes, fsp);
    MYSQL_TIME mt;
    ronsql_timestamp_tv_to_TIME(tv, &mt);
    my_TIME_to_str(mt, buf, fsp);
    break;
  }
  case TemporalDisplay::NONE:
    break; // unreachable (callers guard)
  }
  out << quote << buf << quote;
}

// O-13: a GROUP BY key of a type that print_passthrough_value prints and
// the GROUP BY printers did not: YEAR, TIME, BINARY and VARBINARY.  A
// group key holds the column's stored bytes, as an NdbRecAttr does, so it
// is printed as print_passthrough_value prints the column: YEAR and TIME
// as text, quoted under JSON; BINARY at its full padded length and
// VARBINARY as a base64 string under JSON and as the raw bytes with the
// mysql client's batch-mode escaping under TEXT.
void
ResultPrinter::print_groupby_as_passthrough(std::ostream& out,
                                            NdbAggregator::Column& column,
                                            int fsp)
{
  const unsigned char* data =
      pointer_cast<const unsigned char*>(column.data());
  const size_t size = (size_t)column.byte_size();
  const unsigned char* bytes = data;
  size_t len = size;
  switch (column.type())
  {
  case NdbDictionary::Column::Year:
    require_sch(size >= 1, "Empty YEAR GROUP BY column");
    print_temporal_packed(out, (Uint64)data[0], TemporalDisplay::YEAR, 0,
                          m_quote);
    return;
  case NdbDictionary::Column::Time2:
  {
    // 3 bytes and the fraction, big-endian, re-packed into the Uint64
    // form the shared decoder takes, as print_passthrough_value does.
    require_sch(fsp >= 0 && fsp <= 6,
                "Bad precision of a TIME GROUP BY column");
    const Uint32 n = 3u + (1u + (Uint32)fsp) / 2u;
    require_sch(n <= size, "Short TIME GROUP BY column");
    Uint64 w = 0;
    for (Uint32 i = 0; i < n; i++)
      w = (w << 8) | data[i];
    print_temporal_packed(out, w, TemporalDisplay::TIME2, fsp, m_quote);
    return;
  }
  case NdbDictionary::Column::Binary:
    break;
  case NdbDictionary::Column::Varbinary:
    require_sch(size >= 1 && 1 + (size_t)data[0] <= size,
                "Bad length of a VARBINARY GROUP BY column");
    bytes = &data[1];
    len = (size_t)data[0];
    break;
  case NdbDictionary::Column::Longvarbinary:
    require_sch(size >= 2, "Bad length of a VARBINARY GROUP BY column");
    len = (size_t)data[0] | ((size_t)data[1] << 8);
    require_sch(2 + len <= size, "Bad length of a VARBINARY GROUP BY column");
    bytes = &data[2];
    break;
  default:
    ronsql_bug("A GROUP BY key printed as a pass-through value has a type "
               "that is not YEAR, TIME, BINARY or VARBINARY.");
  }
  if (m_json_output) {
    out << '"';
    print_base64(out, bytes, len);
    out << '"';
  } else if (m_tsv_output) {
    print_binary_tsv(out, bytes, len);
  } else {
    ronsql_bug("The result printer has neither JSON nor TSV output.");
  }
}

// Phase E.3 + I.12 helpers: format a single NdbRecAttr value (or its
// PassthroughValue copy, for a sorted result) for the
// projection-only pass-through path.  Mirrors the type cases the
// aggregator path handles in print_record (see Column::data_*) but
// reads directly from NdbRecAttr instead of NdbAggregator::Column.
// Phase E.3's original supported set was integer, float, double — the
// types build_cte_virtual_tables emits from SUM/COUNT/MIN/MAX.  Phase
// I.12 extends the set to CHAR / VARCHAR / Longvarchar so projection-
// only queries that select a real-table string column alongside CTE
// columns (testCteNdbApiOuterJoin.cpp Test 1 etc.) work.
template <class Attr>
void
ResultPrinter::print_passthrough_value(std::ostream& out,
                                       const Attr* attr,
                                       const ColumnMetadata* meta)
{
  if (attr == NULL || attr->isNULL() == 1) {
    out << m_null_representation;
    return;
  }
  NdbDictionary::Column::Type t = attr->getType();
  // Phase 0b: a CTE MIN/MAX over a temporal column arrives as a
  // Bigunsigned virt column carrying the packed value (D17 + temporal
  // extension); the virt column itself has no temporal type, so the
  // decode is driven by the resolved source column's metadata.
  if (meta != NULL && meta->has_metadata &&
      meta->temporal != TemporalDisplay::NONE &&
      t == NdbDictionary::Column::Bigunsigned)
  {
    print_temporal_packed(out, attr->u_64_value(), meta->temporal,
                          meta->temporal_fsp, m_quote);
    return;
  }
  switch (t) {
  case NdbDictionary::Column::Tinyint:
    out << (int)attr->int8_value(); break;
  case NdbDictionary::Column::Tinyunsigned:
    out << (unsigned)attr->u_8_value(); break;
  case NdbDictionary::Column::Smallint:
    out << (int)attr->short_value(); break;
  case NdbDictionary::Column::Smallunsigned:
    out << (unsigned)attr->u_short_value(); break;
  case NdbDictionary::Column::Mediumint:
    out << (int)attr->medium_value(); break;
  case NdbDictionary::Column::Mediumunsigned:
    out << (unsigned)attr->u_medium_value(); break;
  case NdbDictionary::Column::Int:
    out << (int)attr->int32_value(); break;
  case NdbDictionary::Column::Unsigned:
    out << (unsigned)attr->u_32_value(); break;
  case NdbDictionary::Column::Bigint:
    out << (long long)attr->int64_value(); break;
  case NdbDictionary::Column::Bigunsigned:
    out << (unsigned long long)attr->u_64_value(); break;
  case NdbDictionary::Column::Float:
    print_float_or_double(out, attr->float_value(), true); break;
  case NdbDictionary::Column::Double:
    {
      // D15: a DECIMAL-derived value carried as DOUBLE prints with its source
      // scale (set on the virt column via setScale) to match MySQL, whatever
      // the DECIMAL's precision (O-7).  True DOUBLE columns have scale 0.
      // The value is a DECIMAL in MySQL (SUM/MIN/MAX of a DECIMAL, AVG of
      // an exact type): rounded HALF_UP, never "-0.00" (O-5/O-6).
      const NdbDictionary::Column* col = attr->getColumn();
      int sc = (col != nullptr) ? col->getScale() : 0;
      int pr = (col != nullptr) ? col->getPrecision() : 0;
      if (sc > 0 && pr > 0) {
        print_fixed_scale(out, attr->double_value(), sc);
      } else {
        print_float_or_double(out, attr->double_value(),
                              meta != NULL && meta->float_display);
      }
    }
    break;
  case NdbDictionary::Column::Char:
    {
      const NdbDictionary::Column* col = attr->getColumn();
      require_sch(col != nullptr, "NULL column on CHAR NdbRecAttr");
      CHARSET_INFO* charset = col->getCharset();
      require_sch(charset != nullptr, "Could not find charset for CHAR column");
      LexString content = LexString{ attr->aRef(), (size_t)col->getSizeInBytes() };
      if (m_json_output) {
        out << '"';
        print_string(out, content, charset, true, m_utf8_output, true);
        out << '"';
      } else if (m_tsv_output) {
        print_string(out, content, charset, false, true, true);
      } else {
        ronsql_bug("The result printer has neither JSON nor TSV output.");
      }
      break;
    }
  case NdbDictionary::Column::Varchar:
  case NdbDictionary::Column::Longvarchar:
    {
      const NdbDictionary::Column* col = attr->getColumn();
      require_sch(col != nullptr, "NULL column on VARCHAR NdbRecAttr");
      CHARSET_INFO* charset = col->getCharset();
      require_sch(charset != nullptr, "Could not find charset for VARCHAR column");
      const char* data = attr->aRef();
      LexString content;
      if (t == NdbDictionary::Column::Varchar) {
        content = LexString{ &data[1],
                             (size_t)(unsigned char)data[0] };
      } else {
        content = LexString{ &data[2],
                             (size_t)(unsigned char)data[0] |
                             (((size_t)(unsigned char)data[1]) << 8) };
      }
      out << m_quote;
      print_string(out,
                   content,
                   charset,
                   m_json_output,
                   m_utf8_output || m_tsv_output,
                   false);
      out << m_quote;
      break;
    }
  case NdbDictionary::Column::Date:
    // 3-byte little-endian (year<<9 | month<<5 | day) — identical to
    // the kernel's packed MIN/MAX representation, so the shared decoder
    // applies directly.
    print_temporal_packed(out, (Uint64)attr->u_medium_value(),
                          TemporalDisplay::DATE, 0, m_quote);
    break;
  case NdbDictionary::Column::Year:
    print_temporal_packed(out, (Uint64)attr->u_8_value(),
                          TemporalDisplay::YEAR, 0, m_quote);
    break;
  case NdbDictionary::Column::Datetime2:
  case NdbDictionary::Column::Time2:
  case NdbDictionary::Column::Timestamp2:
    {
      const NdbDictionary::Column* col = attr->getColumn();
      require_sch(col != nullptr, "NULL column on temporal NdbRecAttr");
      int fsp = col->getPrecision();
      Uint32 base = (t == NdbDictionary::Column::Datetime2) ? 5u :
                    (t == NdbDictionary::Column::Time2) ? 3u : 4u;
      Uint32 n = base + (1u + (Uint32)fsp) / 2u;
      // Re-pack the big-endian on-disk bytes into the Uint64 form the
      // shared decoder expects.
      const unsigned char* bytes = (const unsigned char*)attr->aRef();
      Uint64 w = 0;
      for (Uint32 i = 0; i < n; i++)
        w = (w << 8) | bytes[i];
      TemporalDisplay td =
          (t == NdbDictionary::Column::Datetime2)
              ? TemporalDisplay::DATETIME2
              : (t == NdbDictionary::Column::Time2)
                    ? TemporalDisplay::TIME2
                    : TemporalDisplay::TIMESTAMP2;
      print_temporal_packed(out, w, td, fsp, m_quote);
      break;
    }
  case NdbDictionary::Column::Decimal:
  case NdbDictionary::Column::Decimalunsigned:
    {
      const NdbDictionary::Column* col = attr->getColumn();
      require_sch(col != nullptr, "NULL column on DECIMAL NdbRecAttr");
      constexpr int DECIMAL_MAX_STR_LEN_IN_BYTES = 68;
      char decStr[DECIMAL_MAX_STR_LEN_IN_BYTES];
      // O-14: on an error decimal_bin2str() leaves decStr unwritten.
      const int dec_ret = decimal_bin2str((const void*)attr->aRef(),
                                          attr->get_size_in_bytes(),
                                          col->getPrecision(),
                                          col->getScale(),
                                          decStr,
                                          DECIMAL_MAX_STR_LEN_IN_BYTES);
      ronsql_require_bug(dec_ret == E_DEC_OK,
                         "A DECIMAL value could not be converted to text.");
      out << decStr;
      break;
    }
  case NdbDictionary::Column::Binary:
  case NdbDictionary::Column::Varbinary:
  case NdbDictionary::Column::Longvarbinary:
    {
      // RONDB-1124 M1.4 (RONDB-1121 F7): binary pass-through values, the
      // Hopsworks binary / serialized-array features projected through
      // the snowflake templates.  JSON carries them as a base64 string
      // (RFC 4648 with padding, no line breaks — the convention of the
      // RDRS pk-read responses, so a client decodes both endpoints
      // alike); TEXT prints the raw bytes with the mysql client's
      // batch-mode escaping (\0 \t \n \\), byte for byte what the client
      // prints for the same column.  BINARY(n) is printed at its full
      // padded length, as MySQL returns it.  BLOB/TEXT stay unsupported
      // (they need the blob API).
      const unsigned char* bytes;
      size_t len;
      binary_value_bytes(attr, &bytes, &len);
      if (m_json_output) {
        out << '"';
        print_base64(out, bytes, len);
        out << '"';
      } else if (m_tsv_output) {
        print_binary_tsv(out, bytes, len);
      } else {
        ronsql_bug("The result printer has neither JSON nor TSV output.");
      }
      break;
    }
  default:
    // Old temporal formats (pre-5.6 Datetime/Time/Timestamp),
    // Olddecimal, BIT and BLOB/TEXT are not supported in pass-through
    // results.
    *m_err << "Unsupported column type (" << (int)t
           << ") in pass-through result." << endl;
    throw RonSQLPermanentError(
        "Unsupported column type in pass-through result.");
  }
}

template <class Attr>
void
ResultPrinter::print_passthrough_header(const Attr* const* /*attrs*/,
                                         Uint32 num_cols,
                                         std::basic_ostream<char>* out_stream)
{
  assert(out_stream != NULL);
  std::ostream& out = *out_stream;
  if (m_json_output) {
    out << '[';
    return;
  }
  if (m_tsv_output && m_tsv_headers) {
    Outputs* o = m_query->outputs;
    for (Uint32 i = 0; i < num_cols; i++) {
      if (i > 0) out << '\t';
      assert(o != NULL);
      out << o->output_name;
      o = o->next;
    }
    out << '\n';
  }
}

const ResultPrinter::ColumnMetadata*
ResultPrinter::passthrough_column_metadata(const Outputs* o) const
{
  if (m_column_metadata == NULL || o == NULL ||
      o->type != Outputs::Type::COLUMN)
    return NULL;
  return &m_column_metadata[o->column.col_idx];
}

void
ResultPrinter::set_avro_decoder(RonSQLAvroDecoder* decoder,
                                const void* const* handles)
{
  assert(decoder != NULL);
  assert(handles != NULL);
  m_avro_decoder = decoder;
  m_avro_handles = handles;
}

template <class Attr>
void
ResultPrinter::print_passthrough_output(std::ostream& out,
                                        const Attr* attr,
                                        const Outputs* o,
                                        Uint32 pos)
{
  if (m_avro_handles != NULL && m_avro_handles[pos] != NULL) {
    print_avro_value(out, attr, m_avro_handles[pos]);
    return;
  }
  print_passthrough_value(out, attr, passthrough_column_metadata(o));
}

// RONDB-1135: an AVRO(column) value is the JSON text the decoder gives,
// printed as a JSON value (not a string) under JSON, with \u escapes for
// non-ASCII characters under JSON_ASCII, and with the batch-mode escaping
// of a binary value under TEXT.  NULL prints as NULL.
template <class Attr>
void
ResultPrinter::print_avro_value(std::ostream& out,
                                const Attr* attr,
                                const void* avro_handle)
{
  if (attr == NULL || attr->isNULL() == 1) {
    out << m_null_representation;
    return;
  }
  const unsigned char* bytes;
  size_t len;
  binary_value_bytes(attr, &bytes, &len);
  std::string json;
  m_avro_decoder->decode(avro_handle, bytes, len, json);
  if (m_json_output) {
    if (m_utf8_output) {
      out.write(json.data(), json.size());
    } else {
      print_json_ascii(out, json);
    }
  } else if (m_tsv_output) {
    print_binary_tsv(out,
                     pointer_cast<const unsigned char*>(json.data()),
                     json.size());
  } else {
    ronsql_bug("The result printer has neither JSON nor TSV output.");
  }
}

template <class Attr>
void
ResultPrinter::print_passthrough_row(const Attr* const* attrs,
                                      Uint32 num_cols,
                                      bool is_first_row,
                                      std::basic_ostream<char>* out_stream)
{
  assert(out_stream != NULL);
  std::ostream& out = *out_stream;
  if (m_json_output) {
    if (!is_first_row) out << ',';
    out << '{';
    Outputs* o = m_query->outputs;
    for (Uint32 i = 0; i < num_cols; i++) {
      assert(o != NULL);
      if (i > 0) out << ',';
      // The output name is an alias or column name the query spelled,
      // which may hold a quote, a backslash or a control character:
      // escape it like the aggregate printer does (PRINT_STR_JSON).
      out << '"';
      print_string(out,
                   o->output_name,
                   &my_charset_utf8mb4_bin,
                   true,          // json_escape
                   m_utf8_output, // \u-escape non-ASCII under JSON_ASCII
                   false);        // trim_space_suffix
      out << "\":";
      // Numbers print without quotes; strings and temporals quote
      // themselves (m_quote) inside print_passthrough_value.
      print_passthrough_output(out, attrs[i], o, i);
      o = o->next;
    }
    out << '}';
    return;
  }
  if (m_tsv_output) {
    Outputs* o = m_query->outputs;
    for (Uint32 i = 0; i < num_cols; i++) {
      assert(o != NULL);
      if (i > 0) out << '\t';
      print_passthrough_output(out, attrs[i], o, i);
      o = o->next;
    }
    out << '\n';
  }
}

template void ResultPrinter::print_passthrough_header<NdbRecAttr>(
    const NdbRecAttr* const*, Uint32, std::basic_ostream<char>*);
template void ResultPrinter::print_passthrough_header<PassthroughValue>(
    const PassthroughValue* const*, Uint32, std::basic_ostream<char>*);
template void ResultPrinter::print_passthrough_row<NdbRecAttr>(
    const NdbRecAttr* const*, Uint32, bool, std::basic_ostream<char>*);
template void ResultPrinter::print_passthrough_row<PassthroughValue>(
    const PassthroughValue* const*, Uint32, bool, std::basic_ostream<char>*);

const PassthroughValue*
PassthroughValue::copy(const NdbRecAttr* src, ArenaMalloc* amalloc)
{
  // The error the RonSQLPreparer's require_run gave a failed clone.
  if (unlikely(src->isNULL() == -1))
    throw std::runtime_error(
        "Pass-through sort: failed to clone a row value.");
  const Uint32 size = src->get_size_in_bytes();
  const size_t total = arena_bytes(size);
  const size_t data_len = total - sizeof(PassthroughValue);
  void* mem = amalloc->alloc_bytes(total, alignof(Uint64));
  if (unlikely(mem == NULL)) throw std::bad_alloc();
  PassthroughValue* v = new (mem) PassthroughValue;
  v->m_column = src->getColumn();
  v->m_size = size;
  char* data = v->aRef();
  if (size > 0) memcpy(data, src->aRef(), size);
  memset(data + size, 0, data_len - size);
  return v;
}

size_t
PassthroughValue::arena_bytes(Uint32 size)
{
  return sizeof(PassthroughValue) +
         ((size_t(size < 8 ? 8 : size) + 7) & ~size_t(7));
}

Int32
PassthroughValue::medium_value() const
{
  return sint3korr(pointer_cast<const uchar*>(aRef()));
}

Uint32
PassthroughValue::u_medium_value() const
{
  return uint3korr(pointer_cast<const uchar*>(aRef()));
}

void
ResultPrinter::print_passthrough_finish(std::basic_ostream<char>* out_stream)
{
  assert(out_stream != NULL);
  std::ostream& out = *out_stream;
  if (m_json_output) {
    out << "]\n";
  }
}

void
ResultPrinter::print_count_star_result(Uint64 count,
                                       std::basic_ostream<char>* out_stream)
{
  DEB_TRACE();
  assert(out_stream != NULL);
  std::ostream& out = *out_stream;
  /* One result row; LIMIT 0 suppresses it (TSV prints nothing at all,
   * JSON prints the empty array) — the same behavior print_result's
   * cutoff gives.  ORDER BY over one row is a no-op.  COUNT results
   * print as Int64, matching PRINT_AGGREGATE's formatting. */
  const bool suppressed = (m_query->limit == 0);
  if (m_json_output)
  {
    out << '[';
    if (!suppressed)
    {
      out << '{';
      for (Uint32 i = 0; i < m_outputs.size(); i++)
      {
        if (i > 0) out << ',';
        out << '"';
        print_string(out, m_outputs[i]->output_name,
                     &my_charset_utf8mb4_bin, true, m_utf8_output, false);
        out << '"' << ':' << (Int64)count;
      }
      out << "}\n";
    }
    out << "]\n";
  }
  else if (m_tsv_output)
  {
    if (!suppressed)
    {
      if (m_tsv_headers)
      {
        bool first_column = true;
        for (Uint32 i = 0; i < m_outputs.size(); i++)
        {
          if (first_column) first_column = false; else out << '\t';
          out << m_outputs[i]->output_name;
        }
        out << '\n';
      }
      bool first_column = true;
      for (Uint32 i = 0; i < m_outputs.size(); i++)
      {
        if (first_column) first_column = false; else out << '\t';
        out << (Int64)count;
      }
      out << '\n';
    }
  }
  else
  {
    ronsql_bug("The result printer has neither JSON nor TSV output.");
  }
}

void
ResultPrinter::print_result(NdbAggregator* aggregator,
                            std::basic_ostream<char>* out_stream)
{
  DEB_TRACE();
  assert(out_stream != NULL);
  // O-16: only an ORDER BY needs every group before the first is printed.
  // A HAVING, or the sentinel of a cross-table filter (BranchReg filters),
  // decides on each group alone, which print_record cannot: it prints
  // while it reads the record.
  if (m_orderby_specs.size() > 0)
  {
    print_result_ordered(aggregator, out_stream);
    return;
  }
  if (m_query->having_expression != NULL || m_query->sentinel_agg_slot >= 0)
  {
    print_result_filtered(aggregator, out_stream);
    return;
  }
  std::ostream& out = *out_stream;
  if (m_json_output)
  {
    DEB_TRACE();
    out << '[';
    bool first_record = true;
    Int64 row_count = 0;
    for (NdbAggregator::ResultRecord record = aggregator->FetchResultRecord();
         !record.end();
         record = aggregator->FetchResultRecord())
    {
      DEB_TRACE();
      if (m_query->limit >= 0 && row_count >= m_query->limit)
      {
        break;
      }
      if (first_record)
      {
        first_record = false;
      }
      else
      {
        out << ',';
      }
      print_record(record, out);
      row_count++;
    }
    DEB_TRACE();
    out << "]\n";
  }
  else if (m_tsv_output)
  {
    DEB_TRACE();
    bool headers_printed = false;
    Int64 row_count = 0;
    for (NdbAggregator::ResultRecord record = aggregator->FetchResultRecord();
         !record.end();
         record = aggregator->FetchResultRecord())
    {
      DEB_TRACE();
      // Check the LIMIT cutoff before the header so LIMIT 0 prints
      // nothing at all, like the mysql client does for an empty result.
      if (m_query->limit >= 0 && row_count >= m_query->limit)
      {
        break;
      }
      if (!headers_printed)
      {
        DEB_TRACE();
        print_tsv_header(out);
        headers_printed = true;
      }
      print_record(record, out);
      row_count++;
    }
    DEB_TRACE();
  }
  else
  {
    DEB_TRACE();
    ronsql_bug("The result printer has neither JSON nor TSV output.");
  }
  // ================================================================================
}

inline void
ResultPrinter::print_record(NdbAggregator::ResultRecord& record, std::ostream& out)
{
  for (Uint32 cmd_index = 0; cmd_index < m_program.size(); cmd_index++)
  {
    Cmd& cmd = m_program[cmd_index];
    switch (cmd.type)
    {
    case Cmd::Type::STORE_GROUP_BY_COLUMN:
      {
        NdbAggregator::Column column = record.FetchGroupbyColumn();
        if (column.end())
        {
          bug("Got record with fewer GROUP BY columns than expected.");
        }
        m_regs_g[cmd.store_group_by_column.reg_g] = column;
      }
      break;
    case Cmd::Type::END_OF_GROUP_BY_COLUMNS:
      {
        NdbAggregator::Column column = record.FetchGroupbyColumn();
        if (!column.end())
        {
          bug("Got record with more GROUP BY columns than expected.");
        }
      }
      break;
    case Cmd::Type::STORE_AGGREGATE:
      {
        NdbAggregator::Result result = record.FetchAggregationResult();
        if (result.end())
        {
          bug("Got record with fewer aggregates than expected.");
        }
        m_regs_a[cmd.store_aggregate.reg_a] = result;
      }
      break;
    case Cmd::Type::END_OF_AGGREGATES:
      {
        NdbAggregator::Result result = record.FetchAggregationResult();
        if (!result.end())
        {
          bug("Got record with more aggregates than expected.");
        }
      }
      break;
    case Cmd::Type::PRINT_GROUP_BY_COLUMN:
      {
        NdbAggregator::Column column = m_regs_g[cmd.print_group_by_column.reg_g];
        if(column.is_null())
        {
          out << m_null_representation;
          break;
        }
        switch (column.type())
        {
        case NdbDictionary::Column::Type::Undefined:     ///< Undefined. Since this is a result, it means SQL NULL.
          feature_not_implemented("Print GROUP BY column of type Undefined (NULL)");
        case NdbDictionary::Column::Type::Tinyint:       ///< 8 bit. 1 byte signed integer
          // Int8 is ultimately defined in terms a char, so we need to type case
          // in order to print as an integer.
          out << Int32(column.data_int8());
          break;
        case NdbDictionary::Column::Type::Tinyunsigned:  ///< 8 bit. 1 byte unsigned integer
          // Uint8 is ultimately defined in terms a char, so we need to type case
          // in order to print as an integer.
          out << Uint32(column.data_uint8());
          break;
        case NdbDictionary::Column::Type::Smallint:      ///< 16 bit. 2 byte signed integer
          out << column.data_int16();
          break;
        case NdbDictionary::Column::Type::Smallunsigned: ///< 16 bit. 2 byte unsigned integer
          out << column.data_uint16();
          break;
        case NdbDictionary::Column::Type::Mediumint:     ///< 24 bit. 3 byte signed integer
          out << column.data_medium();
          break;
        case NdbDictionary::Column::Type::Mediumunsigned:///< 24 bit. 3 byte unsigned integer
          out << column.data_umedium();
          break;
        case NdbDictionary::Column::Type::Int:           ///< 32 bit. 4 byte signed integer
          out << column.data_int32();
          break;
        case NdbDictionary::Column::Type::Unsigned:      ///< 32 bit. 4 byte unsigned integer
          out << column.data_uint32();
          break;
        case NdbDictionary::Column::Type::Bigint:        ///< 64 bit. 8 byte signed integer
          out << column.data_int64();
          break;
        case NdbDictionary::Column::Type::Bigunsigned:   ///< 64 Bit. 8 byte unsigned integer
          // O-3: a packed CTE MIN / MAX temporal key.
          if (cmd.print_group_by_column.temporal != TemporalDisplay::NONE)
            print_temporal_packed(out, column.data_uint64(),
                                  cmd.print_group_by_column.temporal,
                                  cmd.print_group_by_column.temporal_fsp,
                                  m_quote);
          else
            out << column.data_uint64();
          break;
        case NdbDictionary::Column::Type::Float:         ///< 32-bit float. 4 bytes float
          print_float_or_double(out, column.data_float(), true);
          break;
        case NdbDictionary::Column::Type::Double:        ///< 64-bit float. 8 byte float
          // cte_avg_plan.md V4 (C8): see the buffered-path Double arm.
          {
            int sc = cmd.print_group_by_column.scale;
            int pr = cmd.print_group_by_column.precision;
            if (sc > 0 && pr > 0) {
              print_fixed_scale(out, column.data_double(), sc);
            } else {
              print_float_or_double(out, column.data_double(),
                                    cmd.print_group_by_column.float_display);
            }
          }
          break;
        case NdbDictionary::Column::Type::Olddecimal:    ///< MySQL < 5.0 signed decimal,  Precision, Scale
          feature_not_implemented("Print GROUP BY column of type Olddecimal");
        case NdbDictionary::Column::Type::Olddecimalunsigned:
          feature_not_implemented("Print GROUP BY column of type Olddecimalunsigned");
        case NdbDictionary::Column::Type::Decimal:       ///< MySQL >= 5.0 signed decimal,  Precision, Scale
          [[fallthrough]];
        case NdbDictionary::Column::Type::Decimalunsigned:
          {
            int precision = cmd.print_group_by_column.precision;
            int scale = cmd.print_group_by_column.scale;
            constexpr int DECIMAL_MAX_STR_LEN_IN_BYTES = 68;
            char decStr[DECIMAL_MAX_STR_LEN_IN_BYTES];
            // O-14: on an error decimal_bin2str() leaves decStr unwritten.
            const int dec_ret =
                decimal_bin2str((const void*)column.data(),
                                column.byte_size(),
                                precision,
                                scale,
                                decStr,
                                DECIMAL_MAX_STR_LEN_IN_BYTES);
            ronsql_require_bug(dec_ret == E_DEC_OK,
                               "A DECIMAL GROUP BY value could not be "
                               "converted to text.");
            out << decStr;
            break;
          }
        case NdbDictionary::Column::Type::Char:          ///< Len. A fixed array of 1-byte chars
          {
            CHARSET_INFO* charset = cmd.print_group_by_column.charset;
            require_sch(charset != nullptr, "Could not find charset for CHAR column");
            LexString content = LexString{ column.data(), column.byte_size() };
            // todo it's nowadays ok to put brace on same line. (This todo from review 2024-08-22 with MR)
            if (m_json_output) {
              out << '"';
              print_string(out, content, charset, true, m_utf8_output, true);
              out << '"';
            }
            else if (m_tsv_output) {
              print_string(out, content, charset, false, true, true);
            }
            else {
              ronsql_bug("The result printer has neither JSON nor TSV output.");
            }
            break;
          }
        case NdbDictionary::Column::Type::Varchar:       ///< Length bytes: 1, Max: 255
        case NdbDictionary::Column::Type::Longvarchar:   ///< Length bytes: 2, little-endian
          {
            CHARSET_INFO* charset = cmd.print_group_by_column.charset;
            require_sch(charset != nullptr, "Could not find charset for VARCHAR column");
            LexString content;
            if (column.type() == NdbDictionary::Column::Type::Varchar) {
              content = LexString{ &column.data()[1],
                                   (size_t)(unsigned char)column.data()[0] };
            } else {
              content = LexString{ &column.data()[2],
                                   (size_t)(unsigned char)column.data()[0] |
                                   (((size_t)(unsigned char)column.data()[1]) << 8) };
            }
            out << m_quote;
            print_string(out,
                         content,
                         charset,
                         m_json_output,
                         m_utf8_output || m_tsv_output,
                         false);
            out << m_quote;
            break;
          }
        case NdbDictionary::Column::Type::Binary:        ///< Len
        case NdbDictionary::Column::Type::Varbinary:     ///< Length bytes: 1, Max: 255
        case NdbDictionary::Column::Type::Longvarbinary: ///< Length bytes: 2, little-endian
          // O-13: as in a pass-through result.
          print_groupby_as_passthrough(out, column,
                                       cmd.print_group_by_column.precision);
          break;
        case NdbDictionary::Column::Type::Datetime:      ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
          feature_not_implemented("Print GROUP BY column of type Datetime");
        case NdbDictionary::Column::Type::Date:          ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
          {
            Uint32 date = column.data_uint32();
            Uint32 year = date >> 9;
            Uint32 month = (date >> 5) & 0xf;
            Uint32 day = date & 0x1f;
            out << m_quote << d4(year) << "-" << d2(month) << "-" << d2(day) << m_quote;
            break;
          }
        case NdbDictionary::Column::Type::Blob:          ///< Binary large object (see NdbBlob)
          feature_not_implemented("Print GROUP BY column of type Blob");
        case NdbDictionary::Column::Type::Text:          ///< Text blob
          feature_not_implemented("Print GROUP BY column of type Text");
        case NdbDictionary::Column::Type::Bit:           ///< Bit, length specifies no of bits
          feature_not_implemented("Print GROUP BY column of type Bit");
        case NdbDictionary::Column::Type::Time:          ///< Time without date
          feature_not_implemented("Print GROUP BY column of type Time");
        case NdbDictionary::Column::Type::Year:          ///< Year 1901-2155 (1 byte)
          // O-13: as in a pass-through result.
          print_groupby_as_passthrough(out, column, 0);
          break;
        case NdbDictionary::Column::Type::Timestamp:     ///< Unix time
          feature_not_implemented("Print GROUP BY column of type Timestamp");
        case NdbDictionary::Column::Type::Time2:         ///< 3 bytes + 0-3 fraction
          // O-13: as in a pass-through result.
          print_groupby_as_passthrough(out, column,
                                       cmd.print_group_by_column.precision);
          break;
        case NdbDictionary::Column::Type::Datetime2:     ///< 5 bytes plus 0-3 fraction
          {
            int precision = cmd.print_group_by_column.precision;
            longlong numericDate = my_datetime_packed_from_binary(
              (const unsigned char*)column.data(),
              (unsigned int)precision);
            MYSQL_TIME lTime;
            TIME_from_longlong_datetime_packed(&lTime, numericDate);
            char to[MAX_DATE_STRING_REP_LENGTH];
            my_TIME_to_str(lTime, to, precision);
            out << m_quote << to << m_quote;
            break;
          }
        case NdbDictionary::Column::Type::Timestamp2:    ///< 4 bytes + 0-3 fraction
          {
            int precision = cmd.print_group_by_column.precision;
            my_timeval myTV{};
            my_timestamp_from_binary(&myTV,
                                     (const unsigned char *)column.data(),
                                     (unsigned int) precision);
            MYSQL_TIME lTime;
            ronsql_timestamp_tv_to_TIME(myTV, &lTime);
            char to[MAX_DATE_STRING_REP_LENGTH];
            my_TIME_to_str(lTime, to, precision);
            out << m_quote << to << m_quote;
            break;
          }
        default:
          bug("Unexpected data type when printing GROUP BY column.");
        }
      }
      break;
    case Cmd::Type::PRINT_AGGREGATE:
      {
        NdbAggregator::Result result = m_regs_a[cmd.print_aggregate.reg_a];
        print_aggregate_result(out, result, cmd.print_aggregate.charset,
                               cmd.print_aggregate.scale,
                               cmd.print_aggregate.float_display,
                               cmd.print_aggregate.temporal,
                               cmd.print_aggregate.temporal_fsp);
      }
      break;
    case Cmd::Type::PRINT_AVG:
      {
        print_avg_result(out, m_regs_a[cmd.print_avg.reg_a_sum],
                         m_regs_a[cmd.print_avg.reg_a_count],
                         cmd.print_avg.scale);
      }
      break;
    case Cmd::Type::PRINT_STR:
      out.write(cmd.print_str.content.str, cmd.print_str.content.len);
      break;
    case Cmd::Type::PRINT_STR_JSON:
      if (m_json_output) {
        out << '"';
        print_string(out,
                     cmd.print_str.content,
                     &my_charset_utf8mb4_bin,
                     true,
                     m_utf8_output,
                     false);
        out << '"';
      } else {
        print_string(out,
                     cmd.print_str.content,
                     &my_charset_utf8mb4_bin,
                     false,
                     true,
                     false);
      }
      break;
    default:
      ronsql_bug("The result printer program has a command of unknown type.");
    }
  }
}

// Base64 (RFC 4648, "=" padding, no line breaks) for binary pass-through
// values under JSON output.  Not mysys' base64_encode, which inserts a
// newline every 76 characters.
void
ronsql_printer_internal::print_base64(std::ostream& out,
                                      const unsigned char* bytes, size_t len)
{
  static const char alphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  size_t i = 0;
  char quad[4];
  for (; i + 3 <= len; i += 3)
  {
    Uint32 v = ((Uint32)bytes[i] << 16) | ((Uint32)bytes[i + 1] << 8) |
               (Uint32)bytes[i + 2];
    quad[0] = alphabet[(v >> 18) & 0x3f];
    quad[1] = alphabet[(v >> 12) & 0x3f];
    quad[2] = alphabet[(v >> 6) & 0x3f];
    quad[3] = alphabet[v & 0x3f];
    out.write(quad, 4);
  }
  if (i < len)
  {
    Uint32 v = (Uint32)bytes[i] << 16;
    if (i + 1 < len) v |= (Uint32)bytes[i + 1] << 8;
    quad[0] = alphabet[(v >> 18) & 0x3f];
    quad[1] = alphabet[(v >> 12) & 0x3f];
    quad[2] = (i + 1 < len) ? alphabet[(v >> 6) & 0x3f] : '=';
    quad[3] = '=';
    out.write(quad, 4);
  }
}

// AVRO() values under JSON_ASCII output: the decoder's JSON text with every
// non-ASCII character \u-escaped.  Non-ASCII characters only occur inside
// JSON strings, where the escape means the same character, so a run of
// them is printed as the content of a JSON string would be.
void
ronsql_printer_internal::print_json_ascii(std::ostream& out,
                                          const std::string& json)
{
  size_t i = 0;
  while (i < json.size())
  {
    size_t run = i;
    while (run < json.size() && (unsigned char)json[run] >= 0x80) run++;
    if (run > i)
    {
      print_string(out,
                   LexString{&json[i], run - i},
                   &my_charset_utf8mb4_bin,
                   true,   // json_escape
                   false,  // utf8_output
                   false); // trim_space_suffix
      i = run;
      continue;
    }
    out.put(json[i]);
    i++;
  }
}

void
ronsql_print_json_string(std::ostream& out,
                         const char* str,
                         size_t len,
                         RonSQLExecParams::OutputFormat format)
{
  assert(format == RonSQLExecParams::OutputFormat::JSON ||
         format == RonSQLExecParams::OutputFormat::JSON_ASCII);
  print_string(out,
               LexString{str, len},
               &my_charset_utf8mb4_bin,
               true,   // json_escape
               format == RonSQLExecParams::OutputFormat::JSON, // utf8_output
               false); // trim_space_suffix
}

// Binary pass-through values under TEXT output: the raw bytes with the
// escaping the mysql client applies in batch mode (client/mysql.cc
// tee_write, MY_PRINT_ESC_0 | MY_PRINT_CTRL): NUL, tab, newline and
// backslash are written as \0, \t, \n and \\; every other byte as is.
void
ronsql_printer_internal::print_binary_tsv(std::ostream& out,
                                          const unsigned char* bytes,
                                          size_t len)
{
  for (size_t i = 0; i < len; i++)
  {
    switch (bytes[i])
    {
    case 0x00: out << "\\0"; break;
    case 0x09: out << "\\t"; break;
    case 0x0a: out << "\\n"; break;
    case 0x5c: out << "\\\\"; break;
    default: out.put((char)bytes[i]); break;
    }
  }
}

// Print a representation of ls to out, converting it from the given charset.
// json_escape == true:        Escape characters using JSON standard. (Do not
//                             print quotes.)
// json_escape == false:       Escape characters similarly to mysql CLI.
// utf8_output == true:        Output is UTF-8 encoded.
// utf8_output == false:       Use \u escape for characters with code point
//                             U+00a0 and above. Only to be used with
//                             json_escape == true.
// trim_space_suffix == true:  Ignore trailing spaces
// trim_space_suffix == false: Print trailing spaces
// Inspired by `well_formed_copy_nchars` in ../../../../sql-common/sql_string.cc
void
ronsql_printer_internal::print_string(std::ostream& out,
                                      LexString ls,
                                      CHARSET_INFO* charset,
                                      bool json_escape,
                                      bool utf8_output,
                                      bool trim_space_suffix)
{
  const uchar* str = pointer_cast<const uchar *>(ls.str);
  const uchar* end = pointer_cast<const uchar *>(&ls.str[ls.len]);
  my_wc_t wc;
  my_charset_conv_mb_wc mb_wc = charset->cset->mb_wc;
  static const char* hex = "0123456789abcdef";
  int spaces_withheld = 0;
  while(str < end) {
    int cnvres = (*mb_wc)(charset, &wc, str, end);
    // Convert one character from str to a unicode code point
    if (cnvres > 0) {
      str += cnvres;
    } else if (cnvres == MY_CS_ILSEQ) {
      // Not well-formed according to source charset
      str++;
      wc = 0xfffd;
    } else if (cnvres > MY_CS_TOOSMALL) {
      // A correct multibyte sequence detected, but without Unicode mapping.
      str += (-cnvres);
      wc = 0xfffd;
    } else {
      // Not enough characters.
      assert(str + charset->mbmaxlen >= end);
      str = end;
      wc = 0xfffd;
    }
    // Encode the character in JSON
    if (unlikely(trim_space_suffix)) {
      if (wc == 0x20) {
        spaces_withheld++;
        continue;
      } else {
        while(spaces_withheld) {
          out << ' ';
          spaces_withheld--;
        }
      }
    }
    if (likely(wc < 0x80)) {
      if (likely(json_escape)) {
        static const char *json_encode_lookup =
        /* Code points U+0000 -- U+007f
         *  / Code point
         *  |         / JSON encoding padded to 6 bytes
         *  |         |          / Length
         *  |         |          |        */
          /* U+0000 */ "\\u0000"  "\x06"
          /* U+0001 */ "\\u0001"  "\x06"
          /* U+0002 */ "\\u0002"  "\x06"
          /* U+0003 */ "\\u0003"  "\x06"
          /* U+0004 */ "\\u0004"  "\x06"
          /* U+0005 */ "\\u0005"  "\x06"
          /* U+0006 */ "\\u0006"  "\x06"
          /* U+0007 */ "\\u0007"  "\x06"
          /* U+0008 */ "\\b    "  "\x02"
          /* U+0009 */ "\\t    "  "\x02"
          /* U+000a */ "\\n    "  "\x02"
          /* U+000b */ "\\u000b"  "\x06"
          /* U+000c */ "\\f    "  "\x02"
          /* U+000d */ "\\r    "  "\x02"
          /* U+000e */ "\\u000e"  "\x06"
          /* U+000f */ "\\u000f"  "\x06"
          /* U+0010 */ "\\u0010"  "\x06"
          /* U+0011 */ "\\u0011"  "\x06"
          /* U+0012 */ "\\u0012"  "\x06"
          /* U+0013 */ "\\u0013"  "\x06"
          /* U+0014 */ "\\u0014"  "\x06"
          /* U+0015 */ "\\u0015"  "\x06"
          /* U+0016 */ "\\u0016"  "\x06"
          /* U+0017 */ "\\u0017"  "\x06"
          /* U+0018 */ "\\u0018"  "\x06"
          /* U+0019 */ "\\u0019"  "\x06"
          /* U+001a */ "\\u001a"  "\x06"
          /* U+001b */ "\\u001b"  "\x06"
          /* U+001c */ "\\u001c"  "\x06"
          /* U+001d */ "\\u001d"  "\x06"
          /* U+001e */ "\\u001e"  "\x06"
          /* U+001f */ "\\u001f"  "\x06"
          /* U+0020 */ "      "   "\x01"
          /* U+0021 */ "!     "   "\x01"
          /* U+0022 */ "\\\"    " "\x02"
          /* U+0023 */ "#     "   "\x01"
          /* U+0024 */ "$     "   "\x01"
          /* U+0025 */ "%     "   "\x01"
          /* U+0026 */ "&     "   "\x01"
          /* U+0027 */ "'     "   "\x01"
          /* U+0028 */ "(     "   "\x01"
          /* U+0029 */ ")     "   "\x01"
          /* U+002a */ "*     "   "\x01"
          /* U+002b */ "+     "   "\x01"
          /* U+002c */ ",     "   "\x01"
          /* U+002d */ "-     "   "\x01"
          /* U+002e */ ".     "   "\x01"
          /* U+002f */ "/     "   "\x01"
          /* U+0030 */ "0     "   "\x01"
          /* U+0031 */ "1     "   "\x01"
          /* U+0032 */ "2     "   "\x01"
          /* U+0033 */ "3     "   "\x01"
          /* U+0034 */ "4     "   "\x01"
          /* U+0035 */ "5     "   "\x01"
          /* U+0036 */ "6     "   "\x01"
          /* U+0037 */ "7     "   "\x01"
          /* U+0038 */ "8     "   "\x01"
          /* U+0039 */ "9     "   "\x01"
          /* U+003a */ ":     "   "\x01"
          /* U+003b */ ";     "   "\x01"
          /* U+003c */ "<     "   "\x01"
          /* U+003d */ "=     "   "\x01"
          /* U+003e */ ">     "   "\x01"
          /* U+003f */ "?     "   "\x01"
          /* U+0040 */ "@     "   "\x01"
          /* U+0041 */ "A     "   "\x01"
          /* U+0042 */ "B     "   "\x01"
          /* U+0043 */ "C     "   "\x01"
          /* U+0044 */ "D     "   "\x01"
          /* U+0045 */ "E     "   "\x01"
          /* U+0046 */ "F     "   "\x01"
          /* U+0047 */ "G     "   "\x01"
          /* U+0048 */ "H     "   "\x01"
          /* U+0049 */ "I     "   "\x01"
          /* U+004a */ "J     "   "\x01"
          /* U+004b */ "K     "   "\x01"
          /* U+004c */ "L     "   "\x01"
          /* U+004d */ "M     "   "\x01"
          /* U+004e */ "N     "   "\x01"
          /* U+004f */ "O     "   "\x01"
          /* U+0050 */ "P     "   "\x01"
          /* U+0051 */ "Q     "   "\x01"
          /* U+0052 */ "R     "   "\x01"
          /* U+0053 */ "S     "   "\x01"
          /* U+0054 */ "T     "   "\x01"
          /* U+0055 */ "U     "   "\x01"
          /* U+0056 */ "V     "   "\x01"
          /* U+0057 */ "W     "   "\x01"
          /* U+0058 */ "X     "   "\x01"
          /* U+0059 */ "Y     "   "\x01"
          /* U+005a */ "Z     "   "\x01"
          /* U+005b */ "[     "   "\x01"
          /* U+005c */ "\\\\    " "\x02"
          /* U+005d */ "]     "   "\x01"
          /* U+005e */ "^     "   "\x01"
          /* U+005f */ "_     "   "\x01"
          /* U+0060 */ "`     "   "\x01"
          /* U+0061 */ "a     "   "\x01"
          /* U+0062 */ "b     "   "\x01"
          /* U+0063 */ "c     "   "\x01"
          /* U+0064 */ "d     "   "\x01"
          /* U+0065 */ "e     "   "\x01"
          /* U+0066 */ "f     "   "\x01"
          /* U+0067 */ "g     "   "\x01"
          /* U+0068 */ "h     "   "\x01"
          /* U+0069 */ "i     "   "\x01"
          /* U+006a */ "j     "   "\x01"
          /* U+006b */ "k     "   "\x01"
          /* U+006c */ "l     "   "\x01"
          /* U+006d */ "m     "   "\x01"
          /* U+006e */ "n     "   "\x01"
          /* U+006f */ "o     "   "\x01"
          /* U+0070 */ "p     "   "\x01"
          /* U+0071 */ "q     "   "\x01"
          /* U+0072 */ "r     "   "\x01"
          /* U+0073 */ "s     "   "\x01"
          /* U+0074 */ "t     "   "\x01"
          /* U+0075 */ "u     "   "\x01"
          /* U+0076 */ "v     "   "\x01"
          /* U+0077 */ "w     "   "\x01"
          /* U+0078 */ "x     "   "\x01"
          /* U+0079 */ "y     "   "\x01"
          /* U+007a */ "z     "   "\x01"
          /* U+007b */ "{     "   "\x01"
          /* U+007c */ "|     "   "\x01"
          /* U+007d */ "}     "   "\x01"
          /* U+007e */ "~     "   "\x01"
          /* U+007f */ "\\u007f"  "\x06"
          ;
        out << std::string_view(&json_encode_lookup[wc * 7],
                                json_encode_lookup[wc * 7 + 6]);
      } else {
        switch(char(wc)) {
          case 0x00: out << "\\0"; break;
          case 0x09: out << "\\t"; break;
          case 0x0a: out << "\\n"; break;
          case 0x5c: out << "\\\\"; break;
          default: out << char(wc); break;
        }
      }
   } else if (unlikely(wc <= 0x009f)) {
      if (likely(json_escape)) {
        out << "\\u00"
            << hex[(wc >> 4) & 0x0f]
            << hex[wc & 0x0f];
      } else {
        out << char(0xc2)
            << char(wc);
      }
    } else if (likely(wc <= 0x07ff)) {
      if (likely(utf8_output)) {
        out << char(0xc0 | (wc >> 6))
            << char(0x80 | (wc & 0x3f));
      } else {
        out << "\\u0"
            << hex[wc >> 8]
            << hex[(wc >> 4) & 0x0f]
            << hex[wc & 0x0f];
      }
    } else if (unlikely((wc & (~my_wc_t(0x07ff))) == 0xd800)) {
      // Illegal surrogate
      out << (likely(utf8_output) ? "�" : "\\ufffd");
    } else if (likely(wc <= 0xffff)) {
      if (likely(utf8_output)) {
        out << char(0xe0 | (wc >> 12))
            << char(0x80 | ((wc >> 6) & 0x3f))
            << char(0x80 | (wc & 0x3f));
      } else {
        out << "\\u"
            << hex[wc >> 12]
            << hex[(wc >> 8) & 0x0f]
            << hex[(wc >> 4) & 0x0f]
            << hex[wc & 0x0f];
      }
    } else if (likely(wc <= 0x10ffff)) {
      if (likely(utf8_output)) {
        out << char(0xf0 | (wc >> 18))
            << char(0x80 | ((wc >> 12) & 0x3f))
            << char(0x80 | ((wc >> 6) & 0x3f))
            << char(0x80 | (wc & 0x3f));
      } else {
        my_wc_t wco = wc - 0x010000;
        out << "\\ud"
            << hex[0x08 | (wco >> 18)]
            << hex[(wco >> 14) & 0x0f]
            << hex[(wco >> 10) & 0x0f]
            << "\\ud"
            << hex[0x0c | ((wco >> 8) & 0x03)]
            << hex[(wco >> 4) & 0x0f]
            << hex[wco & 0x0f];
      }
    } else {
      // Illegal code point
      out << (likely(utf8_output) ? "�" : "\\ufffd");
    }
  }
}

/*
 * RONSQL-44: a double is printed with the shortest digits that represent
 * it exactly, in fixed-point notation.  my_fcvt_compact() stops 17 digits
 * after the decimal point, which is exact only from 0.1 up: below that it
 * dropped digits, and below about 5e-18 it printed 0.  For a nonzero value
 * below 0.1, take the shortest exact digits from my_gcvt(), which MySQL
 * uses for DOUBLE, and write its "d.ddde-NN" form (below 1e-15) in fixed
 * point.  'to' holds FLOATING_POINT_BUFFER + 1 bytes; the longest result,
 * -2.2250738585072014e-308, is 327 characters.
 *
 * O-4: from 1e15 up the text is mysqld's, my_gcvt() with the width mysqld
 * gives it (Protocol_text::store_double).  It writes an exponent when the
 * shortest digits end before the decimal point (1e15, -1.5e15,
 * 9.007199254740992e15, 1.2345678901234568e17) and fixed point when they
 * do not (1000000000000000.5).  my_fcvt_compact() wrote 1e20 as
 * 100000000000000000000.  From 0.1 up to 1e15 both functions give the
 * same text.  Inf and NaN set *error in both.
 */
size_t
ronsql_printer_internal::double_to_fixed(double value, char* to, bool* error)
{
  if (!(std::fabs(value) < 1e15))
    return my_gcvt(value, MY_GCVT_ARG_DOUBLE, FLOATING_POINT_BUFFER, to,
                   error);
  if (!(std::fabs(value) < 0.1) || value == 0.0)
    return my_fcvt_compact(value, to, error);
  // Wide enough that my_gcvt() drops no digit in either format.
  char g[64];
  const size_t glen =
      my_gcvt(value, MY_GCVT_ARG_DOUBLE, sizeof(g) - 1, g, error);
  const char* e = static_cast<const char*>(memchr(g, 'e', glen));
  if (*error || e == NULL)
  {
    memcpy(to, g, glen + 1);
    return glen;
  }
  // [-]d[.ddd]e-NN becomes [-]0.<NN - 1 zeros><digits>.
  ronsql_require_bug(e[1] == '-',
                     "A double below 0.1 was printed with a positive exponent.");
  int exponent = 0;
  for (const char* p = e + 2; *p != '\0'; p++)
    exponent = exponent * 10 + (*p - '0');
  const int zeros = exponent - 1;
  const char* src = g;
  ronsql_require_bug(3 + zeros + (e - src) <= FLOATING_POINT_BUFFER + 1,
                     "A double below 0.1 does not fit the fixed-point buffer.");
  char* dst = to;
  if (*src == '-') *dst++ = *src++;
  *dst++ = '0';
  *dst++ = '.';
  for (int i = 0; i < zeros; i++) *dst++ = '0';
  for (; src < e; src++)
    if (*src != '.') *dst++ = *src;
  *dst = '\0';
  return dst - to;
}

inline void
ResultPrinter::print_float_or_double(std::ostream& out, double value,
                                     bool float_display)
{
  char buffer[FLOATING_POINT_BUFFER + 1];
  bool error;
  // Match MySQL's text-protocol FLOAT conversion. SUM/AVG and true DOUBLE
  // values are printed exactly, in fixed point below 1e15 and as mysqld
  // prints a DOUBLE from 1e15 up (double_to_fixed()).
  size_t len = float_display
      ? my_gcvt(value, MY_GCVT_ARG_FLOAT, FLOATING_POINT_BUFFER, buffer, &error)
      : double_to_fixed(value, buffer, &error);
  if (error)
  {
    // value is Inf, -Inf or NaN.
    out << m_null_representation;
    return;
  }
  ronsql_require_bug(len > 0 && buffer[len] == 0,
                     "A double was formatted as an empty or unterminated string.");
  out << buffer;
  return;
}

/*
 * O-5, O-6: SUM, MIN and MAX over a DECIMAL with a scale and AVG over an
 * exact type are DECIMAL results in MySQL, but here they are doubles (the
 * kernel accumulates and widens them as DOUBLE), also as CTE outputs.
 * Format such a value at its scale the way mysqld prints the double result
 * of a pushed SUM or AVG (Item_sum_sum/Item_sum_avg::val_decimal):
 * double2my_decimal() takes the shortest digits of the double, and
 * Item::val_string_from_decimal() rounds them HALF_UP to the scale with
 * decimal_round().  my_fcvt() and snprintf("%.*f") rounded the binary
 * value instead: an exact tie went to even (AVG 1/32 = 0.03125 printed
 * 0.0312, mysqld 0.0313), and a value that rounds to zero kept its sign
 * (a DECIMAL sum of -2.8e-17 printed -0.00).  decimal_round() clears the
 * sign of a zero.
 *
 * The decimal holds 81 digits in 9 words, as a my_decimal.  A value whose
 * integer and scale words do not fit (from 1e45 at scale 30, from 1e72 at
 * scale 9 or less) keeps the rounding of my_fcvt().  A MIN, MAX or AVG of
 * any DECIMAL fits (O-7: precisions above 15 come here too); a SUM gets
 * there only over about 10^8 rows near the maximum of a DECIMAL(65, s).
 * 'to' holds FLOATING_POINT_BUFFER bytes.  Returns the length, or 0 for
 * Inf and NaN.
 */
size_t
ronsql_printer_internal::format_fixed_scale(double value, int scale, char* to)
{
  ronsql_require_bug(scale >= 0 && scale <= 30,
                     "A fixed-scale value has a scale outside 0 to 30.");
  if (!std::isfinite(value))
    return 0;
  decimal_digit_t digits[9];
  decimal_t dec;
  dec.len = 9;
  dec.buf = digits;
  int len = FLOATING_POINT_BUFFER;
  // double2decimal() returns E_DEC_TRUNCATED when it drops digits beyond
  // the 81st decimal, which the rounding below drops anyway.
  if (double2decimal(value, &dec) <= E_DEC_TRUNCATED &&
      decimal_round(&dec, &dec, scale, HALF_UP) == E_DEC_OK &&
      decimal2string(&dec, to, &len) == E_DEC_OK)
    return static_cast<size_t>(len);
  bool error;
  return my_fcvt(value, scale, to, &error);
}

void
ResultPrinter::print_fixed_scale(std::ostream& out, double value, int scale)
{
  char buffer[FLOATING_POINT_BUFFER];
  if (format_fixed_scale(value, scale, buffer) == 0)
  {
    // value is Inf, -Inf or NaN, as in print_float_or_double().
    out << m_null_representation;
    return;
  }
  out << buffer;
}

void
ResultPrinter::print_avg_result(std::ostream& out,
                                NdbAggregator::Result sum,
                                NdbAggregator::Result count,
                                int scale)
{
  if (sum.is_null() &&
      !count.is_null() &&
      count.type() == NdbDictionary::Column::Type::Bigunsigned &&
      count.data_uint64() == 0) {
    out << m_null_representation;
    return;
  }

  const double result =
      convert_result_to_double(sum) / convert_result_to_double(count);
  if (scale < 0) {
    print_float_or_double(out, result);
    return;
  }

  // AVG over an exact type is a DECIMAL in MySQL (O-6).
  ronsql_require_bug(scale <= 30, "The result printer got an AVG scale above 30.");
  print_fixed_scale(out, result, scale);
}

int
ResultPrinter::avg_arg_scale(const Outputs* out) const
{
  ronsql_require_bug(out != NULL && out->type == Outputs::Type::AVG,
                     "The result printer took the AVG scale of an output "
                     "that is not an AVG.");
  const AggregationAPICompiler::Expr* arg = out->avg.arg;
  // Expression result-type inference is separate from column metadata.
  // Keep the existing four-digit formatting for those arguments.
  if (arg == NULL || !arg->isLoad() || m_column_metadata == NULL ||
      m_column_names == NULL || arg->getLoadIdx() >= m_column_names->size())
    return 4;
  // avg_scale is initialized even when the older display metadata is absent
  // (e.g. a DOUBLE CTE result with no fixed scale, or offline EXPLAIN).
  return m_column_metadata[arg->getLoadIdx()].avg_scale;
}

bool
ResultPrinter::aggregate_uses_float_display(const Outputs* out) const
{
  if (out == NULL || out->type != Outputs::Type::AGGREGATE ||
      (out->aggregate.fun != T_MIN && out->aggregate.fun != T_MAX))
    return false;
  // Arithmetic expressions have their own result type; only a column
  // MIN/MAX preserves FLOAT display, and a GREATEST / LEAST of FLOAT
  // columns (O-10: a watermark over FLOAT scalar-CTE outputs).
  bool any_column = false;
  bool all_float = true;
  expr_float_display(out->aggregate.arg, any_column, all_float);
  return any_column && all_float;
}

void
ResultPrinter::expr_float_display(const AggregationAPICompiler::Expr* e,
                                  bool& any_column, bool& all_float) const
{
  if (e == NULL) {
    all_float = false;
    return;
  }
  if (e->isLoad()) {
    any_column = true;
    if (m_column_metadata == NULL || m_column_names == NULL ||
        e->getLoadIdx() >= m_column_names->size() ||
        !m_column_metadata[e->getLoadIdx()].float_display)
      all_float = false;
    return;
  }
  // FLOAT and an integer literal (LONGLONG) aggregate to FLOAT in MySQL.
  if (e->isLoadConstantInt()) return;
  if (e->isGreatest2() || e->isLeast2()) {
    expr_float_display(e->getLeft(), any_column, all_float);
    expr_float_display(e->getRight(), any_column, all_float);
    return;
  }
  all_float = false;
}

CHARSET_INFO*
ResultPrinter::aggregate_arg_charset(const Outputs* out) const
{
  if (out == NULL || out->type != Outputs::Type::AGGREGATE)
    return NULL;
  if (out->aggregate.fun != T_MIN && out->aggregate.fun != T_MAX)
    return NULL;
  const AggregationAPICompiler::Expr* arg = out->aggregate.arg;
  if (arg != NULL && arg->isCase())
  {
    // MIN / MAX (CASE WHEN c THEN x ELSE y END) over strings: the grammar
    // allows one WHEN and requires the ELSE, and NdbAggregator::RepeatAgg
    // refuses string arms of different types or collations, so either
    // arm's column gives the charset.
    CHARSET_INFO* cs = load_arg_charset(arg->getLeft());
    return cs != NULL ? cs : load_arg_charset(arg->getRight());
  }
  return load_arg_charset(arg);
}

CHARSET_INFO*
ResultPrinter::load_arg_charset(const AggregationAPICompiler::Expr* e) const
{
  if (e == NULL || !e->isLoad())
    return NULL;
  Uint32 col_idx = e->getLoadIdx();
  if (m_column_names == NULL || col_idx >= m_column_names->size())
    return NULL;
  if (m_column_metadata == NULL ||
      !m_column_metadata[col_idx].has_metadata)
  {
    return NULL;
  }
  return m_column_metadata[col_idx].charset;
}

// D8: a watermark GREATEST/LEAST over scalar-CTE outputs reaches the printer
// as the arg of the implicit-MAX wrapper — a Greatest2/Least2 expression, not
// a direct Load.  Walk the binary tree to its first Load operand so the source
// DECIMAL scale/precision (carried on that column's metadata) can format the
// result like a direct MIN/MAX.  A watermark's operands share the source
// column, so the first load's scale is representative.
static const AggregationAPICompiler::Expr*
ronsql_first_load_in_expr(const AggregationAPICompiler::Expr* e)
{
  if (e == NULL) return NULL;
  if (e->isLoad()) return e;
  if (e->isGreatest2() || e->isLeast2())
  {
    const AggregationAPICompiler::Expr* l =
        ronsql_first_load_in_expr(e->getLeft());
    if (l != NULL) return l;
    return ronsql_first_load_in_expr(e->getRight());
  }
  return NULL;
}

// Source DECIMAL scale of a MIN/MAX aggregate's argument column, so the
// printer can format the (DOUBLE-widened) result with a fixed scale to match
// MySQL's DECIMAL output (e.g. 20055.00, not 20055).  Returns 0 when the
// argument is not a scaled column — true DOUBLE/FLOAT and integer results then
// keep their compact (my_fcvt_compact) formatting unchanged.
int
ResultPrinter::aggregate_arg_scale(const Outputs* out) const
{
  if (out == NULL || out->type != Outputs::Type::AGGREGATE)
    return 0;
  // D15: MIN/MAX over a scale-bearing DECIMAL-derived column.  D1: SUM over
  // the same — SUM over a scale>0 DECIMAL widens to DOUBLE in the kernel and
  // must print with the source scale (e.g. 15051277.50) like MySQL's exact
  // DECIMAL sum, instead of the compact full-precision DOUBLE form.
  if (out->aggregate.fun != T_MIN && out->aggregate.fun != T_MAX &&
      out->aggregate.fun != T_SUM)
    return 0;
  // D8: handle a direct Load or a Greatest2/Least2 watermark over Loads.
  const AggregationAPICompiler::Expr* loadExpr =
      ronsql_first_load_in_expr(out->aggregate.arg);
  if (loadExpr == NULL)
    return 0;
  Uint32 col_idx = loadExpr->getLoadIdx();
  if (m_column_names == NULL || col_idx >= m_column_names->size())
    return 0;
  if (m_column_metadata == NULL ||
      !m_column_metadata[col_idx].has_metadata)
  {
    return 0;
  }
  return m_column_metadata[col_idx].scale;
}

// Source DECIMAL precision of a MIN/MAX aggregate's argument column.  A
// precision above 0 marks a DECIMAL, whose result prints at its scale.  O-7:
// it no longer limits that to precision 15: print_fixed_scale() rounds the
// double's shortest digits, where snprintf("%.*f") wrote its binary value
// (0.1 at scale 30 as 0.100000000000000005551115123126).
int
ResultPrinter::aggregate_arg_precision(const Outputs* out) const
{
  if (out == NULL || out->type != Outputs::Type::AGGREGATE)
    return 0;
  // D15 (MIN/MAX) + D1 (SUM): see aggregate_arg_scale.
  if (out->aggregate.fun != T_MIN && out->aggregate.fun != T_MAX &&
      out->aggregate.fun != T_SUM)
    return 0;
  // D8: handle a direct Load or a Greatest2/Least2 watermark over Loads.
  const AggregationAPICompiler::Expr* loadExpr =
      ronsql_first_load_in_expr(out->aggregate.arg);
  if (loadExpr == NULL)
    return 0;
  Uint32 col_idx = loadExpr->getLoadIdx();
  if (m_column_names == NULL || col_idx >= m_column_names->size())
    return 0;
  if (m_column_metadata == NULL ||
      !m_column_metadata[col_idx].has_metadata)
  {
    return 0;
  }
  return m_column_metadata[col_idx].precision;
}

// D17 + temporal extension: which temporal decode (if any) applies to a
// MIN/MAX aggregate's source column.  The kernel returns a temporal MIN/MAX
// as a Bigunsigned packed value; this drives the printer to unpack it.
// Mirrors aggregate_arg_scale's column-metadata walk.
ResultPrinter::TemporalDisplay
ResultPrinter::aggregate_arg_temporal(const Outputs* out, int& fsp) const
{
  fsp = 0;
  if (out == NULL || out->type != Outputs::Type::AGGREGATE)
    return TemporalDisplay::NONE;
  if (out->aggregate.fun != T_MIN && out->aggregate.fun != T_MAX)
    return TemporalDisplay::NONE;
  // O-10: also a GREATEST / LEAST watermark over temporal scalar-CTE
  // outputs, as aggregate_arg_scale follows it (D8).
  return expr_temporal(out->aggregate.arg, fsp);
}

ResultPrinter::TemporalDisplay
ResultPrinter::expr_temporal(const AggregationAPICompiler::Expr* e,
                             int& fsp) const
{
  fsp = 0;
  if (e == NULL)
    return TemporalDisplay::NONE;
  if (e->isGreatest2() || e->isLeast2())
  {
    // The preparer refuses operands of different temporal types
    // (check_cte_temporal_outputs); NONE keeps any other mix numeric.
    int left_fsp = 0;
    int right_fsp = 0;
    const TemporalDisplay left = expr_temporal(e->getLeft(), left_fsp);
    const TemporalDisplay right = expr_temporal(e->getRight(), right_fsp);
    if (left == TemporalDisplay::NONE || left != right ||
        left_fsp != right_fsp)
      return TemporalDisplay::NONE;
    fsp = left_fsp;
    return left;
  }
  if (!e->isLoad())
    return TemporalDisplay::NONE;
  Uint32 col_idx = e->getLoadIdx();
  if (m_column_names == NULL || col_idx >= m_column_names->size())
    return TemporalDisplay::NONE;
  if (m_column_metadata == NULL ||
      !m_column_metadata[col_idx].has_metadata)
  {
    return TemporalDisplay::NONE;
  }
  fsp = m_column_metadata[col_idx].temporal_fsp;
  return m_column_metadata[col_idx].temporal;
}

void
ResultPrinter::print_aggregate_result(std::ostream& out,
                                      NdbAggregator::Result result,
                                      CHARSET_INFO* charset,
                                      int scale,
                                      bool float_display,
                                      TemporalDisplay temporal,
                                      int temporal_fsp)
{
  if (result.is_null())
  {
    out << m_null_representation;
    return;
  }

  // D17 + temporal extension: MIN/MAX over a temporal column comes back as a
  // Bigunsigned holding the column's native packed value (the kernel reads
  // the on-disk bytes in their native order — little-endian for DATE/YEAR,
  // big-endian for DATETIME2/TIME2 — into the register, which is monotonic
  // with chronological order so MIN/MAX is exact).  Decode it back here.
  if (temporal != TemporalDisplay::NONE &&
      result.type() == NdbDictionary::Column::Bigunsigned)
  {
    // Quoted like a pass-through temporal column: under JSON output the
    // value is a string, and an empty quote here left the body unparsable
    // (RONDB-1121 F9).
    print_temporal_packed(out, result.data_uint64(), temporal,
                          temporal_fsp, m_quote);
    return;
  }

  switch (result.type())
  {
  case NdbDictionary::Column::Bigint:
    out << result.data_int64();
    break;
  case NdbDictionary::Column::Bigunsigned:
    out << result.data_uint64();
    break;
  case NdbDictionary::Column::Double:
    if (scale > 0)
    {
      // MIN/MAX over a DECIMAL(_, scale) is widened to DOUBLE in the kernel,
      // but MySQL prints it with the source scale (e.g. 20055.00).  Format
      // with fixed scale so the output matches; floating results pass
      // scale == 0 and use their source-type display below.  SUM over
      // such a DECIMAL is accumulated as DOUBLE and may come out as a
      // tiny negative value; like a DECIMAL it prints 0.00, not -0.00
      // (O-5).
      print_fixed_scale(out, result.data_double(), scale);
    }
    else
    {
      print_float_or_double(out, result.data_double(), float_display);
    }
    break;
  case NdbDictionary::Column::Char:
  case NdbDictionary::Column::Varchar:
  case NdbDictionary::Column::Longvarchar:
    {
      require_sch(charset != nullptr,
                  "Could not find charset for string aggregation result");
      Uint32 payload_len = 0;
      const char* payload = result.data_str(&payload_len);
      out << m_quote;
      print_string(out,
                   LexString{payload, payload_len},
                   charset,
                   m_json_output,
                   m_utf8_output || m_tsv_output,
                   result.type() == NdbDictionary::Column::Char);
      out << m_quote;
    }
    break;
  case NdbDictionary::Column::Undefined:
    bug("Unexpected undefined data type in aggregation result.");
  default:
    bug("Unexpected data type in aggregation result.");
  }
}

inline static double
convert_result_to_double(NdbAggregator::Result result)
{
  if (result.is_null())
    return std::numeric_limits<double>::quiet_NaN();
  switch (result.type())
  {
  case NdbDictionary::Column::Type::Bigint:
    return static_cast<double>(result.data_int64());
  case NdbDictionary::Column::Type::Bigunsigned:
    return static_cast<double>(result.data_uint64());
  case NdbDictionary::Column::Type::Double:
    return static_cast<double>(result.data_double());
  default:
    throw RonSQLPermanentError("Unexpected data type in results underlying AVG."
                               " Please report a bug.");
  }
}

void
ResultPrinter::scan_having_max_agg(const ConditionalExpression* expr,
                                   Uint32& max_idx)
{
  if (expr == NULL) return;
  switch (expr->op)
  {
  case T_SUM:
  case T_COUNT:
  case T_MIN:
  case T_MAX:
    max_idx = max(max_idx, expr->having_agg.agg_index + 1);
    break;
  case T_AVG:
    max_idx = max(max_idx, expr->having_agg.agg_index + 1);
    max_idx = max(max_idx, expr->having_agg.agg_index2 + 1);
    break;
  case T_AND:
  case T_OR:
  case T_EQUALS:
  case T_NOT_EQUALS:
  case T_GT:
  case T_GE:
  case T_LT:
  case T_LE:
  case T_PLUS:
  case T_MINUS:
  case T_MULTIPLY:
  case T_SLASH:
    scan_having_max_agg(expr->args.left, max_idx);
    scan_having_max_agg(expr->args.right, max_idx);
    break;
  case T_NOT:
  case T_EXCLAMATION:
    scan_having_max_agg(expr->args.left, max_idx);
    break;
  case T_IS:
    scan_having_max_agg(expr->is.arg, max_idx);
    break;
  case I_IN_SUBQUERY:
    // Subquery substitution runs after register allocation. Only the outer
    // operand contributes aggregate registers to this SELECT.
    scan_having_max_agg(expr->in_subquery.expr, max_idx);
    break;
  case T_IDENTIFIER:
    // Output alias mapped to agg_index during compile()
    max_idx = max(max_idx, expr->having_agg.agg_index + 1);
    break;
  default:
    break;
  }
}

// O-8: HavingValue, one HAVING operand, is in ResultPrinterInternal.hpp.

// The DECIMAL that format_fixed_scale() prints for value: its shortest
// digits rounded HALF_UP at scale.  False for Inf and NaN, and for a value
// whose digits do not fit to (9 words).
static bool
double_to_decimal_at_scale(double value, int scale, decimal_t* to)
{
  if (!std::isfinite(value))
    return false;
  return double2decimal(value, to) <= E_DEC_TRUNCATED &&
         decimal_round(to, to, scale, HALF_UP) == E_DEC_OK;
}

/*
 * O-9: a SUM over a DECIMAL accumulates as a DOUBLE on the data nodes, so
 * sums that are equal DECIMALs in MySQL can differ in their last bits
 * (0.10 + 0.20 is 0.30000000000000004, 0.30 is 0.29999999999999999), and
 * ORDER BY sorted such groups by those bits instead of by its next key.
 * Compare the values that print at the scale, as avg_sort_key() does for
 * AVG.  Rounding keeps the order, so only doubles less than two units of
 * the scale apart can round to the same value: the others compare as
 * they are, without formatting.
 */
int
ronsql_compare_at_scale(double a, double b, int scale)
{
  if (a == b)
    return 0;
  const int raw = (a < b) ? -1 : 1;
  if (!(std::fabs(a - b) < 2 * std::pow(10.0, -scale)))
    return raw;
  decimal_digit_t digits_a[9];
  decimal_digit_t digits_b[9];
  decimal_t dec_a;
  decimal_t dec_b;
  dec_a.len = 9;
  dec_a.buf = digits_a;
  dec_b.len = 9;
  dec_b.buf = digits_b;
  if (!double_to_decimal_at_scale(a, scale, &dec_a) ||
      !double_to_decimal_at_scale(b, scale, &dec_b))
    return raw;
  const int cmp = decimal_cmp(&dec_a, &dec_b);
  return (cmp < 0) ? -1 : (cmp > 0) ? 1 : 0;
}

int
ResultPrinter::double_sort_scale(Uint32 col_idx) const
{
  if (m_column_metadata == NULL || m_column_names == NULL ||
      col_idx >= m_column_names->size())
    return 0;
  const ColumnMetadata& meta = m_column_metadata[col_idx];
  // avg_scale is -1 for a FLOAT or DOUBLE source.
  if (!meta.has_metadata || meta.avg_scale < 0 ||
      meta.temporal != TemporalDisplay::NONE || meta.scale <= 0)
    return 0;
  return meta.scale;
}

int
ResultPrinter::exact_arg_scale(const AggregationAPICompiler::Expr* arg) const
{
  // A column, or a GREATEST / LEAST watermark over columns (D8).
  const AggregationAPICompiler::Expr* load = ronsql_first_load_in_expr(arg);
  if (load == NULL)
    return -1;
  const Uint32 col_idx = load->getLoadIdx();
  if (m_column_metadata == NULL || m_column_names == NULL ||
      col_idx >= m_column_names->size())
    return -1;
  const ColumnMetadata& meta = m_column_metadata[col_idx];
  // avg_scale is -1 for a FLOAT or DOUBLE column, also a CTE output.
  if (!meta.has_metadata || meta.avg_scale < 0 || meta.charset != NULL ||
      meta.temporal != TemporalDisplay::NONE)
    return -1;
  return meta.scale;
}

CHARSET_INFO*
ResultPrinter::string_arg_charset(const AggregationAPICompiler::Expr* arg)
    const
{
  if (arg != NULL && arg->isCase())
  {
    // As aggregate_arg_charset(): either arm's column.
    CHARSET_INFO* cs = load_arg_charset(arg->getLeft());
    return cs != NULL ? cs : load_arg_charset(arg->getRight());
  }
  return load_arg_charset(arg);
}

// O-8: the value of a packed temporal MIN / MAX, decoded as
// print_temporal_packed() decodes it for printing.
void
ronsql_printer_internal::decode_temporal_packed(
    Uint64 w, ResultPrinter::TemporalDisplay temporal, int fsp,
    MYSQL_TIME* mt)
{
  using TemporalDisplay = ResultPrinter::TemporalDisplay;
  memset(mt, 0, sizeof(*mt));
  Uint8 bytes[8];
  const Uint32 flen = (1u + static_cast<Uint32>(fsp)) / 2u;
  switch (temporal)
  {
  case TemporalDisplay::DATE:
    mt->day = static_cast<unsigned int>(w & 31);
    mt->month = static_cast<unsigned int>((w >> 5) & 15);
    mt->year = static_cast<unsigned int>(w >> 9);
    mt->time_type = MYSQL_TIMESTAMP_DATE;
    return;
  case TemporalDisplay::YEAR:
    // 0 is 0000, any other byte the year - 1900.
    mt->year = (w == 0) ? 0 : static_cast<unsigned int>(w + 1900);
    mt->time_type = MYSQL_TIMESTAMP_NONE;
    return;
  case TemporalDisplay::DATETIME2:
  {
    const Uint32 n = 5u + flen;
    for (Uint32 i = 0; i < n; i++)
      bytes[i] = static_cast<Uint8>((w >> (8u * (n - 1u - i))) & 0xFFu);
    TIME_from_longlong_datetime_packed(
        mt, my_datetime_packed_from_binary(bytes, fsp));
    return;
  }
  case TemporalDisplay::TIME2:
  {
    const Uint32 n = 3u + flen;
    for (Uint32 i = 0; i < n; i++)
      bytes[i] = static_cast<Uint8>((w >> (8u * (n - 1u - i))) & 0xFFu);
    TIME_from_longlong_time_packed(mt, my_time_packed_from_binary(bytes, fsp));
    return;
  }
  case TemporalDisplay::TIMESTAMP2:
  {
    // In UTC, as it prints.
    const Uint32 n = 4u + flen;
    for (Uint32 i = 0; i < n; i++)
      bytes[i] = static_cast<Uint8>((w >> (8u * (n - 1u - i))) & 0xFFu);
    my_timeval tv{};
    my_timestamp_from_binary(&tv, bytes, fsp);
    ronsql_timestamp_tv_to_TIME(tv, mt);
    return;
  }
  case TemporalDisplay::NONE:
    break;
  }
  ronsql_bug("A temporal HAVING value has no temporal type.");
}

// O-8: a temporal value where MySQL takes a number: a YEAR is its year;
// a DATE is YYYYMMDD, a DATETIME or TIMESTAMP YYYYMMDDhhmmss and a TIME
// [-]hhmmss, with the fraction of a second.  exact: as the DECIMAL that
// MySQL computes with; otherwise as the DOUBLE that MySQL compares a
// temporal MIN / MAX with a number as (a YEAR is an integer).
static void
having_temporal_to_number(HavingValue& v, bool exact)
{
  const MYSQL_TIME& t = v.time;
  if (v.temporal == ResultPrinter::TemporalDisplay::YEAR)
  {
    longlong2decimal(static_cast<longlong>(t.year), v.exact());
    return;
  }
  if (!exact)
  {
    const double d = TIME_to_double(t);
    v.kind = HavingValue::Kind::APPROX;
    v.dbl = t.neg ? -d : d;
    return;
  }
  char text[48];
  const int len =
      snprintf(text, sizeof(text), "%s%llu.%06lu", t.neg ? "-" : "",
               static_cast<unsigned long long>(TIME_to_ulonglong(t)),
               static_cast<unsigned long>(t.second_part));
  ronsql_require_bug(len > 0 && static_cast<size_t>(len) < sizeof(text),
                     "A temporal HAVING value does not fit its number.");
  const char* end = text + len;
  ronsql_require_bug(string2decimal(text, v.exact(), &end) == E_DEC_OK,
                     "A temporal HAVING value is not a number.");
  v.clear_negative_zero();
}

static double
having_double(HavingValue& v)
{
  if (v.kind == HavingValue::Kind::APPROX)
    return v.dbl;
  double d = 0.0;
  decimal2double(v.decimal(), &d);
  return d;
}

// O-8: an operand of + - * / and unary minus.  A temporal value is its
// number, exactly, as MySQL computes with it.
static void
having_arithmetic_operand(HavingValue& v)
{
  if (v.kind == HavingValue::Kind::TEMPORAL)
  {
    having_temporal_to_number(v, true);
  }
  else if (v.kind == HavingValue::Kind::STRING)
  {
    throw RonSQLPermanentError(
        RonSQLErrorClass::UNSUPPORTED,
        "HAVING does not support arithmetic on a string.");
  }
}

void
ronsql_printer_internal::having_negate(HavingValue& v)
{
  if (v.kind == HavingValue::Kind::NULL_VALUE)
    return;
  having_arithmetic_operand(v);
  if (v.kind == HavingValue::Kind::EXACT)
  {
    if (!decimal_is_zero(v.decimal()))
      v.dec.sign = !v.dec.sign;
    return;
  }
  v.dbl = -v.dbl;
}

/*
 * O-8: + - * / as MySQL computes them: exactly, as DECIMAL, when both
 * operands are integers or DECIMALs, where / keeps four more fractional
 * digits (div_precision_increment) and truncates the quotient at a word
 * of nine digits, as my_decimal_div() does; in DOUBLE when an operand is
 * a FLOAT or DOUBLE.  Division by zero is NULL.  Not as MySQL: an integer
 * result that leaves the BIGINT range (MAX(bigint) + 1 at 2^63 - 1) fails
 * in MySQL, which computes integers as BIGINT, and is exact here.
 */
void
ronsql_printer_internal::having_arithmetic(TokenKind op, HavingValue& left,
                                           HavingValue& right,
                                           HavingValue& out)
{
  if (left.kind == HavingValue::Kind::NULL_VALUE ||
      right.kind == HavingValue::Kind::NULL_VALUE)
  {
    out.kind = HavingValue::Kind::NULL_VALUE;
    return;
  }
  having_arithmetic_operand(left);
  having_arithmetic_operand(right);
  if (left.kind == HavingValue::Kind::EXACT &&
      right.kind == HavingValue::Kind::EXACT)
  {
    int err = E_DEC_OK;
    switch (op)
    {
    case T_PLUS:
      err = decimal_add(left.decimal(), right.decimal(), out.exact());
      break;
    case T_MINUS:
      err = decimal_sub(left.decimal(), right.decimal(), out.exact());
      break;
    case T_MULTIPLY:
      err = decimal_mul(left.decimal(), right.decimal(), out.exact());
      break;
    default:
      if (decimal_is_zero(right.decimal()))
      {
        out.kind = HavingValue::Kind::NULL_VALUE;
        return;
      }
      err = decimal_div(left.decimal(), right.decimal(), out.exact(), 4);
      break;
    }
    if (err > E_DEC_TRUNCATED)
    {
      throw RonSQLPermanentError(
          RonSQLErrorClass::LIMIT,
          "A HAVING expression is out of the DECIMAL range.");
    }
    out.clear_negative_zero();
    return;
  }
  const double l = having_double(left);
  const double r = having_double(right);
  double result = 0.0;
  switch (op)
  {
  case T_PLUS:
    result = l + r;
    break;
  case T_MINUS:
    result = l - r;
    break;
  case T_MULTIPLY:
    result = l * r;
    break;
  default:
    if (r == 0)
    {
      out.kind = HavingValue::Kind::NULL_VALUE;
      return;
    }
    result = l / r;
    break;
  }
  if (std::isnan(result))
  {
    out.kind = HavingValue::Kind::NULL_VALUE;
    return;
  }
  out.kind = HavingValue::Kind::APPROX;
  out.dbl = result;
}

void
ResultPrinter::having_aggregate_value(TokenKind fun,
                                      const AggregationAPICompiler::Expr* arg,
                                      Uint32 agg_index,
                                      HavingValue& out)
{
  NdbAggregator::Result& result = m_regs_a[agg_index];
  if (result.is_null())
  {
    out.kind = HavingValue::Kind::NULL_VALUE;
    return;
  }
  switch (result.type())
  {
  case NdbDictionary::Column::Bigint:
    longlong2decimal(result.data_int64(), out.exact());
    return;
  case NdbDictionary::Column::Bigunsigned:
  {
    // A temporal MIN / MAX is its packed value, as print_aggregate_result()
    // finds it.
    int fsp = 0;
    const TemporalDisplay temporal = (fun == T_MIN || fun == T_MAX)
        ? expr_temporal(arg, fsp)
        : TemporalDisplay::NONE;
    if (temporal != TemporalDisplay::NONE)
    {
      out.kind = HavingValue::Kind::TEMPORAL;
      out.temporal = temporal;
      decode_temporal_packed(result.data_uint64(), temporal, fsp, &out.time);
      return;
    }
    ulonglong2decimal(result.data_uint64(), out.exact());
    return;
  }
  case NdbDictionary::Column::Double:
  {
    // Over an integer or DECIMAL column it is a DECIMAL in MySQL: the
    // double at the column's scale.  Beyond about 15 digits the double
    // has lost digits of the DECIMAL (Q-6).
    const int scale = (fun == T_COUNT) ? -1 : exact_arg_scale(arg);
    if (scale >= 0 &&
        double_to_decimal_at_scale(result.data_double(), scale, out.exact()))
      return;
    out.kind = HavingValue::Kind::APPROX;
    out.dbl = result.data_double();
    return;
  }
  case NdbDictionary::Column::Char:
  case NdbDictionary::Column::Varchar:
  case NdbDictionary::Column::Longvarchar:
  {
    CHARSET_INFO* cs = string_arg_charset(arg);
    require_sch(cs != nullptr,
                "Could not find charset for string aggregation result");
    Uint32 len = 0;
    const char* data = result.data_str(&len);
    if (result.type() == NdbDictionary::Column::Char)
    {
      // MySQL reads a CHAR without its trailing spaces.
      len = static_cast<Uint32>(cs->cset->lengthsp(cs, data, len));
    }
    out.kind = HavingValue::Kind::STRING;
    out.str = data;
    out.len = len;
    out.charset = cs;
    out.node = NULL;
    return;
  }
  default:
    bug("Unexpected data type in an aggregate result of HAVING.");
  }
}

void
ResultPrinter::evaluate_having_value(const ConditionalExpression* expr,
                                     HavingValue& out)
{
  switch (expr->op)
  {
  case T_SUM:
  case T_COUNT:
  case T_MIN:
  case T_MAX:
    having_aggregate_value(expr->op, expr->having_agg.arg,
                           expr->having_agg.agg_index, out);
    return;
  case T_AVG:
  {
    // SUM / COUNT, divided as MySQL divides an AVG: exactly over an
    // integer or DECIMAL (my_decimal_div() with div_precision_increment).
    HavingValue sum;
    HavingValue count;
    having_aggregate_value(T_SUM, expr->having_agg.arg,
                           expr->having_agg.agg_index, sum);
    having_aggregate_value(T_COUNT, NULL, expr->having_agg.agg_index2,
                           count);
    having_arithmetic(T_SLASH, sum, count, out);
    return;
  }
  case T_IDENTIFIER:
  {
    // The alias of a SELECT-list subquery aggregate, an AGGREGATE output
    // since RonSQLPreparer::compile(), which has its function and
    // argument.
    const Outputs* alias = NULL;
    for (Uint32 i = 0; i < m_outputs.size() && alias == NULL; i++)
    {
      const Outputs* o = m_outputs[i];
      if (o->type == Outputs::Type::AGGREGATE &&
          o->aggregate.agg_index == expr->having_agg.agg_index)
        alias = o;
    }
    having_aggregate_value(alias != NULL ? alias->aggregate.fun : T_SUM,
                           alias != NULL ? alias->aggregate.arg : NULL,
                           expr->having_agg.agg_index, out);
    return;
  }
  case T_INT:
    longlong2decimal(expr->constant_integer, out.exact());
    return;
  case T_FLOAT:
  {
    // A literal with a fraction is a DECIMAL in MySQL, and a number that
    // a subquery returned has its exact digits as its text (X-4).  A
    // subquery number without text keeps its double's digits.
    const LexString text = expr->constant_float.ls;
    const char* end = (text.str != NULL) ? text.str + text.len : NULL;
    if (text.str == NULL || text.len == 0 ||
        string2decimal(text.str, out.exact(), &end) > E_DEC_TRUNCATED ||
        end != text.str + text.len)
    {
      if (double2decimal(expr->constant_float.dbl, out.exact()) >
          E_DEC_TRUNCATED)
      {
        out.kind = HavingValue::Kind::APPROX;
        out.dbl = expr->constant_float.dbl;
        return;
      }
    }
    out.clear_negative_zero();
    return;
  }
  case T_STRING:
    // A literal (the preparer allows one only beside a MIN or MAX), or a
    // string a subquery returned.
    out.kind = HavingValue::Kind::STRING;
    out.str = expr->string.str;
    out.len = static_cast<Uint32>(expr->string.len);
    out.charset = NULL;
    out.node = expr;
    return;
  case T_NULL:
    // Also a scalar subquery's NULL.
    out.kind = HavingValue::Kind::NULL_VALUE;
    return;
  case T_PLUS:
  case T_MINUS:
  case T_MULTIPLY:
  case T_SLASH:
  {
    if (expr->op == T_MINUS && expr->args.left == NULL)
    {
      evaluate_having_value(expr->args.right, out);
      having_negate(out);
      return;
    }
    HavingValue left;
    HavingValue right;
    evaluate_having_value(expr->args.left, left);
    evaluate_having_value(expr->args.right, right);
    having_arithmetic(expr->op, left, right, out);
    return;
  }
  default:
    throw RonSQLPermanentError(
      "Unsupported expression type in HAVING clause.");
  }
}

static bool
temporal_with_date(ResultPrinter::TemporalDisplay temporal)
{
  return temporal == ResultPrinter::TemporalDisplay::DATE ||
         temporal == ResultPrinter::TemporalDisplay::DATETIME2 ||
         temporal == ResultPrinter::TemporalDisplay::TIMESTAMP2;
}

// O-8: two temporal MIN / MAX values: dates, datetimes and timestamps as
// datetimes (Arg_comparator::compare_datetime), a TIME with a TIME and a
// YEAR with a YEAR.  MySQL casts a TIME compared with a date to a
// datetime of the current day; that and a YEAR beside another type are
// refused.
static int
compare_having_temporals(const HavingValue& a, const HavingValue& b)
{
  using TemporalDisplay = ResultPrinter::TemporalDisplay;
  longlong x = 0;
  longlong y = 0;
  if (temporal_with_date(a.temporal) && temporal_with_date(b.temporal))
  {
    x = TIME_to_longlong_datetime_packed(a.time);
    y = TIME_to_longlong_datetime_packed(b.time);
  }
  else if (a.temporal == TemporalDisplay::TIME2 &&
           b.temporal == TemporalDisplay::TIME2)
  {
    x = TIME_to_longlong_time_packed(a.time);
    y = TIME_to_longlong_time_packed(b.time);
  }
  else if (a.temporal == TemporalDisplay::YEAR &&
           b.temporal == TemporalDisplay::YEAR)
  {
    x = a.time.year;
    y = b.time.year;
  }
  else
  {
    throw RonSQLPermanentError(
        RonSQLErrorClass::UNSUPPORTED,
        "HAVING compares a TIME only with a TIME, and a YEAR only with a "
        "YEAR or a number.");
  }
  return (x < y) ? -1 : (x > y) ? 1 : 0;
}

// O-8: a string compared with a DATE, DATETIME or TIMESTAMP is a datetime
// (Arg_comparator::get_date_from_const()); RonSQL takes only a valid one,
// as for a WHERE on a temporal CTE output.
static void
having_string_to_datetime(const HavingValue& s, MYSQL_TIME* mt)
{
  MYSQL_TIME_STATUS status;
  const my_time_flags_t flags = TIME_NO_ZERO_IN_DATE | TIME_NO_ZERO_DATE;
  if (str_to_datetime(s.str, s.len, mt, flags, &status) ||
      status.warnings != 0 || status.nanoseconds != 0 ||
      status.m_deprecation.m_kind !=
          MYSQL_TIME_STATUS::DEPRECATION::DP_NONE ||
      (mt->time_type != MYSQL_TIMESTAMP_DATE &&
       mt->time_type != MYSQL_TIMESTAMP_DATETIME))
  {
    throw RonSQLPermanentError(
        RonSQLErrorClass::UNSUPPORTED,
        "HAVING compares a DATE, DATETIME or TIMESTAMP only with a string "
        "that is a valid date or datetime.");
  }
}

LexString
ResultPrinter::having_constant_in_charset(const HavingValue& constant,
                                          CHARSET_INFO* cs)
{
  // A constant is UTF-8.  As RonSQLPreparer::literal_in_column_charset()
  // converts a literal compared with a column (RONSQL-13), once per
  // constant and character set.
  const LexString text(constant.str, constant.len);
  if (text.len == 0 || my_charset_same(cs, &my_charset_utf8mb4_bin))
    return text;
  if (cs->mbminlen == 1 && (cs->state & MY_CS_NONASCII) == 0)
  {
    // ASCII text has the same bytes in an ASCII-based character set.
    size_t i = 0;
    while (i < text.len && static_cast<Uint8>(text.str[i]) < 0x80) i++;
    if (i == text.len)
      return text;
  }
  for (const HavingConstant* c = m_having_constants; c != NULL; c = c->next)
  {
    if (c->node == constant.node && c->charset == cs)
      return c->text;
  }
  ronsql_require_bug(text.len <= UINT32_MAX / cs->mbmaxlen,
                     "A HAVING string constant is too long to convert.");
  const Uint32 cap = static_cast<Uint32>(text.len * cs->mbmaxlen);
  char* buf = m_amalloc->alloc_exc<char>(cap);
  uint errors = 0;
  const size_t len = my_convert(buf, cap, cs, text.str, text.len,
                                &my_charset_utf8mb4_bin, &errors);
  if (errors != 0)
  {
    throw RonSQLPermanentError(
        RonSQLErrorClass::SEMANTIC,
        std::string("HAVING compares a string constant with a MIN or MAX "
                    "in character set ") + cs->csname +
        ", which cannot hold one of its characters.");
  }
  HavingConstant* c = m_amalloc->alloc_exc<HavingConstant>(1);
  c->node = constant.node;
  c->charset = cs;
  c->text = LexString(buf, len);
  c->next = m_having_constants;
  m_having_constants = c;
  return c->text;
}

// O-8: two strings, in the collation of the MIN / MAX (which wins over a
// constant's in MySQL); MySQL refuses two different collations.
int
ResultPrinter::compare_having_strings(HavingValue& left, HavingValue& right)
{
  CHARSET_INFO* cs = (left.charset != NULL) ? left.charset : right.charset;
  if (cs == NULL ||
      (left.charset != NULL && right.charset != NULL &&
       left.charset != right.charset))
  {
    throw RonSQLPermanentError(
        RonSQLErrorClass::UNSUPPORTED,
        "HAVING compares a string only with a MIN or MAX of the same "
        "collation.");
  }
  const LexString a = (left.charset != NULL)
      ? LexString(left.str, left.len)
      : having_constant_in_charset(left, cs);
  const LexString b = (right.charset != NULL)
      ? LexString(right.str, right.len)
      : having_constant_in_charset(right, cs);
  const int cmp = cs->coll->strnncollsp(
      cs, pointer_cast<const uchar*>(a.str), a.len,
      pointer_cast<const uchar*>(b.str), b.len);
  return (cmp < 0) ? -1 : (cmp > 0) ? 1 : 0;
}

/*
 * O-8: two non-NULL HAVING values compared as MySQL compares them
 * (Arg_comparator::set_cmp_func()): two integers or DECIMALs exactly,
 * any other two numbers as doubles; strings in a collation; a DATE,
 * DATETIME or TIMESTAMP with another or with a string as datetimes; a
 * temporal value with a number as a double of its number (a YEAR as an
 * integer).  A string beside a number, a TIME or a YEAR is refused: MySQL
 * converts it with a warning.  Two strings need the printer's constants
 * in the collation of the MIN or MAX; compare_having_typed() compares
 * every other pair (T-16: ResultPrinterUnitTest).
 */
int
ResultPrinter::compare_having_values(HavingValue& left, HavingValue& right)
{
  if (left.kind == HavingValue::Kind::STRING &&
      right.kind == HavingValue::Kind::STRING)
    return compare_having_strings(left, right);
  return compare_having_typed(left, right);
}

int
ronsql_printer_internal::compare_having_typed(HavingValue& left,
                                              HavingValue& right)
{
  using Kind = HavingValue::Kind;
  if (left.kind == Kind::STRING || right.kind == Kind::STRING)
  {
    // Two strings are refused here as a string beside a number is.
    const bool string_left = (left.kind == Kind::STRING);
    const HavingValue& str = string_left ? left : right;
    const HavingValue& other = string_left ? right : left;
    if (other.kind != Kind::TEMPORAL || !temporal_with_date(other.temporal))
    {
      throw RonSQLPermanentError(
          RonSQLErrorClass::UNSUPPORTED,
          "HAVING compares a string only with the MIN or MAX of a string, "
          "DATE, DATETIME or TIMESTAMP column.");
    }
    MYSQL_TIME parsed;
    having_string_to_datetime(str, &parsed);
    const longlong x = TIME_to_longlong_datetime_packed(other.time);
    const longlong y = TIME_to_longlong_datetime_packed(parsed);
    const int cmp = (x < y) ? -1 : (x > y) ? 1 : 0;
    return string_left ? -cmp : cmp;
  }
  if (left.kind == Kind::TEMPORAL && right.kind == Kind::TEMPORAL)
    return compare_having_temporals(left, right);
  if (left.kind == Kind::TEMPORAL)
    having_temporal_to_number(left, false);
  if (right.kind == Kind::TEMPORAL)
    having_temporal_to_number(right, false);
  if (left.kind == Kind::EXACT && right.kind == Kind::EXACT)
  {
    const int cmp = decimal_cmp(left.decimal(), right.decimal());
    return (cmp < 0) ? -1 : (cmp > 0) ? 1 : 0;
  }
  const double l = having_double(left);
  const double r = having_double(right);
  return (l < r) ? -1 : (l > r) ? 1 : 0;
}

// A comparison with a NULL operand is UNKNOWN, NOT keeps UNKNOWN, and
// AND / OR follow SQL, so that e.g. a NULL MAX under `!= 5` or `NOT (> 5)`
// drops the group as in MySQL.
ResultPrinter::HavingTruth
ResultPrinter::evaluate_having(const ConditionalExpression* expr)
{
  switch (expr->op)
  {
  case T_AND:
  {
    const HavingTruth left = evaluate_having(expr->args.left);
    if (left == HAVING_FALSE)
      return HAVING_FALSE;
    const HavingTruth right = evaluate_having(expr->args.right);
    if (right == HAVING_FALSE)
      return HAVING_FALSE;
    return (left == HAVING_TRUE && right == HAVING_TRUE) ? HAVING_TRUE
                                                         : HAVING_UNKNOWN;
  }
  case T_OR:
  {
    const HavingTruth left = evaluate_having(expr->args.left);
    if (left == HAVING_TRUE)
      return HAVING_TRUE;
    const HavingTruth right = evaluate_having(expr->args.right);
    if (right == HAVING_TRUE)
      return HAVING_TRUE;
    return (left == HAVING_FALSE && right == HAVING_FALSE) ? HAVING_FALSE
                                                           : HAVING_UNKNOWN;
  }
  case T_NOT:
  case T_EXCLAMATION:
  {
    const HavingTruth arg = evaluate_having(expr->args.left);
    if (arg == HAVING_UNKNOWN)
      return HAVING_UNKNOWN;
    return (arg == HAVING_TRUE) ? HAVING_FALSE : HAVING_TRUE;
  }
  case T_IS:
  {
    // IS NULL / IS NOT NULL on aggregate result
    bool is_null_check = expr->is.null;  // true = IS NULL, false = IS NOT NULL
    const ConditionalExpression* arg = expr->is.arg;
    // The argument should be an aggregate (T_SUM, T_COUNT, etc.)
    if (arg->op == T_SUM || arg->op == T_MIN || arg->op == T_MAX ||
        arg->op == T_COUNT || arg->op == T_AVG) {
      bool result_is_null = m_regs_a[arg->having_agg.agg_index].is_null();
      return (is_null_check == result_is_null) ? HAVING_TRUE : HAVING_FALSE;
    }
    // For identifiers (output aliases), look up in stored registers
    if (arg->op == T_IDENTIFIER) {
      Uint32 agg_idx = arg->having_agg.agg_index;
      bool result_is_null = m_regs_a[agg_idx].is_null();
      return (is_null_check == result_is_null) ? HAVING_TRUE : HAVING_FALSE;
    }
    throw RonSQLPermanentError(
        "Unsupported expression in HAVING IS [NOT] NULL.");
  }
  case T_GT:
  case T_GE:
  case T_LT:
  case T_LE:
  case T_EQUALS:
  case T_NOT_EQUALS:
  {
    // O-8: compared as MySQL compares the two types.
    HavingValue left;
    HavingValue right;
    evaluate_having_value(expr->args.left, left);
    evaluate_having_value(expr->args.right, right);
    if (left.kind == HavingValue::Kind::NULL_VALUE ||
        right.kind == HavingValue::Kind::NULL_VALUE)
      return HAVING_UNKNOWN;
    const int cmp = compare_having_values(left, right);
    bool result = false;
    switch (expr->op)
    {
    case T_GT: result = cmp > 0; break;
    case T_GE: result = cmp >= 0; break;
    case T_LT: result = cmp < 0; break;
    case T_LE: result = cmp <= 0; break;
    case T_EQUALS: result = cmp == 0; break;
    default: result = cmp != 0; break;
    }
    return result ? HAVING_TRUE : HAVING_FALSE;
  }
  default:
    throw RonSQLPermanentError(
      "Unsupported operator in HAVING clause.");
  }
}

void
ResultPrinter::explain(std::basic_ostream<char>* out_stream)
{
  std::ostream& out = *out_stream;
  const char* format_description = "";
  switch(m_output_format)
  {
  case RonSQLExecParams::OutputFormat::JSON:
    format_description = "UTF-8 encoded JSON";
    break;
  case RonSQLExecParams::OutputFormat::JSON_ASCII:
    format_description = "ASCII encoded JSON";
    break;
  case RonSQLExecParams::OutputFormat::TEXT:
    format_description = "mysql-style tab separated";
    break;
  case RonSQLExecParams::OutputFormat::TEXT_NOHEADER:
    format_description = "mysql-style tab separated, header-less";
    break;
  default:
    ronsql_bug("The result printer got an unknown output format.");
  }
  // m_has_orderby is also set by HAVING alone (to buffer the groups), which
  // leaves nothing to sort by.
  if (m_has_orderby && m_orderby_specs.size() > 0)
  {
    out << "Result sorted by ";
    DynamicArray<LexCString>& column_names = *m_column_names;
    for (Uint32 i = 0; i < m_orderby_specs.size(); i++)
    {
      if (i > 0) out << ", ";
      OrderbySpec& spec = m_orderby_specs[i];
      if (spec.kind == OrderbySpec::Kind::GROUPBY_COL)
      {
        Uint32 col_idx = m_groupby_cols[spec.groupby_idx];
        out << quoted_identifier(column_names[col_idx]);
      }
      else
      {
        // AGGREGATE or AVG: the union holds agg_result_idx or avg_key_idx,
        // not groupby_idx.  Print the SELECT output alias that produced
        // this aggregate result when we can find it, else the result index.
        const bool is_avg = spec.kind == OrderbySpec::Kind::AVG;
        const Uint32 agg_idx = is_avg ? spec.avg_sum_idx : spec.agg_result_idx;
        const char* alias_str = NULL;
        size_t alias_len = 0;
        for (const Outputs* o = m_query->outputs; o != NULL; o = o->next)
        {
          if ((!is_avg && o->type == Outputs::Type::AGGREGATE &&
               o->aggregate.agg_index == agg_idx) ||
              (is_avg && o->type == Outputs::Type::AVG &&
               o->avg.agg_index_sum == agg_idx))
          {
            alias_str = o->output_name.str;
            alias_len = o->output_name.len;
            break;
          }
        }
        if (alias_str != NULL)
          out << quoted_identifier(LexCString(alias_str, alias_len));
        else
          out << "aggregate result " << agg_idx;
      }
      out << (spec.ascending ? " ASC" : " DESC");
    }
    out << ".\n";
  }
  if (m_query->limit >= 0)
  {
    out << "Result limited to " << m_query->limit << " row"
        << (m_query->limit == 1 ? "" : "s") << ".\n";
  }
  out << "Output in " << format_description << " format.\n"
      << "The program for post-processing and output has " << m_program.size()
      << " instructions.\n";
}
