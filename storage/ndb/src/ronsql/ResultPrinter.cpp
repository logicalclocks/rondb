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
#include "RonSQLParser.y.hpp"
#include "define_formatter.hpp"
#include "RonSQLPreparer.hpp"
#include "mysql/strings/dtoa.h"
#include <sql_string.h>
#include <decimal_utils.hpp>
#include "my_time.h"

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

#define feature_not_implemented(description) \
  throw RonSQLPermanentError("RonSQL feature not implemented: " description)
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

static void print_string(std::ostream& output_stream,
                         LexString ls,
                         CHARSET_INFO* charset,
                         bool json_escape,
                         bool utf8_output,
                         bool trim_space_suffix);
static double convert_result_to_double(NdbAggregator::Result result);

// require or investigate schema version
static inline void
require_sch(bool condition, const char* msg)
{
  if (likely(condition)) return;
  throw RonSQLMaybeStaleSchema(msg);
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
    abort();
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
      else if (out->type == Outputs::Type::AGGREGATE ||
               out->type == Outputs::Type::AVG ||
               out->type == Outputs::Type::SUBQUERY_AGG)
      {
        // Aggregate alias — sort by aggregate result
        Uint32 agg_idx = (out->type == Outputs::Type::AVG)
                             ? out->avg.agg_index_sum
                             : (out->type == Outputs::Type::SUBQUERY_AGG)
                                   ? out->subquery_agg.agg_index
                                   : out->aggregate.agg_index;
        OrderbySpec spec;
        spec.kind = OrderbySpec::Kind::AGGREGATE;
        spec.agg_result_idx = agg_idx;
        spec.ascending = ob->ascending;
        spec.charset = out->type == Outputs::Type::AGGREGATE
            ? aggregate_arg_charset(out)
            : NULL;
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
        abort();
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
        abort();
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
        ndbrequire(precision <= 65);
        ndbrequire(scale <= 30);
        ndbrequire(scale <= precision);
        Cmd cmd;
        cmd.type = Cmd::Type::PRINT_GROUP_BY_COLUMN;
        cmd.print_group_by_column.reg_g = m_col_idx_groupby_map[o->column.col_idx];
        cmd.print_group_by_column.charset = charset;
        cmd.print_group_by_column.precision = precision;
        cmd.print_group_by_column.scale = scale;
        m_program.push(cmd);
        break;
      }
      case Outputs::Type::AGGREGATE:
      {
        Cmd cmd;
        cmd.type = Cmd::Type::PRINT_AGGREGATE;
        cmd.print_aggregate.reg_a = o->aggregate.agg_index;
        cmd.print_aggregate.charset = aggregate_arg_charset(o);
        {
          // D15: only format with fixed scale when the source DECIMAL is within
          // DOUBLE's exact range (precision <= 15); wider DECIMALs keep compact
          // formatting (the value is already a lossy DOUBLE).
          int sc = aggregate_arg_scale(o);
          int pr = aggregate_arg_precision(o);
          cmd.print_aggregate.scale = (pr > 0 && pr <= 15) ? sc : 0;
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
        m_program.push(cmd);
        break;
      }
    default:
      abort();
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
    abort();
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
      abort();
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
          out << column.data_uint64();
          break;
        case NdbDictionary::Column::Type::Float:
          print_float_or_double(out, column.data_float());
          break;
        case NdbDictionary::Column::Type::Double:
          print_float_or_double(out, column.data_double());
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
            decimal_bin2str((const void*)column.data(),
                            column.byte_size(),
                            precision,
                            scale,
                            decStr,
                            DECIMAL_MAX_STR_LEN_IN_BYTES);
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
              abort();
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
          feature_not_implemented("Print GROUP BY column of type Binary");
        case NdbDictionary::Column::Type::Varbinary:
          feature_not_implemented("Print GROUP BY column of type Varbinary");
        case NdbDictionary::Column::Type::Longvarbinary:
          feature_not_implemented("Print GROUP BY column of type Longvarbinary");
        case NdbDictionary::Column::Type::Datetime:
          feature_not_implemented("Print GROUP BY column of type Datetime");
        case NdbDictionary::Column::Type::Date:
          {
            Uint32 date = column.data_uint32();
            Uint32 year = date >> 9;
            Uint32 month = (date >> 5) & 0xf;
            Uint32 day = date & 0x1f;
            out << m_quote << year << "-" << d2(month) << "-" << d2(day) << m_quote;
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
          feature_not_implemented("Print GROUP BY column of type Year");
        case NdbDictionary::Column::Type::Timestamp:
          feature_not_implemented("Print GROUP BY column of type Timestamp");
        case NdbDictionary::Column::Type::Time2:
          feature_not_implemented("Print GROUP BY column of type Time2");
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
            // Lock-free UTC epoch -> MYSQL_TIME (no glibc tzset_lock).
            MYSQL_TIME lTime;
            ronsql_utc_sec_to_TIME((time_t)myTV.m_tv_sec, &lTime);
            lTime.second_part = myTV.m_tv_usec;
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
                               cmd.print_aggregate.temporal,
                               cmd.print_aggregate.temporal_fsp);
      }
      break;
    case Cmd::Type::PRINT_AVG:
      {
        NdbAggregator::Result result_sum = m_regs_a[cmd.print_avg.reg_a_sum];
        NdbAggregator::Result result_count = m_regs_a[cmd.print_avg.reg_a_count];
        if (result_sum.is_null() &&
            !result_count.is_null() &&
            result_count.type() == NdbDictionary::Column::Type::Bigunsigned &&
            result_count.data_uint64() == 0) {
          out << m_null_representation;
        } else {
          double numerator = convert_result_to_double(result_sum);
          double denominator = convert_result_to_double(result_count);
          double result = numerator / denominator;
          char buffer[FLOATING_POINT_BUFFER];
          bool error;
          my_fcvt(result, 4, buffer, &error);
          if (error)
            out << m_null_representation;
          else
            out << buffer;
        }
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
      abort();
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
          std::string raw_a;
          std::string raw_b;
          if (prefix != 0)
          {
            raw_a.resize(prefix + len_a);
            raw_b.resize(prefix + len_b);
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
            data_a = raw_a.data();
            data_b = raw_b.data();
          }
          const NdbSqlUtil::Type& sqlType =
              NdbSqlUtil::getType(static_cast<Uint32>(res_a.type()));
          cmp = (*sqlType.m_cmp)(spec.charset,
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
    else
    {
      NdbAggregator::Column& col_a = a.cols[spec.groupby_idx];
      NdbAggregator::Column& col_b = b.cols[spec.groupby_idx];
      // NULL handling: NULLs sort first (smallest) in ASC, matching MySQL
      if (col_a.is_null() && col_b.is_null()) continue;
      if (col_a.is_null()) return spec.ascending ? -1 : 1;
      if (col_b.is_null()) return spec.ascending ? 1 : -1;
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
    // Cross-table filter semantics: suppress groups where no rows passed
    // the filter (sentinel COUNT == 0).
    if (m_query->sentinel_agg_slot >= 0) {
      NdbAggregator::Result sentinel =
          row.results[m_query->sentinel_agg_slot];
      if (sentinel.data_int64() == 0) continue;
    }
    if (m_query->having_expression != NULL &&
        !evaluate_having(m_query->having_expression))
    {
      continue;
    }
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
  // Sort using ORDER BY comparator
  std::sort(sort_array, sort_array + num_rows,
    [this](StoredRow& a, StoredRow& b) -> bool {
      return compare_rows(a, b) < 0;
    });
  // Apply LIMIT
  Uint32 print_count = num_rows;
  if (m_query->limit >= 0 && (Int64)print_count > m_query->limit)
  {
    print_count = (Uint32)m_query->limit;
  }
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
    if (m_tsv_headers)
    {
      bool first_column = true;
      for (Uint32 i = 0; i < m_outputs.size(); i++)
      {
        Outputs* o = m_outputs[i];
        if (first_column) first_column = false; else out << '\t';
        out << o->output_name;
      }
      out << '\n';
    }
    for (Uint32 i = 0; i < print_count; i++)
    {
      print_stored_record(sort_array[i], out);
    }
  }
  else
  {
    DEB_TRACE();
    abort();
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
    abort();
  }
}

// Phase 0b: decode a packed temporal value into its MySQL text form.
// The packed Uint64 is the kernel's Bigunsigned MIN/MAX representation
// — the column's native bytes (little-endian for DATE/YEAR, big-endian
// for DATETIME2/TIME2/TIMESTAMP2) loaded into a register — which is
// also what a raw column re-packs to (see print_passthrough_value).
// `quote` wraps the text ("" for TSV and aggregate results, "\"" for
// JSON pass-through output).
static void
print_temporal_packed(std::ostream& out, Uint64 w,
                      ResultPrinter::TemporalDisplay temporal, int fsp,
                      const char* quote)
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
    ronsql_utc_sec_to_TIME((time_t)tv.m_tv_sec, &mt);
    mt.second_part = (unsigned long)tv.m_tv_usec;
    my_TIME_to_str(mt, buf, fsp);
    break;
  }
  case TemporalDisplay::NONE:
    break; // unreachable (callers guard)
  }
  out << quote << buf << quote;
}

// Phase E.3 + I.12 helpers: format a single NdbRecAttr value for the
// projection-only pass-through path.  Mirrors the type cases the
// aggregator path handles in print_record (see Column::data_*) but
// reads directly from NdbRecAttr instead of NdbAggregator::Column.
// Phase E.3's original supported set was integer, float, double — the
// types build_cte_virtual_tables emits from SUM/COUNT/MIN/MAX.  Phase
// I.12 extends the set to CHAR / VARCHAR / Longvarchar so projection-
// only queries that select a real-table string column alongside CTE
// columns (testCteNdbApiOuterJoin.cpp Test 1 etc.) work.
void
ResultPrinter::print_passthrough_value(std::ostream& out,
                                       const NdbRecAttr* attr,
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
    print_float_or_double(out, (double)attr->float_value()); break;
  case NdbDictionary::Column::Double:
    {
      // D15: a DECIMAL-derived value carried as DOUBLE prints with its source
      // scale (set on the virt column via setScale) to match MySQL — but only
      // within DOUBLE's exact range (precision <= 15).  True DOUBLE columns
      // have scale 0; wider DECIMALs keep the compact formatting.
      const NdbDictionary::Column* col = attr->getColumn();
      int sc = (col != nullptr) ? col->getScale() : 0;
      int pr = (col != nullptr) ? col->getPrecision() : 0;
      if (sc > 0 && pr > 0 && pr <= 15) {
        char buf[FLOATING_POINT_BUFFER];
        snprintf(buf, sizeof(buf), "%.*f", sc, attr->double_value());
        out << buf;
      } else {
        print_float_or_double(out, attr->double_value());
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
        abort();
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
      decimal_bin2str((const void*)attr->aRef(),
                      attr->get_size_in_bytes(),
                      col->getPrecision(),
                      col->getScale(),
                      decStr,
                      DECIMAL_MAX_STR_LEN_IN_BYTES);
      out << decStr;
      break;
    }
  default:
    // Old temporal formats (pre-5.6 Datetime/Time/Timestamp),
    // Olddecimal, BIT, BINARY/VARBINARY and BLOB/TEXT are not
    // supported in pass-through results.
    *m_err << "Unsupported column type (" << (int)t
           << ") in pass-through result." << endl;
    throw RonSQLPermanentError(
        "Unsupported column type in pass-through result.");
  }
}

void
ResultPrinter::print_passthrough_header(const NdbRecAttr* const* /*attrs*/,
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
ResultPrinter::print_passthrough_row(const NdbRecAttr* const* attrs,
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
      out << '"' << o->output_name << "\":";
      // Numbers print without quotes; strings and temporals quote
      // themselves (m_quote) inside print_passthrough_value.
      print_passthrough_value(out, attrs[i],
                              passthrough_column_metadata(o));
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
      print_passthrough_value(out, attrs[i],
                              passthrough_column_metadata(o));
      o = o->next;
    }
    out << '\n';
  }
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
ResultPrinter::print_result(NdbAggregator* aggregator,
                            std::basic_ostream<char>* out_stream)
{
  DEB_TRACE();
  assert(out_stream != NULL);
  // Force buffered path when sentinel filtering is needed (cross-table
  // BranchReg filters) because streaming can't skip already-started records.
  if (m_has_orderby || m_query->sentinel_agg_slot >= 0)
  {
    print_result_ordered(aggregator, out_stream);
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
      if (!headers_printed && m_tsv_headers)
      {
        DEB_TRACE();
        // Print the column names.
        bool first_column = true;
        for (Uint32 i = 0; i < m_outputs.size(); i++)
        {
          Outputs* o = m_outputs[i];
          if (first_column) first_column = false; else out << '\t';
          out << o->output_name;
        }
        out << '\n';
        headers_printed = true;
      }
      if (m_query->limit >= 0 && row_count >= m_query->limit)
      {
        break;
      }
      print_record(record, out);
      row_count++;
    }
    DEB_TRACE();
  }
  else
  {
    DEB_TRACE();
    abort();
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
          out << column.data_uint64();
          break;
        case NdbDictionary::Column::Type::Float:         ///< 32-bit float. 4 bytes float
          print_float_or_double(out, column.data_float());
          break;
        case NdbDictionary::Column::Type::Double:        ///< 64-bit float. 8 byte float
          print_float_or_double(out, column.data_double());
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
            decimal_bin2str((const void*)column.data(),
                            column.byte_size(),
                            precision,
                            scale,
                            decStr,
                            DECIMAL_MAX_STR_LEN_IN_BYTES);
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
              abort();
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
          feature_not_implemented("Print GROUP BY column of type Binary");
        case NdbDictionary::Column::Type::Varbinary:     ///< Length bytes: 1, Max: 255
          feature_not_implemented("Print GROUP BY column of type Varbinary");
        case NdbDictionary::Column::Type::Longvarbinary: ///< Length bytes: 2, little-endian
          feature_not_implemented("Print GROUP BY column of type Longvarbinary");
        case NdbDictionary::Column::Type::Datetime:      ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
          feature_not_implemented("Print GROUP BY column of type Datetime");
        case NdbDictionary::Column::Type::Date:          ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
          {
            Uint32 date = column.data_uint32();
            Uint32 year = date >> 9;
            Uint32 month = (date >> 5) & 0xf;
            Uint32 day = date & 0x1f;
            out << m_quote << year << "-" << d2(month) << "-" << d2(day) << m_quote;
            // todo There must be a function somewhere that does this, but I can't find it. Maybe in my_time.cc.
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
          feature_not_implemented("Print GROUP BY column of type Year");
        case NdbDictionary::Column::Type::Timestamp:     ///< Unix time
          feature_not_implemented("Print GROUP BY column of type Timestamp");
        case NdbDictionary::Column::Type::Time2:         ///< 3 bytes + 0-3 fraction
          feature_not_implemented("Print GROUP BY column of type Time2");
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
            // Lock-free UTC epoch -> MYSQL_TIME (no glibc tzset_lock).
            MYSQL_TIME lTime;
            ronsql_utc_sec_to_TIME((time_t)myTV.m_tv_sec, &lTime);
            lTime.second_part = myTV.m_tv_usec;
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
                               cmd.print_aggregate.temporal,
                               cmd.print_aggregate.temporal_fsp);
      }
      break;
    case Cmd::Type::PRINT_AVG:
      {
        NdbAggregator::Result result_sum = m_regs_a[cmd.print_avg.reg_a_sum];
        NdbAggregator::Result result_count = m_regs_a[cmd.print_avg.reg_a_count];
        if (result_sum.is_null() &&
            !result_count.is_null() &&
            result_count.type() == NdbDictionary::Column::Type::Bigunsigned &&
            result_count.data_uint64() == 0) {
          out << m_null_representation;
        } else {
          double numerator = convert_result_to_double(result_sum);
          double denominator = convert_result_to_double(result_count);
          double result = numerator / denominator;
          char buffer[FLOATING_POINT_BUFFER];
          bool error;
          my_fcvt(result, 4, buffer, &error);
          if (error)
            out << m_null_representation;
          else
            out << buffer;
        }
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
      abort();
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
static void
print_string(std::ostream& out,
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

inline void
ResultPrinter::print_float_or_double(std::ostream& out, double value)
{
  char buffer[FLOATING_POINT_BUFFER];
  bool error;
  size_t len = my_fcvt_compact(value, buffer, &error);
  if (error)
  {
    // value is Inf, -Inf or NaN.
    out << m_null_representation;
    return;
  }
  ndbrequire(len > 0 && buffer[len] == 0);
  out << buffer;
  return;
}

CHARSET_INFO*
ResultPrinter::aggregate_arg_charset(const Outputs* out) const
{
  if (out == NULL || out->type != Outputs::Type::AGGREGATE)
    return NULL;
  if (out->aggregate.fun != T_MIN && out->aggregate.fun != T_MAX)
    return NULL;
  AggregationAPICompiler::Expr* arg = out->aggregate.arg;
  if (arg == NULL || !arg->isLoad())
    return NULL;
  Uint32 col_idx = arg->getLoadIdx();
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

// Source DECIMAL precision of a MIN/MAX aggregate's argument column.  Used to
// gate scale-formatting to the DOUBLE-exact range (precision <= 15): wider
// DECIMALs are unrepresentable as DOUBLE, so fixed-scale formatting would
// expose mantissa noise — those keep the compact my_fcvt_compact form.
int
ResultPrinter::aggregate_arg_precision(const Outputs* out) const
{
  if (out == NULL || out->type != Outputs::Type::AGGREGATE)
    return 0;
  // D15 (MIN/MAX) + D1 (SUM): see aggregate_arg_scale.  The precision gate
  // keeps fixed-scale formatting within the DOUBLE-exact range (<= 15).
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
  AggregationAPICompiler::Expr* arg = out->aggregate.arg;
  if (arg == NULL || !arg->isLoad())
    return TemporalDisplay::NONE;
  Uint32 col_idx = arg->getLoadIdx();
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
    print_temporal_packed(out, result.data_uint64(), temporal,
                          temporal_fsp, "");
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
      // with fixed scale so the output matches; true DOUBLE/FLOAT results
      // pass scale == 0 and keep the compact my_fcvt_compact formatting.
      char buf[FLOATING_POINT_BUFFER];
      snprintf(buf, sizeof(buf), "%.*f", scale, result.data_double());
      out << buf;
    }
    else
    {
      print_float_or_double(out, result.data_double());
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
  case T_IDENTIFIER:
    // Output alias mapped to agg_index during compile()
    max_idx = max(max_idx, expr->having_agg.agg_index + 1);
    break;
  default:
    break;
  }
}

double
ResultPrinter::evaluate_having_value(const ConditionalExpression* expr)
{
  switch (expr->op)
  {
  case T_SUM:
  case T_COUNT:
  case T_MIN:
  case T_MAX:
    return convert_result_to_double(m_regs_a[expr->having_agg.agg_index]);
  case T_AVG:
  {
    double numerator =
      convert_result_to_double(m_regs_a[expr->having_agg.agg_index]);
    double denominator =
      convert_result_to_double(m_regs_a[expr->having_agg.agg_index2]);
    return numerator / denominator;
  }
  case T_INT:
    return static_cast<double>(expr->constant_integer);
  case T_FLOAT:
    return expr->constant_float.dbl;
  case T_IDENTIFIER:
    // Output alias mapped to agg_index — same as aggregate result
    return convert_result_to_double(m_regs_a[expr->having_agg.agg_index]);
  case T_PLUS:
    return evaluate_having_value(expr->args.left) +
           evaluate_having_value(expr->args.right);
  case T_MINUS:
    if (expr->args.left == NULL)
      return -evaluate_having_value(expr->args.right);
    return evaluate_having_value(expr->args.left) -
           evaluate_having_value(expr->args.right);
  case T_MULTIPLY:
    return evaluate_having_value(expr->args.left) *
           evaluate_having_value(expr->args.right);
  case T_SLASH:
    return evaluate_having_value(expr->args.left) /
           evaluate_having_value(expr->args.right);
  default:
    throw RonSQLPermanentError(
      "Unsupported expression type in HAVING clause.");
  }
}

bool
ResultPrinter::evaluate_having(const ConditionalExpression* expr)
{
  switch (expr->op)
  {
  case T_AND:
    return evaluate_having(expr->args.left) &&
           evaluate_having(expr->args.right);
  case T_OR:
    return evaluate_having(expr->args.left) ||
           evaluate_having(expr->args.right);
  case T_NOT:
  case T_EXCLAMATION:
    return !evaluate_having(expr->args.left);
  case T_IS:
  {
    // IS NULL / IS NOT NULL on aggregate result
    bool is_null_check = expr->is.null;  // true = IS NULL, false = IS NOT NULL
    const ConditionalExpression* arg = expr->is.arg;
    // The argument should be an aggregate (T_SUM, T_COUNT, etc.)
    if (arg->op == T_SUM || arg->op == T_MIN || arg->op == T_MAX ||
        arg->op == T_COUNT || arg->op == T_AVG) {
      bool result_is_null = m_regs_a[arg->having_agg.agg_index].is_null();
      return is_null_check ? result_is_null : !result_is_null;
    }
    // For identifiers (output aliases), look up in stored registers
    if (arg->op == T_IDENTIFIER) {
      Uint32 agg_idx = arg->having_agg.agg_index;
      bool result_is_null = m_regs_a[agg_idx].is_null();
      return is_null_check ? result_is_null : !result_is_null;
    }
    throw RonSQLPermanentError(
        "Unsupported expression in HAVING IS [NOT] NULL.");
  }
  case T_GT:
    return evaluate_having_value(expr->args.left) >
           evaluate_having_value(expr->args.right);
  case T_GE:
    return evaluate_having_value(expr->args.left) >=
           evaluate_having_value(expr->args.right);
  case T_LT:
    return evaluate_having_value(expr->args.left) <
           evaluate_having_value(expr->args.right);
  case T_LE:
    return evaluate_having_value(expr->args.left) <=
           evaluate_having_value(expr->args.right);
  case T_EQUALS:
    return evaluate_having_value(expr->args.left) ==
           evaluate_having_value(expr->args.right);
  case T_NOT_EQUALS:
    return evaluate_having_value(expr->args.left) !=
           evaluate_having_value(expr->args.right);
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
    abort();
  }
  if (m_has_orderby)
  {
    out << "Result sorted by ";
    DynamicArray<LexCString>& column_names = *m_column_names;
    for (Uint32 i = 0; i < m_orderby_specs.size(); i++)
    {
      if (i > 0) out << ", ";
      OrderbySpec& spec = m_orderby_specs[i];
      Uint32 col_idx = m_groupby_cols[spec.groupby_idx];
      out << quoted_identifier(column_names[col_idx])
          << (spec.ascending ? " ASC" : " DESC");
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
