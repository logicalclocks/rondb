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

#include <iomanip>
#include "m_string.h"
#include "ResultPrinter.hpp"
#include "define_formatter.hpp"
#include "RonSQLPreparer.hpp"
#include "mysql/strings/dtoa.h"
#include <sql_string.h>

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
using std::runtime_error;

#define feature_not_implemented(description) \
  throw runtime_error("RonSQL feature not implemented: " description)

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

static inline void
soft_assert(bool condition, const char* msg)
{
  if (likely(condition)) return;
  throw runtime_error(msg);
}

ResultPrinter::ResultPrinter(ArenaMalloc* amalloc,
                             struct SelectStatement* query,
                             DynamicArray<LexCString>* column_names,
                             CHARSET_INFO** column_charset_map,
                             RonSQLExecParams::OutputFormat output_format,
                             std::basic_ostream<char>* err):
  m_amalloc(amalloc),
  m_query(query),
  m_column_names(column_names),
  m_column_charset_map(column_charset_map),
  m_output_format(output_format),
  m_err(err),
  m_program(amalloc),
  m_groupby_cols(amalloc),
  m_outputs(amalloc),
  m_col_idx_groupby_map(amalloc)
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
            throw runtime_error("Ungrouped column in non-aggregated SELECT expression.");
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
  switch (m_output_format)
  {
  case RonSQLExecParams::OutputFormat::TEXT:
    m_json_output = false;
    m_utf8_output = true;
    m_tsv_output = true;
    m_tsv_headers = true;
    m_null_representation = LexString{"NULL", 4};
    break;
  case RonSQLExecParams::OutputFormat::TEXT_NOHEADER:
    m_json_output = false;
    m_utf8_output = true;
    m_tsv_output = true;
    m_tsv_headers = false;
    m_null_representation = LexString{"NULL", 4};
    break;
  case RonSQLExecParams::OutputFormat::JSON:
    m_json_output = true;
    m_utf8_output = true;
    m_tsv_output = false;
    m_tsv_headers = false;
    m_null_representation = LexString{"null", 4};
    break;
  case RonSQLExecParams::OutputFormat::JSON_ASCII:
    m_json_output = true;
    m_utf8_output = false;
    m_tsv_output = false;
    m_tsv_headers = false;
    m_null_representation = LexString{"null", 4};
    break;
  default:
    abort();
  }
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
        Cmd cmd;
        cmd.type = Cmd::Type::PRINT_GROUP_BY_COLUMN;
        cmd.print_group_by_column.reg_g = m_col_idx_groupby_map[o->column.col_idx];
        // During EXPLAIN SELECT without access to ndb we still need to compile,
        // and then charset will always be NULL. That's ok, because it won't be
        // used.
        cmd.print_group_by_column.charset =
          m_column_charset_map != NULL
          ? m_column_charset_map[o->column.col_idx]
          : NULL;
        m_program.push(cmd);
        break;
      }
      case Outputs::Type::AGGREGATE:
      {
        Cmd cmd;
        cmd.type = Cmd::Type::PRINT_AGGREGATE;
        cmd.print_aggregate.reg_a = o->aggregate.agg_index;
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

void
ResultPrinter::print_result(NdbAggregator* aggregator,
                            std::basic_ostream<char>* out_stream)
{
  DEB_TRACE();
  assert(out_stream != NULL);
  std::ostream& out = *out_stream;
  if (m_json_output)
  {
    DEB_TRACE();
    out << '[';
    bool first_record = true;
    for (NdbAggregator::ResultRecord record = aggregator->FetchResultRecord();
         !record.end();
         record = aggregator->FetchResultRecord())
    {
      DEB_TRACE();
      if (first_record)
      {
        first_record = false;
      }
      else
      {
        out << ',';
      }
      print_record(record, out);
    }
    DEB_TRACE();
    out << "]\n";
  }
  else if (m_tsv_output)
  {
    DEB_TRACE();
    bool first_record = true;
    for (NdbAggregator::ResultRecord record = aggregator->FetchResultRecord();
         !record.end();
         record = aggregator->FetchResultRecord())
    {
      DEB_TRACE();
      if (first_record && m_tsv_headers)
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
        first_record = false;
      }
      print_record(record, out);
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

DEFINE_FORMATTER(d2, uint, {
  if (value < 10) os << '0';
  os << value;
})

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
          throw std::runtime_error("Got record with fewer GROUP BY columns than expected.");
        }
        m_regs_g[cmd.store_group_by_column.reg_g] = column;
      }
      break;
    case Cmd::Type::END_OF_GROUP_BY_COLUMNS:
      {
        NdbAggregator::Column column = record.FetchGroupbyColumn();
        if (!column.end())
        {
          throw std::runtime_error("Got record with more GROUP BY columns than expected.");
        }
      }
      break;
    case Cmd::Type::STORE_AGGREGATE:
      {
        NdbAggregator::Result result = record.FetchAggregationResult();
        if (result.end())
        {
          throw std::runtime_error("Got record with fewer aggregates than expected.");
        }
        m_regs_a[cmd.store_aggregate.reg_a] = result;
      }
      break;
    case Cmd::Type::END_OF_AGGREGATES:
      {
        NdbAggregator::Result result = record.FetchAggregationResult();
        if (!result.end())
        {
          throw std::runtime_error("Got record with more aggregates than expected.");
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
          feature_not_implemented("Print GROUP BY column of type Float");
        case NdbDictionary::Column::Type::Double:        ///< 64-bit float. 8 byte float
          feature_not_implemented("Print GROUP BY column of type Double");
        case NdbDictionary::Column::Type::Olddecimal:    ///< MySQL < 5.0 signed decimal,  Precision, Scale
          feature_not_implemented("Print GROUP BY column of type Olddecimal");
        case NdbDictionary::Column::Type::Olddecimalunsigned:
          feature_not_implemented("Print GROUP BY column of type Olddecimalunsigned");
        case NdbDictionary::Column::Type::Decimal:       ///< MySQL >= 5.0 signed decimal,  Precision, Scale
          feature_not_implemented("Print GROUP BY column of type Decimal");
        case NdbDictionary::Column::Type::Decimalunsigned:
          feature_not_implemented("Print GROUP BY column of type Decimalunsigned");
        case NdbDictionary::Column::Type::Char:          ///< Len. A fixed array of 1-byte chars
          {
            CHARSET_INFO* charset = cmd.print_group_by_column.charset;
            soft_assert(charset != nullptr, "Could not find charset for CHAR column");
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
          {
            CHARSET_INFO* charset = cmd.print_group_by_column.charset;
            soft_assert(charset != nullptr, "Could not find charset for VARCHAR column");
            LexString content = LexString{ &column.data()[1],
                                           (size_t)column.data()[0] };
            if (m_json_output)
            {
              out << '"';
              print_string(out, content, charset, true, m_utf8_output, false);
              out << '"';
            }
            else if (m_tsv_output)
            {
              print_string(out, content, charset, false, true, false);
            }
            else
            {
              abort();
            }
            break;
          }
        case NdbDictionary::Column::Type::Binary:        ///< Len
          feature_not_implemented("Print GROUP BY column of type Binary");
        case NdbDictionary::Column::Type::Varbinary:     ///< Length bytes: 1, Max: 255
          feature_not_implemented("Print GROUP BY column of type Varbinary");
        case NdbDictionary::Column::Type::Datetime:      ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
          feature_not_implemented("Print GROUP BY column of type Datetime");
        case NdbDictionary::Column::Type::Date:          ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
          {
            Uint32 date = column.data_uint32();
            Uint32 year = date >> 9;
            Uint32 month = (date >> 5) & 0xf;
            Uint32 day = date & 0x1f;
            out << year << "-" << d2(month) << "-" << d2(day);
            // todo There must be a function somewhere that does this, but I can't find it. Maybe in my_time.cc.
            break;
          }
        case NdbDictionary::Column::Type::Blob:          ///< Binary large object (see NdbBlob)
          feature_not_implemented("Print GROUP BY column of type Blob");
        case NdbDictionary::Column::Type::Text:          ///< Text blob
          feature_not_implemented("Print GROUP BY column of type Text");
        case NdbDictionary::Column::Type::Bit:           ///< Bit, length specifies no of bits
          feature_not_implemented("Print GROUP BY column of type Bit");
        case NdbDictionary::Column::Type::Longvarchar:   ///< Length bytes: 2, little-endian
          feature_not_implemented("Print GROUP BY column of type Longvarchar");
        case NdbDictionary::Column::Type::Longvarbinary: ///< Length bytes: 2, little-endian
          feature_not_implemented("Print GROUP BY column of type Longvarbinary");
        case NdbDictionary::Column::Type::Time:          ///< Time without date
          feature_not_implemented("Print GROUP BY column of type Time");
        case NdbDictionary::Column::Type::Year:          ///< Year 1901-2155 (1 byte)
          feature_not_implemented("Print GROUP BY column of type Year");
        case NdbDictionary::Column::Type::Timestamp:     ///< Unix time
          feature_not_implemented("Print GROUP BY column of type Timestamp");
        case NdbDictionary::Column::Type::Time2:         ///< 3 bytes + 0-3 fraction
          feature_not_implemented("Print GROUP BY column of type Time2");
        case NdbDictionary::Column::Type::Datetime2:     ///< 5 bytes plus 0-3 fraction
          feature_not_implemented("Print GROUP BY column of type Datetime2");
        case NdbDictionary::Column::Type::Timestamp2:    ///< 4 bytes + 0-3 fraction
          feature_not_implemented("Print GROUP BY column of type Timestamp2");
        default:
          abort(); // Unknown type
        }
      }
      break;
    case Cmd::Type::PRINT_AGGREGATE:
      {
        NdbAggregator::Result result = m_regs_a[cmd.print_aggregate.reg_a];
        if(result.is_null())
        {
          out << m_null_representation;
          break;
        }
        // todo conform format for sum(int) to mysql CLI
        switch (result.type())
        {
        case NdbDictionary::Column::Bigint:
          out << result.data_int64();
          break;
        case NdbDictionary::Column::Bigunsigned:
          out << result.data_uint64();
          break;
        case NdbDictionary::Column::Double:
          print_float_or_double(out, result.data_double(), true);
          break;
        case NdbDictionary::Column::Undefined:
          abort();
          break;
        default:
          abort();
        }
      }
      break;
    case Cmd::Type::PRINT_AVG:
      {
        NdbAggregator::Result result_sum = m_regs_a[cmd.print_avg.reg_a_sum];
        NdbAggregator::Result result_count = m_regs_a[cmd.print_avg.reg_a_count];
        // todo this must be tested thoroughly against MySQL.
        double numerator = convert_result_to_double(result_sum);
        double denominator = convert_result_to_double(result_count);
        double result = numerator / denominator;
        print_float_or_double(out, result, true);
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
      out << "�"; // U+fffd
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
      out << "�"; // U+fffd
    }
  }
}

inline void
ResultPrinter::print_float_or_double(std::ostream& out,
                                     double value,
                                     bool is_double)
{
  // todo perhaps do not evaluate this branch every time
  if (m_json_output && is_double) {
   if(is_double)
   {
     out << std::fixed << std::setprecision(6) << value;
   }
   else
   {
     abort(); // todo test the following
     out << std::fixed << std::setprecision(6) << static_cast<float>(value);
   }
   return;
  }
  if (m_tsv_output)
  {
    char buffer[129];
    bool error;
    size_t len = my_gcvt(value,
                         is_double ? MY_GCVT_ARG_DOUBLE : MY_GCVT_ARG_FLOAT,
                         128, buffer, &error);
    (void) len;
    if (error)
    {
      // value is Inf, -Inf or NaN.
      out << m_null_representation;
      return;
    }
    assert(len > 0 && buffer[len] ==0);
    out << buffer;
    return;
  }
  abort();
}

inline static double
convert_result_to_double(NdbAggregator::Result result)
{
  switch (result.type())
  {
  case NdbDictionary::Column::Type::Bigint:
    return static_cast<double>(result.data_int64());
  case NdbDictionary::Column::Type::Bigunsigned:
    return static_cast<double>(result.data_uint64());
  case NdbDictionary::Column::Type::Double:
    return static_cast<double>(result.data_double());
  default:
    abort();
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
  out << "Output in " << format_description << " format.\n"
      << "The program for post-processing and output has " << m_program.size()
      << " instructions.\n";
}
