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

#include <RonSQLPreparer.hpp>
#include <my_sys.h> // Only needed for MY_GIVE_INFO
#include <getopt.h>
#include <chrono>

using std::cerr;
using std::cout;
using std::endl;

struct Config
{
  bool help = false;
  ExecutionParameters params;
  bool time_info = false;
  const char* connectstring = NULL;
  const char* database = NULL;
};

static void print_help(const char* argv0);
static int parse_cmdline_arguments(int argc, char** argv, Config& config);
static void read_stdin(ArenaAllocator* aalloc, char** buffer, size_t* buffer_len);
static int run_ronsql(ExecutionParameters& params);

int
main(int argc, char** argv)
{
  Config config;
  ExecutionParameters& params = config.params;
  ArenaAllocator aalloc;
  params.aalloc = &aalloc;
  params.query_output_stream = &cout;
  params.explain_output_stream = &cout;
  params.err_output_stream = &cerr;
  params.query_output_format = isatty(fileno(stdin)) && isatty(fileno(stdout))
    ? ExecutionParameters::QueryOutputFormat::JSON_UTF8
    : ExecutionParameters::QueryOutputFormat::TSV;
  int exit_code = 0;

  // Parse command-line arguments
  exit_code = parse_cmdline_arguments(argc, argv, config);
  if (config.help)
  {
    print_help(argv[0]);
  }
  if (config.help || exit_code > 0)
  {
    return exit_code;
  }

  // If no query has been given, read it from stdin
  if (params.sql_buffer == NULL)
  {
    try
    {
      read_stdin(params.aalloc, &params.sql_buffer, &params.sql_len);
      assert(params.sql_buffer != NULL);
    }
    catch (std::runtime_error& e)
    {
      cerr << "Caught exception: " << e.what() << endl;
      return 1;
    }
    assert(params.sql_buffer != NULL);
  }

  // If no connectstring, run without connection
  if (config.connectstring == NULL)
  {
    return run_ronsql(params);
  }

  ndb_init();
  // Block scope for ndb cluster connection
  {
    // Ndb connection
    Ndb_cluster_connection cluster_connection(config.connectstring);
    if (cluster_connection.connect(4, 5, 1))
    {
      cerr << "Unable to connect to cluster within 30 secs." << endl;
      return 1;
    }
    // Connect and wait for the storage nodes (ndbd's)
    if (cluster_connection.wait_until_ready(30,0) < 0)
    {
      cerr << "Cluster was not ready within 30 secs." << endl;
      return 1;
    }
    Ndb myNdb(&cluster_connection, config.database);
    // Set max 1024  parallel transactions
    if (myNdb.init(1024) == -1) {
      auto error = myNdb.getNdbError();
      cerr << "ndbapi error " << error.code << ": " << error.message << endl;
      return 1;
    }
    params.ndb = &myNdb;
    // Execute, perhaps timing it
    if (config.time_info)    {
      auto start = std::chrono::high_resolution_clock::now();
      exit_code = run_ronsql(params);
      auto end = std::chrono::high_resolution_clock::now();
      std::chrono::duration<double> elapsed = end - start;
      cerr << "\nElapsed time: " << elapsed.count() << " s" << endl;
    }
    else
    {
      exit_code = run_ronsql(params);
    }
  }
  // End of block scope. This is necessary to clean up the cluster connection
  // before calling ndb_end.
  ndb_end(config.time_info ? MY_GIVE_INFO : 0);

  return exit_code;
}

static void
print_help(const char* argv0)
{
  cout <<
    "Usage: " << argv0 << " [OPTIONS]\n"
    "\n"
    "If neither --execute nor --execute-file is given, the query will be read from\n"
    "stdin.\n"
    "\n"
    "Options that mimic mysql:\n"
    "  -?, --help                    Display this help message\n"
    "  -T, --debug-info              Print some debug info at exit, e.g. timing\n"
    "  -D, --database name           Database name. Required if --connect-string is\n"
    "                                given.\n"
    "  -e, --execute query           Execute query and output results.\n"
    "  -s, --silent                  Set query output format to TSV if stdin and\n"
    "                                stdout are both ttys, otherwise to TSV_DATA.\n"
    "Options specific to RonSQL:\n"
    "  --execute-file <FILE>         Execute query from file.\n"
    "  --connect-string <STRING>     Ndb connection string (If not given, then no\n"
    "                                connection is made. Without a connection,\n"
    "                                queries are not supported and explain output\n"
    "                                will be somewhat limited. Execution mode must be\n"
    "                                set to either ALLOW_EXPLAIN_ONLY or\n"
    "                                EXPLAIN_OVERRIDE.)\n"
    // See RonSQLCommon.hpp for comment about execution mode
    "  --execution-mode <MODE>       Set execution mode. <MODE> can be one of:\n"
    "                                - ALLOW_BOTH_QUERY_AND_EXPLAIN (default)\n"
    "                                - ALLOW_QUERY_ONLY\n"
    "                                - ALLOW_EXPLAIN_ONLY\n"
    "                                - QUERY_OVERRIDE\n"
    "                                - EXPLAIN_OVERRIDE\n"
    // See RonSQLCommon.hpp for comment about query output format
    "  --query-output-format <FMT>   Set query output format. <FMT> can be one of:\n"
    "                                - JSON_UTF8, default if stdin and stdout are\n"
    "                                  both ttys, i.e. the same case where mysql\n"
    "                                  defaults to formatted table output.\n"
    "                                - JSON_ASCII\n"
    "                                - TSV, default if stdin or stdout is not a tty.\n"
    "                                  This mimics mysql behavior."
    "                                - TSV_DATA\n"
    // See RonSQLCommon.hpp for comment about explain output format
    "  --explain-output-format <FMT> Set explain output format. <FMT> can be one of:\n"
    "                                - TEXT (default)\n"
    "                                - JSON_UTF8\n"
    ;
}

#define ARG_FAIL(EXPLANATION) do \
  { \
    cerr << "Error parsing command line arguments: " << EXPLANATION << endl; \
    config.help = true; \
    return 1; \
  } while (0);

static int
parse_cmdline_arguments(int argc, char** argv, Config& config)
{
  ExecutionParameters& params = config.params;
  int opt;

  // Unique values for long-only options
  enum {
    OPT_EXECUTE_FILE = 256,
    OPT_CONNECT_STRING,
    OPT_EXECUTION_MODE,
    OPT_QUERY_OUTPUT_FORMAT,
    OPT_EXPLAIN_OUTPUT_FORMAT,
  };

  static struct option long_options[] = {
    {"help", no_argument, 0, '?'},
    {"debug-info", no_argument, 0, 'T'},
    {"database", required_argument, 0, 'D'},
    {"execute", required_argument, 0, 'e'},
    {"silent", required_argument, 0, 's'},
    {"execute-file", required_argument, 0, OPT_EXECUTE_FILE},
    {"connect-string", required_argument, 0, OPT_CONNECT_STRING},
    {"execution-mode", required_argument, 0, OPT_EXECUTION_MODE},
    {"query-output-format", required_argument, 0, OPT_QUERY_OUTPUT_FORMAT},
    {"explain-output-format", required_argument, 0, OPT_EXPLAIN_OUTPUT_FORMAT},
    {0, 0, 0, 0}
  };

  while ((opt = getopt_long(argc, argv, "?TD:e:s", long_options, NULL)) != -1)
  {
    switch (opt)
    {
      // opt can be '?' in two cases:
      // 1) -? or --help is explicitly given and optopt == 0
      // 2) an unknown option was given and optopt != 0
    case '?': config.help = true; return optopt ? 1 : 0;
    case 'T': config.time_info = true; break;
    case 'D':
      config.database = optarg;
      break;
    case 'e':
      if (params.sql_buffer != NULL)
      {
        ARG_FAIL("Only one query may be specified.");
      }
      else
      {
        static_assert(sizeof(char) == 1);
        char* sql_query = optarg;
        uint sql_query_len = strlen(sql_query);
        size_t parse_len = sql_query_len + 2;
        char* parse_str = params.aalloc->alloc<char>(parse_len);
        memcpy(parse_str, sql_query, sql_query_len);
        parse_str[sql_query_len] = '\0';
        parse_str[sql_query_len+1] = '\0';
        params.sql_buffer = parse_str;
        params.sql_len = parse_len;
      }
      break;
    case 's':
      params.query_output_format = isatty(fileno(stdin)) && isatty(fileno(stdout))
        ? ExecutionParameters::QueryOutputFormat::TSV
        : ExecutionParameters::QueryOutputFormat::TSV_DATA;
      break;
    case OPT_EXECUTE_FILE:
      if (params.sql_buffer != NULL)
      {
        ARG_FAIL("Only one query may be specified.");
      }
      else
      {
        static_assert(sizeof(char) == 1);
        char* file_name = optarg;
        FILE* file = fopen(file_name, "r");
        if (file == NULL)
        {
          cerr << "Error opening file: " << file_name << endl;
          return 1;
        }
        fseek(file, 0, SEEK_END);
        size_t sql_query_len = ftell(file);
        fseek(file, 0, SEEK_SET);
        size_t parse_len = sql_query_len + 2;
        char* parse_str = params.aalloc->alloc<char>(parse_len);
        size_t read_size = fread(parse_str, 1, parse_len, file);
        if (read_size != sql_query_len)
        {
          cerr << "Error reading file: " << file_name << endl;
          fclose(file);
          return 1;
        }
        fclose(file);
        parse_str[sql_query_len] = '\0';
        parse_str[sql_query_len+1] = '\0';
        params.sql_buffer = parse_str;
        params.sql_len = parse_len;
      }
      break;
    case OPT_CONNECT_STRING: config.connectstring = optarg; break;
    case OPT_EXECUTION_MODE:
      if (strcmp(optarg, "ALLOW_BOTH_QUERY_AND_EXPLAIN") == 0)
        params.mode = ExecutionParameters::ExecutionMode::ALLOW_BOTH_QUERY_AND_EXPLAIN;
      else if (strcmp(optarg, "ALLOW_QUERY_ONLY") == 0)
        params.mode = ExecutionParameters::ExecutionMode::ALLOW_QUERY_ONLY;
      else if (strcmp(optarg, "ALLOW_EXPLAIN_ONLY") == 0)
        params.mode = ExecutionParameters::ExecutionMode::ALLOW_EXPLAIN_ONLY;
      else if (strcmp(optarg, "QUERY_OVERRIDE") == 0)
        params.mode = ExecutionParameters::ExecutionMode::QUERY_OVERRIDE;
      else if (strcmp(optarg, "EXPLAIN_OVERRIDE") == 0)
        params.mode = ExecutionParameters::ExecutionMode::EXPLAIN_OVERRIDE;
      else
        ARG_FAIL("Invalid execution mode.");
      break;
    case OPT_QUERY_OUTPUT_FORMAT:
      if (strcmp(optarg, "JSON_UTF8") == 0)
        params.query_output_format = ExecutionParameters::QueryOutputFormat::JSON_UTF8;
      else if (strcmp(optarg, "JSON_ASCII") == 0)
        params.query_output_format = ExecutionParameters::QueryOutputFormat::JSON_ASCII;
      else if (strcmp(optarg, "TSV") == 0)
        params.query_output_format = ExecutionParameters::QueryOutputFormat::TSV;
      else if (strcmp(optarg, "TSV_DATA") == 0)
        params.query_output_format = ExecutionParameters::QueryOutputFormat::TSV_DATA;
      else
        ARG_FAIL("Invalid query output format.");
      break;
    case OPT_EXPLAIN_OUTPUT_FORMAT:
      if (strcmp(optarg, "TEXT") == 0)
        params.explain_output_format = ExecutionParameters::ExplainOutputFormat::TEXT;
      else if (strcmp(optarg, "JSON_UTF8") == 0)
        params.explain_output_format = ExecutionParameters::ExplainOutputFormat::JSON_UTF8;
      else
        ARG_FAIL("Invalid explain output format.");
      break;
    default:
      ARG_FAIL("Invalid option.");
    }
  }
  // Make sure no arguments remain
  if (optind != argc)
  {
    ARG_FAIL("No positional arguments allowed.");
  }
  // Validate
  if (config.connectstring != NULL && config.database == NULL)
  {
    ARG_FAIL("Database name is required if connect string is given.");
  }
  return 0;
}

static void
read_stdin(ArenaAllocator* aalloc, char** buffer, size_t* buffer_len)
{
  uint alloclen = 1024;
  char* buf = aalloc->alloc<char>(alloclen);
  uint contentlen = 0;
  while (true)
  {
    assert(contentlen < alloclen);
    size_t readlen = fread(buf + contentlen,
                           sizeof(char),
                           alloclen - contentlen, stdin);
    contentlen += readlen;
    if (feof(stdin))
    {
      break;
    }
    int error = ferror(stdin);
    if (error)
    {
      throw std::runtime_error("Error reading from stdin.");
    }
    if (contentlen == alloclen)
    {
      buf = aalloc->realloc(buf, alloclen * 2, alloclen);
      alloclen *= 2;
    }
  }
  if (contentlen + 2 > alloclen)
  {
    buf = aalloc->realloc(buf, contentlen + 2, alloclen);
    // alloclen = contentlen + 2;
  }
  buf[contentlen] = '\0';
  buf[contentlen+1] = '\0';
  *buffer = buf;
  *buffer_len = contentlen + 2;
}

static int
run_ronsql(ExecutionParameters& params)
{
  try
  {
    RonSQLPreparer executor(params);
    executor.execute();
    return 0;
  }
  catch (RonSQLPreparer::TemporaryError& e)
  {
    cerr << "Caught temporary error: " << e.what() << endl;
    // Use exit code 3 to distinguish temporary errors.
    // Avoid exit code 2 as it is used by e.g. bash.
      return 3;
  }
  catch (std::runtime_error& e)
  {
    cerr << "Caught exception: " << e.what() << endl;
    return 1;
  }
  // Unreachable
  abort();
}
