package shell

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/client"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

type Shell struct {
	rondisClient *client.RondisClient
	mysqlClient  *client.MySQLClient
	host         string
	rondisPort   int
	mysqlPort    int
	mysqlUser    string
	mysqlPass    string
}

func Run() error {
	return RunWithConfig("localhost", 6379, 3306)
}

func RunWithConfig(host string, rondisPort, mysqlPort int) error {
	// Get credentials from environment (default: root with no password)
	mysqlUser := os.Getenv("RONDB_MYSQL_USER")
	if mysqlUser == "" {
		mysqlUser = "root"
	}
	mysqlPass := os.Getenv("RONDB_MYSQL_PASSWORD")

	s := &Shell{
		host:       host,
		rondisPort: rondisPort,
		mysqlPort:  mysqlPort,
		mysqlUser:  mysqlUser,
		mysqlPass:  mysqlPass,
	}

	if err := s.connect(); err != nil {
		fmt.Println(ui.Error(fmt.Sprintf("Connection failed: %v", err)))
		fmt.Println(ui.Info("Check that RonDB is running:"))
		fmt.Println(ui.Info(fmt.Sprintf("  Rondis: %s:%d", host, rondisPort)))
		fmt.Println(ui.Info(fmt.Sprintf("  MySQL:  %s:%d (user: %s)", host, mysqlPort, mysqlUser)))
		return err
	}
	defer s.close()

	fmt.Println()
	fmt.Println(ui.Welcome())
	fmt.Println()
	fmt.Println(ui.Connected("24.10"))
	fmt.Println()

	return s.loop()
}

func (s *Shell) connect() error {
	var err error

	// MySQL is required
	s.mysqlClient, err = client.NewMySQLClient(s.host, s.mysqlPort, s.mysqlUser, s.mysqlPass)
	if err != nil {
		return fmt.Errorf("mysql (%s:%d): %w", s.host, s.mysqlPort, err)
	}

	// Rondis is optional - don't fail if not available
	s.rondisClient, err = client.NewRondisClient(s.host, s.rondisPort)
	if err != nil {
		fmt.Println(ui.Info(fmt.Sprintf("Rondis not available on port %d (SQL-only mode)", s.rondisPort)))
		s.rondisClient = nil
	}

	return nil
}

func (s *Shell) close() {
	if s.rondisClient != nil {
		s.rondisClient.Close()
	}
	if s.mysqlClient != nil {
		s.mysqlClient.Close()
	}
}

func (s *Shell) loop() error {
	historyFile := filepath.Join(os.Getenv("HOME"), ".rondb_history")

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 ui.Prompt(),
		HistoryFile:            historyFile,
		InterruptPrompt:        "^C",
		EOFPrompt:              "exit",
		HistorySearchFold:      true,
		AutoComplete:           s.getCompleter(),
		DisableAutoSaveHistory: false,
	})
	if err != nil {
		return err
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			continue
		} else if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if err := s.execute(line); err != nil {
			fmt.Println(ui.Error(err.Error()))
		}
	}

	fmt.Println()
	fmt.Println(ui.Disconnected())
	return nil
}

func (s *Shell) execute(line string) error {
	lower := strings.ToLower(line)

	// Internal commands
	if strings.HasPrefix(lower, ".") {
		return s.executeInternal(line)
	}

	// SQL detection
	if isSQLCommand(lower) {
		return s.executeSQL(line)
	}

	// Default: Rondis command
	return s.executeRondis(line)
}

func (s *Shell) executeInternal(line string) error {
	parts := strings.Fields(strings.TrimPrefix(line, "."))
	if len(parts) == 0 {
		return fmt.Errorf("empty command")
	}
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "help":
		s.printHelp()
	case "tables":
		return s.listTables()
	case "demo":
		return s.runDemo()
	case "bench":
		numOps := 1000
		if len(parts) > 1 {
			n, err := strconv.Atoi(parts[1])
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid number of operations: %s", parts[1])
			}
			numOps = n
		}
		return s.runBench(numOps)
	case "quit", "exit", "q":
		fmt.Println(ui.Disconnected())
		os.Exit(0)
	default:
		return fmt.Errorf("unknown command: %s (try .help)", line)
	}

	return nil
}

func (s *Shell) executeRondis(line string) error {
	if s.rondisClient == nil {
		return fmt.Errorf("Rondis not connected. Use SQL commands or restart with Rondis enabled.")
	}

	args := parseArgs(line)
	if len(args) == 0 {
		return nil
	}

	result, duration, err := s.rondisClient.Execute(args)
	if err != nil {
		return err
	}

	fmt.Println(result)
	fmt.Println(ui.Timing(duration))
	return nil
}

func (s *Shell) executeSQL(line string) error {
	lower := strings.ToLower(strings.TrimSpace(line))

	// Check if it's a SELECT or SHOW (returns rows)
	if strings.HasPrefix(lower, "select") || strings.HasPrefix(lower, "show") || strings.HasPrefix(lower, "describe") || strings.HasPrefix(lower, "explain") {
		columns, rows, duration, err := s.mysqlClient.Query(line)
		if err != nil {
			return err
		}

		output := ui.RenderSQLResultWithDuration(columns, rows, duration)
		fmt.Print(output)
		return nil
	}

	// Otherwise it's an execute (INSERT, UPDATE, DELETE, CREATE, etc.)
	affected, duration, err := s.mysqlClient.Execute(line)
	if err != nil {
		return err
	}

	fmt.Println(ui.Success(fmt.Sprintf("OK, %d rows affected", affected)))
	fmt.Println(ui.Timing(duration))
	return nil
}

func (s *Shell) runDemo() error {
	fmt.Println()
	fmt.Println(ui.Info("Running demo..."))
	fmt.Println()

	// Demo data
	users := []struct {
		key   string
		value string
	}{
		{"demo:user:1", `{"name":"Alice","role":"admin"}`},
		{"demo:user:2", `{"name":"Bob","role":"developer"}`},
		{"demo:user:3", `{"name":"Charlie","role":"designer"}`},
		{"demo:user:4", `{"name":"Diana","role":"developer"}`},
		{"demo:user:5", `{"name":"Eve","role":"security"}`},
	}

	// Write with Rondis
	fmt.Println("📝 Writing 5 records via Rondis...")
	for _, u := range users {
		_, duration, err := s.rondisClient.Execute([]string{"SET", u.key, u.value})
		if err != nil {
			return err
		}
		fmt.Printf("   SET %s %s\n", ui.Key(u.key), ui.Timing(duration))
	}
	fmt.Println()

	// Read with Rondis
	fmt.Println("📖 Reading back via Rondis...")
	for _, u := range users {
		result, duration, err := s.rondisClient.Execute([]string{"GET", u.key})
		if err != nil {
			return err
		}
		fmt.Printf("   GET %s → %s %s\n", ui.Key(u.key), result, ui.Timing(duration))
	}
	fmt.Println()

	// Query with SQL
	fmt.Println("🔍 Querying via SQL...")
	columns, rows, duration, err := s.mysqlClient.Query("SELECT redis_key, value_start FROM redis_0.string_keys WHERE redis_key LIKE 'demo:user:%'")
	if err != nil {
		return err
	}
	output := ui.RenderSQLResultWithDuration(columns, rows, duration)
	fmt.Print(output)
	fmt.Println()

	// Cleanup
	fmt.Println("🧹 Cleaning up...")
	for _, u := range users {
		s.rondisClient.Execute([]string{"DEL", u.key})
	}
	fmt.Println(ui.Success("Demo complete!"))
	fmt.Println()

	return nil
}

func (s *Shell) runBench(numOps int) error {
	if s.rondisClient == nil {
		return fmt.Errorf("Rondis not connected. Benchmark requires Rondis.")
	}

	fmt.Println()
	if numOps > 10000 {
		fmt.Println(ui.Warning(fmt.Sprintf("Running %d operations - this may take a while...", numOps)))
	}
	fmt.Println(ui.Info(fmt.Sprintf("Running benchmark (%d operations)...", numOps)))
	fmt.Println()

	// Write benchmark
	fmt.Printf("📝 Writing %d keys via Rondis...\n", numOps)
	writeStart := time.Now()
	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		value := fmt.Sprintf(`{"id":%d,"data":"benchmark test data for key %d"}`, i, i)
		_, _, err := s.rondisClient.Execute([]string{"SET", key, value})
		if err != nil {
			return err
		}
	}
	writeDuration := time.Since(writeStart)
	writeOpsPerSec := float64(numOps) / writeDuration.Seconds()
	fmt.Printf("   %d writes in %v (%.0f ops/sec)\n", numOps, writeDuration.Round(time.Millisecond), writeOpsPerSec)
	fmt.Println()

	// Read benchmark
	fmt.Printf("📖 Reading %d keys via Rondis...\n", numOps)
	readStart := time.Now()
	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		_, _, err := s.rondisClient.Execute([]string{"GET", key})
		if err != nil {
			return err
		}
	}
	readDuration := time.Since(readStart)
	readOpsPerSec := float64(numOps) / readDuration.Seconds()
	fmt.Printf("   %d reads in %v (%.0f ops/sec)\n", numOps, readDuration.Round(time.Millisecond), readOpsPerSec)
	fmt.Println()

	// SQL count to prove data is there
	fmt.Println("🔍 Verifying via SQL...")
	_, rows, duration, err := s.mysqlClient.Query("SELECT COUNT(*) as count FROM redis_0.string_keys WHERE redis_key LIKE 'bench:key:%'")
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		fmt.Printf("   COUNT(*) = %v %s\n", rows[0][0], ui.Timing(duration))
	}
	fmt.Println()

	// SQL sample query
	fmt.Println("🔍 Sample SQL query (LIMIT 5)...")
	columns, rows, duration, err := s.mysqlClient.Query("SELECT redis_key, value_start FROM redis_0.string_keys WHERE redis_key LIKE 'bench:key:%' LIMIT 5")
	if err != nil {
		return err
	}
	output := ui.RenderSQLResultWithDuration(columns, rows, duration)
	fmt.Print(output)
	fmt.Println()

	// Cleanup
	fmt.Printf("🧹 Cleaning up %d keys...\n", numOps)
	cleanStart := time.Now()
	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		s.rondisClient.Execute([]string{"DEL", key})
	}
	cleanDuration := time.Since(cleanStart)
	fmt.Printf("   Cleanup in %v\n", cleanDuration.Round(time.Millisecond))
	fmt.Println()

	fmt.Println(ui.Success("Benchmark complete!"))
	fmt.Printf("   Writes: %.0f ops/sec\n", writeOpsPerSec)
	fmt.Printf("   Reads:  %.0f ops/sec\n", readOpsPerSec)
	fmt.Println()

	return nil
}

func (s *Shell) listTables() error {
	tables, err := s.mysqlClient.ListTables()
	if err != nil {
		return err
	}

	if len(tables) == 0 {
		fmt.Println(ui.Info("No tables found"))
		return nil
	}

	for _, t := range tables {
		fmt.Println(t)
	}
	return nil
}

func (s *Shell) printHelp() {
	help := `
RonDB CLI - Rondis commands. SQL queries. One database.

Commands:
  Rondis commands (Redis protocol):
    SET key value       Store a value
    GET key             Retrieve a value
    DEL key             Delete a key
    MGET key1 key2      Get multiple values
    INCR key            Increment a counter

  SQL queries:
    SELECT * FROM ...   Query tables
    INSERT INTO ...     Insert data
    CREATE TABLE ...    Create tables
    USE database        Switch database

  Internal commands:
    .demo               Run a quick demo (write, read, query)
    .bench [N]          Run benchmark (default 1000 ops, shows throughput)
    .help               Show this help
    .tables             List all tables
    .quit               Exit the shell

The magic: Your Rondis data is queryable with SQL!
  SET user:123 '{"name":"Alice"}'
  SELECT * FROM redis_0.string_keys WHERE redis_key = 'user:123'
`
	fmt.Println(help)
}

func isSQLCommand(lower string) bool {
	sqlKeywords := []string{
		"select", "insert", "update", "delete", "create", "drop", "alter",
		"show", "describe", "explain", "use", "grant", "revoke", "truncate",
	}
	for _, kw := range sqlKeywords {
		if strings.HasPrefix(lower, kw) {
			return true
		}
	}
	return false
}

func parseArgs(line string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, r := range line {
		if !inQuote && (r == '"' || r == '\'') {
			inQuote = true
			quoteChar = r
		} else if inQuote && r == quoteChar {
			inQuote = false
			quoteChar = 0
		} else if !inQuote && r == ' ' {
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}

func (s *Shell) getCompleter() *readline.PrefixCompleter {
	return readline.NewPrefixCompleter(
		// Rondis commands
		readline.PcItem("GET"),
		readline.PcItem("SET"),
		readline.PcItem("DEL"),
		readline.PcItem("MGET"),
		readline.PcItem("MSET"),
		readline.PcItem("INCR"),
		readline.PcItem("DECR"),
		readline.PcItem("HGET"),
		readline.PcItem("HSET"),
		readline.PcItem("HDEL"),
		readline.PcItem("KEYS"),
		readline.PcItem("PING"),

		// SQL with nested completions
		readline.PcItem("SELECT",
			readline.PcItem("*",
				readline.PcItem("FROM",
					readline.PcItem("redis_0.string_keys"),
				),
			),
		),
		readline.PcItem("SHOW",
			readline.PcItem("DATABASES"),
			readline.PcItem("TABLES"),
			readline.PcItem("CREATE",
				readline.PcItem("TABLE"),
			),
		),
		readline.PcItem("DESCRIBE"),
		readline.PcItem("USE"),
		readline.PcItem("INSERT",
			readline.PcItem("INTO"),
		),
		readline.PcItem("UPDATE"),
		readline.PcItem("DELETE",
			readline.PcItem("FROM"),
		),
		readline.PcItem("CREATE",
			readline.PcItem("TABLE"),
			readline.PcItem("DATABASE"),
		),
		readline.PcItem("DROP",
			readline.PcItem("TABLE"),
			readline.PcItem("DATABASE"),
		),

		// Internal commands
		readline.PcItem(".bench"),
		readline.PcItem(".demo"),
		readline.PcItem(".help"),
		readline.PcItem(".tables"),
		readline.PcItem(".quit"),
	)
}
