package shell

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chzyer/readline"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/client"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/dsl"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/tui"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

// Config holds shell configuration
type Config struct {
	Host       string // General host (used for Rondis)
	MySQLHost  string // MySQL host (can be different from Host)
	RDRSHost   string // RDRS/REST API host (can be different from Host)
	RondisPort int
	MySQLPort  int
	RestPort   int
	MySQLUser  string
	MySQLPass  string
	TLS        bool   // Enable TLS for Rondis and MySQL
	RDRSTLS    bool   // Enable TLS (HTTPS) for RDRS/REST API
	RDRSAPIKey string // API key for RDRS/REST API authentication
	Verbose    int    // Verbose level (0=normal, 1=connection info, 2=debug)
	NoMySQL    bool   // Disable MySQL connection
	NoRDRS     bool   // Disable RDRS/REST API connection
	NoRondis   bool   // Disable Rondis connection
}

type Shell struct {
	rondisClient   *client.RondisClient
	mysqlClient    *client.MySQLClient
	restClient     *client.RestClient
	config         Config
	mysqlUser      string
	mysqlPass      string
	rl             *readline.Instance // Store readline instance for multi-line input
	debug          bool               // Debug mode for printing requests/responses in benchmarks
	verbose        int                // Verbose level: 0=normal, 1=connect/disconnect info, 2=debug mode
	clientID       int                // Client ID prefix for benchmark keys (default 0)
	ronsqlDatabase string             // Database for RonSQL queries
	ronsqlFormat   string             // Output format for RonSQL queries (default "JSON")
	ronsqlExplain  string             // Explain mode for RonSQL queries (default "ALLOW")
}

func Run() error {
	return RunWithConfig(Config{
		Host:       "localhost",
		RondisPort: 6379,
		MySQLPort:  3306,
		RestPort:   4406,
	})
}

func RunWithConfig(cfg Config) error {
	s := &Shell{
		config:         cfg,
		mysqlUser:      cfg.MySQLUser,
		mysqlPass:      cfg.MySQLPass,
		verbose:        cfg.Verbose,
		debug:          cfg.Verbose >= 2,
		ronsqlDatabase: "test",
		ronsqlFormat:   "JSON",
		ronsqlExplain:  "ALLOW",
	}

	if err := s.connect(); err != nil {
		fmt.Println(ui.Error(fmt.Sprintf("Connection failed: %v", err)))
		fmt.Println()
		fmt.Println(ui.Info("Check that RonDB is running:"))
		fmt.Println(ui.Info(fmt.Sprintf("  Rondis:   %s:%d", cfg.Host, cfg.RondisPort)))
		fmt.Println(ui.Info(fmt.Sprintf("  MySQL:    %s:%d (user: %s)", cfg.MySQLHost, cfg.MySQLPort, cfg.MySQLUser)))
		fmt.Println(ui.Info(fmt.Sprintf("  REST API: %s:%d", cfg.RDRSHost, cfg.RestPort)))
		fmt.Println()
		fmt.Println(ui.Info("Command line flags:"))
		fmt.Println(ui.Info("  --host             RonDB host (default: localhost, sets all hosts)"))
		fmt.Println(ui.Info("  --mysql-host       MySQL host (overrides --host)"))
		fmt.Println(ui.Info("  --rdrs-host        RDRS/REST API host (overrides --host)"))
		fmt.Println(ui.Info("  --rondis-port      Rondis port (default: 6379)"))
		fmt.Println(ui.Info("  --mysql-port       MySQL port (default: 3306)"))
		fmt.Println(ui.Info("  --rdrs-port        RDRS/REST API port (default: 4406)"))
		fmt.Println(ui.Info("  --mysql-user       MySQL username (default: root)"))
		fmt.Println(ui.Info("  --mysql-password   MySQL password (default: empty)"))
		fmt.Println(ui.Info("  -p, --password     Prompt for MySQL password (silent input)"))
		fmt.Println(ui.Info("  --tls              Enable TLS for all connections"))
		fmt.Println(ui.Info("  --rdrs-tls         Enable TLS (HTTPS) for RDRS/REST API only"))
		fmt.Println(ui.Info("  --rdrs-api-key     API key for RDRS/REST API authentication"))
		fmt.Println(ui.Info("  --verbose          Verbose level (0=normal, 1=connection info, 2=debug)"))
		fmt.Println(ui.Info("  --no-mysql         Disable MySQL connection"))
		fmt.Println(ui.Info("  --no-rdrs          Disable RDRS/REST API connection"))
		fmt.Println(ui.Info("  --no-rondis        Disable Rondis connection"))
		fmt.Println()
		fmt.Println(ui.Info("Environment variables:"))
		fmt.Println(ui.Info("  RONDB_HOST           RonDB host (sets all hosts)"))
		fmt.Println(ui.Info("  MYSQL_HOST           MySQL host (overrides RONDB_HOST)"))
		fmt.Println(ui.Info("  RDRS_HOST            RDRS/REST API host (overrides RONDB_HOST)"))
		fmt.Println(ui.Info("  RONDB_RONDIS_PORT    Rondis port"))
		fmt.Println(ui.Info("  RONDB_MYSQL_PORT     MySQL port"))
		fmt.Println(ui.Info("  RONDB_RDRS_PORT      RDRS/REST API port"))
		fmt.Println(ui.Info("  RONDB_MYSQL_USER     MySQL username"))
		fmt.Println(ui.Info("  RONDB_MYSQL_PASSWORD MySQL password"))
	fmt.Println(ui.Info("  RONDB_RDRS_API_KEY   RDRS/REST API key"))
		return err
	}
	defer s.close()

	fmt.Println()
	fmt.Println(ui.Welcome())
	fmt.Println()
	fmt.Println(ui.Connected("25.10"))
	fmt.Println()

	return s.loop()
}

func (s *Shell) connect() error {
	var err error

	// MySQL connection (unless disabled)
	if s.config.NoMySQL {
		if s.verbose >= 1 {
			fmt.Println("[*] MySQL disabled via --no-mysql")
		}
		s.mysqlClient = nil
	} else {
		if s.verbose >= 1 {
			tlsInfo := ""
			if s.config.TLS {
				tlsInfo = " (TLS)"
			}
			fmt.Printf("[*] Connecting to MySQL at %s:%d%s...\n", s.config.MySQLHost, s.config.MySQLPort, tlsInfo)
		}
		s.mysqlClient, err = client.NewMySQLClientWithOptions(client.MySQLOptions{
			Host:     s.config.MySQLHost,
			Port:     s.config.MySQLPort,
			User:     s.mysqlUser,
			Password: s.mysqlPass,
			TLS:      s.config.TLS,
		})
		if err != nil {
			return fmt.Errorf("mysql (%s:%d): %w", s.config.MySQLHost, s.config.MySQLPort, err)
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] MySQL connected as %s\n", s.mysqlUser)
		}
	}

	// REST API connection (unless disabled)
	if s.config.NoRDRS {
		if s.verbose >= 1 {
			fmt.Println("[*] RDRS/REST API disabled via --no-rdrs")
		}
		s.restClient = nil
	} else {
		if s.verbose >= 1 {
			protocol := "http"
			if s.config.RDRSTLS {
				protocol = "https"
			}
			fmt.Printf("[*] Connecting to REST API at %s://%s:%d...\n", protocol, s.config.RDRSHost, s.config.RestPort)
		}
		s.restClient, err = client.NewRestClientWithOptions(client.RestOptions{
			Host:   s.config.RDRSHost,
			Port:   s.config.RestPort,
			TLS:    s.config.RDRSTLS,
			APIKey: s.config.RDRSAPIKey,
		})
		if err != nil {
			return fmt.Errorf("rest api (%s:%d): %w", s.config.RDRSHost, s.config.RestPort, err)
		}
		if s.verbose >= 1 {
			fmt.Println("[*] REST API connected")
		}
	}

	// Rondis connection (unless disabled) - optional, don't fail if not available
	if s.config.NoRondis {
		if s.verbose >= 1 {
			fmt.Println("[*] Rondis disabled via --no-rondis")
		}
		s.rondisClient = nil
	} else {
		if s.verbose >= 1 {
			tlsInfo := ""
			if s.config.TLS {
				tlsInfo = " (TLS)"
			}
			fmt.Printf("[*] Connecting to Rondis at %s:%d%s...\n", s.config.Host, s.config.RondisPort, tlsInfo)
		}
		s.rondisClient, err = client.NewRondisClientWithOptions(client.RondisOptions{
			Host: s.config.Host,
			Port: s.config.RondisPort,
			TLS:  s.config.TLS,
		})
		if err != nil {
			if s.verbose >= 1 {
				fmt.Printf("[*] Rondis not available: %v\n", err)
			}
			fmt.Println(ui.Info(fmt.Sprintf("Rondis not available on %s:%d (SQL-only mode)", s.config.Host, s.config.RondisPort)))
			s.rondisClient = nil
		} else if s.verbose >= 1 {
			fmt.Println("[*] Rondis connected")
		}
	}

	return nil
}

func (s *Shell) close() {
	if s.verbose >= 1 {
		fmt.Println(ui.Info("Closing connections..."))
	}
	if s.rondisClient != nil {
		s.rondisClient.Close()
		if s.verbose >= 1 {
			fmt.Println(ui.Info(fmt.Sprintf("  Rondis (%s:%d): disconnected", s.config.Host, s.config.RondisPort)))
		}
	}
	if s.mysqlClient != nil {
		s.mysqlClient.Close()
		if s.verbose >= 1 {
			fmt.Println(ui.Info(fmt.Sprintf("  MySQL (%s:%d): disconnected", s.config.MySQLHost, s.config.MySQLPort)))
		}
	}
	if s.restClient != nil {
		s.restClient.Close()
		if s.verbose >= 1 {
			fmt.Println(ui.Info(fmt.Sprintf("  REST API (%s:%d): disconnected", s.config.RDRSHost, s.config.RestPort)))
		}
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

	// Store readline instance for multi-line input
	s.rl = rl

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

		// Check if command is complete (ends with ;;) or needs more input
		fullCommand, err := s.readFullCommand(line)
		if err != nil {
			if err.Error() == "input cancelled" {
				continue
			}
			fmt.Println(ui.Error(err.Error()))
			continue
		}

		if err := s.execute(fullCommand); err != nil {
			fmt.Println(ui.Error(err.Error()))
		}
	}

	fmt.Println()
	fmt.Println(ui.Disconnected())
	return nil
}

// readFullCommand reads a complete command, supporting multi-line input
// Commands end with ; or are single-line if they don't need continuation
func (s *Shell) readFullCommand(firstLine string) (string, error) {
	// If line ends with ;, it's complete
	if strings.HasSuffix(firstLine, ";") {
		return strings.TrimSuffix(firstLine, ";"), nil
	}

	// For simple single-line commands (internal commands, simple Rondis), return as-is
	lower := strings.ToLower(firstLine)
	if strings.HasPrefix(lower, ".") {
		return firstLine, nil
	}

	// quit/exit/q don't need semicolon
	if lower == "quit" || lower == "exit" || lower == "q" {
		return firstLine, nil
	}

	// RONSQL commands are handled by executeRonSQL - return as-is
	if lower == "ronsql" || strings.HasPrefix(lower, "ronsql ") {
		return firstLine, nil
	}

	// For BATCH without ;, use the existing multi-line BATCH handling
	if lower == "batch" || strings.HasPrefix(lower, "batch ") {
		return firstLine, nil // Let executeBatch handle multi-line
	}

	// For other commands without ;, check if it looks like it might need more input
	// Simple heuristic: if it's a short Rondis command (GET, SET, etc.) or USE, it's complete
	simpleCommands := []string{"get ", "set ", "del ", "incr ", "decr ", "ping", "keys ", "mget ", "mset ", "hget ", "hset ", "hdel ", "use "}
	for _, cmd := range simpleCommands {
		if strings.HasPrefix(lower, cmd) || lower == strings.TrimSpace(cmd) {
			return firstLine, nil
		}
	}

	// For SQL and READ commands, allow multi-line input until ;
	var lines []string
	lines = append(lines, firstLine)

	// Change prompt for continuation
	originalPrompt := s.rl.Config.Prompt
	s.rl.SetPrompt("   ...> ")
	defer s.rl.SetPrompt(originalPrompt)

	for {
		line, err := s.rl.Readline()
		if err == readline.ErrInterrupt {
			return "", fmt.Errorf("input cancelled")
		} else if err != nil {
			return "", err
		}

		line = strings.TrimSpace(line)

		// Check for end marker
		if strings.HasSuffix(line, ";") {
			line = strings.TrimSuffix(line, ";")
			if line != "" {
				lines = append(lines, line)
			}
			break
		}

		lines = append(lines, line)
	}

	return strings.Join(lines, " "), nil
}

func (s *Shell) execute(line string) error {
	lower := strings.ToLower(line)

	// Internal commands
	if strings.HasPrefix(lower, ".") {
		return s.executeInternal(line)
	}

	// Quit/exit without dot prefix
	if lower == "quit" || lower == "exit" || lower == "q" {
		fmt.Println(ui.Disconnected())
		os.Exit(0)
	}

	// MYSQL prefix: send rest of command to MySQL
	if strings.HasPrefix(lower, "mysql ") {
		sqlCmd := strings.TrimSpace(line[6:]) // Remove "MYSQL " prefix
		if sqlCmd == "" {
			return fmt.Errorf("MYSQL requires a command")
		}
		return s.executeSQL(sqlCmd)
	}

	// RONSQL prefix: send query to RonSQL REST API
	if lower == "ronsql" || strings.HasPrefix(lower, "ronsql ") {
		return s.executeRonSQL(line)
	}

	// SQL detection
	if isSQLCommand(lower) {
		return s.executeSQL(line)
	}

	// REST API: Single READ command
	if strings.HasPrefix(lower, "read ") {
		return s.executeREAD(line)
	}

	// REST API: BATCH (single-line or multi-line mode)
	if lower == "batch" || strings.HasPrefix(lower, "batch ") {
		return s.executeBatch(line)
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
		if len(parts) > 1 {
			switch parts[1] {
			case "internal":
				s.printHelpInternal()
			case "start":
				s.printHelpStart()
			default:
				s.printHelp()
			}
		} else {
			s.printHelp()
		}
	case "debug":
		if len(parts) < 2 {
			if s.debug {
				fmt.Println("Debug mode: ON")
			} else {
				fmt.Println("Debug mode: OFF")
			}
			return nil
		}
		switch parts[1] {
		case "1", "on", "true":
			s.debug = true
			fmt.Println(ui.Success("Debug mode enabled"))
		case "0", "off", "false":
			s.debug = false
			fmt.Println(ui.Info("Debug mode disabled"))
		default:
			return fmt.Errorf("invalid debug value: use 0/1, on/off, or true/false")
		}
	case "verbose":
		if len(parts) < 2 {
			fmt.Printf("Verbose level: %d\n", s.verbose)
			return nil
		}
		level, err := strconv.Atoi(parts[1])
		if err != nil || level < 0 || level > 2 {
			return fmt.Errorf("invalid verbose level: use 0, 1, or 2")
		}
		s.verbose = level
		if level >= 2 {
			s.debug = true
			fmt.Println(ui.Success(fmt.Sprintf("Verbose level set to %d (debug mode enabled)", level)))
		} else {
			fmt.Println(ui.Success(fmt.Sprintf("Verbose level set to %d", level)))
		}
	case "client":
		if len(parts) < 2 {
			fmt.Printf("Client ID: %d\n", s.clientID)
			return nil
		}
		id, err := strconv.Atoi(parts[1])
		if err != nil || id < 0 {
			return fmt.Errorf("invalid client ID: must be a non-negative integer")
		}
		s.clientID = id
		fmt.Println(ui.Success(fmt.Sprintf("Client ID set to %d", s.clientID)))
	case "ronsql_database":
		if len(parts) < 2 {
			if s.ronsqlDatabase == "" {
				fmt.Println("RonSQL database: (not set)")
			} else {
				fmt.Printf("RonSQL database: %s\n", s.ronsqlDatabase)
			}
			return nil
		}
		s.ronsqlDatabase = parts[1]
		fmt.Println(ui.Success(fmt.Sprintf("RonSQL database set to %s", s.ronsqlDatabase)))
	case "ronsql_format":
		if len(parts) < 2 {
			fmt.Printf("RonSQL format: %s\n", s.ronsqlFormat)
			return nil
		}
		format := strings.ToUpper(parts[1])
		if format != "JSON" && format != "JSON_ASCII" && format != "TEXT" && format != "TEXT_NOHEADER" {
			return fmt.Errorf("invalid format: use JSON, JSON_ASCII, TEXT, or TEXT_NOHEADER")
		}
		s.ronsqlFormat = format
		fmt.Println(ui.Success(fmt.Sprintf("RonSQL format set to %s", s.ronsqlFormat)))
	case "ronsql_explain":
		if len(parts) < 2 {
			fmt.Printf("RonSQL explain mode: %s\n", s.ronsqlExplain)
			return nil
		}
		mode := strings.ToUpper(parts[1])
		if mode != "ALLOW" && mode != "FORBID" && mode != "REQUIRE" && mode != "REMOVE" && mode != "FORCE" {
			return fmt.Errorf("invalid explain mode: use ALLOW, FORBID, REQUIRE, REMOVE, or FORCE")
		}
		s.ronsqlExplain = mode
		fmt.Println(ui.Success(fmt.Sprintf("RonSQL explain mode set to %s", s.ronsqlExplain)))
	case "tables":
		return s.listTables()
	case "demo":
		return s.runDemo()
	case "load_rondis":
		if s.rondisClient == nil {
			return fmt.Errorf("Rondis not connected. Cannot run load_rondis.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		return s.runLoadRondis(numThreads, numOps, rowsPerOp)
	case "load_sql":
		if s.mysqlClient == nil {
			return fmt.Errorf("MySQL not connected. Cannot run load_sql.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		return s.runLoadSQL(numThreads, numOps, rowsPerOp)
	case "del_sql":
		if s.mysqlClient == nil {
			return fmt.Errorf("MySQL not connected. Cannot run del_sql.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		return s.runDelSQL(numThreads, numOps, rowsPerOp)
	case "drop_sql":
		if s.mysqlClient == nil {
			return fmt.Errorf("MySQL not connected. Cannot run drop_sql.")
		}
		return s.runDropSQL()
	case "bench_sql":
		if s.mysqlClient == nil {
			return fmt.Errorf("MySQL not connected. Cannot run bench_sql.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		writePct := 0 // default: 100% reads
		if len(parts) > 4 {
			n, err := strconv.Atoi(parts[4])
			if err != nil || n < 0 || n > 100 {
				return fmt.Errorf("invalid write percentage (0-100): %s", parts[4])
			}
			writePct = n
		}
		return s.runBenchSQL(numThreads, numOps, rowsPerOp, writePct)
	case "bench_sql_cont":
		if s.mysqlClient == nil {
			return fmt.Errorf("MySQL not connected. Cannot run bench_sql_cont.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		writePct := 0 // default: 100% reads
		if len(parts) > 4 {
			n, err := strconv.Atoi(parts[4])
			if err != nil || n < 0 || n > 100 {
				return fmt.Errorf("invalid write percentage (0-100): %s", parts[4])
			}
			writePct = n
		}
		durationSec := 60 // default: 60 seconds
		if len(parts) > 5 {
			n, err := strconv.Atoi(parts[5])
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid duration in seconds: %s", parts[5])
			}
			durationSec = n
		}
		return s.runBenchSQLCont(numThreads, numOps, rowsPerOp, writePct, durationSec)
	case "bench_sql_scan":
		if s.mysqlClient == nil {
			return fmt.Errorf("MySQL not connected. Cannot run bench_sql_scan.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		return s.runBenchSQLScan(numThreads, numOps, rowsPerOp)
	case "bench_sql_scan_cont":
		if s.mysqlClient == nil {
			return fmt.Errorf("MySQL not connected. Cannot run bench_sql_scan_cont.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		durationSec := 60 // default: 60 seconds
		if len(parts) > 4 {
			n, err := strconv.Atoi(parts[4])
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid duration in seconds: %s", parts[4])
			}
			durationSec = n
		}
		return s.runBenchSQLScanCont(numThreads, numOps, rowsPerOp, durationSec)
	case "bench_rondis":
		if s.rondisClient == nil {
			return fmt.Errorf("Rondis not connected. Cannot run bench_rondis.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		writePct := 0 // default: 100% reads
		if len(parts) > 4 {
			n, err := strconv.Atoi(parts[4])
			if err != nil || n < 0 || n > 100 {
				return fmt.Errorf("invalid write percentage (0-100): %s", parts[4])
			}
			writePct = n
		}
		return s.runBenchRondis(numThreads, numOps, rowsPerOp, writePct)
	case "bench_rondis_cont":
		if s.rondisClient == nil {
			return fmt.Errorf("Rondis not connected. Cannot run bench_rondis_cont.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		writePct := 0 // default: 100% reads
		if len(parts) > 4 {
			n, err := strconv.Atoi(parts[4])
			if err != nil || n < 0 || n > 100 {
				return fmt.Errorf("invalid write percentage (0-100): %s", parts[4])
			}
			writePct = n
		}
		durationSec := 60 // default: 60 seconds
		if len(parts) > 5 {
			n, err := strconv.Atoi(parts[5])
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid duration in seconds: %s", parts[5])
			}
			durationSec = n
		}
		return s.runBenchRondisCont(numThreads, numOps, rowsPerOp, writePct, durationSec)
	case "del_rondis":
		if s.rondisClient == nil {
			return fmt.Errorf("Rondis not connected. Cannot run del_rondis.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		return s.runDelRondis(numThreads, numOps, rowsPerOp)
	case "bench_rdrs":
		if s.restClient == nil {
			return fmt.Errorf("REST API not connected. Cannot run bench_rdrs.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		return s.runBenchRDRS(numThreads, numOps, rowsPerOp)
	case "bench_rdrs_cont":
		if s.restClient == nil {
			return fmt.Errorf("REST API not connected. Cannot run bench_rdrs_cont.")
		}
		numThreads, numOps, rowsPerOp, err := parseBenchParams(parts)
		if err != nil {
			return err
		}
		durationSec := 60 // default: 60 seconds
		if len(parts) > 4 {
			n, err := strconv.Atoi(parts[4])
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid duration in seconds: %s", parts[4])
			}
			durationSec = n
		}
		return s.runBenchRDRSCont(numThreads, numOps, rowsPerOp, durationSec)
	case "browse", "ui":
		if s.mysqlClient == nil {
			return fmt.Errorf("MySQL not connected. Cannot run browse/ui.")
		}
		return tui.Run(s.mysqlClient)
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
	if len(args) == 0 || args[0] == "" {
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
	if s.mysqlClient == nil {
		return fmt.Errorf("MySQL not connected. Use --no-mysql=false or restart without --no-mysql.")
	}

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

// RonSQLRequest represents a RonSQL query request
type RonSQLRequest struct {
	Query        string `json:"query"`
	Database     string `json:"database,omitempty"`
	ExplainMode  string `json:"explainMode,omitempty"`
	OutputFormat string `json:"outputFormat,omitempty"`
	OperationID  string `json:"operationId,omitempty"`
}

// executeRonSQL handles RONSQL commands via REST API
func (s *Shell) executeRonSQL(line string) error {
	// Remove "RONSQL" prefix (case-insensitive) - handle both "ronsql" and "ronsql ..."
	var rest string
	if len(line) > 7 {
		rest = strings.TrimSpace(line[7:])
	}

	// Tokenize for robust command parsing (handles extra spaces)
	tokens := strings.Fields(rest)
	if len(tokens) == 0 {
		return fmt.Errorf("RONSQL requires a command (e.g., RONSQL SET DATABASE <name> or RONSQL SELECT ...)")
	}

	// Handle RONSQL SET DATABASE <name> (doesn't need REST client)
	if len(tokens) >= 3 && strings.EqualFold(tokens[0], "set") && strings.EqualFold(tokens[1], "database") {
		dbName := tokens[2]
		if dbName == "" {
			return fmt.Errorf("database name required")
		}
		s.ronsqlDatabase = dbName
		fmt.Println(ui.Success(fmt.Sprintf("RonSQL database set to %s", s.ronsqlDatabase)))
		return nil
	}

	// For actual queries, we need REST client
	if s.restClient == nil {
		return fmt.Errorf("REST API not connected. RonSQL requires REST API.")
	}

	// Use query as-is, explainMode controlled by .ronsql_explain setting
	query := rest
	if query == "" {
		return fmt.Errorf("RONSQL requires a query")
	}

	// RonSQL requires queries to end with semicolon
	if !strings.HasSuffix(query, ";") {
		query = query + ";"
	}

	// Build request
	req := RonSQLRequest{
		Query:        query,
		Database:     s.ronsqlDatabase,
		ExplainMode:  s.ronsqlExplain,
		OutputFormat: s.ronsqlFormat,
	}

	// Print JSON request
	reqJSON, _ := json.MarshalIndent(req, "", "  ")
	fmt.Println(ui.Info("Request:"))
	fmt.Println(string(reqJSON))
	fmt.Println()

	// Send request
	endpoint := "/0.1.0/ronsql"
	data, duration, err := s.restClient.Post(endpoint, req)

	// Print JSON response (even on error, as it may contain useful info)
	fmt.Println(ui.Info("Response:"))
	if len(data) > 0 {
		fmt.Println(client.PrettyJSON(data))
	}
	fmt.Println(ui.Timing(duration))

	if err != nil {
		return fmt.Errorf("RonSQL error: %w", err)
	}
	return nil
}

// executeREAD handles single READ commands via REST API
func (s *Shell) executeREAD(line string) error {
	if s.restClient == nil {
		return fmt.Errorf("REST API not connected. Use --no-rdrs=false or restart without --no-rdrs.")
	}

	database, table, req, err := dsl.ParseSingleRead(line)
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	endpoint := fmt.Sprintf("/0.1.0/%s/%s/pk-read", database, table)

	// Pretty print the request
	reqJSON, _ := json.MarshalIndent(req, "", "  ")
	fmt.Println(ui.Info(fmt.Sprintf("POST %s", endpoint)))
	fmt.Println(string(reqJSON))
	fmt.Println()

	data, duration, err := s.restClient.Post(endpoint, req)
	if err != nil {
		return err
	}

	fmt.Println(ui.Info("Response:"))
	fmt.Println(client.PrettyJSON(data))
	fmt.Println(ui.Timing(duration))
	return nil
}

// executeBatch handles BATCH commands via REST API (single-line or multi-line)
// Single-line: end with ; (e.g., BATCH db.table b READ FILTER a=1, READ FILTER a=2;)
// Multi-line: end with ; on its own line
// Use , to separate READ operations in header syntax
func (s *Shell) executeBatch(line string) error {
	if s.restClient == nil {
		return fmt.Errorf("REST API not connected. Use --no-rdrs=false or restart without --no-rdrs.")
	}

	var lines []string

	// Check if there's content after BATCH on the same line
	content := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(line, "BATCH"), "batch"))

	// Check if line ends with ; (single-line complete marker)
	if content != "" && strings.HasSuffix(content, ";") {
		// Single-line mode: remove trailing ;
		content = strings.TrimSuffix(content, ";")
		content = strings.TrimSpace(content)
		lines = []string{content}
	} else if content != "" {
		// Multi-line mode starting with content: read more until ';' on its own line
		fmt.Println(ui.Info("Continue BATCH input. End with ';' on its own line."))
		moreLines, err := s.readBatchLines()
		if err != nil {
			return err
		}
		lines = append([]string{content}, moreLines...)
	} else {
		// Multi-line mode: read until ';' on its own line
		fmt.Println(ui.Info("Entering BATCH mode. End with ';' on its own line."))
		var err error
		lines, err = s.readBatchLines()
		if err != nil {
			return err
		}
	}

	req, err := dsl.ParseBatch(lines)
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	endpoint := "/0.1.0/batch"

	// Pretty print the request
	reqJSON, _ := json.MarshalIndent(req, "", "  ")
	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("POST %s", endpoint)))
	fmt.Println(string(reqJSON))
	fmt.Println()

	data, duration, err := s.restClient.Post(endpoint, req)
	if err != nil {
		return err
	}

	fmt.Println(ui.Info("Response:"))
	fmt.Println(client.PrettyJSON(data))
	fmt.Println(ui.Timing(duration))
	return nil
}

// readBatchLines reads lines until a line containing only ';' is entered
func (s *Shell) readBatchLines() ([]string, error) {
	var lines []string

	// Change prompt for multi-line input
	originalPrompt := s.rl.Config.Prompt
	s.rl.SetPrompt("batch> ")
	defer s.rl.SetPrompt(originalPrompt)

	for {
		line, err := s.rl.Readline()
		if err == readline.ErrInterrupt {
			return nil, fmt.Errorf("batch input cancelled")
		} else if err != nil {
			return nil, err
		}

		trimmed := strings.TrimSpace(line)

		// Check for end marker
		if trimmed == ";" {
			break
		}

		lines = append(lines, line)
	}

	if len(lines) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	return lines, nil
}

func (s *Shell) runDemo() error {
	if s.rondisClient == nil {
		return fmt.Errorf("Rondis not connected. Demo requires Rondis.")
	}

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
	fmt.Println("Writing 5 records via Rondis...")
	for _, u := range users {
		_, duration, err := s.rondisClient.Execute([]string{"SET", u.key, u.value})
		if err != nil {
			return err
		}
		fmt.Printf("   SET %s %s\n", ui.Key(u.key), ui.Timing(duration))
	}
	fmt.Println()

	// Read with Rondis
	fmt.Println("Reading back via Rondis...")
	for _, u := range users {
		result, duration, err := s.rondisClient.Execute([]string{"GET", u.key})
		if err != nil {
			return err
		}
		fmt.Printf("   GET %s -> %s %s\n", ui.Key(u.key), result, ui.Timing(duration))
	}
	fmt.Println()

	// Query with SQL - THE magic moment
	fmt.Println("Now querying the SAME data via SQL...")
	columns, rows, duration, err := s.mysqlClient.Query("SELECT redis_key, value_start FROM redis_0.string_keys WHERE redis_key LIKE 'demo:user:%'")
	if err != nil {
		return err
	}
	output := ui.RenderSQLResultWithDuration(columns, rows, duration)
	fmt.Print(output)
	fmt.Println()

	fmt.Println(ui.Success("Demo complete!"))
	fmt.Println()
	fmt.Println("Data persisted. Try these:")
	fmt.Println("   SELECT * FROM redis_0.string_keys WHERE redis_key LIKE 'demo:%'")
	fmt.Println("   GET demo:user:1")
	fmt.Println()

	return nil
}

// LatencyCollector collects latencies from multiple goroutines
type LatencyCollector struct {
	mu              sync.Mutex
	intervalLatencies []time.Duration
	totalLatencies    []time.Duration
}

// NewLatencyCollector creates a new latency collector
func NewLatencyCollector() *LatencyCollector {
	return &LatencyCollector{
		intervalLatencies: make([]time.Duration, 0, 10000),
		totalLatencies:    make([]time.Duration, 0, 100000),
	}
}

// Record adds a latency measurement
func (lc *LatencyCollector) Record(d time.Duration) {
	lc.mu.Lock()
	lc.intervalLatencies = append(lc.intervalLatencies, d)
	lc.totalLatencies = append(lc.totalLatencies, d)
	lc.mu.Unlock()
}

// GetIntervalStats returns interval stats and resets interval buffer
// Returns: min, max, avg, p99, count
func (lc *LatencyCollector) GetIntervalStats() (min, max, avg, p99 time.Duration, count int) {
	lc.mu.Lock()
	latencies := lc.intervalLatencies
	lc.intervalLatencies = make([]time.Duration, 0, 10000)
	lc.mu.Unlock()

	return calculateLatencyStats(latencies)
}

// GetTotalStats returns total stats
// Returns: min, max, avg, p95, p99, p999, count
func (lc *LatencyCollector) GetTotalStats() (min, max, avg, p95, p99, p999 time.Duration, count int) {
	lc.mu.Lock()
	latencies := make([]time.Duration, len(lc.totalLatencies))
	copy(latencies, lc.totalLatencies)
	lc.mu.Unlock()

	minV, maxV, avgV, p99V, cnt := calculateLatencyStats(latencies)
	if cnt == 0 {
		return 0, 0, 0, 0, 0, 0, 0
	}

	// Sort for percentile calculation
	sortDurations(latencies)
	p95V := percentile(latencies, 95)
	p999V := percentile(latencies, 99.9)

	return minV, maxV, avgV, p95V, p99V, p999V, cnt
}

// Maximum number of errors to store in benchmarks
const maxBenchmarkErrors = 100

// ErrorCollector collects errors from benchmark runs (thread-safe, limited)
type ErrorCollector struct {
	mu     sync.Mutex
	errors []string
	count  int64 // Total error count (may exceed len(errors))
}

// NewErrorCollector creates a new error collector
func NewErrorCollector() *ErrorCollector {
	return &ErrorCollector{
		errors: make([]string, 0, maxBenchmarkErrors),
	}
}

// Record adds an error to the collection (thread-safe)
// Returns true if the error was stored, false if limit reached
func (ec *ErrorCollector) Record(err error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.count++
	if len(ec.errors) < maxBenchmarkErrors {
		ec.errors = append(ec.errors, err.Error())
	}
}

// Count returns the total number of errors (may exceed stored errors)
func (ec *ErrorCollector) Count() int64 {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return ec.count
}

// GetErrors returns a copy of the stored errors
func (ec *ErrorCollector) GetErrors() []string {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	result := make([]string, len(ec.errors))
	copy(result, ec.errors)
	return result
}

// PrintErrors prints all collected errors if any exist
func (ec *ErrorCollector) PrintErrors() {
	ec.mu.Lock()
	errCount := ec.count
	storedErrors := make([]string, len(ec.errors))
	copy(storedErrors, ec.errors)
	ec.mu.Unlock()

	if errCount == 0 {
		return
	}

	fmt.Println()
	fmt.Println(ui.Error(fmt.Sprintf("Errors encountered (%d total):", errCount)))
	for i, errMsg := range storedErrors {
		fmt.Printf("   %d. %s\n", i+1, errMsg)
	}
	if errCount > int64(len(storedErrors)) {
		fmt.Printf("   ... and %d more errors not shown\n", errCount-int64(len(storedErrors)))
	}
}

// calculateLatencyStats calculates min, max, avg, p99 from latencies
func calculateLatencyStats(latencies []time.Duration) (min, max, avg, p99 time.Duration, count int) {
	count = len(latencies)
	if count == 0 {
		return 0, 0, 0, 0, 0
	}

	// Sort for percentile calculation
	sortDurations(latencies)

	min = latencies[0]
	max = latencies[count-1]

	var total time.Duration
	for _, d := range latencies {
		total += d
	}
	avg = total / time.Duration(count)

	p99 = percentile(latencies, 99)

	return min, max, avg, p99, count
}

// sortDurations sorts a slice of durations in place
func sortDurations(d []time.Duration) {
	// Simple insertion sort for small slices, otherwise use stdlib
	if len(d) < 100 {
		for i := 1; i < len(d); i++ {
			for j := i; j > 0 && d[j] < d[j-1]; j-- {
				d[j], d[j-1] = d[j-1], d[j]
			}
		}
	} else {
		// Use a proper sort
		for i := 0; i < len(d); i++ {
			minIdx := i
			for j := i + 1; j < len(d); j++ {
				if d[j] < d[minIdx] {
					minIdx = j
				}
			}
			d[i], d[minIdx] = d[minIdx], d[i]
		}
	}
}

// percentile returns the p-th percentile from sorted durations
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p / 100)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// formatLatency formats a duration for display
func formatLatency(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fus", float64(d.Nanoseconds())/1000)
	}
	return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1000000)
}

// parseBenchParams parses the common parameters for benchmark commands
func parseBenchParams(parts []string) (numThreads, numOps, rowsPerOp int, err error) {
	numThreads = 4
	numOps = 100
	rowsPerOp = 10
	if len(parts) > 1 {
		n, err := strconv.Atoi(parts[1])
		if err != nil || n <= 0 {
			return 0, 0, 0, fmt.Errorf("invalid number of threads: %s", parts[1])
		}
		numThreads = n
	}
	if len(parts) > 2 {
		n, err := strconv.Atoi(parts[2])
		if err != nil || n <= 0 {
			return 0, 0, 0, fmt.Errorf("invalid number of requests: %s", parts[2])
		}
		numOps = n
	}
	if len(parts) > 3 {
		n, err := strconv.Atoi(parts[3])
		if err != nil || n <= 0 {
			return 0, 0, 0, fmt.Errorf("invalid rows per request: %s", parts[3])
		}
		rowsPerOp = n
	}
	return numThreads, numOps, rowsPerOp, nil
}

// createRondisClients creates multiple Rondis clients for parallel operations
func (s *Shell) createRondisClients(numThreads int) ([]*client.RondisClient, error) {
	clients := make([]*client.RondisClient, numThreads)
	if s.verbose >= 1 {
		fmt.Printf("[*] Creating %d Rondis connections to %s:%d...\n", numThreads, s.config.Host, s.config.RondisPort)
	}
	for i := 0; i < numThreads; i++ {
		c, err := client.NewRondisClientWithOptions(client.RondisOptions{
			Host: s.config.Host,
			Port: s.config.RondisPort,
			TLS:  s.config.TLS,
		})
		if err != nil {
			// Close already created clients
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return nil, fmt.Errorf("failed to create client for thread %d: %w", i, err)
		}
		clients[i] = c
	}
	if s.verbose >= 1 {
		fmt.Printf("[*] %d Rondis connections established\n", numThreads)
	}
	return clients, nil
}

// runLoadRondis loads data using MSET/SET
func (s *Shell) runLoadRondis(numThreads int, numOps int, rowsPerOp int) error {
	if s.rondisClient == nil {
		return fmt.Errorf("Rondis not connected. Load requires Rondis.")
	}

	totalOps := numThreads * numOps
	totalKeys := totalOps * rowsPerOp
	useMulti := rowsPerOp > 1

	fmt.Println()
	if totalKeys > 10000 {
		fmt.Println(ui.Warning(fmt.Sprintf("Loading %d total keys - this may take a while...", totalKeys)))
	}
	if useMulti {
		fmt.Println(ui.Info(fmt.Sprintf("Loading data: %d threads × %d requests × %d rows = %d total keys", numThreads, numOps, rowsPerOp, totalKeys)))
	} else {
		fmt.Println(ui.Info(fmt.Sprintf("Loading data: %d threads × %d requests = %d total keys", numThreads, numOps, totalKeys)))
	}
	fmt.Println()

	clients, err := s.createRondisClients(numThreads)
	if err != nil {
		return err
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d Rondis connections closed\n", numThreads)
		}
	}()

	if useMulti {
		fmt.Printf("Writing %d keys via MSET (%d threads × %d calls × %d rows)...\n", totalKeys, numThreads, numOps, rowsPerOp)
	} else {
		fmt.Printf("Writing %d keys via SET (%d threads × %d calls)...\n", totalKeys, numThreads, numOps)
	}

	var completedOps int64
	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()
	writeStart := time.Now()

	// Progress reporting goroutine
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ops := atomic.LoadInt64(&completedOps)
				keys := ops * int64(rowsPerOp)
				elapsed := time.Since(writeStart)
				keysPerSec := float64(keys) / elapsed.Seconds()
				pct := float64(ops) / float64(totalOps) * 100
				fmt.Printf("   Progress: %d/%d ops (%.1f%%), %d keys, %.0f keys/sec\n", ops, totalOps, pct, keys, keysPerSec)
			case <-stopProgress:
				return
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, rondisClient *client.RondisClient) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				if useMulti {
					args := []string{"MSET"}
					for j := 0; j < rowsPerOp; j++ {
						key := fmt.Sprintf("bench:key:%d:%d:%d:%d", clientID, threadID, i, j)
						value := fmt.Sprintf(`{"client":%d,"id":%d,"key":%d,"row":%d,"data":"benchmark test data"}`, clientID, threadID, i, j)
						args = append(args, key, value)
					}
					if debugMode {
						fmt.Printf("[DEBUG] REQ: %v\n", args)
					}
					opStart := time.Now()
					resp, _, err := rondisClient.Execute(args)
					latencyCollector.Record(time.Since(opStart))
					if debugMode {
						fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
					}
					if err != nil {
						errorCollector.Record(err)
					}
				} else {
					key := fmt.Sprintf("bench:key:%d:%d:%d:%d", clientID, threadID, i, 0)
					value := fmt.Sprintf(`{"client":%d,"id":%d,"key":%d,"row":%d,"data":"benchmark test data"}`, clientID, threadID, i, 0)
					args := []string{"SET", key, value}
					if debugMode {
						fmt.Printf("[DEBUG] REQ: %v\n", args)
					}
					opStart := time.Now()
					resp, _, err := rondisClient.Execute(args)
					latencyCollector.Record(time.Since(opStart))
					if debugMode {
						fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
					}
					if err != nil {
						errorCollector.Record(err)
					}
				}
				atomic.AddInt64(&completedOps, 1)
			}
		}(t, clients[t])
	}
	wg.Wait()
	close(stopProgress)
	writeDuration := time.Since(writeStart)

	// Get latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	writeKeysPerSec := float64(totalKeys) / writeDuration.Seconds()
	writeOpsPerSec := float64(totalOps) / writeDuration.Seconds()
	if useMulti {
		fmt.Printf("   %d keys in %v (%.0f keys/sec, %.0f MSET/sec)\n", totalKeys, writeDuration.Round(time.Millisecond), writeKeysPerSec, writeOpsPerSec)
	} else {
		fmt.Printf("   %d keys in %v (%.0f SET/sec)\n", totalKeys, writeDuration.Round(time.Millisecond), writeOpsPerSec)
	}
	errCount := errorCollector.Count()
	if errCount > 0 {
		fmt.Printf("   %d write errors\n", errCount)
	}
	fmt.Println()

	// Results
	fmt.Println(ui.Success("Load complete!"))
	fmt.Printf("   Configuration: %d threads × %d requests × %d rows = %d total keys\n", numThreads, numOps, rowsPerOp, totalKeys)
	if useMulti {
		fmt.Printf("   Writes: %.0f keys/sec (%.0f MSET/sec)\n", writeKeysPerSec, writeOpsPerSec)
	} else {
		fmt.Printf("   Writes: %.0f SET/sec\n", writeOpsPerSec)
	}
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runLoadSQL loads data into SQL table using INSERT statements
func (s *Shell) runLoadSQL(numThreads int, numOps int, rowsPerOp int) error {
	totalOps := numThreads * numOps
	totalKeys := totalOps * rowsPerOp

	fmt.Println()
	if totalKeys > 10000 {
		fmt.Println(ui.Warning(fmt.Sprintf("Loading %d total rows via SQL - this may take a while...", totalKeys)))
	}
	fmt.Println(ui.Info(fmt.Sprintf("Loading SQL data: %d threads × %d requests × %d rows = %d total rows", numThreads, numOps, rowsPerOp, totalKeys)))
	fmt.Println()

	// Ensure test database exists
	fmt.Println("Creating test database and table...")
	_, _, err := s.mysqlClient.Execute("CREATE DATABASE IF NOT EXISTS test")
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create the sql_test table
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS test.sql_test (
			user_id VARCHAR(64) NOT NULL,
			event_time BIGINT NOT NULL,
			description VARCHAR(128),
			value_int BIGINT DEFAULT 0,
			event_type BIGINT DEFAULT 0,
			PRIMARY KEY (user_id, event_time)
		) ENGINE=NDB`
	_, _, err = s.mysqlClient.Execute(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	fmt.Println("   Database and table ready")
	fmt.Println()

	// Create MySQL clients for each thread
	clients := make([]*client.MySQLClient, numThreads)
	if s.verbose >= 1 {
		fmt.Printf("[*] Creating %d MySQL connections to %s:%d...\n", numThreads, s.config.MySQLHost, s.config.MySQLPort)
	}
	for i := 0; i < numThreads; i++ {
		c, err := client.NewMySQLClientWithOptions(client.MySQLOptions{
			Host:     s.config.MySQLHost,
			Port:     s.config.MySQLPort,
			User:     s.mysqlUser,
			Password: s.mysqlPass,
			TLS:      s.config.TLS,
		})
		if err != nil {
			// Close already created clients
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create MySQL client for thread %d: %w", i, err)
		}
		clients[i] = c
	}
	if s.verbose >= 1 {
		fmt.Printf("[*] %d MySQL connections established\n", numThreads)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d MySQL connections closed\n", numThreads)
		}
	}()

	fmt.Printf("Inserting %d rows (%d threads × %d requests × %d rows)...\n", totalKeys, numThreads, numOps, rowsPerOp)

	errorCollector := NewErrorCollector()
	var completedOps int64
	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	writeStart := time.Now()

	// Progress reporting goroutine
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ops := atomic.LoadInt64(&completedOps)
				rows := ops * int64(rowsPerOp)
				elapsed := time.Since(writeStart)
				rowsPerSec := float64(rows) / elapsed.Seconds()
				pct := float64(ops) / float64(totalOps) * 100
				fmt.Printf("   Progress: %d/%d ops (%.1f%%), %d rows, %.0f rows/sec\n", ops, totalOps, pct, rows, rowsPerSec)
			case <-stopProgress:
				return
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, mysqlClient *client.MySQLClient) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				// Build batch INSERT for rowsPerOp rows
				// user_id = bench:key:threadid:requestId, event_time = row within request
				userID := fmt.Sprintf("bench:key:%d:%d:%d", clientID, threadID, i)
				var values []string
				for j := 0; j < rowsPerOp; j++ {
					// id=threadID, key=requestId (second part of user_id), row=event_time
					description := fmt.Sprintf(`{"client":%d,"id":%d,"key":%d,"row":%d,"data":"benchmark test data"}`, clientID, threadID, i, j)
					eventTime := j // row id within the request
					values = append(values, fmt.Sprintf("('%s', %d, '%s', 0, 0)", userID, eventTime, description))
				}
				insertSQL := fmt.Sprintf("INSERT INTO test.sql_test (user_id, event_time, description, value_int, event_type) VALUES %s", strings.Join(values, ", "))
				if debugMode {
					fmt.Printf("[DEBUG] SQL: %s\n", insertSQL)
				}
				opStart := time.Now()
				_, _, err := mysqlClient.Execute(insertSQL)
				latencyCollector.Record(time.Since(opStart))
				if debugMode {
					fmt.Printf("[DEBUG] Result: err=%v\n", err)
				}
				if err != nil {
					errorCollector.Record(err)
				}
				atomic.AddInt64(&completedOps, 1)
			}
		}(t, clients[t])
	}
	wg.Wait()
	close(stopProgress)
	writeDuration := time.Since(writeStart)

	// Get latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	writeRowsPerSec := float64(totalKeys) / writeDuration.Seconds()
	writeOpsPerSec := float64(totalOps) / writeDuration.Seconds()
	fmt.Printf("   %d rows in %v (%.0f rows/sec, %.0f INSERT/sec)\n", totalKeys, writeDuration.Round(time.Millisecond), writeRowsPerSec, writeOpsPerSec)
	writeErrors := errorCollector.Count()
	if writeErrors > 0 {
		fmt.Printf("   %d write errors\n", writeErrors)
	}
	fmt.Println()

	// Results
	fmt.Println(ui.Success("SQL Load complete!"))
	fmt.Printf("   Configuration: %d threads × %d requests × %d rows = %d total rows\n", numThreads, numOps, rowsPerOp, totalKeys)
	fmt.Printf("   Writes: %.0f rows/sec (%.0f INSERT/sec)\n", writeRowsPerSec, writeOpsPerSec)
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runDelSQL deletes rows generated by runLoadSQL from test.sql_test
func (s *Shell) runDelSQL(numThreads int, numOps int, rowsPerOp int) error {
	if s.mysqlClient == nil {
		return fmt.Errorf("MySQL not connected. Delete requires MySQL.")
	}

	totalOps := numThreads * numOps
	totalKeys := totalOps * rowsPerOp

	fmt.Println()
	if totalKeys > 10000 {
		fmt.Println(ui.Warning(fmt.Sprintf("Deleting %d total rows via SQL - this may take a while...", totalKeys)))
	}
	fmt.Println(ui.Info(fmt.Sprintf("Deleting SQL data: %d threads × %d requests × %d rows = %d total rows", numThreads, numOps, rowsPerOp, totalKeys)))
	fmt.Println()

	// Create MySQL clients for each thread
	clients := make([]*client.MySQLClient, numThreads)
	if s.verbose >= 1 {
		fmt.Printf("[*] Creating %d MySQL connections to %s:%d...\n", numThreads, s.config.MySQLHost, s.config.MySQLPort)
	}
	for i := 0; i < numThreads; i++ {
		c, err := client.NewMySQLClientWithOptions(client.MySQLOptions{
			Host:     s.config.MySQLHost,
			Port:     s.config.MySQLPort,
			User:     s.mysqlUser,
			Password: s.mysqlPass,
			TLS:      s.config.TLS,
		})
		if err != nil {
			// Close already created clients
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create MySQL client for thread %d: %w", i, err)
		}
		clients[i] = c
	}
	if s.verbose >= 1 {
		fmt.Printf("[*] %d MySQL connections established\n", numThreads)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d MySQL connections closed\n", numThreads)
		}
	}()

	fmt.Printf("Deleting %d rows (%d threads × %d DELETE statements)...\n", totalKeys, numThreads, numOps)

	errorCollector := NewErrorCollector()
	var completedOps int64
	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	delStart := time.Now()

	// Progress reporting goroutine
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ops := atomic.LoadInt64(&completedOps)
				rows := ops * int64(rowsPerOp)
				errs := errorCollector.Count()
				elapsed := time.Since(delStart)
				rowsPerSec := float64(rows) / elapsed.Seconds()
				pct := float64(ops) / float64(totalOps) * 100
				fmt.Printf("   Progress: %d/%d ops (%.1f%%), %d rows deleted, errors=%d, %.0f rows/sec\n",
					ops, totalOps, pct, rows, errs, rowsPerSec)
			case <-stopProgress:
				return
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, mysqlClient *client.MySQLClient) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				// Delete by user_id - this deletes all rowsPerOp rows with that user_id
				userID := fmt.Sprintf("bench:key:%d:%d:%d", clientID, threadID, i)
				deleteSQL := fmt.Sprintf("DELETE FROM test.sql_test WHERE user_id = '%s'", userID)
				if debugMode {
					fmt.Printf("[DEBUG] SQL: %s\n", deleteSQL)
				}
				opStart := time.Now()
				_, _, err := mysqlClient.Execute(deleteSQL)
				latencyCollector.Record(time.Since(opStart))
				if debugMode {
					fmt.Printf("[DEBUG] Result: err=%v\n", err)
				}
				if err != nil {
					errorCollector.Record(err)
				}
				atomic.AddInt64(&completedOps, 1)
			}
		}(t, clients[t])
	}
	wg.Wait()
	close(stopProgress)
	delDuration := time.Since(delStart)

	// Get latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	delRowsPerSec := float64(totalKeys) / delDuration.Seconds()
	delOpsPerSec := float64(totalOps) / delDuration.Seconds()
	fmt.Printf("   %d rows in %v (%.0f rows/sec, %.0f DELETE/sec)\n", totalKeys, delDuration.Round(time.Millisecond), delRowsPerSec, delOpsPerSec)
	delErrors := errorCollector.Count()
	if delErrors > 0 {
		fmt.Printf("   %d delete errors\n", delErrors)
	}
	fmt.Println()

	// Results
	fmt.Println(ui.Success("SQL Delete complete!"))
	fmt.Printf("   Configuration: %d threads × %d requests × %d rows = %d total rows\n", numThreads, numOps, rowsPerOp, totalKeys)
	fmt.Printf("   Deletes: %.0f rows/sec (%.0f DELETE/sec)\n", delRowsPerSec, delOpsPerSec)
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runDropSQL drops the sql_test table
func (s *Shell) runDropSQL() error {
	fmt.Println()
	fmt.Println("Dropping test.sql_test table...")

	_, _, err := s.mysqlClient.Execute("DROP TABLE IF EXISTS test.sql_test")
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	fmt.Println(ui.Success("Table test.sql_test dropped"))
	fmt.Println()

	return nil
}

// runBenchSQL benchmarks using SELECT and UPDATE operations on test.sql_test
func (s *Shell) runBenchSQL(numThreads int, numOps int, rowsPerOp int, writePct int) error {
	if s.mysqlClient == nil {
		return fmt.Errorf("MySQL not connected. Benchmark requires MySQL.")
	}

	totalOps := numThreads * numOps
	totalKeys := totalOps * rowsPerOp

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("SQL Benchmarking: %d threads × %d requests × %d rows = %d total rows (%d%% writes, %d%% reads)", numThreads, numOps, rowsPerOp, totalKeys, writePct, 100-writePct)))
	fmt.Println()

	// Create MySQL clients for each thread
	clients := make([]*client.MySQLClient, numThreads)
	if s.verbose >= 1 {
		fmt.Printf("[*] Creating %d MySQL connections to %s:%d...\n", numThreads, s.config.MySQLHost, s.config.MySQLPort)
	}
	for i := 0; i < numThreads; i++ {
		c, err := client.NewMySQLClientWithOptions(client.MySQLOptions{
			Host:     s.config.MySQLHost,
			Port:     s.config.MySQLPort,
			User:     s.mysqlUser,
			Password: s.mysqlPass,
			TLS:      s.config.TLS,
		})
		if err != nil {
			// Close already created clients
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create MySQL client for thread %d: %w", i, err)
		}
		clients[i] = c
	}
	if s.verbose >= 1 {
		fmt.Printf("[*] %d MySQL connections established\n", numThreads)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d MySQL connections closed\n", numThreads)
		}
	}()

	fmt.Printf("Running %d ops (%d threads × %d calls × %d rows)...\n", totalKeys, numThreads, numOps, rowsPerOp)

	var readOps int64
	var writeOps int64
	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()
	benchStart := time.Now()

	// Progress reporting goroutine
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				reads := atomic.LoadInt64(&readOps)
				writes := atomic.LoadInt64(&writeOps)
				errs := errorCollector.Count()
				ops := reads + writes
				elapsed := time.Since(benchStart)
				opsPerSec := float64(ops) / elapsed.Seconds()
				pct := float64(ops) / float64(totalOps) * 100
				fmt.Printf("   Progress: %d/%d ops (%.1f%%), reads=%d, writes=%d, errors=%d, %.0f ops/sec\n",
					ops, totalOps, pct, reads, writes, errs, opsPerSec)
			case <-stopProgress:
				return
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, mysqlClient *client.MySQLClient) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(threadID)))

			for i := 0; i < numOps; i++ {
				isWrite := rng.Intn(100) < writePct

				// Build user_id and event_time list
				userID := fmt.Sprintf("bench:key:%d:%d:%d", clientID, threadID, i)
				var eventTimes []string
				for j := 0; j < rowsPerOp; j++ {
					eventTimes = append(eventTimes, strconv.Itoa(j))
				}
				eventTimeList := strings.Join(eventTimes, ", ")

				if isWrite {
					// UPDATE
					description := fmt.Sprintf(`{"client":%d,"id":%d,"key":%d,"row":0,"data":"benchmark update"}`, clientID, threadID, i)
					updateSQL := fmt.Sprintf("UPDATE test.sql_test SET description = '%s', value_int = value_int + 1 WHERE user_id = '%s' AND event_time IN (%s)",
						description, userID, eventTimeList)
					if debugMode {
						fmt.Printf("[DEBUG] SQL: %s\n", updateSQL)
					}
					opStart := time.Now()
					_, _, err := mysqlClient.Execute(updateSQL)
					latencyCollector.Record(time.Since(opStart))
					if debugMode {
						fmt.Printf("[DEBUG] Result: err=%v\n", err)
					}
					if err != nil {
						errorCollector.Record(err)
					} else {
						atomic.AddInt64(&writeOps, 1)
					}
				} else {
					// SELECT all columns
					selectSQL := fmt.Sprintf("SELECT user_id, event_time, description, value_int, event_type FROM test.sql_test WHERE user_id = '%s' AND event_time IN (%s)",
						userID, eventTimeList)
					if debugMode {
						fmt.Printf("[DEBUG] SQL: %s\n", selectSQL)
					}
					opStart := time.Now()
					_, _, err := mysqlClient.Execute(selectSQL)
					latencyCollector.Record(time.Since(opStart))
					if debugMode {
						fmt.Printf("[DEBUG] Result: err=%v\n", err)
					}
					if err != nil {
						errorCollector.Record(err)
					} else {
						atomic.AddInt64(&readOps, 1)
					}
				}
			}
		}(t, clients[t])
	}
	wg.Wait()
	close(stopProgress)

	benchDuration := time.Since(benchStart)

	// Get latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	// Report results
	fmt.Println()
	totalOpsCompleted := readOps + writeOps
	totalRowsProcessed := totalOpsCompleted * int64(rowsPerOp)
	opsPerSec := float64(totalOpsCompleted) / benchDuration.Seconds()
	rowsPerSec := float64(totalRowsProcessed) / benchDuration.Seconds()

	fmt.Println(ui.Success(fmt.Sprintf("SQL Benchmark completed in %.2fs", benchDuration.Seconds())))
	fmt.Printf("   Reads:  %d operations (%d rows)\n", readOps, readOps*int64(rowsPerOp))
	fmt.Printf("   Writes: %d operations (%d rows)\n", writeOps, writeOps*int64(rowsPerOp))
	fmt.Printf("   Errors: %d\n", errorCollector.Count())
	fmt.Printf("   Throughput: %.0f ops/sec (%.0f rows/sec)\n", opsPerSec, rowsPerSec)
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runBenchSQLScan benchmarks using SELECT queries that scan by user_id only (no IN clause)
func (s *Shell) runBenchSQLScan(numThreads int, numOps int, rowsPerOp int) error {
	if s.mysqlClient == nil {
		return fmt.Errorf("MySQL not connected. Benchmark requires MySQL.")
	}

	totalOps := numThreads * numOps
	totalKeys := totalOps * rowsPerOp

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("SQL Scan Benchmark: %d threads × %d requests × %d rows = %d total rows (scan by user_id)", numThreads, numOps, rowsPerOp, totalKeys)))
	fmt.Println()

	// Create MySQL clients for each thread
	clients := make([]*client.MySQLClient, numThreads)
	if s.verbose >= 1 {
		fmt.Printf("[*] Creating %d MySQL connections to %s:%d...\n", numThreads, s.config.MySQLHost, s.config.MySQLPort)
	}
	for i := 0; i < numThreads; i++ {
		c, err := client.NewMySQLClientWithOptions(client.MySQLOptions{
			Host:     s.config.MySQLHost,
			Port:     s.config.MySQLPort,
			User:     s.mysqlUser,
			Password: s.mysqlPass,
			TLS:      s.config.TLS,
		})
		if err != nil {
			// Close already created clients
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create MySQL client for thread %d: %w", i, err)
		}
		clients[i] = c
	}
	if s.verbose >= 1 {
		fmt.Printf("[*] %d MySQL connections established\n", numThreads)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d MySQL connections closed\n", numThreads)
		}
	}()

	fmt.Printf("Running %d scan operations (%d threads × %d calls, each scanning %d rows)...\n", totalOps, numThreads, numOps, rowsPerOp)

	var readOps int64
	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()
	benchStart := time.Now()

	// Progress reporting goroutine
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ops := atomic.LoadInt64(&readOps)
				errs := errorCollector.Count()
				rows := ops * int64(rowsPerOp)
				elapsed := time.Since(benchStart)
				opsPerSec := float64(ops) / elapsed.Seconds()
				pct := float64(ops) / float64(totalOps) * 100
				fmt.Printf("   Progress: %d/%d ops (%.1f%%), %d rows, errors=%d, %.0f scans/sec\n",
					ops, totalOps, pct, rows, errs, opsPerSec)
			case <-stopProgress:
				return
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, mysqlClient *client.MySQLClient) {
			defer wg.Done()

			for i := 0; i < numOps; i++ {
				// SELECT using only user_id (scans all rows for that user_id)
				userID := fmt.Sprintf("bench:key:%d:%d:%d", clientID, threadID, i)
				selectSQL := fmt.Sprintf("SELECT user_id, event_time, description, value_int, event_type FROM test.sql_test WHERE user_id = '%s'", userID)
				if debugMode {
					fmt.Printf("[DEBUG] SQL: %s\n", selectSQL)
				}
				opStart := time.Now()
				_, _, err := mysqlClient.Execute(selectSQL)
				latencyCollector.Record(time.Since(opStart))
				if debugMode {
					fmt.Printf("[DEBUG] Result: err=%v\n", err)
				}
				if err != nil {
					errorCollector.Record(err)
				} else {
					atomic.AddInt64(&readOps, 1)
				}
			}
		}(t, clients[t])
	}
	wg.Wait()
	close(stopProgress)

	benchDuration := time.Since(benchStart)

	// Get latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	// Report results
	fmt.Println()
	totalRowsProcessed := readOps * int64(rowsPerOp)
	opsPerSec := float64(readOps) / benchDuration.Seconds()
	rowsPerSec := float64(totalRowsProcessed) / benchDuration.Seconds()

	fmt.Println(ui.Success(fmt.Sprintf("SQL Scan Benchmark completed in %.2fs", benchDuration.Seconds())))
	fmt.Printf("   Scans:  %d operations (%d rows scanned)\n", readOps, totalRowsProcessed)
	fmt.Printf("   Errors: %d\n", errorCollector.Count())
	fmt.Printf("   Throughput: %.0f scans/sec (%.0f rows/sec)\n", opsPerSec, rowsPerSec)
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runBenchSQLScanCont runs continuous SQL scan benchmark for specified duration
func (s *Shell) runBenchSQLScanCont(numThreads int, numOps int, rowsPerOp int, durationSec int) error {
	if s.mysqlClient == nil {
		return fmt.Errorf("MySQL not connected. Benchmark requires MySQL.")
	}

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("Continuous SQL Scan benchmark: %d threads, %d rows/scan, running for %d seconds",
		numThreads, rowsPerOp, durationSec)))
	fmt.Println()

	// Create MySQL clients for each thread
	clients := make([]*client.MySQLClient, numThreads)
	if s.verbose >= 1 {
		fmt.Printf("[*] Creating %d MySQL connections to %s:%d...\n", numThreads, s.config.MySQLHost, s.config.MySQLPort)
	}
	for i := 0; i < numThreads; i++ {
		c, err := client.NewMySQLClientWithOptions(client.MySQLOptions{
			Host:     s.config.MySQLHost,
			Port:     s.config.MySQLPort,
			User:     s.mysqlUser,
			Password: s.mysqlPass,
			TLS:      s.config.TLS,
		})
		if err != nil {
			// Close already created clients
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create MySQL client for thread %d: %w", i, err)
		}
		clients[i] = c
	}
	if s.verbose >= 1 {
		fmt.Printf("[*] %d MySQL connections established\n", numThreads)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d MySQL connections closed\n", numThreads)
		}
	}()

	fmt.Printf("Running for %d seconds (reporting every 10s)...\n", durationSec)
	fmt.Println()

	// Total counters
	var totalReadOps int64

	// Interval counters (for 10-second reporting)
	var intervalReadOps int64
	var intervalErrors int64

	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()

	// Use a channel to signal stop
	stopCh := make(chan struct{})

	benchStart := time.Now()

	// Start timer to close stopCh after duration
	go func() {
		time.Sleep(time.Duration(durationSec) * time.Second)
		close(stopCh)
	}()

	// Start reporting goroutine
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		intervalStart := time.Now()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				// Read and reset interval counters
				reads := atomic.SwapInt64(&intervalReadOps, 0)
				errs := atomic.SwapInt64(&intervalErrors, 0)

				// Get interval latency stats
				minLat, maxLat, avgLat, p99Lat, _ := latencyCollector.GetIntervalStats()

				intervalDuration := time.Since(intervalStart)
				intervalStart = time.Now()

				rows := reads * int64(rowsPerOp)
				scansPerSec := float64(reads) / intervalDuration.Seconds()
				rowsPerSec := float64(rows) / intervalDuration.Seconds()

				elapsed := time.Since(benchStart).Seconds()
				fmt.Printf("[%5.0fs] scans: %d, errors: %d, %.0f scans/sec (%.0f rows/sec), latency: min=%s avg=%s max=%s p99=%s\n",
					elapsed, reads, errs, scansPerSec, rowsPerSec,
					formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat), formatLatency(p99Lat))
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, mysqlClient *client.MySQLClient) {
			defer wg.Done()
			keyCounter := 0

			for {
				select {
				case <-stopCh:
					return
				default:
					// Use modulo to cycle through the same keys as .bench_sql_scan
					requestId := keyCounter % numOps
					keyCounter++

					// SELECT using only user_id (scans all rows for that user_id)
					userID := fmt.Sprintf("bench:key:%d:%d:%d", clientID, threadID, requestId)
					selectSQL := fmt.Sprintf("SELECT user_id, event_time, description, value_int, event_type FROM test.sql_test WHERE user_id = '%s'", userID)
					if debugMode {
						fmt.Printf("[DEBUG] SQL: %s\n", selectSQL)
					}
					opStart := time.Now()
					_, _, err := mysqlClient.Execute(selectSQL)
					latencyCollector.Record(time.Since(opStart))
					if debugMode {
						fmt.Printf("[DEBUG] Result: err=%v\n", err)
					}
					if err != nil {
						errorCollector.Record(err)
						atomic.AddInt64(&intervalErrors, 1)
					} else {
						atomic.AddInt64(&totalReadOps, 1)
						atomic.AddInt64(&intervalReadOps, 1)
					}
				}
			}
		}(t, clients[t])
	}
	wg.Wait()
	<-reportDone // Wait for reporting goroutine to finish

	benchDuration := time.Since(benchStart)

	// Get total latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	// Report total results
	fmt.Println()
	totalRowsProcessed := totalReadOps * int64(rowsPerOp)
	scansPerSec := float64(totalReadOps) / benchDuration.Seconds()
	rowsPerSec := float64(totalRowsProcessed) / benchDuration.Seconds()

	fmt.Println(ui.Success(fmt.Sprintf("Continuous SQL Scan Benchmark completed in %.2fs", benchDuration.Seconds())))
	fmt.Printf("   Total Scans:  %d operations (%d rows scanned)\n", totalReadOps, totalRowsProcessed)
	fmt.Printf("   Total Errors: %d\n", errorCollector.Count())
	fmt.Printf("   Avg Throughput: %.0f scans/sec (%.0f rows/sec)\n", scansPerSec, rowsPerSec)
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runBenchSQLCont runs continuous SQL benchmark for specified duration
func (s *Shell) runBenchSQLCont(numThreads int, numOps int, rowsPerOp int, writePct int, durationSec int) error {
	if s.mysqlClient == nil {
		return fmt.Errorf("MySQL not connected. Benchmark requires MySQL.")
	}

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("Continuous SQL benchmark: %d threads, %d rows/req, %d%% writes, %d%% reads, running for %d seconds",
		numThreads, rowsPerOp, writePct, 100-writePct, durationSec)))
	fmt.Println()

	// Create MySQL clients for each thread
	clients := make([]*client.MySQLClient, numThreads)
	if s.verbose >= 1 {
		fmt.Printf("[*] Creating %d MySQL connections to %s:%d...\n", numThreads, s.config.MySQLHost, s.config.MySQLPort)
	}
	for i := 0; i < numThreads; i++ {
		c, err := client.NewMySQLClientWithOptions(client.MySQLOptions{
			Host:     s.config.MySQLHost,
			Port:     s.config.MySQLPort,
			User:     s.mysqlUser,
			Password: s.mysqlPass,
			TLS:      s.config.TLS,
		})
		if err != nil {
			// Close already created clients
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create MySQL client for thread %d: %w", i, err)
		}
		clients[i] = c
	}
	if s.verbose >= 1 {
		fmt.Printf("[*] %d MySQL connections established\n", numThreads)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d MySQL connections closed\n", numThreads)
		}
	}()

	fmt.Printf("Running for %d seconds (reporting every 10s)...\n", durationSec)
	fmt.Println()

	// Total counters
	var totalReadOps int64
	var totalWriteOps int64

	// Interval counters (for 10-second reporting)
	var intervalReadOps int64
	var intervalWriteOps int64
	var intervalErrors int64

	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()

	// Use a channel to signal stop
	stopCh := make(chan struct{})

	benchStart := time.Now()

	// Start timer to close stopCh after duration
	go func() {
		time.Sleep(time.Duration(durationSec) * time.Second)
		close(stopCh)
	}()

	// Start reporting goroutine
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		intervalStart := time.Now()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				// Read and reset interval counters
				reads := atomic.SwapInt64(&intervalReadOps, 0)
				writes := atomic.SwapInt64(&intervalWriteOps, 0)
				errs := atomic.SwapInt64(&intervalErrors, 0)

				// Get interval latency stats
				minLat, maxLat, avgLat, p99Lat, _ := latencyCollector.GetIntervalStats()

				intervalDuration := time.Since(intervalStart)
				intervalStart = time.Now()

				ops := reads + writes
				rows := ops * int64(rowsPerOp)
				opsPerSec := float64(ops) / intervalDuration.Seconds()
				rowsPerSec := float64(rows) / intervalDuration.Seconds()

				elapsed := time.Since(benchStart).Seconds()
				fmt.Printf("[%5.0fs] reads: %d, writes: %d, errors: %d, %.0f ops/sec (%.0f rows/sec), latency: min=%s avg=%s max=%s p99=%s\n",
					elapsed, reads, writes, errs, opsPerSec, rowsPerSec,
					formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat), formatLatency(p99Lat))
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, mysqlClient *client.MySQLClient) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(threadID)))
			keyCounter := 0

			for {
				select {
				case <-stopCh:
					return
				default:
					isWrite := rng.Intn(100) < writePct
					// Use modulo to cycle through the same keys as .bench_sql
					requestId := keyCounter % numOps
					keyCounter++

					// Build user_id and event_time list
					userID := fmt.Sprintf("bench:key:%d:%d:%d", clientID, threadID, requestId)
					var eventTimes []string
					for j := 0; j < rowsPerOp; j++ {
						eventTimes = append(eventTimes, strconv.Itoa(j))
					}
					eventTimeList := strings.Join(eventTimes, ", ")

					if isWrite {
						// UPDATE
						description := fmt.Sprintf(`{"client":%d,"id":%d,"key":%d,"row":0,"data":"benchmark update"}`, clientID, threadID, requestId)
						updateSQL := fmt.Sprintf("UPDATE test.sql_test SET description = '%s', value_int = value_int + 1 WHERE user_id = '%s' AND event_time IN (%s)",
							description, userID, eventTimeList)
						if debugMode {
							fmt.Printf("[DEBUG] SQL: %s\n", updateSQL)
						}
						opStart := time.Now()
						_, _, err := mysqlClient.Execute(updateSQL)
						latencyCollector.Record(time.Since(opStart))
						if debugMode {
							fmt.Printf("[DEBUG] Result: err=%v\n", err)
						}
						if err != nil {
							errorCollector.Record(err)
							atomic.AddInt64(&intervalErrors, 1)
						} else {
							atomic.AddInt64(&totalWriteOps, 1)
							atomic.AddInt64(&intervalWriteOps, 1)
						}
					} else {
						// SELECT all columns
						selectSQL := fmt.Sprintf("SELECT user_id, event_time, description, value_int, event_type FROM test.sql_test WHERE user_id = '%s' AND event_time IN (%s)",
							userID, eventTimeList)
						if debugMode {
							fmt.Printf("[DEBUG] SQL: %s\n", selectSQL)
						}
						opStart := time.Now()
						_, _, err := mysqlClient.Execute(selectSQL)
						latencyCollector.Record(time.Since(opStart))
						if debugMode {
							fmt.Printf("[DEBUG] Result: err=%v\n", err)
						}
						if err != nil {
							errorCollector.Record(err)
							atomic.AddInt64(&intervalErrors, 1)
						} else {
							atomic.AddInt64(&totalReadOps, 1)
							atomic.AddInt64(&intervalReadOps, 1)
						}
					}
				}
			}
		}(t, clients[t])
	}
	wg.Wait()
	<-reportDone // Wait for reporting goroutine to finish

	benchDuration := time.Since(benchStart)

	// Get total latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	// Report total results
	fmt.Println()
	totalOpsCompleted := totalReadOps + totalWriteOps
	totalRowsProcessed := totalOpsCompleted * int64(rowsPerOp)
	opsPerSec := float64(totalOpsCompleted) / benchDuration.Seconds()
	rowsPerSec := float64(totalRowsProcessed) / benchDuration.Seconds()

	fmt.Println(ui.Success(fmt.Sprintf("Continuous SQL Benchmark completed in %.2fs", benchDuration.Seconds())))
	fmt.Printf("   Total Reads:  %d operations (%d rows)\n", totalReadOps, totalReadOps*int64(rowsPerOp))
	fmt.Printf("   Total Writes: %d operations (%d rows)\n", totalWriteOps, totalWriteOps*int64(rowsPerOp))
	fmt.Printf("   Total Errors: %d\n", errorCollector.Count())
	fmt.Printf("   Avg Throughput: %.0f ops/sec (%.0f rows/sec)\n", opsPerSec, rowsPerSec)
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runBenchRondis benchmarks using mixed MGET/GET and MSET/SET operations
// writePct: 0 = 100% reads, 100 = 100% writes
func (s *Shell) runBenchRondis(numThreads int, numOps int, rowsPerOp int, writePct int) error {
	if s.rondisClient == nil {
		return fmt.Errorf("Rondis not connected. Benchmark requires Rondis.")
	}

	totalOps := numThreads * numOps
	totalKeys := totalOps * rowsPerOp
	useMulti := rowsPerOp > 1

	fmt.Println()
	if useMulti {
		fmt.Println(ui.Info(fmt.Sprintf("Benchmarking: %d threads × %d requests × %d rows = %d total keys (%d%% writes, %d%% reads)", numThreads, numOps, rowsPerOp, totalKeys, writePct, 100-writePct)))
	} else {
		fmt.Println(ui.Info(fmt.Sprintf("Benchmarking: %d threads × %d requests = %d total keys (%d%% writes, %d%% reads)", numThreads, numOps, totalKeys, writePct, 100-writePct)))
	}
	fmt.Println()

	clients, err := s.createRondisClients(numThreads)
	if err != nil {
		return err
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d Rondis connections closed\n", numThreads)
		}
	}()

	if useMulti {
		fmt.Printf("Running %d ops (%d threads × %d calls × %d rows)...\n", totalKeys, numThreads, numOps, rowsPerOp)
	} else {
		fmt.Printf("Running %d ops (%d threads × %d calls)...\n", totalKeys, numThreads, numOps)
	}

	var readOps int64
	var writeOps int64
	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()
	benchStart := time.Now()

	// Progress reporting goroutine
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				reads := atomic.LoadInt64(&readOps)
				writes := atomic.LoadInt64(&writeOps)
				errs := errorCollector.Count()
				ops := reads + writes
				keys := ops * int64(rowsPerOp)
				elapsed := time.Since(benchStart)
				keysPerSec := float64(keys) / elapsed.Seconds()
				pct := float64(ops) / float64(totalOps) * 100
				fmt.Printf("   Progress: %d/%d ops (%.1f%%), reads=%d, writes=%d, errors=%d, %.0f keys/sec\n",
					ops, totalOps, pct, reads, writes, errs, keysPerSec)
			case <-stopProgress:
				return
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, rondisClient *client.RondisClient) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(threadID)))

			for i := 0; i < numOps; i++ {
				isWrite := rng.Intn(100) < writePct

				if useMulti {
					if isWrite {
						// MSET
						args := []string{"MSET"}
						for j := 0; j < rowsPerOp; j++ {
							key := fmt.Sprintf("bench:key:%d:%d:%d:%d", clientID, threadID, i, j)
							value := fmt.Sprintf(`{"client":%d,"id":%d,"key":%d,"row":%d,"data":"benchmark test data"}`, clientID, threadID, i, j)
							args = append(args, key, value)
						}
						if debugMode {
							fmt.Printf("[DEBUG] REQ: %v\n", args)
						}
						opStart := time.Now()
						resp, _, err := rondisClient.Execute(args)
						latencyCollector.Record(time.Since(opStart))
						if debugMode {
							fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
						}
						if err != nil {
							errorCollector.Record(err)
						} else {
							atomic.AddInt64(&writeOps, 1)
						}
					} else {
						// MGET
						args := []string{"MGET"}
						for j := 0; j < rowsPerOp; j++ {
							key := fmt.Sprintf("bench:key:%d:%d:%d:%d", clientID, threadID, i, j)
							args = append(args, key)
						}
						if debugMode {
							fmt.Printf("[DEBUG] REQ: %v\n", args)
						}
						opStart := time.Now()
						resp, _, err := rondisClient.Execute(args)
						latencyCollector.Record(time.Since(opStart))
						if debugMode {
							fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
						}
						if err != nil {
							errorCollector.Record(err)
						} else {
							atomic.AddInt64(&readOps, 1)
						}
					}
				} else {
					key := fmt.Sprintf("bench:key:%d:%d:%d:%d", clientID, threadID, i, 0)
					if isWrite {
						// SET
						value := fmt.Sprintf(`{"client":%d,"id":%d,"key":%d,"row":%d,"data":"benchmark test data"}`, clientID, threadID, i, 0)
						args := []string{"SET", key, value}
						if debugMode {
							fmt.Printf("[DEBUG] REQ: %v\n", args)
						}
						opStart := time.Now()
						resp, _, err := rondisClient.Execute(args)
						latencyCollector.Record(time.Since(opStart))
						if debugMode {
							fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
						}
						if err != nil {
							errorCollector.Record(err)
						} else {
							atomic.AddInt64(&writeOps, 1)
						}
					} else {
						// GET
						args := []string{"GET", key}
						if debugMode {
							fmt.Printf("[DEBUG] REQ: %v\n", args)
						}
						opStart := time.Now()
						resp, _, err := rondisClient.Execute(args)
						latencyCollector.Record(time.Since(opStart))
						if debugMode {
							fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
						}
						if err != nil {
							errorCollector.Record(err)
						} else {
							atomic.AddInt64(&readOps, 1)
						}
					}
				}
			}
		}(t, clients[t])
	}
	wg.Wait()
	close(stopProgress)
	benchDuration := time.Since(benchStart)

	// Get latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	keysPerSec := float64(totalKeys) / benchDuration.Seconds()
	opsPerSec := float64(totalOps) / benchDuration.Seconds()

	actualWritePct := float64(writeOps) / float64(writeOps+readOps) * 100
	actualReadPct := float64(readOps) / float64(writeOps+readOps) * 100

	if useMulti {
		fmt.Printf("   %d keys in %v (%.0f keys/sec, %.0f ops/sec)\n", totalKeys, benchDuration.Round(time.Millisecond), keysPerSec, opsPerSec)
	} else {
		fmt.Printf("   %d keys in %v (%.0f ops/sec)\n", totalKeys, benchDuration.Round(time.Millisecond), opsPerSec)
	}
	fmt.Printf("   Actual mix: %.1f%% writes (%d), %.1f%% reads (%d)\n", actualWritePct, writeOps, actualReadPct, readOps)
	errCount := errorCollector.Count()
	if errCount > 0 {
		fmt.Printf("   %d errors\n", errCount)
	}
	fmt.Println()

	// Results
	fmt.Println(ui.Success("Benchmark complete!"))
	fmt.Printf("   Configuration: %d threads × %d requests × %d rows = %d total keys\n", numThreads, numOps, rowsPerOp, totalKeys)
	fmt.Printf("   Throughput: %.0f ops/sec\n", opsPerSec)
	if useMulti {
		fmt.Printf("   Keys/sec: %.0f\n", keysPerSec)
	}
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runBenchRondisCont runs continuous benchmark for specified duration
// writePct: 0 = 100% reads, 100 = 100% writes
func (s *Shell) runBenchRondisCont(numThreads int, numOps int, rowsPerOp int, writePct int, durationSec int) error {
	if s.rondisClient == nil {
		return fmt.Errorf("Rondis not connected. Benchmark requires Rondis.")
	}

	useMulti := rowsPerOp > 1

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("Continuous Rondis benchmark: %d threads, %d rows/req, %d%% writes, %d%% reads, running for %d seconds",
		numThreads, rowsPerOp, writePct, 100-writePct, durationSec)))
	fmt.Println()

	clients, err := s.createRondisClients(numThreads)
	if err != nil {
		return err
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d Rondis connections closed\n", numThreads)
		}
	}()

	fmt.Printf("Running for %d seconds (reporting every 10s)...\n", durationSec)
	fmt.Println()

	// Total counters
	var totalReadOps int64
	var totalWriteOps int64

	// Interval counters (for 10-second reporting)
	var intervalReadOps int64
	var intervalWriteOps int64
	var intervalErrors int64

	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()

	// Use a channel to signal stop
	stopCh := make(chan struct{})

	benchStart := time.Now()

	// Start timer to close stopCh after duration
	go func() {
		time.Sleep(time.Duration(durationSec) * time.Second)
		close(stopCh)
	}()

	// Start reporting goroutine
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		intervalStart := time.Now()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				// Read and reset interval counters
				reads := atomic.SwapInt64(&intervalReadOps, 0)
				writes := atomic.SwapInt64(&intervalWriteOps, 0)
				errs := atomic.SwapInt64(&intervalErrors, 0)

				// Get interval latency stats
				minLat, maxLat, avgLat, p99Lat, _ := latencyCollector.GetIntervalStats()

				intervalDuration := time.Since(intervalStart)
				intervalStart = time.Now()

				ops := reads + writes
				keys := ops * int64(rowsPerOp)
				opsPerSec := float64(ops) / intervalDuration.Seconds()
				keysPerSec := float64(keys) / intervalDuration.Seconds()

				elapsed := time.Since(benchStart).Seconds()
				fmt.Printf("[%5.0fs] reads: %d, writes: %d, errors: %d, %.0f ops/sec (%.0f keys/sec), latency: min=%s avg=%s max=%s p99=%s\n",
					elapsed, reads, writes, errs, opsPerSec, keysPerSec,
					formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat), formatLatency(p99Lat))
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, rondisClient *client.RondisClient) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(threadID)))
			keyCounter := 0

			for {
				select {
				case <-stopCh:
					return
				default:
					isWrite := rng.Intn(100) < writePct
					// Use modulo to cycle through the same keys as .bench_rondis
					requestId := keyCounter % numOps
					keyCounter++

					if useMulti {
						if isWrite {
							// MSET
							args := []string{"MSET"}
							for j := 0; j < rowsPerOp; j++ {
								key := fmt.Sprintf("bench:key:%d:%d:%d:%d", clientID, threadID, requestId, j)
								value := fmt.Sprintf(`{"client":%d,"id":%d,"key":%d,"row":%d,"data":"benchmark test data"}`, clientID, threadID, requestId, j)
								args = append(args, key, value)
							}
							if debugMode {
								fmt.Printf("[DEBUG] REQ: %v\n", args)
							}
							opStart := time.Now()
							resp, _, err := rondisClient.Execute(args)
							latencyCollector.Record(time.Since(opStart))
							if debugMode {
								fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
							}
							if err != nil {
								errorCollector.Record(err)
								atomic.AddInt64(&intervalErrors, 1)
							} else {
								atomic.AddInt64(&totalWriteOps, 1)
								atomic.AddInt64(&intervalWriteOps, 1)
							}
						} else {
							// MGET
							args := []string{"MGET"}
							for j := 0; j < rowsPerOp; j++ {
								key := fmt.Sprintf("bench:key:%d:%d:%d:%d", clientID, threadID, requestId, j)
								args = append(args, key)
							}
							if debugMode {
								fmt.Printf("[DEBUG] REQ: %v\n", args)
							}
							opStart := time.Now()
							resp, _, err := rondisClient.Execute(args)
							latencyCollector.Record(time.Since(opStart))
							if debugMode {
								fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
							}
							if err != nil {
								errorCollector.Record(err)
								atomic.AddInt64(&intervalErrors, 1)
							} else {
								atomic.AddInt64(&totalReadOps, 1)
								atomic.AddInt64(&intervalReadOps, 1)
							}
						}
					} else {
						key := fmt.Sprintf("bench:key:%d:%d:%d:%d", clientID, threadID, requestId, 0)
						if isWrite {
							// SET
							value := fmt.Sprintf(`{"client":%d,"id":%d,"key":%d,"row":%d,"data":"benchmark test data"}`, clientID, threadID, requestId, 0)
							args := []string{"SET", key, value}
							if debugMode {
								fmt.Printf("[DEBUG] REQ: %v\n", args)
							}
							opStart := time.Now()
							resp, _, err := rondisClient.Execute(args)
							latencyCollector.Record(time.Since(opStart))
							if debugMode {
								fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
							}
							if err != nil {
								errorCollector.Record(err)
								atomic.AddInt64(&intervalErrors, 1)
							} else {
								atomic.AddInt64(&totalWriteOps, 1)
								atomic.AddInt64(&intervalWriteOps, 1)
							}
						} else {
							// GET
							args := []string{"GET", key}
							if debugMode {
								fmt.Printf("[DEBUG] REQ: %v\n", args)
							}
							opStart := time.Now()
							resp, _, err := rondisClient.Execute(args)
							latencyCollector.Record(time.Since(opStart))
							if debugMode {
								fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
							}
							if err != nil {
								errorCollector.Record(err)
								atomic.AddInt64(&intervalErrors, 1)
							} else {
								atomic.AddInt64(&totalReadOps, 1)
								atomic.AddInt64(&intervalReadOps, 1)
							}
						}
					}
				}
			}
		}(t, clients[t])
	}

	wg.Wait()
	<-reportDone // Wait for reporting goroutine to finish

	benchDuration := time.Since(benchStart)

	// Get total latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	totalOps := totalReadOps + totalWriteOps
	totalKeys := totalOps * int64(rowsPerOp)
	opsPerSec := float64(totalOps) / benchDuration.Seconds()
	keysPerSec := float64(totalKeys) / benchDuration.Seconds()

	var actualWritePct, actualReadPct float64
	if totalOps > 0 {
		actualWritePct = float64(totalWriteOps) / float64(totalOps) * 100
		actualReadPct = float64(totalReadOps) / float64(totalOps) * 100
	}

	fmt.Println()
	fmt.Println(ui.Success("Continuous Rondis benchmark complete!"))
	fmt.Printf("   Duration: %v\n", benchDuration.Round(time.Millisecond))
	fmt.Printf("   Total requests: %d (%.0f ops/sec)\n", totalOps, opsPerSec)
	fmt.Printf("   Total keys: %d (%.0f keys/sec)\n", totalKeys, keysPerSec)
	fmt.Printf("   Total Writes: %d (%.1f%%)\n", totalWriteOps, actualWritePct)
	fmt.Printf("   Total Reads: %d (%.1f%%)\n", totalReadOps, actualReadPct)
	errCount := errorCollector.Count()
	if errCount > 0 {
		fmt.Printf("   Total Errors: %d\n", errCount)
	}
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runDelRondis deletes data using DEL (Redis doesn't have MDEL, using DEL with multiple keys)
func (s *Shell) runDelRondis(numThreads int, numOps int, rowsPerOp int) error {
	if s.rondisClient == nil {
		return fmt.Errorf("Rondis not connected. Delete requires Rondis.")
	}

	totalOps := numThreads * numOps
	totalKeys := totalOps * rowsPerOp
	useMulti := rowsPerOp > 1

	fmt.Println()
	if totalKeys > 10000 {
		fmt.Println(ui.Warning(fmt.Sprintf("Deleting %d total keys - this may take a while...", totalKeys)))
	}
	if useMulti {
		fmt.Println(ui.Info(fmt.Sprintf("Deleting data: %d threads × %d requests × %d rows = %d total keys", numThreads, numOps, rowsPerOp, totalKeys)))
	} else {
		fmt.Println(ui.Info(fmt.Sprintf("Deleting data: %d threads × %d requests = %d total keys", numThreads, numOps, totalKeys)))
	}
	fmt.Println()

	clients, err := s.createRondisClients(numThreads)
	if err != nil {
		return err
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d Rondis connections closed\n", numThreads)
		}
	}()

	if useMulti {
		fmt.Printf("Deleting %d keys via DEL (%d threads × %d calls × %d rows)...\n", totalKeys, numThreads, numOps, rowsPerOp)
	} else {
		fmt.Printf("Deleting %d keys via DEL (%d threads × %d calls)...\n", totalKeys, numThreads, numOps)
	}

	errorCollector := NewErrorCollector()
	var completedOps int64
	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	delStart := time.Now()

	// Progress reporting goroutine
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ops := atomic.LoadInt64(&completedOps)
				keys := ops * int64(rowsPerOp)
				errs := errorCollector.Count()
				elapsed := time.Since(delStart)
				keysPerSec := float64(keys) / elapsed.Seconds()
				pct := float64(ops) / float64(totalOps) * 100
				fmt.Printf("   Progress: %d/%d ops (%.1f%%), %d keys deleted, errors=%d, %.0f keys/sec\n",
					ops, totalOps, pct, keys, errs, keysPerSec)
			case <-stopProgress:
				return
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, rondisClient *client.RondisClient) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				if useMulti {
					// DEL supports multiple keys
					args := []string{"DEL"}
					for j := 0; j < rowsPerOp; j++ {
						key := fmt.Sprintf("bench:key:%d:%d:%d:%d", clientID, threadID, i, j)
						args = append(args, key)
					}
					if debugMode {
						fmt.Printf("[DEBUG] REQ: %v\n", args)
					}
					opStart := time.Now()
					resp, _, err := rondisClient.Execute(args)
					latencyCollector.Record(time.Since(opStart))
					if debugMode {
						fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
					}
					if err != nil {
						errorCollector.Record(err)
					}
				} else {
					key := fmt.Sprintf("bench:key:%d:%d:%d:%d", clientID, threadID, i, 0)
					args := []string{"DEL", key}
					if debugMode {
						fmt.Printf("[DEBUG] REQ: %v\n", args)
					}
					opStart := time.Now()
					resp, _, err := rondisClient.Execute(args)
					latencyCollector.Record(time.Since(opStart))
					if debugMode {
						fmt.Printf("[DEBUG] RESP: %s, err: %v\n", resp, err)
					}
					if err != nil {
						errorCollector.Record(err)
					}
				}
				atomic.AddInt64(&completedOps, 1)
			}
		}(t, clients[t])
	}
	wg.Wait()
	close(stopProgress)
	delDuration := time.Since(delStart)

	// Get latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	delKeysPerSec := float64(totalKeys) / delDuration.Seconds()
	delOpsPerSec := float64(totalOps) / delDuration.Seconds()
	if useMulti {
		fmt.Printf("   %d keys in %v (%.0f keys/sec, %.0f DEL/sec)\n", totalKeys, delDuration.Round(time.Millisecond), delKeysPerSec, delOpsPerSec)
	} else {
		fmt.Printf("   %d keys in %v (%.0f DEL/sec)\n", totalKeys, delDuration.Round(time.Millisecond), delOpsPerSec)
	}
	delErrors := errorCollector.Count()
	if delErrors > 0 {
		fmt.Printf("   %d delete errors\n", delErrors)
	}
	fmt.Println()

	// Results
	fmt.Println(ui.Success("Delete complete!"))
	fmt.Printf("   Configuration: %d threads × %d requests × %d rows = %d total keys\n", numThreads, numOps, rowsPerOp, totalKeys)
	if useMulti {
		fmt.Printf("   Deletes: %.0f keys/sec (%.0f DEL/sec)\n", delKeysPerSec, delOpsPerSec)
	} else {
		fmt.Printf("   Deletes: %.0f DEL/sec\n", delOpsPerSec)
	}
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runBenchRDRS benchmarks REST API using batch pk-read operations on test.sql_test
func (s *Shell) runBenchRDRS(numThreads int, numOps int, rowsPerOp int) error {
	if s.restClient == nil {
		return fmt.Errorf("REST API not connected. Benchmark requires RDRS.")
	}

	totalOps := numThreads * numOps
	totalRows := totalOps * rowsPerOp

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("RDRS Benchmarking: %d threads × %d requests × %d rows = %d total rows (read-only)",
		numThreads, numOps, rowsPerOp, totalRows)))
	fmt.Println()

	// Create REST clients for each thread
	clients := make([]*client.RestClient, numThreads)
	if s.verbose >= 1 {
		fmt.Printf("[*] Creating %d REST connections to %s:%d...\n", numThreads, s.config.RDRSHost, s.config.RestPort)
	}
	for i := 0; i < numThreads; i++ {
		c, err := client.NewRestClientWithOptions(client.RestOptions{
			Host:   s.config.RDRSHost,
			Port:   s.config.RestPort,
			TLS:    s.config.RDRSTLS,
			APIKey: s.config.RDRSAPIKey,
		})
		if err != nil {
			// Close already created clients
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create REST client for thread %d: %w", i, err)
		}
		clients[i] = c
	}
	if s.verbose >= 1 {
		fmt.Printf("[*] %d REST connections established\n", numThreads)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d REST connections closed\n", numThreads)
		}
	}()

	fmt.Printf("Running %d batch requests (%d threads × %d calls × %d rows/batch)...\n",
		totalOps, numThreads, numOps, rowsPerOp)

	// Define read columns for non-PK columns in test.sql_test
	readColumns := []dsl.ReadColumn{
		{Column: "description", DataReturnType: "default"},
		{Column: "value_int", DataReturnType: "default"},
		{Column: "event_type", DataReturnType: "default"},
	}

	var readOps int64
	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()
	benchStart := time.Now()

	// Progress reporting goroutine
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ops := atomic.LoadInt64(&readOps)
				errs := errorCollector.Count()
				rows := ops * int64(rowsPerOp)
				elapsed := time.Since(benchStart)
				opsPerSec := float64(ops) / elapsed.Seconds()
				rowsPerSec := float64(rows) / elapsed.Seconds()
				pct := float64(ops) / float64(totalOps) * 100
				fmt.Printf("   Progress: %d/%d ops (%.1f%%), %d rows, errors=%d, %.0f batch/sec, %.0f rows/sec\n",
					ops, totalOps, pct, rows, errs, opsPerSec, rowsPerSec)
			case <-stopProgress:
				return
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, restClient *client.RestClient) {
			defer wg.Done()

			for i := 0; i < numOps; i++ {
				// Build batch request with rowsPerOp operations
				userID := fmt.Sprintf("bench:key:%d:%d:%d", clientID, threadID, i)
				var operations []dsl.BatchOperation

				for j := 0; j < rowsPerOp; j++ {
					op := dsl.BatchOperation{
						Method:      "POST",
						RelativeURL: "test/sql_test/pk-read",
						Body: dsl.PkReadRequest{
							Filters: []dsl.Filter{
								{Column: "user_id", Value: userID},
								{Column: "event_time", Value: j},
							},
							ReadColumns: readColumns,
							OperationID: fmt.Sprintf("%d", j),
						},
					}
					operations = append(operations, op)
				}

				batchReq := dsl.BatchRequest{Operations: operations}
				if debugMode {
					reqJSON, _ := json.MarshalIndent(batchReq, "", "  ")
					fmt.Printf("[DEBUG] REQ: POST /0.1.0/batch\n%s\n", reqJSON)
				}
				opStart := time.Now()
				resp, _, err := restClient.Post("/0.1.0/batch", batchReq)
				latencyCollector.Record(time.Since(opStart))
				if debugMode {
					fmt.Printf("[DEBUG] RESP: %s, err: %v\n", string(resp), err)
				}
				if err != nil {
					errorCollector.Record(err)
				} else {
					atomic.AddInt64(&readOps, 1)
				}
			}
		}(t, clients[t])
	}
	wg.Wait()
	close(stopProgress)

	benchDuration := time.Since(benchStart)

	// Get latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	// Report results
	fmt.Println()
	totalRowsProcessed := readOps * int64(rowsPerOp)
	opsPerSec := float64(readOps) / benchDuration.Seconds()
	rowsPerSec := float64(totalRowsProcessed) / benchDuration.Seconds()

	fmt.Println(ui.Success(fmt.Sprintf("RDRS Benchmark completed in %.2fs", benchDuration.Seconds())))
	fmt.Printf("   Batch requests: %d\n", readOps)
	fmt.Printf("   Rows read: %d\n", totalRowsProcessed)
	fmt.Printf("   Errors: %d\n", errorCollector.Count())
	fmt.Printf("   Throughput: %.0f batch/sec (%.0f rows/sec)\n", opsPerSec, rowsPerSec)
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}

// runBenchRDRSCont runs continuous RDRS benchmark for specified duration
func (s *Shell) runBenchRDRSCont(numThreads int, numOps int, rowsPerOp int, durationSec int) error {
	if s.restClient == nil {
		return fmt.Errorf("REST API not connected. Benchmark requires RDRS.")
	}

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("Continuous RDRS benchmark: %d threads, %d rows/batch, running for %d seconds",
		numThreads, rowsPerOp, durationSec)))
	fmt.Println()

	// Create REST clients for each thread
	clients := make([]*client.RestClient, numThreads)
	if s.verbose >= 1 {
		fmt.Printf("[*] Creating %d REST connections to %s:%d...\n", numThreads, s.config.RDRSHost, s.config.RestPort)
	}
	for i := 0; i < numThreads; i++ {
		c, err := client.NewRestClientWithOptions(client.RestOptions{
			Host:   s.config.RDRSHost,
			Port:   s.config.RestPort,
			TLS:    s.config.RDRSTLS,
			APIKey: s.config.RDRSAPIKey,
		})
		if err != nil {
			// Close already created clients
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create REST client for thread %d: %w", i, err)
		}
		clients[i] = c
	}
	if s.verbose >= 1 {
		fmt.Printf("[*] %d REST connections established\n", numThreads)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
		if s.verbose >= 1 {
			fmt.Printf("[*] %d REST connections closed\n", numThreads)
		}
	}()

	fmt.Printf("Running for %d seconds (reporting every 10s)...\n", durationSec)
	fmt.Println()

	// Define read columns for non-PK columns in test.sql_test
	readColumns := []dsl.ReadColumn{
		{Column: "description", DataReturnType: "default"},
		{Column: "value_int", DataReturnType: "default"},
		{Column: "event_type", DataReturnType: "default"},
	}

	// Total counters
	var totalReadOps int64

	// Interval counters (for 10-second reporting)
	var intervalReadOps int64
	var intervalErrors int64

	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()

	// Use a channel to signal stop
	stopCh := make(chan struct{})

	benchStart := time.Now()

	// Start timer to close stopCh after duration
	go func() {
		time.Sleep(time.Duration(durationSec) * time.Second)
		close(stopCh)
	}()

	// Start reporting goroutine
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		intervalStart := time.Now()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				// Read and reset interval counters
				reads := atomic.SwapInt64(&intervalReadOps, 0)
				errs := atomic.SwapInt64(&intervalErrors, 0)

				// Get interval latency stats
				minLat, maxLat, avgLat, p99Lat, _ := latencyCollector.GetIntervalStats()

				intervalDuration := time.Since(intervalStart)
				intervalStart = time.Now()

				rows := reads * int64(rowsPerOp)
				opsPerSec := float64(reads) / intervalDuration.Seconds()
				rowsPerSec := float64(rows) / intervalDuration.Seconds()

				elapsed := time.Since(benchStart).Seconds()
				fmt.Printf("[%5.0fs] batches: %d, errors: %d, %.0f batch/sec (%.0f rows/sec), latency: min=%s avg=%s max=%s p99=%s\n",
					elapsed, reads, errs, opsPerSec, rowsPerSec,
					formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat), formatLatency(p99Lat))
			}
		}
	}()

	debugMode := s.debug
	clientID := s.clientID
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, restClient *client.RestClient) {
			defer wg.Done()
			keyCounter := 0

			for {
				select {
				case <-stopCh:
					return
				default:
					// Use modulo to cycle through the same keys as .bench_rdrs
					requestId := keyCounter % numOps
					keyCounter++

					// Build batch request with rowsPerOp operations
					userID := fmt.Sprintf("bench:key:%d:%d:%d", clientID, threadID, requestId)
					var operations []dsl.BatchOperation

					for j := 0; j < rowsPerOp; j++ {
						op := dsl.BatchOperation{
							Method:      "POST",
							RelativeURL: "test/sql_test/pk-read",
							Body: dsl.PkReadRequest{
								Filters: []dsl.Filter{
									{Column: "user_id", Value: userID},
									{Column: "event_time", Value: j},
								},
								ReadColumns: readColumns,
								OperationID: fmt.Sprintf("%d", j),
							},
						}
						operations = append(operations, op)
					}

					batchReq := dsl.BatchRequest{Operations: operations}
					if debugMode {
						reqJSON, _ := json.MarshalIndent(batchReq, "", "  ")
						fmt.Printf("[DEBUG] REQ: POST /0.1.0/batch\n%s\n", reqJSON)
					}
					opStart := time.Now()
					resp, _, err := restClient.Post("/0.1.0/batch", batchReq)
					latencyCollector.Record(time.Since(opStart))
					if debugMode {
						fmt.Printf("[DEBUG] RESP: %s, err: %v\n", string(resp), err)
					}
					if err != nil {
						errorCollector.Record(err)
						atomic.AddInt64(&intervalErrors, 1)
					} else {
						atomic.AddInt64(&totalReadOps, 1)
						atomic.AddInt64(&intervalReadOps, 1)
					}
				}
			}
		}(t, clients[t])
	}
	wg.Wait()
	<-reportDone // Wait for reporting goroutine to finish

	benchDuration := time.Since(benchStart)

	// Get total latency stats
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	// Report total results
	fmt.Println()
	totalRowsProcessed := totalReadOps * int64(rowsPerOp)
	opsPerSec := float64(totalReadOps) / benchDuration.Seconds()
	rowsPerSec := float64(totalRowsProcessed) / benchDuration.Seconds()

	fmt.Println(ui.Success(fmt.Sprintf("Continuous RDRS Benchmark completed in %.2fs", benchDuration.Seconds())))
	fmt.Printf("   Total Batches: %d\n", totalReadOps)
	fmt.Printf("   Total Rows: %d\n", totalRowsProcessed)
	fmt.Printf("   Total Errors: %d\n", errorCollector.Count())
	fmt.Printf("   Avg Throughput: %.0f batch/sec (%.0f rows/sec)\n", opsPerSec, rowsPerSec)
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
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
RonDB CLI - Rondis commands. SQL queries. REST API. One database.

Multi-line input: End any command with ; or type ; on its own line.

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
    MYSQL <command>     Send any command directly to MySQL

  REST API (pk-read):
    READ db.table col1, col2 FILTER id=1, name="foo";
                        Single pk-read via REST API

    BATCH READ db.table col1 FILTER id=1 [OP id] [READ ...];
                        Batch with full READ per operation

    BATCH db.table col1, col2 READ FILTER id=1, READ FILTER id=2;
                        Batch with shared table/columns (use , to separate READs)

  RonSQL (REST API):
    RONSQL <query>             Execute query via RonSQL REST API
    RONSQL SET DATABASE <db>   Set database for RonSQL queries
    .ronsql_database [db]      Show/set RonSQL database
    .ronsql_format [format]    Show/set RonSQL output format (JSON, JSON_ASCII, TEXT, TEXT_NOHEADER)
    .ronsql_explain [mode]     Show/set RonSQL explain mode (ALLOW, FORBID, REQUIRE, REMOVE, FORCE)

  Internal commands:
    .browse             Open database browser (TUI)
    .demo               Run a quick demo (write, read, query)
    .tables             List all tables
    .help               Show this help
    .help internal      Show benchmark commands
    .help start         Show connection flags and environment variables
    quit, exit, q       Exit the shell

The magic: Your Rondis data is queryable with SQL!
  SET user:123 '{"name":"Alice"}'
  SELECT * FROM redis_0.string_keys WHERE redis_key = 'user:123'
`
	fmt.Println(help)
}

func (s *Shell) printHelpInternal() {
	help := `
Internal benchmark commands (T=threads, N=requests, R=rows/req, W=write%, S=seconds):

  Settings:
    .client [N]                          Set/show client ID for key prefix (default 0)
    .debug [0|1]                         Toggle debug mode (print requests/responses)
    .verbose [0|1|2]                     Set verbose level (0=normal, 1=connect/disconnect, 2=+debug)

  Rondis benchmarks:
    .load_rondis [T] [N] [R]             Load data via MSET
    .bench_rondis [T] [N] [R] [W]        Benchmark (W=write%, 0=all reads, 100=all writes)
    .bench_rondis_cont [T] [N] [R] [W] [S]  Continuous benchmark for S seconds
    .del_rondis [T] [N] [R]              Delete data via DEL

  SQL benchmarks:
    .load_sql [T] [N] [R]                Load data via INSERT into test.sql_test
    .bench_sql [T] [N] [R] [W]           Benchmark (W=write%, 0=all reads, 100=all writes)
    .bench_sql_cont [T] [N] [R] [W] [S]  Continuous benchmark for S seconds
    .bench_sql_scan [T] [N] [R]          Scan benchmark (SELECT by user_id only, no IN clause)
    .bench_sql_scan_cont [T] [N] [R] [S] Continuous scan benchmark for S seconds
    .del_sql [T] [N] [R]                 Delete data via DELETE from test.sql_test
    .drop_sql                            Drop the test.sql_test table

  RDRS benchmarks:
    .bench_rdrs [T] [N] [R]              Batch pk-read benchmark (read-only)
    .bench_rdrs_cont [T] [N] [R] [S]     Continuous benchmark for S seconds

Key format: bench:key:<client>:<thread>:<key>:<row>
Defaults: T=2, N=1000, R=1, W=0, S=60, client=0
`
	fmt.Println(help)
}

func (s *Shell) printHelpStart() {
	fmt.Println()
	fmt.Println(ui.Info("Connection settings:"))
	fmt.Println(ui.Info(fmt.Sprintf("  Rondis:   %s:%d", s.config.Host, s.config.RondisPort)))
	fmt.Println(ui.Info(fmt.Sprintf("  MySQL:    %s:%d (user: %s)", s.config.MySQLHost, s.config.MySQLPort, s.mysqlUser)))
	fmt.Println(ui.Info(fmt.Sprintf("  REST API: %s:%d", s.config.RDRSHost, s.config.RestPort)))
	fmt.Println()
	fmt.Println(ui.Info("Command line flags:"))
	fmt.Println(ui.Info("  --host             RonDB host (default: localhost, sets all hosts)"))
	fmt.Println(ui.Info("  --mysql-host       MySQL host (overrides --host)"))
	fmt.Println(ui.Info("  --rdrs-host        RDRS/REST API host (overrides --host)"))
	fmt.Println(ui.Info("  --rondis-port      Rondis port (default: 6379)"))
	fmt.Println(ui.Info("  --mysql-port       MySQL port (default: 3306)"))
	fmt.Println(ui.Info("  --rdrs-port        RDRS/REST API port (default: 4406)"))
	fmt.Println(ui.Info("  --mysql-user       MySQL username (default: root)"))
	fmt.Println(ui.Info("  --mysql-password   MySQL password (default: empty)"))
	fmt.Println(ui.Info("  -p, --password     Prompt for MySQL password (silent input)"))
	fmt.Println(ui.Info("  --tls              Enable TLS for all connections"))
	fmt.Println(ui.Info("  --rdrs-tls         Enable TLS (HTTPS) for RDRS/REST API only"))
		fmt.Println(ui.Info("  --rdrs-api-key     API key for RDRS/REST API authentication"))
	fmt.Println(ui.Info("  --verbose          Verbose level (0=normal, 1=connection info, 2=debug)"))
	fmt.Println(ui.Info("  --no-mysql         Disable MySQL connection"))
	fmt.Println(ui.Info("  --no-rdrs          Disable RDRS/REST API connection"))
	fmt.Println(ui.Info("  --no-rondis        Disable Rondis connection"))
	fmt.Println()
	fmt.Println(ui.Info("Environment variables:"))
	fmt.Println(ui.Info("  RONDB_HOST           RonDB host (sets all hosts)"))
	fmt.Println(ui.Info("  MYSQL_HOST           MySQL host (overrides RONDB_HOST)"))
	fmt.Println(ui.Info("  RDRS_HOST            RDRS/REST API host (overrides RONDB_HOST)"))
	fmt.Println(ui.Info("  RONDB_RONDIS_PORT    Rondis port"))
	fmt.Println(ui.Info("  RONDB_MYSQL_PORT     MySQL port"))
	fmt.Println(ui.Info("  RONDB_RDRS_PORT      RDRS/REST API port"))
	fmt.Println(ui.Info("  RONDB_MYSQL_USER     MySQL username"))
	fmt.Println(ui.Info("  RONDB_MYSQL_PASSWORD MySQL password"))
	fmt.Println(ui.Info("  RONDB_RDRS_API_KEY   RDRS/REST API key"))
	fmt.Println()
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
		readline.PcItem("MYSQL"),

		// REST API commands
		readline.PcItem("READ"),
		readline.PcItem("BATCH"),
		readline.PcItem("RONSQL",
			readline.PcItem("SELECT"),
			readline.PcItem("SET",
				readline.PcItem("DATABASE"),
			),
		),

		// Internal commands
		readline.PcItem(".ronsql_database"),
		readline.PcItem(".ronsql_format",
			readline.PcItem("JSON"),
			readline.PcItem("JSON_ASCII"),
			readline.PcItem("TEXT"),
			readline.PcItem("TEXT_NOHEADER"),
		),
		readline.PcItem(".ronsql_explain",
			readline.PcItem("ALLOW"),
			readline.PcItem("FORBID"),
			readline.PcItem("REQUIRE"),
			readline.PcItem("REMOVE"),
			readline.PcItem("FORCE"),
		),
		readline.PcItem(".load_rondis"),
		readline.PcItem(".load_sql"),
		readline.PcItem(".del_sql"),
		readline.PcItem(".drop_sql"),
		readline.PcItem(".bench_sql"),
		readline.PcItem(".bench_sql_cont"),
		readline.PcItem(".bench_sql_scan"),
		readline.PcItem(".bench_sql_scan_cont"),
		readline.PcItem(".bench_rondis"),
		readline.PcItem(".bench_rondis_cont"),
		readline.PcItem(".del_rondis"),
		readline.PcItem(".bench_rdrs"),
		readline.PcItem(".bench_rdrs_cont"),
		readline.PcItem(".browse"),
		readline.PcItem(".demo"),
		readline.PcItem(".debug"),
		readline.PcItem(".verbose",
			readline.PcItem("0"),
			readline.PcItem("1"),
			readline.PcItem("2"),
		),
		readline.PcItem(".client"),
		readline.PcItem(".help",
			readline.PcItem("internal"),
			readline.PcItem("start"),
		),
		readline.PcItem(".tables"),
		readline.PcItem("quit"),
		readline.PcItem("exit"),
	)
}
