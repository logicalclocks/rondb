package cmd

import (
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/client"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/shell"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

const Version = "0.1.0"

var (
	host       string
	mysqlHost  string
	rdrsHost   string
	rondisPort int
	mysqlPort  int
	restPort   int
	mysqlUser  string
	mysqlPass  string
	promptPass bool
	useTLS     bool
	rdrsTLS    bool
	rdrsAPIKey string
	verbose    int
	noMySQL    bool
	noRDRS     bool
	noRondis   bool
)

var rootCmd = &cobra.Command{
	Use:   "rondb",
	Short: "RonDB CLI",
	Long:  "RonDB CLI - Rondis commands. SQL queries. One database.",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Prompt for password if -p flag is used
		if promptPass {
			pass, err := readPasswordSilent("Enter MySQL password: ")
			if err != nil {
				return fmt.Errorf("failed to read password: %w", err)
			}
			mysqlPass = pass
		}

		// Compute effective hosts (specific host flags override general host)
		effectiveMySQLHost := host
		if mysqlHost != "" {
			effectiveMySQLHost = mysqlHost
		}
		effectiveRDRSHost := host
		if rdrsHost != "" {
			effectiveRDRSHost = rdrsHost
		}

		return shell.RunWithConfig(shell.Config{
			Host:       host,
			MySQLHost:  effectiveMySQLHost,
			RDRSHost:   effectiveRDRSHost,
			RondisPort: rondisPort,
			MySQLPort:  mysqlPort,
			RestPort:   restPort,
			MySQLUser:  mysqlUser,
			MySQLPass:  mysqlPass,
			TLS:        useTLS,
			RDRSTLS:    useTLS || rdrsTLS,
			RDRSAPIKey: rdrsAPIKey,
			Verbose:    verbose,
			NoMySQL:    noMySQL,
			NoRDRS:     noRDRS,
			NoRondis:   noRondis,
		})
	},
}

// readPasswordSilent prompts for a password without echoing input
func readPasswordSilent(prompt string) (string, error) {
	fmt.Print(prompt)
	password, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println() // Print newline after password input
	if err != nil {
		return "", err
	}
	return string(password), nil
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check RonDB connection status",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Prompt for password if -p flag is used
		if promptPass {
			pass, err := readPasswordSilent("Enter MySQL password: ")
			if err != nil {
				return fmt.Errorf("failed to read password: %w", err)
			}
			mysqlPass = pass
		}

		// Compute effective hosts
		effectiveMySQLHost := host
		if mysqlHost != "" {
			effectiveMySQLHost = mysqlHost
		}
		effectiveRDRSHost := host
		if rdrsHost != "" {
			effectiveRDRSHost = rdrsHost
		}

		fmt.Println("Checking connections...")

		// Check Rondis (uses general host)
		rondisClient, err := client.NewRondisClientWithOptions(client.RondisOptions{
			Host: host,
			Port: rondisPort,
			TLS:  useTLS,
		})
		if err != nil {
			fmt.Println(ui.Error(fmt.Sprintf("Rondis (%s:%d): %v", host, rondisPort, err)))
		} else {
			fmt.Println(ui.Success(fmt.Sprintf("Rondis (%s:%d): connected", host, rondisPort)))
			rondisClient.Close()
		}

		// Check MySQL
		mysqlClient, err := client.NewMySQLClientWithOptions(client.MySQLOptions{
			Host:     effectiveMySQLHost,
			Port:     mysqlPort,
			User:     mysqlUser,
			Password: mysqlPass,
			TLS:      useTLS,
		})
		if err != nil {
			fmt.Println(ui.Error(fmt.Sprintf("MySQL (%s:%d): %v", effectiveMySQLHost, mysqlPort, err)))
		} else {
			fmt.Println(ui.Success(fmt.Sprintf("MySQL (%s:%d): connected as %s", effectiveMySQLHost, mysqlPort, mysqlUser)))
			mysqlClient.Close()
		}

		// Check REST API
		restClient, err := client.NewRestClientWithOptions(client.RestOptions{
			Host: effectiveRDRSHost,
			Port: restPort,
			TLS:  useTLS || rdrsTLS,
		})
		if err != nil {
			fmt.Println(ui.Error(fmt.Sprintf("REST API (%s:%d): %v", effectiveRDRSHost, restPort, err)))
		} else {
			fmt.Println(ui.Success(fmt.Sprintf("REST API (%s:%d): connected", effectiveRDRSHost, restPort)))
			restClient.Close()
		}

		return nil
	},
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("rondb-cli %s\n", Version)
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&host, "host", "localhost", "RonDB host (sets both MySQL and RDRS hosts)")
	rootCmd.PersistentFlags().StringVar(&mysqlHost, "mysql-host", "", "MySQL host (overrides --host)")
	rootCmd.PersistentFlags().StringVar(&rdrsHost, "rdrs-host", "", "RDRS/REST API host (overrides --host)")
	rootCmd.PersistentFlags().IntVar(&rondisPort, "rondis-port", 6379, "Rondis port")
	rootCmd.PersistentFlags().IntVar(&mysqlPort, "mysql-port", 3306, "MySQL port")
	rootCmd.PersistentFlags().IntVar(&restPort, "rdrs-port", 4406, "RDRS/REST API port")
	rootCmd.PersistentFlags().StringVar(&mysqlUser, "mysql-user", "root", "MySQL username")
	rootCmd.PersistentFlags().StringVar(&mysqlPass, "mysql-password", "", "MySQL password")
	rootCmd.PersistentFlags().BoolVarP(&promptPass, "password", "p", false, "Prompt for MySQL password")
	rootCmd.PersistentFlags().BoolVar(&useTLS, "tls", false, "Enable TLS for all connections")
	rootCmd.PersistentFlags().BoolVar(&rdrsTLS, "rdrs-tls", false, "Enable TLS (HTTPS) for RDRS/REST API only")
	rootCmd.PersistentFlags().StringVar(&rdrsAPIKey, "rdrs-api-key", "", "API key for RDRS/REST API authentication")
	rootCmd.PersistentFlags().IntVar(&verbose, "verbose", 0, "Verbose level (0=normal, 1=connection info, 2=debug)")
	rootCmd.PersistentFlags().BoolVar(&noMySQL, "no-mysql", false, "Disable MySQL connection")
	rootCmd.PersistentFlags().BoolVar(&noRDRS, "no-rdrs", false, "Disable RDRS/REST API connection")
	rootCmd.PersistentFlags().BoolVar(&noRondis, "no-rondis", false, "Disable Rondis connection")

	// Also support env vars (env vars override defaults, flags override env vars)
	// RONDB_HOST sets both MySQL and RDRS hosts
	if h := os.Getenv("RONDB_HOST"); h != "" {
		host = h
	}
	// Specific host env vars override RONDB_HOST
	if h := os.Getenv("MYSQL_HOST"); h != "" {
		mysqlHost = h
	}
	if h := os.Getenv("RDRS_HOST"); h != "" {
		rdrsHost = h
	}
	if p := os.Getenv("RONDB_RONDIS_PORT"); p != "" {
		if port, err := strconv.Atoi(p); err == nil {
			rondisPort = port
		}
	}
	if p := os.Getenv("RONDB_MYSQL_PORT"); p != "" {
		if port, err := strconv.Atoi(p); err == nil {
			mysqlPort = port
		}
	}
	if p := os.Getenv("RONDB_RDRS_PORT"); p != "" {
		if port, err := strconv.Atoi(p); err == nil {
			restPort = port
		}
	}
	if u := os.Getenv("RONDB_MYSQL_USER"); u != "" {
		mysqlUser = u
	}
	if p := os.Getenv("RONDB_MYSQL_PASSWORD"); p != "" {
		mysqlPass = p
	}
	if k := os.Getenv("RONDB_RDRS_API_KEY"); k != "" {
		rdrsAPIKey = k
	}

	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(versionCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
