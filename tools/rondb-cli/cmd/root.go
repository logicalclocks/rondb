package cmd

import (
	"fmt"
	"os"
	"strconv"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/client"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/shell"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
	"github.com/spf13/cobra"
)

const Version = "0.1.0"

var (
	host       string
	rondisPort int
	mysqlPort  int
)

var rootCmd = &cobra.Command{
	Use:   "rondb",
	Short: "RonDB CLI",
	Long:  "RonDB CLI - Rondis commands. SQL queries. One database.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return shell.RunWithConfig(host, rondisPort, mysqlPort)
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check RonDB connection status",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Checking %s...\n", host)

		// Check Rondis
		rondisClient, err := client.NewRondisClient(host, rondisPort)
		if err != nil {
			fmt.Println(ui.Error(fmt.Sprintf("Rondis (:%d): %v", rondisPort, err)))
		} else {
			fmt.Println(ui.Success(fmt.Sprintf("Rondis (:%d): connected", rondisPort)))
			rondisClient.Close()
		}

		// Check MySQL
		mysqlUser := os.Getenv("RONDB_MYSQL_USER")
		if mysqlUser == "" {
			mysqlUser = "root"
		}
		mysqlPass := os.Getenv("RONDB_MYSQL_PASSWORD")

		mysqlClient, err := client.NewMySQLClient(host, mysqlPort, mysqlUser, mysqlPass)
		if err != nil {
			fmt.Println(ui.Error(fmt.Sprintf("MySQL (:%d): %v", mysqlPort, err)))
		} else {
			fmt.Println(ui.Success(fmt.Sprintf("MySQL (:%d): connected as %s", mysqlPort, mysqlUser)))
			mysqlClient.Close()
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
	rootCmd.PersistentFlags().StringVar(&host, "host", "localhost", "RonDB host")
	rootCmd.PersistentFlags().IntVar(&rondisPort, "rondis-port", 6379, "Rondis port")
	rootCmd.PersistentFlags().IntVar(&mysqlPort, "mysql-port", 3306, "MySQL port")

	// Also support env vars
	if h := os.Getenv("RONDB_HOST"); h != "" {
		host = h
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

	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(versionCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
