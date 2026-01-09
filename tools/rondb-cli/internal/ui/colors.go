package ui

import (
	"fmt"
	"time"

	"github.com/charmbracelet/lipgloss"
)

var (
	successStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	errorStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	warningStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	infoStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
	promptStyle    = lipgloss.NewStyle().Bold(true)
	timingStyle    = lipgloss.NewStyle().Faint(true)
	keyStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("4"))
	valueStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("7"))
)

// Success renders a message with green styling
func Success(msg string) string {
	return successStyle.Render("[OK] " + msg)
}

// Error renders a message with red styling
func Error(msg string) string {
	return errorStyle.Render("[ERROR] " + msg)
}

// Warning renders a message with yellow styling
func Warning(msg string) string {
	return warningStyle.Render("[WARN] " + msg)
}

// Info renders a message with cyan styling
func Info(msg string) string {
	return infoStyle.Render("[*] " + msg)
}

// Prompt returns the styled "rondb> " prompt
func Prompt() string {
	return promptStyle.Render("rondb> ")
}

// Timing returns a styled timing string in the format "(X.Xms)"
func Timing(duration time.Duration) string {
	ms := duration.Milliseconds()
	us := duration.Microseconds() % 1000
	return timingStyle.Render(fmt.Sprintf("(%.1fms)", float64(ms)+float64(us)/1000.0))
}

// Key renders a string with blue styling for Redis keys
func Key(k string) string {
	return keyStyle.Render(k)
}

// Value renders a string with white styling for values
func Value(v string) string {
	return valueStyle.Render(v)
}

// Connected returns a styled connection message
func Connected(version string) string {
	return successStyle.Render("Connected to RonDB " + version)
}

// Welcome returns a welcome banner
func Welcome() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	subtitleStyle := lipgloss.NewStyle().Faint(true)

	banner := titleStyle.Render("RonDB CLI") + " - " + subtitleStyle.Render("Rondis commands. SQL queries. One database.")
	hint := subtitleStyle.Render("Type .help for commands, Tab for autocomplete")

	return banner + "\n" + hint
}

// Disconnected returns a styled disconnection message
func Disconnected() string {
	return errorStyle.Render("Disconnected")
}
