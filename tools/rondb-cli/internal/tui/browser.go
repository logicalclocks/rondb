/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/client"
)

// Colors - RonDB theme: black + orange
var (
	orange     = lipgloss.Color("#FFA500")
	dimOrange  = lipgloss.Color("#CC8400")
	darkGray   = lipgloss.Color("#333333")
	lightGray  = lipgloss.Color("#888888")
	white      = lipgloss.Color("#FFFFFF")
	black      = lipgloss.Color("#000000")

	// Styles
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(orange).
			Background(black).
			Padding(0, 1)

	selectedStyle = lipgloss.NewStyle().
			Foreground(black).
			Background(orange).
			Bold(true)

	normalStyle = lipgloss.NewStyle().
			Foreground(white)

	dimStyle = lipgloss.NewStyle().
			Foreground(lightGray)

	panelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(darkGray).
			Padding(0, 1)

	activePanelStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(orange).
				Padding(0, 1)

	headerStyle = lipgloss.NewStyle().
			Foreground(orange).
			Bold(true)

	typeStyle = lipgloss.NewStyle().
			Foreground(dimOrange)

	helpStyle = lipgloss.NewStyle().
			Foreground(lightGray).
			Italic(true)
)

// TableInfo holds table metadata
type TableInfo struct {
	Database string
	Name     string
	Columns  []client.ColumnInfo
}

// DBClient interface for database operations
type DBClient interface {
	ListDatabases() ([]string, error)
	ListTablesInDB(database string) ([]string, error)
	DescribeTable(database, table string) ([]client.ColumnInfo, error)
	QueryTableData(database, table string, limit int) (*client.TableData, error)
}

// Model is the main TUI model
type Model struct {
	client       DBClient
	databases    []string
	tables       map[string][]string // database -> tables
	selectedDB   int
	selectedTbl  int
	focusLeft    bool
	expanded     map[string]bool // which databases are expanded
	currentTable *TableInfo
	tableData    *client.TableData // current table data
	showData     bool              // toggle between schema and data view
	dataScroll   int               // scroll offset for data view
	width        int
	height       int
	err          error
	quitting     bool
}

// keyMap defines keybindings
type keyMap struct {
	Up     key.Binding
	Down   key.Binding
	Left   key.Binding
	Right  key.Binding
	Enter  key.Binding
	Tab    key.Binding
	Data   key.Binding
	Quit   key.Binding
}

var keys = keyMap{
	Up:    key.NewBinding(key.WithKeys("up", "k")),
	Down:  key.NewBinding(key.WithKeys("down", "j")),
	Left:  key.NewBinding(key.WithKeys("left", "h")),
	Right: key.NewBinding(key.WithKeys("right", "l")),
	Enter: key.NewBinding(key.WithKeys("enter", " ")),
	Tab:   key.NewBinding(key.WithKeys("tab")),
	Data:  key.NewBinding(key.WithKeys("d")),
	Quit:  key.NewBinding(key.WithKeys("q", "esc", "ctrl+c")),
}

// NewModel creates a new TUI model
func NewModel(client DBClient) Model {
	return Model{
		client:    client,
		tables:    make(map[string][]string),
		expanded:  make(map[string]bool),
		focusLeft: true,
	}
}

// Init initializes the model
func (m Model) Init() tea.Cmd {
	return m.loadDatabases
}

func (m *Model) loadDatabases() tea.Msg {
	dbs, err := m.client.ListDatabases()
	if err != nil {
		return errMsg{err}
	}
	return dbsLoadedMsg{dbs}
}

type dbsLoadedMsg struct{ databases []string }
type tablesLoadedMsg struct {
	database string
	tables   []string
}
type schemaLoadedMsg struct{ table *TableInfo }
type dataLoadedMsg struct{ data *client.TableData }
type errMsg struct{ err error }

// Update handles messages
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch {
		case key.Matches(msg, keys.Quit):
			m.quitting = true
			return m, tea.Quit

		case key.Matches(msg, keys.Tab):
			m.focusLeft = !m.focusLeft
			return m, nil

		case key.Matches(msg, keys.Up):
			if m.focusLeft {
				m.moveUp()
			}
			return m, nil

		case key.Matches(msg, keys.Down):
			if m.focusLeft {
				m.moveDown()
			}
			return m, nil

		case key.Matches(msg, keys.Enter):
			if m.focusLeft {
				return m, m.handleEnter()
			}
			return m, nil

		case key.Matches(msg, keys.Left):
			if m.focusLeft {
				m.collapse()
			}
			return m, nil

		case key.Matches(msg, keys.Right):
			if m.focusLeft {
				return m, m.expand()
			}
			return m, nil

		case key.Matches(msg, keys.Data):
			// Toggle data view or load data
			if m.currentTable != nil {
				if m.showData && m.tableData != nil {
					m.showData = false // Switch back to schema
				} else {
					m.showData = true
					return m, m.loadData(m.currentTable.Database, m.currentTable.Name)
				}
			}
			return m, nil
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case dbsLoadedMsg:
		m.databases = msg.databases
		// Auto-expand first database
		if len(m.databases) > 0 {
			return m, m.loadTables(m.databases[0])
		}
		return m, nil

	case tablesLoadedMsg:
		m.tables[msg.database] = msg.tables
		m.expanded[msg.database] = true
		return m, nil

	case schemaLoadedMsg:
		m.currentTable = msg.table
		m.tableData = nil // Reset data when switching tables
		m.showData = false
		return m, nil

	case dataLoadedMsg:
		m.tableData = msg.data
		m.dataScroll = 0
		return m, nil

	case errMsg:
		m.err = msg.err
		return m, nil
	}

	return m, nil
}

func (m *Model) moveUp() {
	items := m.flattenTree()
	idx := m.currentIndex(items)
	if idx > 0 {
		m.selectIndex(items, idx-1)
	}
}

func (m *Model) moveDown() {
	items := m.flattenTree()
	idx := m.currentIndex(items)
	if idx < len(items)-1 {
		m.selectIndex(items, idx+1)
	}
}

type treeItem struct {
	isDB     bool
	database string
	table    string
}

func (m *Model) flattenTree() []treeItem {
	var items []treeItem
	for i, db := range m.databases {
		items = append(items, treeItem{isDB: true, database: db})
		if m.expanded[db] {
			for _, tbl := range m.tables[db] {
				items = append(items, treeItem{isDB: false, database: db, table: tbl})
			}
		}
		_ = i
	}
	return items
}

func (m *Model) currentIndex(items []treeItem) int {
	for i, item := range items {
		if item.isDB {
			if m.selectedTbl == -1 && m.databases[m.selectedDB] == item.database {
				return i
			}
		} else {
			if m.selectedTbl >= 0 && m.databases[m.selectedDB] == item.database {
				tables := m.tables[item.database]
				if m.selectedTbl < len(tables) && tables[m.selectedTbl] == item.table {
					return i
				}
			}
		}
	}
	return 0
}

func (m *Model) selectIndex(items []treeItem, idx int) {
	if idx < 0 || idx >= len(items) {
		return
	}
	item := items[idx]
	if item.isDB {
		for i, db := range m.databases {
			if db == item.database {
				m.selectedDB = i
				m.selectedTbl = -1
				break
			}
		}
	} else {
		for i, db := range m.databases {
			if db == item.database {
				m.selectedDB = i
				tables := m.tables[db]
				for j, tbl := range tables {
					if tbl == item.table {
						m.selectedTbl = j
						break
					}
				}
				break
			}
		}
	}
}

func (m *Model) handleEnter() tea.Cmd {
	if m.selectedTbl == -1 {
		// Database selected - toggle expand
		db := m.databases[m.selectedDB]
		if m.expanded[db] {
			m.expanded[db] = false
		} else {
			return m.loadTables(db)
		}
	} else {
		// Table selected - load schema
		db := m.databases[m.selectedDB]
		tbl := m.tables[db][m.selectedTbl]
		return m.loadSchema(db, tbl)
	}
	return nil
}

func (m *Model) expand() tea.Cmd {
	if m.selectedTbl == -1 && len(m.databases) > 0 {
		db := m.databases[m.selectedDB]
		if !m.expanded[db] {
			return m.loadTables(db)
		}
	}
	return nil
}

func (m *Model) collapse() {
	if m.selectedTbl >= 0 {
		m.selectedTbl = -1
	} else if len(m.databases) > 0 {
		db := m.databases[m.selectedDB]
		m.expanded[db] = false
	}
}

func (m *Model) loadTables(database string) tea.Cmd {
	return func() tea.Msg {
		tables, err := m.client.ListTablesInDB(database)
		if err != nil {
			return errMsg{err}
		}
		return tablesLoadedMsg{database, tables}
	}
}

func (m *Model) loadSchema(database, table string) tea.Cmd {
	return func() tea.Msg {
		cols, err := m.client.DescribeTable(database, table)
		if err != nil {
			return errMsg{err}
		}
		return schemaLoadedMsg{&TableInfo{
			Database: database,
			Name:     table,
			Columns:  cols,
		}}
	}
}

func (m *Model) loadData(database, table string) tea.Cmd {
	return func() tea.Msg {
		data, err := m.client.QueryTableData(database, table, 100)
		if err != nil {
			return errMsg{err}
		}
		return dataLoadedMsg{data}
	}
}

// View renders the UI
func (m Model) View() string {
	if m.quitting {
		return ""
	}

	if m.width == 0 {
		return "Loading..."
	}

	// Calculate panel widths
	leftWidth := m.width * 30 / 100
	if leftWidth < 25 {
		leftWidth = 25
	}
	rightWidth := m.width - leftWidth - 4 // borders

	// Heights
	contentHeight := m.height - 4 // title + help

	// Left panel - tree view
	leftPanel := m.renderTree(leftWidth-4, contentHeight-2)
	leftStyle := panelStyle.Width(leftWidth).Height(contentHeight)
	if m.focusLeft {
		leftStyle = activePanelStyle.Width(leftWidth).Height(contentHeight)
	}

	// Right panel - schema view
	rightPanel := m.renderSchema(rightWidth-4, contentHeight-2)
	rightStyle := panelStyle.Width(rightWidth).Height(contentHeight)
	if !m.focusLeft {
		rightStyle = activePanelStyle.Width(rightWidth).Height(contentHeight)
	}

	// Compose
	title := titleStyle.Render(" RonDB Browser ")
	panels := lipgloss.JoinHorizontal(
		lipgloss.Top,
		leftStyle.Render(leftPanel),
		rightStyle.Render(rightPanel),
	)
	help := helpStyle.Render("  ↑↓ navigate  ←→ collapse/expand  Enter select  Tab panel  d data  q quit")

	return lipgloss.JoinVertical(lipgloss.Left, title, panels, help)
}

func (m *Model) renderTree(width, height int) string {
	var lines []string

	lines = append(lines, headerStyle.Render("DATABASES"))
	lines = append(lines, "")

	for i, db := range m.databases {
		prefix := "▸ "
		if m.expanded[db] {
			prefix = "▾ "
		}

		style := normalStyle
		if i == m.selectedDB && m.selectedTbl == -1 {
			style = selectedStyle
		}

		dbLine := fmt.Sprintf("%s%s", prefix, db)
		if len(dbLine) > width {
			dbLine = dbLine[:width-1] + "…"
		}
		lines = append(lines, style.Render(dbLine))

		if m.expanded[db] {
			for j, tbl := range m.tables[db] {
				tblStyle := dimStyle
				if i == m.selectedDB && j == m.selectedTbl {
					tblStyle = selectedStyle
				}

				tblLine := fmt.Sprintf("   %s", tbl)
				if len(tblLine) > width {
					tblLine = tblLine[:width-1] + "…"
				}
				lines = append(lines, tblStyle.Render(tblLine))
			}
		}
	}

	// Pad to height
	for len(lines) < height {
		lines = append(lines, "")
	}

	return strings.Join(lines[:height], "\n")
}

func (m *Model) renderSchema(width, height int) string {
	var lines []string

	if m.currentTable == nil {
		lines = append(lines, headerStyle.Render("SCHEMA"))
		lines = append(lines, "")
		lines = append(lines, dimStyle.Render("Select a table to view schema"))
		lines = append(lines, dimStyle.Render("Press 'd' to view data"))
	} else if m.showData {
		// Data view
		title := fmt.Sprintf("%s.%s [DATA]", m.currentTable.Database, m.currentTable.Name)
		lines = append(lines, headerStyle.Render(title))

		if m.tableData == nil {
			lines = append(lines, "")
			lines = append(lines, dimStyle.Render("Loading..."))
		} else if len(m.tableData.Rows) == 0 {
			lines = append(lines, "")
			lines = append(lines, dimStyle.Render("(empty table)"))
		} else {
			// Show row count
			countStr := fmt.Sprintf("%d rows", len(m.tableData.Rows))
			if m.tableData.Total > len(m.tableData.Rows) {
				countStr = fmt.Sprintf("%d of %d rows", len(m.tableData.Rows), m.tableData.Total)
			}
			lines = append(lines, dimStyle.Render(countStr))
			lines = append(lines, "")

			// Calculate column widths (simplified)
			colWidths := make([]int, len(m.tableData.Columns))
			for i, col := range m.tableData.Columns {
				colWidths[i] = len(col)
				if colWidths[i] < 8 {
					colWidths[i] = 8
				}
				if colWidths[i] > 25 {
					colWidths[i] = 25
				}
			}

			// Header row
			var headerParts []string
			for i, col := range m.tableData.Columns {
				c := col
				if len(c) > colWidths[i] {
					c = c[:colWidths[i]-1] + "…"
				}
				headerParts = append(headerParts, fmt.Sprintf("%-*s", colWidths[i], c))
			}
			lines = append(lines, dimStyle.Render(strings.Join(headerParts, " │ ")))
			lines = append(lines, dimStyle.Render(strings.Repeat("─", min(width, 80))))

			// Data rows
			for _, row := range m.tableData.Rows {
				var rowParts []string
				for i, val := range row {
					v := val
					if len(v) > colWidths[i] {
						v = v[:colWidths[i]-1] + "…"
					}
					rowParts = append(rowParts, fmt.Sprintf("%-*s", colWidths[i], v))
				}
				lines = append(lines, normalStyle.Render(strings.Join(rowParts, " │ ")))
			}
		}
		lines = append(lines, "")
		lines = append(lines, dimStyle.Render("Press 'd' for schema"))
	} else {
		// Schema view
		title := fmt.Sprintf("%s.%s", m.currentTable.Database, m.currentTable.Name)
		lines = append(lines, headerStyle.Render(title))
		lines = append(lines, "")

		// Column headers
		header := fmt.Sprintf("%-20s %-15s %-8s %-6s", "COLUMN", "TYPE", "NULL", "KEY")
		lines = append(lines, dimStyle.Render(header))
		lines = append(lines, dimStyle.Render(strings.Repeat("─", min(width, 55))))

		for _, col := range m.currentTable.Columns {
			nullable := "YES"
			if col.Nullable == "NO" {
				nullable = "NO"
			}
			keyStr := col.Key
			if keyStr == "" {
				keyStr = "-"
			}

			colName := col.Name
			if len(colName) > 20 {
				colName = colName[:17] + "..."
			}

			colType := col.Type
			if len(colType) > 15 {
				colType = colType[:12] + "..."
			}

			line := fmt.Sprintf("%-20s ", colName)
			line += typeStyle.Render(fmt.Sprintf("%-15s ", colType))
			line += fmt.Sprintf("%-8s %-6s", nullable, keyStr)

			lines = append(lines, normalStyle.Render(line))
		}
		lines = append(lines, "")
		lines = append(lines, dimStyle.Render("Press 'd' for data"))
	}

	if m.err != nil {
		lines = append(lines, "")
		lines = append(lines, lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render(
			fmt.Sprintf("Error: %v", m.err),
		))
	}

	// Pad to height
	for len(lines) < height {
		lines = append(lines, "")
	}

	return strings.Join(lines[:height], "\n")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Run starts the TUI
func Run(client DBClient) error {
	p := tea.NewProgram(NewModel(client), tea.WithAltScreen())
	_, err := p.Run()
	return err
}
