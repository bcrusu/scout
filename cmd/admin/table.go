package main

import (
	"os"
	"slices"

	"github.com/bcrusu/scout/internal/utils"
	"github.com/olekukonko/tablewriter"
)

type getRow[T any] func(int, T) row
type sortFn[T any] func(T, T) int
type row = []string

func mapToTable[K comparable, V any](in map[K]V, sortFn sortFn[V], getRow getRow[V]) []row {
	items := utils.MakeValueSlice(in)
	return sliceToTable(items, sortFn, getRow)
}

func sliceToTable[V any](items []V, sortFn sortFn[V], getRow getRow[V]) []row {
	slices.SortFunc(items, sortFn)
	rows := make([]row, len(items))

	for i, item := range items {
		rows[i] = getRow(i+1, item)
	}

	return rows
}

func newTable(headers []string, rows []row) *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(headers)
	table.AppendBulk(rows)
	return table
}

func renderTable(headers []string, rows []row) {
	table := newTable(headers, rows)
	table.Render()
}
