package main

import (
	"os"
	"slices"

	"github.com/bcrusu/scout/internal/utils"
	"github.com/olekukonko/tablewriter"
)

type getRow[T any] func(T) []string
type sortFn[T any] func(T, T) int

func mapToTable[K comparable, V any](in map[K]V, sortFn sortFn[V], getRow getRow[V]) [][]string {
	items := utils.MakeValueSlice(in)
	return sliceToTable(items, sortFn, getRow)
}

func sliceToTable[V any](items []V, sortFn sortFn[V], getRow getRow[V]) [][]string {
	slices.SortFunc(items, sortFn)

	rows := make([][]string, len(items))

	for i, item := range items {
		rows[i] = getRow(item)
	}

	return rows
}

func newTable(headers []string, rows [][]string) *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(headers)
	table.AppendBulk(rows)
	return table
}

func renderTable(headers []string, rows [][]string) {
	table := newTable(headers, rows)
	table.Render()
}
