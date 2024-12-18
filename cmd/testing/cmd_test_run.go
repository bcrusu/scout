package main

import (
	"fmt"
	"path"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/testing/jepsen"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newTestRunCmd() *cobra.Command {
	runPrefix := "run"

	getConfig := func(c *cobra.Command) (jepsen.Config, error) {
		socketPath, err := getSocketPath(c)
		if err != nil {
			return jepsen.Config{}, err
		}

		workDir, err := getWorkDir(c)
		if err != nil {
			return jepsen.Config{}, err
		}

		runsDir := path.Join(workDir, "runs")
		if err := utils.MkdirsAll(runsDir); err != nil {
			return jepsen.Config{}, err
		}

		lastRun, err := utils.GetLastSuffix(runsDir, runPrefix)
		if err != nil {
			return jepsen.Config{}, err
		}

		runId := lastRun + 1

		return jepsen.Config{
			RunId:          runId,
			ClusterName:    clusterName,
			SocketPath:     socketPath,
			OutputDir:      path.Join(workDir, "runs", fmt.Sprintf("%s%05d", runPrefix, runId)),
			Concurrency:    errors.Assert2(c.Flags().GetInt("concurrency")),
			Duration:       errors.Assert2(c.Flags().GetDuration("duration")),
			ReadWriteRatio: errors.Assert2(c.Flags().GetFloat64("rw-ratio")),
			RequestRate:    errors.Assert2(c.Flags().GetInt("request-rate")),
			RequestMinKeys: errors.Assert2(c.Flags().GetInt("min-keys")),
			RequestMaxKeys: errors.Assert2(c.Flags().GetInt("max-keys")),
		}, nil
	}

	c := &cobra.Command{
		Use:           "run",
		Aliases:       []string{"r"},
		Short:         "Executes a single Jepsen-style test run.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			config, err := getConfig(c)
			if err != nil {
				return err
			}

			log := logging.New("runner")
			runner := jepsen.NewRunner(c.Context(), config)
			return utils.LifecycleRun(c.Context(), log, runner)
		},
	}

	c.PersistentFlags().IntP("concurrency", "c", 1, "Number of workers to run.")
	c.PersistentFlags().DurationP("duration", "d", time.Minute, "Total test runtime.")
	c.PersistentFlags().IntP("request-rate", "r", 10, "Total request rate (per second).")
	c.PersistentFlags().Float64("rw-ratio", 1, "Read/Write request ratio.")
	c.PersistentFlags().Int("min-keys", 1, "Request min key count.")
	c.PersistentFlags().Int("max-keys", 3, "Request max key count.")
	c.PersistentFlags().StringSliceP("nemesis", "n", nil, "Nemesis list to enable.")
	c.PersistentFlags().Duration("nemesis-interval", 2*time.Second, "Duration between nemesis operations.")

	return c
}
