package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

// Runs the Elle transactional safety checker for a test run history:
//   - https://github.com/jepsen-io/elle
//   - https://github.com/ligurio/elle-cli
//   - Paper: "Elle: Inferring Isolation Anomalies from Experimental Observations" by
//     Kyle Kingsbury (Jepsen) and Peter Alvaro (UC Santa Cruz).
func newTestCheckCmd() *cobra.Command {
	runElle := func(imageName string, targetDir string) error {
		outputDir := fmt.Sprintf("/scout/elle%d", time.Now().Unix())

		args := []string{
			"run",
			"--rm",
			"-v", targetDir + ":/scout",
			imageName,
			"-m", "rw-register",
			"-f", "json",
			"-d", outputDir,
			"-p", "svg",
			"-v",
			"/scout/history.json",
		}

		cmd := exec.Command("docker", args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}

	c := &cobra.Command{
		Use:           "check TEST",
		Aliases:       []string{"c"},
		Short:         "Checks a test run history using Elle.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.Error("expected single test name/id arg")
			}

			workDir, err := getWorkDir(c)
			if err != nil {
				return err
			}

			arg := args[0]
			var targetDir string

			if runId, err := strconv.ParseUint(arg, 10, 64); err == nil {
				targetDir = path.Join(workDir, "runs", fmt.Sprintf("%s%05d", "run", runId))
			} else {
				targetDir = path.Join(workDir, "runs", arg)
			}

			if exists, err := utils.PathExists(targetDir); err != nil || !exists {
				return errors.Error("run dir does not exist")
			}

			imageName, err := c.Flags().GetString("image")
			if err != nil {
				return err
			}

			return runElle(imageName, targetDir)
		},
	}

	c.PersistentFlags().String("image", "scout/elle_cli", "Elle-cli docker image name.")

	return c
}
