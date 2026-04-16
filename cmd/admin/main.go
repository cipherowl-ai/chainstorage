package main

import (
	"fmt"
	"os"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:          "admin",
		Short:        "admin is a utility for managing chainstorage",
		SilenceUsage: true,
	}
)

func init() {
	// Match the GOMEMLIMIT wiring done for the fx-driven binaries via
	// fxparams.Module. Admin uses Cobra and does not import fxparams, so set
	// the limit directly. Errors (e.g., not running in a cgroup) are silenced
	// since they're not actionable for a CLI.
	_, _ = memlimit.SetGoMemLimitWithOpts()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to execute command: %+v\n", err)
		os.Exit(1)
	}
}
