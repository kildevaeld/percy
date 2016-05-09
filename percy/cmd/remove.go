// Copyright © 2015 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/kildevaeld/percy"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var removeBucket bool

// removeCmd represents the remove command
var removeCmd = &cobra.Command{
	Use:   "remove",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: Work your own magic here
		debug := viper.GetBool("debug")
		if len(args) == 0 {
			fmt.Printf("must provide a bucket\n")
			os.Exit(0)
		}
		if debug {
			fmt.Printf("using database at path: %s\n\n", viper.GetString("path"))
		}

		db, err := percy.Open(percy.StoreConfig{
			Path:  viper.GetString("path"),
			Debug: debug,
		})

		if err != nil {
			fmt.Printf("Error while opening database\n  %v\n\n", err)
			os.Exit(1)
		}
		start := time.Now()
		if removeBucket {
			fmt.Printf("Removing bucket... ")
			err := db.RemoveBucket([]byte(args[0]))
			if err != nil {
				fmt.Printf("Error while removing bucket: %s\n  %v\n\n", args[0], err)
				os.Exit(1)
			}

		}

		elapsed := time.Since(start)
		fmt.Printf("done (%s)\n\n", elapsed)

	},
}

func init() {
	RootCmd.AddCommand(removeCmd)

	removeCmd.Flags().BoolVarP(&removeBucket, "bucket", "b", false, "")
	removeCmd.Aliases = []string{"rm"}
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// removeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// removeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
