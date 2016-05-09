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

	"github.com/kildevaeld/percy"
	"github.com/kildevaeld/percy/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: Work your own magic here
		debug := viper.GetBool("debug")
		if len(args) != 2 {
			fmt.Printf("must provide a bucket\n")
			os.Exit(0)
		}
		if debug {
			fmt.Printf("using database at path: %s\n\n", viper.GetString("path"))
		}

		db, err := percy.Open(percy.StoreConfig{
			Path:     viper.GetString("path"),
			Debug:    debug,
			ReadOnly: false,
		})

		if err != nil {
			fmt.Printf("Error while opening database\n  %v\n\n", err)
			os.Exit(1)
		}
		var m utils.Map
		if err = db.Get([]byte(args[0]), args[1], &m); err != nil {
			fmt.Printf("Error while gettings: %s from %s\n  %v\n\n", args[0], args[1], err)
		}

		fmt.Printf("m %v", m)
	},
}

func init() {
	RootCmd.AddCommand(getCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// getCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// getCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
