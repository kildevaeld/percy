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
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var verbose bool

// indexCmd represents the index command
var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {

		if len(args) == 0 {
			fmt.Printf("must provide a bucket\n")
			os.Exit(0)
		}

		fmt.Printf("using database at path: %s\n\n", viper.GetString("path"))

		db, err := percy.Open(percy.StoreConfig{
			Path: viper.GetString("path"),
		})

		if err != nil {
			fmt.Printf("err %v", err)
		}

		fmt.Printf("Indexes for bucket: %s \n\n", args[0])
		err = db.Indexes(args[0], func(i percy.IndexEntry) error {
			if len(i.Predicate) == 12 {
				fmt.Printf("%x <= %d\n", i.Predicate, len(i.Targets))
				if verbose {
					for _, t := range i.Targets {
						fmt.Printf("  %s\n", t)
					}
				}
				return nil
			}

			if verbose {
				fmt.Printf("%s => %d\n", i.Predicate, len(i.Targets))
				for _, t := range i.Targets {
					fmt.Printf("  %x\n", t)
				}
			} else {
				fmt.Printf("%s => %d\n", i.Predicate, len(i.Targets))
			}

			return nil
		})

		if err != nil {
			fmt.Printf("Error while gettings indexes for %s\n  %s\n", args[0], err.Error())
			os.Exit(1)
		}

		fmt.Println("")

	},
}

func init() {
	RootCmd.AddCommand(indexCmd)

	indexCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// indexCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// indexCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
