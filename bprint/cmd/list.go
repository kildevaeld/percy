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
	"encoding/json"
	"fmt"
	"os"

	"github.com/kildevaeld/percy"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var jsonFormat bool

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list",
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

		db, err := store.Open(store.StoreConfig{
			Path:  viper.GetString("path"),
			Debug: debug,
		})

		if err != nil {
			fmt.Printf("Error while opening database\n  %v\n\n", err)
			os.Exit(1)
		}

		var m []store.Map

		if err := db.List([]byte(args[0]), &m); err != nil {
			fmt.Printf("Error while gettings items in %s\n  %v\n\n", args[0], err)
			os.Exit(1)
		}

		for _, i := range m {
			if jsonFormat {
				b, _ := json.MarshalIndent(&i, "", "  ")
				fmt.Printf("%s\n", b)
			} else {
				for k, v := range i {
					fmt.Printf("%s = %v\n", k, v)
				}
				fmt.Println("")
			}
		}

	},
}

func init() {
	RootCmd.AddCommand(listCmd)

	listCmd.Aliases = []string{"ls"}
	listCmd.Flags().BoolVarP(&jsonFormat, "json", "j", false, "")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// listCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// listCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
