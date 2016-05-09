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
	ut "github.com/kildevaeld/percy/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// addCmd represents the add command
var addCmd = &cobra.Command{
	Use:   "add",
	Short: "A brief description of your command",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: Work your own magic here
		debug := viper.GetBool("debug")
		if len(args) != 2 {
			fmt.Printf("must provide a bucket and json\n")
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

		var m ut.Map

		err = json.Unmarshal([]byte(args[1]), &m)

		if err != nil {
			fmt.Printf("Malformed json:\n  %v\n\n", err)
			os.Exit(1)
		}

		m["id"] = ut.NewSid()

		err = db.Create([]byte(args[0]), m["id"], m)
		if err != nil {
			fmt.Printf("Error while inserting into bucket: %s\n  %s\n\n", args[0], err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(addCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// addCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// addCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
