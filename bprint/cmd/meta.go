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
	"text/tabwriter"

	"github.com/kildevaeld/percy"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// metaCmd represents the meta command
var metaCmd = &cobra.Command{
	Use:   "meta",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: Work your own magic here

		/*if len(args) == 0 {
			fmt.Printf("must provide a bucket\n")
			os.Exit(0)
		}*/

		db, _ := store.Open(store.StoreConfig{
			Path: viper.GetString("path"),
		})

		meta := db.Meta()

		fmt.Printf("Datbase version: %d\n", meta.Version)
		fmt.Printf("Buckets (%d):\n", len(meta.Buckets))

		w := new(tabwriter.Writer)
		w.Init(os.Stdout, 0, 8, 2, '\t', 0)
		fmt.Fprintln(w, "Name\tIndexes\tItems")
		for _, bucket := range meta.Buckets {
			fmt.Fprintf(w, "%s\t%d\t%d\n", bucket.Name, len(bucket.Indexes), bucket.Items)
			//fmt.Printf("  %s indexes: %d, items: %d \n", bucket.Name, len(bucket.Indexes), bucket.Items)
		}
		w.Flush()
		fmt.Printf("")
	},
}

func init() {
	RootCmd.AddCommand(metaCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// metaCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// metaCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
