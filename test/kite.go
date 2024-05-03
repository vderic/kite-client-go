package main

import (
	"encoding/json"
	"fmt"
	"github.com/vderic/kite-client-go"
	"github.com/vderic/kite-client-go/xrg"
	"io/ioutil"
	"os"
)

func main() {

	// kite://localhost:7878/tmp/gpdb/gpdb*.parquet
	hosts := []string{"localhost:7878"}

	spec := kite.NewParquetFileSpec()
	//spec := kite.NewCsvFileSpec(",", "\"", "\"", "", false)

	//schema := []kite.Coldef{{Name: "col1", Type: "int8"}, {Name: "col2", Type: "fp32", Precision: 1, Scale: 2}}

	jfile, err := os.Open("data/gpdb0.schema")
	if err != nil {
		fmt.Println(err)
		return
	}

	bv, err := ioutil.ReadAll(jfile)
	if err != nil {
		fmt.Println(err)
		return
	}

	var schema []kite.Coldef
	json.Unmarshal(bv, &schema)

	sql := "select * from \"tmp/gpdb/gpdb*.parquet\""

	cli := kite.NewKiteClient()
	cli.Schema(schema).Sql(sql).Fragment(-1, 3).FileSpec(spec).Host(hosts)
	err = cli.Submit()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cli.Close()

	n := 0
	for {
		it, err := cli.NextRow()
		if err != nil {
			fmt.Println(err)
			return
		}
		if it == nil {
			// done
			break
		}

		for i := 0; i < it.Nvec; i++ {
			if i > 0 {
				fmt.Print(",")
			}
			if it.Flag[i] != 0 {
				fmt.Print("NULL")
			} else {
				v, ok := it.Value[i].(xrg.ArrayType)
				if ok {
					fmt.Print(v.Values)
				} else {
					fmt.Print(it.Value[i])
				}

			}
		}
		fmt.Print("\n")
		n++
	}

	fmt.Println("#rows = ", n)
}
