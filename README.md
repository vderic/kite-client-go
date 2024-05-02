kite-client-go is the kite client for golang


```

    // kite://localhost:7878/tmp/gpdb/gpdb*.parquet
    hosts := []string{"localhost:7878"}

    spec := kite.ParquetFileSpec{"parquet"}
    //spec := kite.CsvFileSpec{"csv", ",", "\"", "\"", "", false}

    schema := []kite.Coldef{{Name: "col1", Type: "int8"}, {Name: "col2", Type: "fp32", Precision: 1, Scale: 2}}

    sql := "select * from \"bucket/path/gpdb*.parquet\""

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
            // no more data
            break
        }

        for i := 0; i < it.Nvec; i++ {
            if i > 0 {
                fmt.Print(",")
            }
            if it.Flag[i] != 0 {
                fmt.Print("NULL")
            } else {
                fmt.Print(it.Value[i])
            }
        }
        fmt.Print("\n")
        n++
    }

    fmt.Println("#rows = ", n)

```
