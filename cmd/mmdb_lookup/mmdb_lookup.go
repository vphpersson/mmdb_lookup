package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Motmedel/utils_go/utils_go"
	"github.com/maxmind/mmdbinspect/pkg/mmdbinspect"
	"io"
	"log/slog"
	"mmdb_lookup/pkg/mmdb_lookup"
	"os"
	"sync"
)

func main() {
	logger := slog.Default()

	var databaseFilePath string
	flag.StringVar(&databaseFilePath, "db", "", "A path of the database file.")

	var ipAddressesFilePath string
	flag.StringVar(&ipAddressesFilePath, "f", "-", "A path of a file of IP addresses.")

	flag.Parse()

	if databaseFilePath == "" {
		fmt.Fprintln(os.Stderr, "error: -db is required\n")
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
		return
	}

	var ipAddressesInput io.Reader

	if ipAddressesFilePath == "-" {
		ipAddressesInput = os.Stdin
	} else {
		var err error
		ipAddressesInput, err = os.Open(ipAddressesFilePath)
		if err != nil {
			utils_go.LogFatal("An error occurred when opening the IP addresses file.", err, logger, 1)
		}
		defer ipAddressesInput.(*os.File).Close()
	}

	databaseReader, err := mmdbinspect.OpenDB(databaseFilePath)
	if err != nil {
		utils_go.LogFatal("An error occurred when opening the mmdb database file.", err, logger, 1)
	}

	var printMutex sync.Mutex
	scanner := bufio.NewScanner(ipAddressesInput)

	mmdb_lookup.LookupNetworkIterator(
		utils_go.CtxWithLogger(context.Background(), logger),
		func(yield func(string) bool) {
			for scanner.Scan() {
				ipAddressString := scanner.Text()
				if ipAddressString == "" {
					continue
				}

				if !yield(ipAddressString) {
					break
				}
			}
		},
		databaseReader,
		func(maybeNetwork string, records []*mmdbinspect.RecordForNetwork) {
			for _, record := range records {
				recordMap, ok := record.Record.(map[string]any)
				if !ok {
					utils_go.LogFatal(
						"recordForNetwork.Record could not be converted to a map[string]any.",
						nil,
						logger,
						1,
					)
				}
				recordMap["ip_address"] = maybeNetwork

				data, err := json.Marshal(recordMap)
				if err != nil {
					utils_go.LogError("An error occurred when marshalling a record map.", err, logger)
					continue
				}
				printMutex.Lock()
				fmt.Println(string(data))
				printMutex.Unlock()
			}
		},
	)
}
