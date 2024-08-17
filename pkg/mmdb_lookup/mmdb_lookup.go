package mmdb_lookup

import (
	"context"
	"errors"
	"github.com/Motmedel/utils_go/utils_go"
	"github.com/maxmind/mmdbinspect/pkg/mmdbinspect"
	"github.com/oschwald/maxminddb-golang"
	"iter"
	"slices"
	"sync"
)

func Lookup(maybeNetwork string, reader *maxminddb.Reader) ([]*mmdbinspect.RecordForNetwork, error) {
	recordsAny, err := mmdbinspect.RecordsForNetwork(*reader, true, maybeNetwork)
	if err != nil {
		return nil, &utils_go.InputError{
			Message: "An error occurred when obtaining records.",
			Cause:   err,
			Input:   maybeNetwork,
		}
	}

	records, ok := recordsAny.([]any)
	if !ok {
		return nil, errors.New("recordsAny could not be converted into a slice of any")
	}

	var recordForNetworkSlice []*mmdbinspect.RecordForNetwork

	for _, record := range records {
		recordForNetwork, ok := record.(mmdbinspect.RecordForNetwork)
		if !ok {
			return nil, errors.New("a record could not be converted into a RecordForNetwork")
		}
		recordForNetworkSlice = append(recordForNetworkSlice, &recordForNetwork)
	}

	return recordForNetworkSlice, nil
}

func LookupNetworkIterator(
	ctx context.Context,
	seq iter.Seq[string],
	reader *maxminddb.Reader,
	callback func(string, []*mmdbinspect.RecordForNetwork),
) []*mmdbinspect.RecordForNetwork {
	var allRecords []*mmdbinspect.RecordForNetwork

	var waitGroup sync.WaitGroup
	var recordsMutex sync.Mutex

	for maybeNetwork := range seq {
		if maybeNetwork == "" {
			continue
		}

		maybeNetwork := maybeNetwork

		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()

			records, err := Lookup(maybeNetwork, reader)
			if err != nil {
				utils_go.LogError(
					"An error occurred when retrieving records.",
					err,
					utils_go.GetLoggerFromCtxWithDefault(ctx, nil),
				)
			}

			if len(records) != 0 {
				if callback != nil {
					callback(maybeNetwork, records)
				}
				recordsMutex.Lock()
				allRecords = append(allRecords, records...)
				recordsMutex.Unlock()
			}
		}()
	}

	waitGroup.Wait()

	return allRecords
}

func LookupNetworks(
	ctx context.Context,
	networks []string,
	reader *maxminddb.Reader,
	callback func(string, []*mmdbinspect.RecordForNetwork),
) []*mmdbinspect.RecordForNetwork {
	return LookupNetworkIterator(
		ctx,
		slices.Values(networks),
		reader,
		callback,
	)
}
