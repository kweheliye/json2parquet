package parse

import (
	"context"
	"encoding/csv"
	"io"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/kweheliye/json2parquet/utils"

	"github.com/kweheliye/json2parquet/utils/cloud"
	"github.com/spf13/viper"
)

// loadServiceList loads a list of services from a csv file and returns a stringSet of the services.
// The csv file is expected to have a header row, with first column being the
// CPT/HCPCS service code, and subsequent columns being ignored.
func loadServiceList(uri string) StringSet {
	var f io.ReadCloser
	var err error
	var services StringSet = mapset.NewSet[string]()

	// if empty, get from config file
	if uri == "" {
		log.Infof("uri is empty %v", uri)
		uri = viper.GetString("services.file")
		uri = "services.csv"
	}
	log.Infof("uri is empty %v", uri)

	f, err = cloud.NewReader(context.TODO(), uri)
	utils.ExitOnError(err)

	defer func(f io.ReadCloser) {
		err = f.Close()
		if err != nil {
			utils.ExitOnError(err)
		}
	}(f)

	csvReader := csv.NewReader(f)
	serviceData, err := csvReader.ReadAll()
	utils.ExitOnError(err)

	// extract the first column, the CPT/HCPCS code, from csv,
	// skipping the header row
	for _, s := range serviceData[1:] { // skip header
		services.Add(s[0])
	}

	return services
}
