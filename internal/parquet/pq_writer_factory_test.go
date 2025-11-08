package parquet

import (
	"context"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/kweheliye/json2parquet/models"
	"github.com/kweheliye/json2parquet/utils"
	"github.com/spf13/viper"
)

func TestPqWriteCloserURIIncrement(t *testing.T) {
	const (
		expectedURIZero = "/tmp/mrf_0000.zstd.parquet"
		expectedURIOne  = "/tmp/mrf_0001.zstd.parquet"
	)

	pwf := NewPqWriterFactory("mrf", "/tmp/")
	pwc, err := pwf.CreateWriter(context.TODO())
	assert.NoError(t, err)

	err = pwc.Close()
	assert.NoError(t, err)

	assert.Equal(t, expectedURIZero, pwc.URI())

	pwc, err = pwf.CreateWriter(context.TODO())
	assert.NoError(t, err)

	err = pwc.Close()
	assert.NoError(t, err)

	assert.Equal(t, expectedURIOne, pwc.URI())
}

// test writing data to a parquet file
func TestPqWriteCloserWrite(t *testing.T) {
	var mrfList = []*models.Mrf{{UUID: utils.GetUniqueID(), ParentUUID: utils.GetUniqueID()},
		{UUID: utils.GetUniqueID(), ParentUUID: utils.GetUniqueID()}}

	pwf := NewPqWriterFactory("mrf", "/tmp/")
	pwc, err := pwf.CreateWriter(context.TODO())
	assert.NoError(t, err)

	rows, err := pwc.Write(mrfList)
	assert.NoError(t, err)

	err = pwc.Close()
	assert.NoError(t, err)

	assert.Equal(t, 2, rows)
}

// test NewPqWriterFactory config
func TestNewPqWriterFactoryConfig(t *testing.T) {
	const (
		expectedMaxRowsPerFile  = 555
		expectedMaxRowsPerGroup = 666
		expectedOutputTemplate  = "_mrf.parquet"
	)

	viper.Set("writer.max_rows_per_file", expectedMaxRowsPerFile)
	viper.Set("writer.filename_template", expectedOutputTemplate)
	viper.Set("writer.max_rows_per_group", expectedMaxRowsPerGroup)

	pwf := NewPqWriterFactory("file", "/tmp/output")
	assert.Equal(t, expectedMaxRowsPerFile, pwf.MaxRowsPerFile)
	assert.Equal(t, expectedMaxRowsPerGroup, pwf.MaxRowsPerGroup)
	assert.Equal(t, "/tmp/output/file"+expectedOutputTemplate, pwf.filenameTemplate)
}
