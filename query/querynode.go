package query

import (
	"fmt"
	"github.com/influxdata/influxdb1-client/v2"
	"log"
)

func QueryDiffData(address string, port int, dbname, msname, diffStaTimeStr, diffEndTimeStr string) []client.Result {
	conn := connInflux(address, port)
	var qs = fmt.Sprintf("select * from %s where time >= '%s' and time <= '%s'",
		msname, diffStaTimeStr, diffEndTimeStr)
	res, err := QueryDB(conn, dbname, qs)
	if err != nil {
		log.Fatal(err)
	}
	return res
}

func QueryDurTime(address string, port int, dbname, msname, starttime, endtime string) (countV [][]interface{}, isEmpty bool) {
	conn := connInflux(address, port)
	var qs = fmt.Sprintf("select * from %s where time >= '%s' and time <= '%s'",
		msname, starttime, endtime)
	res, err := QueryDB(conn, dbname, qs)
	if err != nil {
		log.Fatal(err)
	}
	if len(res[0].Series) == 0 {
		return [][]interface{}{}, true
	} else {
		countV := res[0].Series[0].Values
		return countV, false
	}
}

func QueryTagKeys(address string, port int, dbname, msname string) [][]interface{} {
	conn := connInflux(address, port)
	var qs = fmt.Sprintf("show tag keys from %s",
		msname)
	res, err := QueryDB(conn, dbname, qs)
	if err != nil {
		log.Fatal(err)
	}
	result := res[0].Series[0].Values
	return result
}

func QueryFieldKeys(address string, port int, dbname, msname string) [][]interface{} {
	conn := connInflux(address, port)
	var qs = fmt.Sprintf("show field keys from %s",
		msname)
	res, err := QueryDB(conn, dbname, qs)
	if err != nil {
		log.Fatal(err)
	}
	result := res[0].Series[0].Values
	return result
}

func connInflux(address string, port int) client.Client {
	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: fmt.Sprintf("%s:%d", address, port),
	})
	if err != nil {
		log.Fatal(err)
	}
	return cli
}

func QueryDB(cli client.Client, dbname, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: dbname,
	}
	if response, err := cli.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	fmt.Println("no here")
	return res, nil
}
