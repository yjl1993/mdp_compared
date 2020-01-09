package synchronize

import (
	"encoding/json"
	"fmt"
	"github.com/influxdata/influxdb1-client/v2"
	"mdp_compared/query"
	"strconv"
	"time"
)

func Synchronize1(addra, addrb string, port int, dbname, msname,
	diffStaTimeStr, diffEndTimeStr string, fieldKeys, tagKeys [][]interface{}) {
	start := time.Now()
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database: dbname,
	})
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: fmt.Sprintf("%s:%d", addrb, port),
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()
	resA := query.QueryDiffData(addra, port, dbname, msname, diffStaTimeStr, diffEndTimeStr)
	resB := query.QueryDiffData(addrb, port, dbname, msname, diffStaTimeStr, diffEndTimeStr)
	if len(resA[0].Series) != 0 {
		valuesListA := resA[0].Series[0].Values
		tagKeysListA := resA[0].Series[0].Columns
		fieldValueType := fieldKeys[0][1].(string)
		measurement := resA[0].Series[0].Name
		if len(resB[0].Series) == 0 { //如果该时间段的时间间隔数据为0，直接将A节点数据写入节点B中
			for i := 0; i < len(valuesListA); i++ {
				pointMap := make(map[string]interface{}, len(tagKeysListA))
				for j, k := range tagKeysListA {
					pointMap[k] = valuesListA[i][j]
				}

				tagMap := make(map[string]string, len(tagKeys))
				for _, v := range tagKeys {
					tagMap[v[0].(string)] = pointMap[v[0].(string)].(string)
				}

				fieldMap := make(map[string]interface{}, len(fieldKeys))
				for _, v := range fieldKeys {
					fieldValue, err := json.Marshal(pointMap[v[0].(string)])
					if err != nil {
						fmt.Println("create json faild", err)
					}
					switch fieldValueType {
					case "float":
						fieldValues, _ := strconv.ParseFloat(string(fieldValue), 64)
						fieldMap[v[0].(string)] = fieldValues
					case "integer":
						fieldValues, _ := strconv.ParseInt(string(fieldValue), 10, 64)
						fieldMap[v[0].(string)] = fieldValues
					case "string":
						fieldV := string(fieldValue)[1 : len(fieldValue)-1]
						fieldMap[v[0].(string)] = fieldV
					}
				}
				timestemp := pointMap["time"]
				timeS, _ := time.Parse(time.RFC3339, timestemp.(string))
				pt, err := client.NewPoint(measurement, tagMap, fieldMap, timeS)
				if err != nil {
					fmt.Println("Create point:", pt.String(), "failed!")
				}
				bp.AddPoint(pt)
				if i%1000 == 0 {
					err := c.Write(bp)
					if err != nil {
						fmt.Println("write error", err)
						RestartClient(addrb, bp)
					}
					bp.ClearPoints()
				}
			}
			err := c.Write(bp)
			bp.ClearPoints()
			if err != nil {
				fmt.Println("write error", err)
			}
		} else {
			valuesListB := resB[0].Series[0].Values
			valuesListBMap := make(map[string][]interface{}, len(valuesListB))
			for i := 0; i < len(valuesListB); i++ {
				valuesStr, _ := json.Marshal(valuesListB[i])
				valuesListBMap[string(valuesStr)] = valuesListB[i]
			}
			for i := 0; i < len(valuesListA); i++ {
				valuesListAmaptag, _ := json.Marshal(valuesListA[i])
				if _, ok := valuesListBMap[string(valuesListAmaptag)]; ok {
					continue
				} else {
					pointMap := make(map[string]interface{}, len(tagKeysListA))
					for j, k := range tagKeysListA {
						pointMap[k] = valuesListA[i][j]
					}

					tagMap := make(map[string]string, len(tagKeys))
					for _, v := range tagKeys {
						tagMap[v[0].(string)] = pointMap[v[0].(string)].(string)
					}

					fieldMap := make(map[string]interface{}, len(fieldKeys))
					for _, v := range fieldKeys {
						fieldValue, _ := json.Marshal(pointMap[v[0].(string)])
						//fieldValueType := v[1].(string)
						switch fieldValueType {
						case "float":
							fieldValues, _ := strconv.ParseFloat(string(fieldValue), 64)
							fieldMap[v[0].(string)] = fieldValues
						case "integer":
							fieldValues, _ := strconv.ParseInt(string(fieldValue), 10, 64)
							fieldMap[v[0].(string)] = fieldValues
						case "string":
							fieldValues := string(fieldValue)[1 : len(fieldValue)-1] //去掉“”
							fieldMap[v[0].(string)] = fieldValues
						}
					}
					timestemp := pointMap["time"]
					timeStr, _ := time.Parse(time.RFC3339, timestemp.(string))
					pt, err := client.NewPoint(measurement, tagMap, fieldMap, timeStr)
					if err != nil {
						fmt.Println("Create point:", pt.String(), "failed!")
					}
					bp.AddPoint(pt)
					if i%1000 == 0 {
						err := c.Write(bp)
						if err != nil {
							fmt.Println("write error", err)
							RestartClient(addrb, bp)
						}
						bp.ClearPoints()
					}
				}
			}
			err := c.Write(bp)
			if err != nil {
				fmt.Println("write error", err)
			}
		}
	}
	end := time.Now()
	fmt.Println("\nEnd of synchronization", diffStaTimeStr, ":::::", diffEndTimeStr,
		"used time:", end.Sub(start))
}

func RestartClient(addr string, bp client.BatchPoints) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: addr,
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}

	err = c.Write(bp)
	if err != nil {
		fmt.Println("write points error: ", err)
		RestartClient(addr, bp)
	}
	bp.ClearPoints()
}

func SynchronizeCq1(addra, addrb string, port int, dbname, cqmsname,
	starttime, endtime string) {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database: dbname,
	})
	addr := fmt.Sprintf("%s:%d", addrb, port)
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: addr,
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()
	res := query.QueryDiffData(addra, port, dbname, cqmsname, starttime, endtime)
	valuesList := res[0].Series[0].Values
	tagKeysList := res[0].Series[0].Columns
	fieldKeys := query.QueryFieldKeys(addra, port, dbname, cqmsname)
	measurement := res[0].Series[0].Name
	for i := 0; i < len(valuesList); i++ {
		pointMap := make(map[string]interface{}, len(tagKeysList))
		for j, k := range tagKeysList {
			pointMap[k] = valuesList[i][j]
		}

		tagMap := make(map[string]string)

		fieldMap := make(map[string]interface{}, len(fieldKeys))
		for _, v := range fieldKeys {
			fieldValue, _ := json.Marshal(pointMap[v[0].(string)])
			fieldValues, _ := strconv.ParseInt(string(fieldValue), 10, 64)
			fieldMap[v[0].(string)] = fieldValues
		}

		timestemp := valuesList[i][0]
		timeStr, _ := time.Parse(time.RFC3339, timestemp.(string))
		pt, err := client.NewPoint(measurement, tagMap, fieldMap, timeStr)
		if err != nil {
			fmt.Println("Create point:", pt.String(), "failed!")
		}
		bp.AddPoint(pt)
		if i%10 == 0 {
			err := c.Write(bp)
			if err != nil {
				fmt.Println("write error", err)
			}
			bp.ClearPoints()
		}
	}
	c.Write(bp)
}
