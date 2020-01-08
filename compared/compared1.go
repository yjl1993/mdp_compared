package compared

import (
	"encoding/json"
	"fmt"
	client "github.com/influxdata/influxdb1-client/v2"
	"mdp_compared/query"
	"mdp_compared/synchronize"
	"reflect"
	"sync"
	"time"
)

func ComparedCQ1(addr1, addr2, addr3 string, port, worker int, dbname,
	msname, cqmsname, starttime, endtime, groupbytime, model string) {
	cqcount1, _ := query.QueryDurTime(addr1, port, dbname, cqmsname, starttime, endtime)
	cqcount2, isEmpty2 := query.QueryDurTime(addr2, port, dbname, cqmsname, starttime, endtime)
	//cqcount3, isEmpty3 := query.QueryDurTime(addr3,port,dbname,cqmsname,starttime,endtime)
	fieldKeys := query.QueryFieldKeys(addr1, port, dbname, msname)
	tagKeys := query.QueryTagKeys(addr1, port, dbname, msname)
	fmt.Printf("Start comparing %s and %s. \n", addr1, addr2)
	match1To2 := reflect.DeepEqual(cqcount1, cqcount2)
	if match1To2 == false {
		Compared1(addr1, addr2, port, worker, dbname, msname, groupbytime, isEmpty2, cqcount1, cqcount2, fieldKeys, tagKeys)
		synchronize.SynchronizeCq1(addr1, addr2, port, dbname, cqmsname, starttime, endtime)
		fmt.Println("End of synchronization")
	} else {
		fmt.Printf("Node %s and node %s are synchronized. \n", addr1, addr2)
	}

	//match1To3 := reflect.DeepEqual(cqcount1, cqcount3)
	//if match1To3 == false {
	//	Compared1(addr1, addr3, port, worker, dbname, msname,groupbytime, isEmpty3, cqcount1, cqcount3,fieldKeys,tagKeys)
	//	//synchronize.SynchronizeCq1(addr1,addr3,port,dbname,cqmsname,starttime,endtime)
	//	fmt.Println("End of synchronization")
	//} else {
	//	fmt.Printf("Node %s and node %s are synchronized. \n", addr1, addr3)
	//}
}

func Compared1(addr1, addr2 string, port, worker int, dbname, msname, groupbytime string,
	isemptyB bool, cqcountlistA, cqcountlistB, fieldKeys, tagKeys [][]interface{}) {
	if isemptyB { //如果这个时间段B节点的count是空的，不用比较，直接将节点A的数据写入B节点中
		diffStaTimeStrList := make([]string, 0, len(cqcountlistA))
		diffEndTimeStrList := make([]string, 0, len(cqcountlistB))
		for i := 0; i < len(cqcountlistA); i++ { //[[2019-12-19T06:31:40Z 14335] [2019-12-19T06:32:00Z 151590]]
			countAStr, err := json.Marshal(cqcountlistA[i][1])
			if err != nil {
				fmt.Println("生成json字符串失败", err)
			}
			if string(countAStr) != "0" { //这个时间段A节点不为0的
				diffStaTimeStr := cqcountlistA[i][0].(string)
				diffStaTimeStrList = append(diffStaTimeStrList, diffStaTimeStr)
				diffStaTime, _ := time.Parse(time.RFC3339, diffStaTimeStr)
				durtime, _ := time.ParseDuration(groupbytime)
				diffEndTimeStr := diffStaTime.Add(durtime).Format(time.RFC3339)
				diffEndTimeStrList = append(diffEndTimeStrList, diffEndTimeStr)
			}
		}
		wg := &sync.WaitGroup{}
		diffStaTimeChan := make(chan string, len(diffStaTimeStrList))
		diffEndTimeChan := make(chan string, len(diffEndTimeStrList))
		for i := 0; i < len(diffStaTimeStrList); i++ {
			diffStaTimeChan <- diffStaTimeStrList[i]
			diffEndTimeChan <- diffEndTimeStrList[i]
		}
		for i := 0; i < len(diffStaTimeStrList); i++ {
			if i%worker == 0 {
				for j := 0; j < worker; j++ {
					select {
					case diffStaTimeStr := <-diffStaTimeChan:
						diffEndTimeStr := <-diffEndTimeChan
						wg.Add(1)
						go func(diffStaTimeStr, diffEndTimeStr string) {
							defer wg.Done()
							synchronize.Synchronize1(addr1, addr2, port, dbname, msname,
								diffStaTimeStr, diffEndTimeStr, fieldKeys, tagKeys)
						}(diffStaTimeStr, diffEndTimeStr)
					default:
						fmt.Println("channel has no data")
					}
				}
				wg.Wait()
			}
		}
	} else {
		diffStaTimeStrList := make([]string, 0, len(cqcountlistA))
		diffEndTimeStrList := make([]string, 0, len(cqcountlistB))
		cqcountlistAMap := make(map[string]interface{}, len(cqcountlistA))
		for i := 0; i < len(cqcountlistA); i++ {
			tagstr, _ := json.Marshal(cqcountlistA[i])
			cqcountlistAMap[string(tagstr)] = cqcountlistA[i]
		}

		cqcountlistBMap := make(map[string]interface{}, len(cqcountlistB))
		for i := 0; i < len(cqcountlistB); i++ {
			tagstr, _ := json.Marshal(cqcountlistB[i])
			cqcountlistBMap[string(tagstr)] = cqcountlistB[i]
		}

		for i := 0; i < len(cqcountlistA); i++ {
			//判断两个节点这个时间段哪个COUNT数不一致，并拿到该count数起止时间戳
			countAStr, err := json.Marshal(cqcountlistA[i][1])
			if err != nil {
				fmt.Println("生成json字符串失败", err)
			}
			if string(countAStr) != "0" {
				valuesListAmaptag, _ := json.Marshal(cqcountlistA[i])
				if _, ok := cqcountlistBMap[string(valuesListAmaptag)]; ok {
					continue
				} else {
					diffStaTimeStr := cqcountlistA[i][0].(string)
					diffStaTimeStrList = append(diffStaTimeStrList, diffStaTimeStr)
					diffEndTime, _ := time.Parse(time.RFC3339, diffStaTimeStr)
					durtime, _ := time.ParseDuration(groupbytime)
					diffEndTimeStr := diffEndTime.Add(durtime).Format(time.RFC3339)
					diffEndTimeStrList = append(diffEndTimeStrList, diffEndTimeStr)
				}
			}
		}
		wg := &sync.WaitGroup{}
		diffStaTimeChan := make(chan string, len(diffStaTimeStrList))
		diffEndTimeChan := make(chan string, len(diffEndTimeStrList))
		for i := 0; i < len(diffStaTimeStrList); i++ {
			diffStaTimeChan <- diffStaTimeStrList[i]
			diffEndTimeChan <- diffEndTimeStrList[i]
		}
		for i := 0; i < len(diffStaTimeStrList); i++ {
			if i%worker == 0 {
				fmt.Println("moshi", i)
				for j := 0; j < worker; j++ {
					bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
						Database: dbname,
					})
					addr := fmt.Sprintf("%s:%d", addr2, port)
					c, err := client.NewHTTPClient(client.HTTPConfig{
						Addr: addr,
					})
					if err != nil {
						fmt.Println("Error creating InfluxDB Client: ", err.Error())
					}
					defer c.Close()
					select {
					case diffStaTimeStr := <-diffStaTimeChan:
						diffEndTimeStr := <-diffEndTimeChan
						wg.Add(1)
						go func(diffStaTimeStr, diffEndTimeStr string, j int, c client.Client, bp client.BatchPoints) {
							defer wg.Done()
							synchronize.Synchronize1(addr1, addr2, port, dbname, msname,
								diffStaTimeStr, diffEndTimeStr, fieldKeys, tagKeys)
						}(diffStaTimeStr, diffEndTimeStr, j, c, bp)
					default:
						fmt.Println("channel has no data")
					}
				}
				wg.Wait()
			}
		}
	}
}
