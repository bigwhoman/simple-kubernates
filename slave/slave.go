// socket-client project main.go
package main

import (
	"encoding/json"
	. "fmt"
	"io"
	"net"
	"os"
	"os/exec"
	_ "os/exec"
	"strconv"
	"strings"
	_ "strings"
	"sync"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
)

type SysInfo struct {
	Hostname string `bson:hostname`
	Platform string `bson:platform`
	CPU      string `bson:cpu`
	RAM      uint64 `bson:ram`
	Disk     uint64 `bson:disk`
}

var m sync.Mutex
var resource = "4"

const (
	SERVER_HOST = "localhost"
	SERVER_PORT = "9988"
	SERVER_TYPE = "tcp"
)

var name = ""
var status = "not busy"
var job_logs []string = make([]string, 0)
var max_jobs = 2
var jobs_now = 0
var key = "12345"

func main() {
	//establish connection
	connection, buffer, file := initNode()
	defer file.Close()
	defer connection.Close()
	defer os.Remove("logs/" + name + "_logs.txt")
	for {

		mLen, err := connection.Read(buffer)
		if err != nil {
			Println("err : ", err.Error())
			os.Remove("logs/" + name + "_logs.txt")
			break
		}

		var jsonResult map[string][]string
		json.Unmarshal(buffer[:mLen], &jsonResult)
		executable := ""
		if _, ok := jsonResult["executable"]; ok {
			executable = jsonResult["executable"][0]
		}
		if executable != "" {
			println("mike cokc ",executable)
			bufferFileName := make([]byte, 64)
			bufferFileSize := make([]byte, 10)
			connection.Write([]byte("ack"))
			connection.Read(bufferFileSize)
			fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)
			connection.Read(bufferFileName)
			fileName := strings.Trim(string(bufferFileName), ":")
			newFile, err := os.Create(fileName)
			if err != nil {
				panic(err)
			}
			var receivedBytes int64
			for {
				if (fileSize - receivedBytes) < 1024 {
					io.CopyN(newFile, connection, (fileSize - receivedBytes))
					connection.Read(make([]byte, (receivedBytes+1024)-fileSize))
					break
				}
				io.CopyN(newFile, connection, 1024)
				receivedBytes += 1024
			}
			newFile.Close()
		}

		if val, ok := jsonResult["ack"]; ok {
			go parseAcks(val, connection)
		} else if jobs_now <= max_jobs {
			m.Lock()
			jobs_now = jobs_now + 1
			m.Unlock()
			go doJob(buffer, mLen, connection, file)
		}
	}
}

func doJob(buffer []byte, mLen int, connection net.Conn, file *os.File) {
	var jsonResult map[string][]string
	json.Unmarshal(buffer[:mLen], &jsonResult)
	if val, ok := jsonResult["commands"]; ok {
		job_id := jsonResult["job_id"][0]
		str := parseCommands(job_id, val, connection)
		file.WriteString("{ job : " + job_id + "\n")
		for _, res := range str {
			file.WriteString(res + "\n")
		}
		file.WriteString("}\n")
	}
}

func parseAcks(val []string, connection net.Conn) {
	// println(val)
	ack := val[0]
	sendVal := ""
	if ack == "status" {
		sendVal = status
	} else if ack == "logs" {
		for _, res := range job_logs {
			sendVal += res + "\n"
		}
	} else if ack == "exit" {
		// err := os.Remove("./logs/" + name + "_logs.txt")
		// Println(err,err2)
		os.Exit(0)
	} else if ack == "top" {
		hostStat, _ := host.Info()
		cpuStat, _ := cpu.Info()
		vmStat, _ := mem.VirtualMemory()
		diskStat, _ := disk.Usage("\\") // If you're in Unix change this "\\" for "/"

		info := new(SysInfo)

		info.Hostname = hostStat.Hostname
		info.Platform = hostStat.Platform
		info.CPU = cpuStat[0].ModelName
		info.RAM = vmStat.Total / 1024 / 1024
		info.Disk = diskStat.Total
		sendVal = "top -- CPU : " + info.CPU + " RAM :" + strconv.Itoa(int(info.RAM)) +
			" DISK :" + strconv.Itoa(int(info.Disk)) + " maximum jobs : " +
			strconv.Itoa(int(max_jobs)) + " jobs_now : " + strconv.Itoa(int(jobs_now))
	} else {
		sendVal = "wrong operation!!!"
	}
	connection.Write([]byte(sendVal))
}

func parseCommands(job_id string, val []string, connection net.Conn) []string {
	var logs []string
	if jobs_now >= max_jobs {
		status = "busy"
	}
	failed := false
	for i := range val {
		c := strings.Split(string(val[i]), " ")
		out, err := exec.Command(c[0], c[1:]...).Output()
		if err != nil {
			Println("Error reading:", err.Error())
			logs = append(logs, "task : "+string(val[i])+", failed!, Error : "+string(err.Error()))
			connection.Write([]byte("job_id : " + job_id + " task : " + string(val[i]) + ", failed!, Error : " + string(err.Error())))
			job_logs = append(job_logs, "job_id : "+job_id+" task : "+string(val[i])+", failed!, Error : "+string(err.Error()))
			failed = true
			continue
		}
		logs = append(logs, "task : "+string(val[i])+", successful, Result : "+string(out))
		job_logs = append(job_logs, "task : "+string(val[i])+", successful, Result : "+string(out))
		connection.Write([]byte("job_id : " + job_id + " task : " + string(val[i]) + ", successful, Result : " + string(out)))
	}
	if !failed {
		connection.Write([]byte("job : " + job_id + " successful"))
	}
	if failed {
		connection.Write([]byte("job : " + job_id + " failed"))
	}
	status = "not busy"
	m.Lock()
	jobs_now = jobs_now - 1
	m.Unlock()
	return logs
}

func initNode() (net.Conn, []byte, *os.File) {
	connection, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	if err != nil {
		panic(err)
	}
	buffer := make([]byte, 1024)
	_, err = connection.Write([]byte("Node connected! " + key +" "+resource))
	if err != nil {
		Println("err")
	}
	node_name, err := connection.Read(buffer)
	if err != nil {
		Println("err")
	}
	name = string(buffer[:node_name])
	Println(name)
	file, err := os.Create("logs/" + name + "_logs.txt")
	return connection, buffer, file
}
