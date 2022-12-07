// socket-client project main.go
package main

import (
	"encoding/json"
	. "fmt"
	"net"
	"os"
	"os/exec"
	_ "os/exec"
	"strings"
	_ "strings"
)

const (
	SERVER_HOST = "localhost"
	SERVER_PORT = "9988"
	SERVER_TYPE = "tcp"
)

var name = ""
var status = "not busy"
var job_logs []string = make([]string, 0)

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
			continue
		}
		go doJob(buffer, mLen, connection, file)
	}
}

func doJob(buffer []byte, mLen int, connection net.Conn, file *os.File) {
	var jsonResult map[string][]string
	json.Unmarshal(buffer[:mLen], &jsonResult)
	if val, ok := jsonResult["commands"]; ok {
		str := parseCommands(val, connection)
		file.WriteString("{\n")
		for _, res := range str {
			file.WriteString(res + "\n")
		}
		file.WriteString("}\n")
	} else if val, ok := jsonResult["ack"]; ok {
		parseAcks(val, connection)
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
	} else if ack=="exit"{
		// err := os.Remove("./logs/" + name + "_logs.txt")
		// Println(err,err2)
		os.Exit(0)
	} else {
		sendVal = "wrong operation!!!"
	}
	connection.Write([]byte(sendVal))
}

func parseCommands(val []string, connection net.Conn) []string {
	var logs []string
	status = "busy"
	for i := range val {
		c := strings.Split(string(val[i]), " ")
		out, err := exec.Command(c[0], c[1:]...).Output()
		if err != nil {
			Println("Error reading:", err.Error())
			logs = append(logs, "task : "+string(val[i])+", failed!, Error : "+string(err.Error()))
			job_logs = append(job_logs, "task : "+string(val[i])+", failed!, Error : "+string(err.Error()))
			continue
		}
		logs = append(logs, "task : "+string(val[i])+", done, Result : "+string(out))
		job_logs = append(job_logs, "task : "+string(val[i])+", done, Result : "+string(out))
	}
	status = "not busy"
	return logs
}

func initNode() (net.Conn, []byte, *os.File) {
	connection, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	if err != nil {
		panic(err)
	}
	buffer := make([]byte, 1024)
	_, err = connection.Write([]byte("Node connected! "))
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
