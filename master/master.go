// socket-server project main.go
package main

import (
	_ "bufio"
	"encoding/json"
	_ "encoding/json"
	. "fmt"
	// "io"
	_ "io/ioutil"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goombaio/namegenerator"
)

const (
	SERVER_HOST = "localhost"
	SERVER_PORT = "9988"
	SERVER_TYPE = "tcp"
)

var m sync.Mutex

var child_nodes map[string]net.Conn = make(map[string]net.Conn, 0)
var child_status map[string]string = make(map[string]string, 0)
var child_resource map[string]int = make(map[string]int, 0)
var job_ids map[string]string = make(map[string]string, 0)
var job_logs map[string][]string = make(map[string][]string, 0)
var node_status map[string]string = make(map[string]string, 0)
var child_top map[string]string = make(map[string]string, 0)

var client net.Conn

var send_commands bool

var runable_files []string = make([]string, 0)

var node_key = "12345"
var client_key = "123"

var j_id = 0

func main() {
	num_of_children := 5
	_ = num_of_children
	Println("Server Running...")
	server, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	if err != nil {
		Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer server.Close()
	Println("Listening on " + SERVER_HOST + ":" + SERVER_PORT)
	Println("Waiting for client...")
	connectToSlaves(server)
}

func connectToClients(connection net.Conn) {
	client = connection
	getCommands()
}

func addToJobPool(job_id string, msg []byte, needed_resource int) {
	for {
		time.Sleep(2 * time.Second)
		for _, v := range child_nodes {
			v.Write([]byte(`{"ack":["status"]}`))
		}
		for child_name, v := range child_status {
			if v == "not busy" && needed_resource <= child_resource[child_name] {
				job_ids[job_id] = "running" + " by " + child_name
				child_nodes[child_name].Write(msg)
				return
			}
		}
	}
}

func connectToSlaves(server net.Listener) {
	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)

	buffer := make([]byte, 1024)
	for {
		connection, err := server.Accept()
		mLen, err := connection.Read(buffer)
		if strings.HasPrefix(string(buffer[:mLen]), "Node ") {
			rndName := nameGenerator.Generate()
			m.Lock()
			child_nodes[rndName] = connection
			node_status[rndName] = "Active"
			child_resource[rndName], err = strconv.Atoi(strings.Split(string(buffer[:mLen]), " ")[3])
			m.Unlock()
			if err != nil {
				Println("Error accepting: ", err.Error())
				os.Exit(1)
			}
			Println("slave connected")
			go processClient(connection, rndName)
		} else if strings.HasPrefix(string(buffer[:mLen]), "Client ") {
			client_kk := strings.Split(string(buffer[:mLen]), " ")[5]
			if client_kk == client_key {
				go connectToClients(connection)
				Println("client connected")
			} else {
				connection.Write([]byte("Wrong key not authenticated"))
				connection.Close()
			}
		}
	}
}

func parseCommand(message string) {
	if strings.HasPrefix(message, "get logs") {
		child_name := strings.Split(string(message), " ")[2]
		if val, ok := child_nodes[child_name]; ok {
			val.Write([]byte(`{"ack":["logs"]}`))
		} else {
			println("no child with this name !!")
		}
	} else if strings.HasPrefix(message, "exit child") {
		child_name := strings.Split(string(message), " ")[2]
		if val, ok := child_nodes[child_name]; ok {
			val.Write([]byte(`{"ack":["exit"]}`))
		} else {
			println("no child with this name !!")
		}
	} else if message == "get children status" {
		for _, v := range child_nodes {
			v.Write([]byte(`{"ack":["status"]}`))
		}
	} else {
		println("Invalid Command!!!")
	}
}

func processClient(connection net.Conn, name string) {
	connection.Write([]byte(name))
	defer connection.Close()
	readMessage(connection)
}

func fillString(retunString string, toLength int) string {
	for {
		lengtString := len(retunString)
		if lengtString < toLength {
			retunString = retunString + ":"
			continue
		}
		break
	}
	return retunString
}

var proceed bool

func getCommands() {
	buffer := make([]byte, 1024)
	for _, v := range child_nodes {
		v.Write([]byte(`{"ack":["status"]}`))
	}
	for {
		empty := ""
		mLen, err := client.Read(buffer)
		if err != nil {
			Println("Error reading:", err.Error())
			break
		}
		var jsonResult map[string][]string
		json.Unmarshal(buffer[:mLen], &jsonResult)
		if val, ok := jsonResult["job"]; ok {
			if val[0] == "add job" {
				m.Lock()
				job_ids[jsonResult["job_id"][0]] = "pending"
				m.Unlock()
				for _, v := range child_nodes {
					v.Write([]byte(`{"ack":["status"]}`))
				}
				executable := ""
				if _, ok := jsonResult["executable"]; ok {
					executable = jsonResult["executable"][0]
				}
				needed_resource, err := strconv.Atoi(jsonResult["resource"][0])
				if err != nil {
					Println("Error reading resource")
				}
				for child_name, v := range child_status {
					if v == "not busy" && child_resource[child_name] >= needed_resource {
						if executable != "" {
							child_nodes[child_name].Write(buffer[:mLen])
							m.Lock()
							job_ids[jsonResult["job_id"][0]] = "running" + "   by   " + child_name
							m.Unlock()
							bufferFileName := make([]byte, 64)
							bufferFileSize := make([]byte, 10)
							time.Sleep(2 * time.Second)
							client.Read(bufferFileSize)
							client.Read(bufferFileName)
							child_nodes[child_name].Write(bufferFileSize)
							child_nodes[child_name].Write(bufferFileName)

							filqeSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

							var receivedBytes int64

							for {
								if (filqeSize - receivedBytes) < 1024 {
									client.Read(make([]byte, (receivedBytes+1024)-filqeSize))
									child_nodes[child_name].Write(make([]byte, (receivedBytes+1024)-filqeSize))
									Println("file transfered")
									break
								}
								client.Read(buffer)
								child_nodes[child_name].Write(buffer)
								receivedBytes += 1024
							}

							break
						}
						m.Lock()
						job_ids[jsonResult["job_id"][0]] = "running" + "   by   " + child_name
						m.Unlock()
						child_nodes[child_name].Write(buffer[:mLen])
						break
					}
				}
				if job_ids[jsonResult["job_id"][0]] == "pending" {
					go addToJobPool(job_ids[jsonResult["job_id"][0]], buffer[:mLen], needed_resource)
				}
			} else if val[0] == "list" {
				empty += "--------------    jobs     --------------" + "\n"
				for key, value := range job_ids {
					if _, err := strconv.Atoi(key); err != nil {
						delete(job_ids, key)
						continue
					}
					empty += ("job id : " + key + "     === state :     " + value) + "\n"
				}
				empty += "-----------------------------------------" + "\n"
			} else if val[0] == "logs" {
				m.Lock()
				job_id := jsonResult["job_id"][0]
				tailHead := jsonResult["tailHead"][0]
				count := jsonResult["count"][0]
				m.Unlock()
				empty += ("--------------    jobs logs of " + job_id + "  --------------") + "\n"
				if tailHead != "" {
					num, _ := strconv.Atoi(count)
					if tailHead == "-tail" {
						for i := len(job_logs[job_id]) - num; i < len(job_logs[job_id]); i++ {
							// println(job_logs[job_id][i])
							empty += job_logs[job_id][i] + "\n"
						}
					} else {
						for i := 0; i < num; i++ {
							// println(job_logs[job_id][i])
							empty += job_logs[job_id][i] + "\n"
						}
					}

				} else {
					for i := int(math.Max(float64(len(job_logs[job_id])-5), 0)); i < len(job_logs[job_id]); i++ {
						// println(job_logs[job_id][i])
						empty += job_logs[job_id][i] + "\n"
					}
				}
				// println("-----------------------------------------")
				empty += "-----------------------------------------" + "\n"
			}
		} else if a, ok := jsonResult["node"]; ok {
			if a[0] == "list" {
				// println("-----------------------   node status -----------------------")
				empty += "-----------------------   node status -----------------------" + "\n"
				for key, val := range node_status {
					// println(`"` + key + `"   status : "` + val + `"`)
					empty += `"` + key + `"   status : "` + val + `"` + "\n"
				}
				// println("-------------------------------------------------------------")
				empty += "-------------------------------------------------------------" + "\n"
			} else if a[0] == "top" {
				for _, v := range child_nodes {
					v.Write([]byte(`{"ack":["top"]}`))
				}
				time.Sleep(3 * time.Second)
				// println("-----------------------   node top -----------------------")
				empty += "-----------------------   node top -----------------------" + "\n"
				for key, value := range child_top {
					empty += "child node : " + key + "  " + value + "\n\n"
				}
				// println("-------------------------------------------------------------")
				empty += "-------------------------------------------------------------" + "\n"
			}
		}
		time.Sleep(1 * time.Second)
		client.Write([]byte(empty))
	}
}

func readMessage(connection net.Conn) {
	buffer := make([]byte, 1024)
	var child_name string
	for key, v := range child_nodes {
		if v == connection {
			child_name = key
		}
	}
	for {
		mLen, err := connection.Read(buffer)
		if err != nil {
			Println("Error reading:", err.Error())
			m.Lock()
			delete(child_nodes, child_name)
			delete(child_status, child_name)
			delete(child_resource, child_name)
			m.Unlock()
			m.Lock()
			node_status[child_name] = "Dead"
			m.Unlock()
			break
		}
		message := string(buffer[:mLen])
		if message == "busy" || message == "not busy" {
			m.Lock()
			child_status[child_name] = message
			m.Unlock()
		}
		if strings.HasPrefix(message, "job : ") {
			m.Lock()
			job_id := strings.Split(string(message), " ")[2]
			job_ids[job_id] = strings.Split(string(message), " ")[3] + "    by    " + child_name
			m.Unlock()
		}
		if strings.HasPrefix(message, "job_id : ") {
			m.Lock()
			job_id := strings.Split(string(message), " ")[2]
			job_logs[job_id] = append(job_logs[job_id], string(message))
			m.Unlock()
		}
		if strings.HasPrefix(message, "top --") {
			m.Lock()
			child_top[child_name] = message
			m.Unlock()
		}
		if message == "ack" {
			proceed = true
		}

		Println("Child : ", child_name, " === sent message : ", message)
		client.Write([]byte("Child : " + child_name + " === sent message : " + message))
	}
}
