// Usage: go run filestoreB.go [file-store-B ip:port] [frontend ip:port] r
// - [file-store-B ip:port] : the ip and TCP port on which this client is listening for file server connections
// - [client ip:port] : the ip and TCP port on which client is listening for client connections.
//
package main 

import (
	"./rpcc"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
	//"strconv"
	//"io/ioutil"
	"encoding/gob"
	"github.com/arcaneiceman/GoVector/govec"
	"strings"
)

//======================================= SERVICE =======================================
type FilestoreServiceB int

//======================================= STRUCTS =======================================
type ValArgs struct {
	File_Name    string
	Text_content string
	Secret_info  string
	ErrorCode    int
}

type ValMetadata struct {
	FilestoreMapA map[int]NodeInfo
	FilestoreMapB map[int]NodeInfo
}

type ValReply struct {
	Val string // value; depends on the call
}

// Type 0 is metadata server, 1 is auth server, 2 is file storage A, 3 is file storage B
type NodeInfoCache struct {
	Id      int
	Type    int
	Addr    string
	Service string
	Maps    []map[string]string
}

type NodeInfo struct {
	Id      int
	Type    int
	Addr    string
	Service string
}

type CacheContent struct {
	Maps []map[string]string
}

type Msg struct {
	Content       string
	RealTimestamp string
}

//======================================= VARIABLES =======================================
const NodeType int = 4
const NodeService string = "FilestoreServiceB"
const StoreEntryFunction string = "FBStore"
const RetrieveEntryFunction string = "FBRetrieve"
const ListEntryFunction string = "FBList"

const (
	INCOMPLETE_CHAIN     = iota
	SUCCESSFUL_COMPLETED = iota
	INVALID_AUTH_ERROR   = iota
)

const MetadataService string = "MetadataService"
const FileBasePath = "./StorageFilesB/"

var FileContentMapB map[string]string

var Logger *govec.GoLog

//======================================= SERVICE METHODS =======================================
func (fsb *FilestoreServiceB) FBStore(chain rpcc.RPCChain, reply *bool) error {
	fmt.Println("STORE RPCC:")

	cmd := "STORE"
	HandleLog(cmd, chain)

	dbEntity := chain.FindEntity(MetadataService)

	if dbEntity != nil {
		dbConn, err := rpcc.Dial(*dbEntity)

		if err == nil {
			args := chain.FirstEntity().Args.(ValArgs)
			args.ErrorCode = SUCCESSFUL_COMPLETED
			chain.FirstEntity().Args = args

			logBuf := GenerateReturnLog(cmd, chain, 1)
			chain.AddLogToChain(logBuf)
			chain.ChangeDirection()

			err := chain.CallIndex(1, 10000)

			if err == nil {
				arg := ValReply{
					Val: args.File_Name,
				}

				var kvVal ValReply
				serviceMethod := dbEntity.Service_info + "." + "StoreValidation"
				err := dbConn.Call(serviceMethod, arg, &kvVal)
				fmt.Println(kvVal.Val)
				checkError(err)
				dbConn.Close()

				FileContentMapB[args.File_Name] = args.Text_content
			}
		}
	}

	fmt.Println(chain)
	*reply = true
	return nil
}
func (fsb *FilestoreServiceB) FBRetrieve(chain rpcc.RPCChain, reply *bool) error {
	fmt.Println("RETRIEVE RPCC:")

	cmd := "RETRIEVE"
	HandleLog(cmd, chain)

	args := chain.FirstEntity().Args.(ValArgs)

	// Populate the return value with the content
	if _, ok := FileContentMapB[args.File_Name]; ok {
		args.Text_content = FileContentMapB[args.File_Name]
	} else {
		args.Text_content = ""
	}

	args.ErrorCode = SUCCESSFUL_COMPLETED

	// Update the args and call frontend
	chain.FirstEntity().Args = args
	logBuf := GenerateReturnLog(cmd, chain, 1)
	chain.AddLogToChain(logBuf)
	chain.ChangeDirection()
	chain.CallIndex(1, 10000)

	fmt.Println(chain)
	*reply = true
	return nil
}

func (fsb *FilestoreServiceB) FBList(chain rpcc.RPCChain, reply *bool) error {

	fmt.Println("LIST RPCC:")

	cmd := "LIST"
	HandleLog(cmd, chain)

	lfg := listFilesGet() //new

	args := chain.FirstEntity().Args.(ValArgs)
	if args.File_Name == "" {
		args.File_Name = lfg
	} else {
		args.File_Name = args.File_Name + "," + lfg
	}

	args.ErrorCode = SUCCESSFUL_COMPLETED
	chain.FirstEntity().Args = args

	//logBuf := GenerateLog(cmd, chain)
	//chain.AddLogToChain(logBuf)

	logBuf := GenerateReturnLog(cmd, chain, 1)
	chain.AddLogToChain(logBuf)
	chain.ChangeDirection()

	chain.CallIndex(1, 10000)

	fmt.Println(chain)

	*reply = true
	return nil
}

func (fsa *FilestoreServiceB) UpdateConsistency(arg *CacheContent, reply *ValReply) error {
	reply.Val = "Replica updated"
	if len(arg.Maps) == 1 {
		FileContentMapB = arg.Maps[0]
	}
	return nil
}

//======================================= MAIN =======================================

func main() {

	Logger = govec.Initialize("fsB", "fsB")

	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 4 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	gob.Register(ValArgs{})
	gob.Register(ValMetadata{})
	gob.Register(NodeInfo{})

	filestoreAddr := os.Args[1]
	frontendAddr := os.Args[2]
	replicationFactor := os.Args[3]
	fmt.Println("filestoreAddrB:", filestoreAddr, " frontendAddr:", frontendAddr, " replicationFactor:", replicationFactor)

	FileContentMapB = make(map[string]string)

	go initListener(filestoreAddr)
	go printMaps()

	serviceFE, err := rpc.Dial("tcp", frontendAddr)
	checkError(err)
	UpdateToFrontEnd(NodeType, NodeService, filestoreAddr, serviceFE)
	//counter:=0
	for {
		//counter++
		//fmt.Println(counter)
		serviceFE, err = rpc.Dial("tcp", frontendAddr)
		if err == nil {
			UpdateToFrontEnd(NodeType, NodeService, filestoreAddr, serviceFE)
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

//======================================= HELPER FUNCTIONS =======================================
func GenerateLog(callType string, chain rpcc.RPCChain) []byte {
	nextChain := chain.CurrentPosition + 1
	logMessage := Msg{"FSB " + callType + "log.", time.Now().String()}
	fmt.Println(chain.EntityList[nextChain].Service_info)
	logBuf := Logger.PrepareSend(callType+" request to "+chain.EntityList[nextChain].Service_info, logMessage)
	return logBuf

}

func GenerateReturnLog(callType string, chain rpcc.RPCChain, returnChain int) []byte {
	logMessage := Msg{"FSB Return" + callType + "log.", time.Now().String()}
	fmt.Println(chain.EntityList[returnChain].Service_info)
	logBuf := Logger.PrepareSend(callType+" return to "+chain.EntityList[returnChain].Service_info, logMessage)
	return logBuf

}

func HandleLog(cmd string, chain rpcc.RPCChain) {
	logMessage := new(Msg)
	prevChain := chain.CurrentPosition - 1
	Logger.UnpackReceive(cmd+" request received from "+chain.EntityList[prevChain].Service_info, chain.Log, &logMessage)
	fmt.Println(logMessage.String())

}

func (m Msg) String() string {
	return "content: " + m.Content + "\ntime: " + m.RealTimestamp
}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}

// Register RPC services and listens for incoming dialing
func initListener(address string) {
	service := new(FilestoreServiceB)
	rpc.Register(service)
	ln, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal("listen error:", e)
	} else {
		go rpc.Accept(ln)
	}
}

func printMaps() {
	for {
		fmt.Println()
		fmt.Println("Printing stored files...")
		fileCount := 1
		for k, v := range FileContentMapB {
			fmt.Println(fileCount, "FILENAME:", k)
			fmt.Println("  CONTENT:", v)
			fileCount++
		}
		time.Sleep(3000 * time.Millisecond)
	}
}

// send this node's information to front end
func UpdateToFrontEnd(typeArg int, service string, address string, dialservice *rpc.Client) {

	list := make([]map[string]string, 1)
	list[0] = FileContentMapB

	node := NodeInfoCache{
		Type:    typeArg,
		Addr:    address,
		Service: service,
		Maps:    list,
	}

	var kvVal ValReply

	dialservice.Call("FrontEndServiceFilestoreB.ReportServerActivity", node, &kvVal)
	//checkError(err)
	//fmt.Println("ReportServerActivity err:",err)
	//fmt.Println("Updated activity status:", node, kvVal.Val)

}

//Dial to address
func dialAddr(addr string) (*rpc.Client, error) {

	service, err := rpc.Dial("tcp", addr)
	return service, err
}

//func returns a list of all the files in that directory StorageFiles which has all the storage files
//*assumption that the file is being run from the parent directory
func listFilesGet() string {
	fileList := make([]string, len(FileContentMapB))
	// files, _ := ioutil.ReadDir(FileBasePath)//"./StorageFiles")
	// for _, f := range files {
	// 	fileList = append(fileList, f.Name())
	// }
	// for i := range fileList{
	// 	fmt.Println(fileList[i])     //prints out each file name on seperate line
	// }
	//return fileList
	i := 0
	for k, _ := range FileContentMapB {
		fileList[i] = k
		i++
	}
	//convert to a single string before returning
	singleString := strings.Join(fileList, ",")
	return singleString
}
