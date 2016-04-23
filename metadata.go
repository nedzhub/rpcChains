// Usage: go run metadata.go [metadata ip:port] [frontend ip:port] [auth ip:port] r
//
// - [metadata ip:port] : the ip and TCP port on which this metadata server is listening for front end connections.
// - [auth ip:port] : the ip and TCP port on which auth is listening for metadata server connections.
// - [file-store-B ip:port] : the ip and TCP port on which file storage server B is listening for metadata server connections.
// - r : replication factor
//
package main  

import (
	"./rpcc"
	"encoding/gob"
	"fmt"
	"github.com/arcaneiceman/GoVector/govec"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

//======================================= SERVICE =======================================
type MetadataService int

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
const NodeType int = 1
const NodeService string = "MetadataService"
const StoreEntryFunction string = "MDStore"
const RetrieveEntryFunction string = "MDRetrieve"
const ListEntryFunction string = "MDList"

const AuthService string = "AuthService"
const AuthStoreEntryFunction string = "AStore"
const AuthRetrieveEntryFunction string = ""
const AuthListEntryFunction string = ""

const FileStoreAService string = "FilestoreServiceA"
const FileStoreAStoreEntryFunction string = "FAStore"
const FileStoreARetrieveEntryFunction string = "FARetrieve"
const FileStoreAListEntryFunction string = "FAList"

const FileStoreBService string = "FilestoreServiceB"
const FileStoreBStoreEntryFunction string = "FBStore"
const FileStoreBRetrieveEntryFunction string = "FBRetrieve"
const FileStoreBListEntryFunction string = "FBList"

var FilestoreMapA map[string]string
var FilestoreMapB map[string]string
var ValidationMap map[string]string

var Logger *govec.GoLog

//======================================= SERVICE METHODS =======================================
func (ms *MetadataService) MDStore(chain *rpcc.RPCChain, reply *bool) error {

	args := chain.FirstEntity().Args.(ValArgs)

	cmd := "STORE"
	HandleLog(cmd, chain)

	if args.Secret_info != "" {
		ValidationMap[args.File_Name] = "A"
	} else {
		ValidationMap[args.File_Name] = "B"
	}

	logBuf := GenerateLog(cmd, chain)
	chain.AddLogToChain(logBuf)

	chain.CallNext(10000)

	fmt.Println("STORE RPCC:")
	fmt.Println(chain)
	fmt.Println()

	*reply = true
	return nil
}

func (ms *MetadataService) MDRetrieve(chain *rpcc.RPCChain, reply *bool) error {

	cmd := "RETRIEVE"
	HandleLog(cmd, chain)

	args := chain.FirstEntity().Args.(ValArgs)
	metaArgs := chain.CurrentEntity().Args.(ValMetadata)

	// TODO: check to see if the file needs a secret before forwarding
	var node NodeInfo
	var function string
	//if 'Yes' is provided as the extra optional paramter when client issuing retrieve request
	//triggers file retrieval from FSA, as 'Yes' indicates client wants secure file access
	//(alternate strings will also trigger this, as long as secret field is not empty which is default)
	if args.Secret_info != "" {
		fmt.Println("RETRIEVE RPCC A:")
		node = metaArgs.FilestoreMapA[getFirstMapKey(metaArgs.FilestoreMapA)]
		function = "FARetrieve"
	} else {
		fmt.Println("RETRIEVE RPCC B:")
		node = metaArgs.FilestoreMapB[getFirstMapKey(metaArgs.FilestoreMapB)]
		function = "FBRetrieve"
	}

	chain.AddToChain(node.Addr, node.Service, function, nil, -1)
	logBuf := GenerateLog(cmd, chain)
	chain.AddLogToChain(logBuf)
	chain.CallNext(10000)
	fmt.Println(chain)
	fmt.Println()

	*reply = true
	return nil
}

/*
func (ms *MetadataService) MDList(chain *rpcc.RPCChain, reply *bool) error {

	cmd := "LIST"
	HandleLog(cmd, chain)

	logBuf := GenerateLog(cmd, chain)
    chain.AddLogToChain(logBuf)

    chain.CallNext(10000)

    *reply = true
	return nil
}
*/

func (ms *MetadataService) StoreValidation(key *ValReply, reply *ValReply) error {

	if v, ok := ValidationMap[key.Val]; ok {
		if v == "A" {
			FilestoreMapA[key.Val] = v
			delete(ValidationMap, key.Val)
		} else if v == "B" {
			FilestoreMapB[key.Val] = v
			delete(ValidationMap, key.Val)
		}
		reply.Val = "Validated data in METADATA \n"
		return nil
	} else {
		reply.Val = "Not in validation map METADATA"
		return nil
	}
}

func (ms *MetadataService) UpdateConsistency(arg *CacheContent, reply *ValReply) error {
	reply.Val = "Replica updated"
	//fmt.Println(arg)
	if len(arg.Maps) == 2 {
		FilestoreMapA = arg.Maps[0]
		FilestoreMapB = arg.Maps[1]
	}
	return nil
}

//======================================= MAIN =======================================

func main() {

	Logger = govec.Initialize("metadata", "metadata")

	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 4 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	gob.Register(ValArgs{})
	gob.Register(ValMetadata{})
	gob.Register(NodeInfo{})

	metadataAddr := os.Args[1]
	frontendAddr := os.Args[2]
	replicationFactor, _ := strconv.Atoi(os.Args[3])
	fmt.Println("metadataAddr:", metadataAddr, " frontendAddr:", frontendAddr, " replicationFactor:", replicationFactor)

	FilestoreMapA = make(map[string]string)
	FilestoreMapB = make(map[string]string)
	ValidationMap = make(map[string]string)

	go initListener(metadataAddr)
	go printMaps()

	serviceFE, err := rpc.Dial("tcp", frontendAddr)
	checkError(err)
	UpdateToFrontEnd(NodeType, NodeService, metadataAddr, serviceFE)
	//counter:=0
	for {
		//counter++
		//fmt.Println(counter)
		//UpdateToFrontEnd(myID, NodeType, NodeService, metadataAddr, serviceFE)
		//time.Sleep(1000*time.Millisecond)

		//serviceFE, err = rpc.Dial("tcp", frontendAddr)
		//if err == nil {
		UpdateToFrontEnd(NodeType, NodeService, metadataAddr, serviceFE)
		//}
		time.Sleep(1000 * time.Millisecond)
	}
}

//======================================= HELPER FUNCTIONS =======================================
func GenerateLog(callType string, chain *rpcc.RPCChain) []byte {
	nextChain := chain.CurrentPosition + 1
	logMessage := Msg{"Client " + callType + "log.", time.Now().String()}
	fmt.Println(chain.EntityList[nextChain].Service_info)
	logBuf := Logger.PrepareSend(callType+" request to "+chain.EntityList[nextChain].Service_info, logMessage)
	return logBuf

}

func HandleLog(cmd string, chain *rpcc.RPCChain) {
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
	service := new(MetadataService)
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
		fmt.Println("FilestoreMapA:", FilestoreMapA)
		fmt.Println("FilestoreMapB:", FilestoreMapB)
		fmt.Println("ValidationMap:", ValidationMap)
		fmt.Println()
		time.Sleep(3000 * time.Millisecond)
	}
}

// send this node's information to front end
func UpdateToFrontEnd(typeArg int, service string, address string, dialservice *rpc.Client) {
	list := make([]map[string]string, 2)
	list[0] = FilestoreMapA
	list[1] = FilestoreMapB

	node := NodeInfoCache{
		Id:      0,
		Type:    typeArg,
		Addr:    address,
		Service: service,
		Maps:    list,
	}

	var kvVal ValReply

	_ = dialservice.Call("FrontEndServiceMetadata.ReportServerActivity", node, &kvVal)
	//checkError(err)
	//fmt.Println("ReportServerActivity err:",err)
	//checkError(err)
	//fmt.Println("Updated activity status:", node, kvVal.Val)
}

//get available map element with the smallest id
func getFirstMapKey(argMap map[int]NodeInfo) int {
	i := 0
	for i = 0; i < len(argMap); i++ {
		if (argMap[i] != NodeInfo{}) {
			break
		}
	}

	return i
}

//Dial to address
func dialAddr(addr string) (*rpc.Client, error) {

	service, err := rpc.Dial("tcp", addr)

	return service, err
}
