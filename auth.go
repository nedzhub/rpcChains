// Usage: go run auth.go [auth ip:port] [frontend ip:port]
//
// - [auth ip:port] : the ip and TCP port on which this authentication server is listening for metadata server connections
// - [file-store-A ip:port] : the ip and TCP port on file storage server A is listening for auth connections.
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
	"encoding/gob"
	"github.com/arcaneiceman/GoVector/govec"
)

//======================================= SERVICE =======================================
type AuthService int

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

type NodeInfo struct {
	Id      int
	Type    int
	Addr    string
	Service string
}

// Type 0 is metadata server, 1 is auth server, 2 is file storage A, 3 is file storage B
type NodeInfoCache struct {
	Type    int
	Addr    string
	Service string
	Entry   string
	Maps    []map[string]string
}

type CacheContent struct {
	Maps []map[string]string
}

type Msg struct {
	Content       string
	RealTimestamp string
}

//======================================= VARIABLES =======================================
const NodeType int = 2
const NodeService string = "AuthService"
const StoreEntryFunction string = "AStore"
const RetrieveEntryFunction string = ""
const ListEntryFunction string = ""

const (
	INCOMPLETE_CHAIN     = iota
	SUCCESSFUL_COMPLETED = iota
	INVALID_AUTH_ERROR   = iota
)

var CredentialMap map[string]string
var ValidationMap map[string]string

var Logger *govec.GoLog

//======================================= SERVICE METHODS =======================================
func (as *AuthService) AStore(chain *rpcc.RPCChain, reply *bool) error {

	cmd := "STORE"
	HandleLog(cmd, chain)

	args := chain.EntityList[0].Args.(ValArgs)

	validAuth := false
	if secret, ok := CredentialMap[args.File_Name]; ok {
		//Check secret match, store in validation map to confirm overwrite
		if secret == args.Secret_info {
			fmt.Println("STORE RPCC:")
			ValidationMap[args.File_Name] = args.Secret_info

			logBuf := GenerateLog(cmd, chain)
			chain.AddLogToChain(logBuf)
			fmt.Println(string(chain.Log))

			chain.CallNext(10000)
			fmt.Println(chain)

			validAuth = true
		}
	} else {
		//Refuse store if file name in validation map
		if _, ok := ValidationMap[args.File_Name]; !ok {
			ValidationMap[args.File_Name] = args.Secret_info

			logBuf := GenerateLog(cmd, chain)
			chain.AddLogToChain(logBuf)

			chain.CallNext(10000)
			fmt.Println(chain)

			validAuth = true
		}
	}

	if !validAuth {
		args := chain.FirstEntity().Args.(ValArgs)
		args.ErrorCode = INVALID_AUTH_ERROR
		chain.FirstEntity().Args = args

		//TODO
		logBuf := GenerateReturnLogError(cmd, chain, 1)
		chain.AddLogToChain(logBuf)
		chain.ChangeDirection()

		chain.CallIndex(1, 10000)
	}

	fmt.Println()
	*reply = true
	return nil
}

func (as *AuthService) StoreValidation(key *ValReply, reply *ValReply) error {

	if v, ok := ValidationMap[key.Val]; ok {
		CredentialMap[key.Val] = v
		delete(ValidationMap, key.Val)
		reply.Val = "Moved to storage map AUTH"
		return nil
	} else {
		reply.Val = "Not in validation map AUTH"
		return nil
	}
}

func (as *AuthService) UpdateConsistency(arg *CacheContent, reply *ValReply) error {
	reply.Val = "Replica updated"
	CredentialMap = arg.Maps[0]
	return nil
}

func (as *AuthService) AuthRecovery(arg *CacheContent, reply *ValReply) error {
	reply.Val = "Auth data stored"

	if len(CredentialMap) == 0 {
		CredentialMap = arg.Maps[0]
	}

	return nil
}

//======================================= MAIN =======================================

func main() {

	Logger = govec.Initialize("auth", "auth")

	// parse argsx`
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	gob.Register(ValArgs{})
	gob.Register(ValMetadata{})
	gob.Register(NodeInfo{})

	authAddr := os.Args[1]
	frontendAddr := os.Args[2]
	fmt.Println("authAddr:", authAddr, " frontendAddr:", frontendAddr)

	CredentialMap = make(map[string]string)
	ValidationMap = make(map[string]string)

	go initListener(authAddr)
	go printMaps()

	serviceFE, err := rpc.Dial("tcp", frontendAddr)
	checkError(err)
	//myID := "1"//HandShake(serviceFE)

	//counter:=0
	for {
		//counter++
		//fmt.Println(counter)
		UpdateToFrontEnd(NodeType, NodeService, authAddr, serviceFE)
		backupAuth(frontendAddr)
		time.Sleep(1000 * time.Millisecond)
	}

}

//======================================= HELPER FUNCTIONS =======================================
func GenerateLog(callType string, chain *rpcc.RPCChain) []byte {
	nextChain := chain.CurrentPosition + 1
	logMessage := Msg{"AUTH " + callType + "log.", time.Now().String()}
	fmt.Println(chain.EntityList[nextChain].Service_info)
	logBuf := Logger.PrepareSend(callType+" request to "+chain.EntityList[nextChain].Service_info, logMessage)
	return logBuf

}

func GenerateReturnLogError(callType string, chain *rpcc.RPCChain, returnChain int) []byte {
	logMessage := Msg{"AUTH Return" + callType + "log.", time.Now().String()}
	fmt.Println(chain.EntityList[returnChain].Service_info)
	logBuf := Logger.PrepareSend(callType+" error return to "+chain.EntityList[returnChain].Service_info, logMessage)
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
	service := new(AuthService)
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
		fmt.Println("CredentialMap:", CredentialMap)
		fmt.Println("ValidationMap:", ValidationMap)
		fmt.Println()
		time.Sleep(3000 * time.Millisecond)
	}
}

// remove temporary storage in validation map when exceeding exepected time of 60sec
func mapGarbageCollector() {
	for {
		ValidationMap = make(map[string]string)
		time.Sleep(60000 * time.Millisecond)
	}
}

// handshake to get my id key in front end map
func HandShake(dialservice *rpc.Client) string {
	args := ValReply{Val: "hello"}
	var kvVal ValReply

	dialservice.Call("FrontEndServiceAuth.Handshake", args, &kvVal)
	//checkError(err)

	return kvVal.Val
}

// send this node's information to front end
func UpdateToFrontEnd(typeArg int, service string, address string, dialservice *rpc.Client) {
	list := make([]map[string]string, 1)
	list[0] = CredentialMap

	node := NodeInfoCache{
		Type:    typeArg,
		Addr:    address,
		Service: service,
		Maps:    list,
	}

	var kvVal ValReply

	err := dialservice.Call("FrontEndServiceAuth.ReportServerActivity", node, &kvVal)
	checkError(err)
	//fmt.Println("ReportServerActivity err:",err)
	//fmt.Println("Updated activity status:", node, kvVal.Val)

}

func backupAuth(frontendAddr string) error {
	maps := make([]map[string]string, 1)
	maps[0] = CredentialMap
	cache := CacheContent{Maps: maps}

	dialservice, err := rpc.Dial("tcp", frontendAddr)
	if err == nil {
		var kvVal ValReply
		serviceMethod := "FrontEndMapService" + "." + "AuthRecovery"
		dialservice.Call(serviceMethod, cache, &kvVal)
		//checkError(err)
	}

	return nil
}
