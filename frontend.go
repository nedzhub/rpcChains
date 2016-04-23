// Usage: go run frontend.go [front-end ip:port]
//
// - [front-end-client ip:port] : the ip and TCP port on which this front end is listening for client connections.
// - [front-end-metadata ip:port] : the ip and TCP port on which this front end is listening for metadata connections.
// - [front-end-auth ip:port] : the ip and TCP port on which this front end is listening for auth connections.
// - [front-end-filestoreA ip:port] : the ip and TCP port on which this front end is listening for filestoreA connections.
// - [front-end-filestoreB ip:port] : the ip and TCP port on which this front end is listening for filestoreB connections.
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
	"strings"
	"time"
)

//======================================= SERVICE =======================================
type FrontEndServiceClient int
type FrontEndServiceMetadata int
type FrontEndServiceAuth int
type FrontEndServiceFilestoreA int
type FrontEndServiceFilestoreB int

type FrontEndMapService int

//======================================= STRUCTS =======================================

type ValReply struct {
	Val string // value; depends on the call
}

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

const (
	INCOMPLETE_CHAIN     = iota
	SUCCESSFUL_COMPLETED = iota
	INVALID_AUTH_ERROR   = iota
)

const NodeService string = "FrontEndMapService"
const StoreEntryFunction string = "FStore"
const RetrieveEntryFunction string = "FRetrieve"
const ListEntryFunction string = "FList"

const ReplicationFactor = 2

var AuthMap map[int]NodeInfo
var MetadataMap map[int]NodeInfo
var FilestoreMapA map[int]NodeInfo
var FilestoreMapB map[int]NodeInfo

//var ChainInfoMap map[int]NodeInfo

var AuthWaitlistMap map[int]NodeInfo
var MetadataWaitlistMap map[int]NodeInfo
var FilestoreWaitlistMapA map[int]NodeInfo
var FilestoreWaitlistMapB map[int]NodeInfo

var ActivityMap map[string]int64

var nextChainID int
var nextMetadataID int
var nextAuthID int
var nextFilestoreIDA int
var nextFilestoreIDB int

var extraADDR string

var Logger *govec.GoLog

//======================================= SERVICE METHODS =======================================

func (fsc *FrontEndServiceClient) FStore(chain rpcc.RPCChain, reply *bool) error {

	cmd := "STORE"
	HandleLog(cmd, chain)
	// fmt.Println("content:", rpcc.Args.Text_content)
	// fmt.Println("secret:", rpcc.Args.Secret_info)
	args := chain.FirstEntity().Args.(ValArgs)
	if args.ErrorCode == INCOMPLETE_CHAIN {
		if len(MetadataMap) != 0 && len(FilestoreMapA) != 0 && len(FilestoreMapB) != 0 {
			//reply before synchronous call
			generateHops(&chain)
			fmt.Println(chain)

			logBuf := GenerateLog(cmd, chain)
			chain.AddLogToChain(logBuf)
			chain.CallNext(10000)

			*reply = true
		} else {
			fmt.Println("System chain disrupted, error")
			*reply = false
		}
	} else {
		*reply = true

		logBuf := GenerateReturnLog(cmd, chain, 0)
		chain.AddLogToChain(logBuf)

		chain.CallIndex(0, 10000)
	}

	fmt.Println()

	return nil
}

func (fsc *FrontEndServiceClient) FRetrieve(chain rpcc.RPCChain, reply *bool) error {

	cmd := "RETRIEVE"
	HandleLog(cmd, chain)

	args := chain.FirstEntity().Args.(ValArgs)
	if args.ErrorCode == INCOMPLETE_CHAIN {
		if len(MetadataMap) != 0 && len(FilestoreMapA) != 0 && len(FilestoreMapB) != 0 {
			valMeta := ValMetadata{
				FilestoreMapA: FilestoreMapA,
				FilestoreMapB: FilestoreMapB,
			}

			argMap := MetadataMap[getFirstMapKey(MetadataMap)]
			chain.AddToChain(argMap.Addr, argMap.Service, "MDRetrieve", valMeta, -1)

			fmt.Println(chain)

			logBuf := GenerateLog(cmd, chain)
			chain.AddLogToChain(logBuf)
			chain.CallNext(10000)

			*reply = true
		} else {
			fmt.Println("System chain disrupted, error")
			*reply = false
		}
	} else {
		logBuf := GenerateReturnLog(cmd, chain, 0)
		chain.AddLogToChain(logBuf)
		chain.CallIndex(0, 10000)
		*reply = true
	}

	fmt.Println()

	return nil
}

func (fsc *FrontEndServiceClient) FList(chain rpcc.RPCChain, reply *bool) error {

	cmd := "LIST"
	HandleLog(cmd, chain)

	args := chain.FirstEntity().Args.(ValArgs)
	if args.ErrorCode == INCOMPLETE_CHAIN {
		fmt.Println("List request receieved")

		if len(MetadataMap) != 0 && len(FilestoreMapA) != 0 && len(FilestoreMapB) != 0 {
			//argMap := MetadataMap[getFirstMapKey(MetadataMap)]
			argMapA := FilestoreMapA[getFirstMapKey(FilestoreMapA)]
			argMapB := FilestoreMapB[getFirstMapKey(FilestoreMapB)]

			//chain.AddToChain(argMap.Addr, argMap.Service, "MDList", nil, -1)
			chain.AddToChain(argMapA.Addr, argMapA.Service, "FAList", nil, -1)
			chain.AddToChain(argMapB.Addr, argMapB.Service, "FBList", nil, -1)

			fmt.Println(chain)

			logBuf := GenerateLog(cmd, chain)
			chain.AddLogToChain(logBuf)
			chain.CallNext(10000)

			*reply = true
		} else {
			fmt.Println("System chain disrupted, error")
			*reply = false
		}
	} else {

		logBuf := GenerateReturnLog(cmd, chain, 0)
		chain.AddLogToChain(logBuf)

		chain.CallIndex(0, 10000)
		*reply = true
	}

	fmt.Println()

	return nil
}

// when servers join assign a map to keep track of their activity
func (fsmd *FrontEndServiceMetadata) ReportServerActivity(args *NodeInfoCache, reply *ValReply) error {
	return processNodeConnections(*args, 1, MetadataMap, MetadataWaitlistMap, reply)
}

// func (fsa *FrontEndServiceAuth) Handshake(arg *ValReply, reply *ValReply) error {

// 	return handleHandshake(2, *arg, reply)
// }

func (fsa *FrontEndServiceAuth) ReportServerActivity(args *NodeInfoCache, reply *ValReply) error {
	return processNodeConnections(*args, 2, AuthMap, AuthWaitlistMap, reply)
}

func (fsfa *FrontEndServiceFilestoreA) ReportServerActivity(args *NodeInfoCache, reply *ValReply) error {
	return processNodeConnections(*args, 3, FilestoreMapA, FilestoreWaitlistMapA, reply)
}

func (fsfb *FrontEndServiceFilestoreB) ReportServerActivity(args *NodeInfoCache, reply *ValReply) error {
	return processNodeConnections(*args, 4, FilestoreMapB, FilestoreWaitlistMapB, reply)
}

func (fm *FrontEndMapService) Extra(rpcc *rpcc.RPCChain, reply *ValReply) error {
	return nil
}

func (fm *FrontEndMapService) AuthRecovery(arg *CacheContent, reply *ValReply) error {
	backupAuth(*arg)
	return nil
}

//======================================= MAIN =======================================

func main() {

	Logger = govec.Initialize("frontend", "frontend")

	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 8 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	gob.Register(ValArgs{})
	gob.Register(ValMetadata{})
	gob.Register(NodeInfo{})

	clientAddr := os.Args[1]
	metadataAddr := os.Args[2]
	authAddr := os.Args[3]
	filestoreAAddr := os.Args[4]
	filestoreBAddr := os.Args[5]
	extraAddr := os.Args[6]
	ReplicationFactor := os.Args[7]

	fmt.Println("clientAddr:", clientAddr, " metadataAddr:", metadataAddr, " authAddr:", authAddr, " filestoreAAddr:", filestoreAAddr, " filestoreBAddr:", filestoreBAddr, "ReplicationFactor:", ReplicationFactor)

	//initialize maps
	//ChainInfoMap = make(map[int]NodeInfo)
	MetadataMap = make(map[int]NodeInfo)
	AuthMap = make(map[int]NodeInfo)
	FilestoreMapA = make(map[int]NodeInfo)
	FilestoreMapB = make(map[int]NodeInfo)
	ActivityMap = make(map[string]int64)

	MetadataWaitlistMap = make(map[int]NodeInfo)
	FilestoreWaitlistMapA = make(map[int]NodeInfo)
	FilestoreWaitlistMapB = make(map[int]NodeInfo)

	nextChainID = 0
	extraADDR = extraAddr

	go initListener(clientAddr, 0)
	go initListener(metadataAddr, 1)
	go initListener(authAddr, 2)
	go initListener(filestoreAAddr, 3)
	go initListener(filestoreBAddr, 4)
	go initListener(extraAddr, 9)
	go printMaps()

	//counter:=0
	for {
		//counter++
		//fmt.Println(counter)
		// fmt.Println("ChainInfoMap:",ChainInfoMap)
		checkActivityStatus()
	}
}

//======================================= HELPER FUNCTIONS =======================================
func GenerateLog(callType string, chain rpcc.RPCChain) []byte {
	nextChain := chain.CurrentPosition + 1
	logMessage := Msg{"FE " + callType + "log.", time.Now().String()}
	fmt.Println(chain.EntityList[nextChain].Service_info)
	logBuf := Logger.PrepareSend(callType+" request to "+chain.EntityList[nextChain].Service_info, logMessage)
	return logBuf

}

func GenerateReturnLog(callType string, chain rpcc.RPCChain, returnChain int) []byte {
	logMessage := Msg{"FE" + callType + "log.", time.Now().String()}
	fmt.Println(chain.EntityList[returnChain].Service_info)
	logBuf := Logger.PrepareSend(callType+" return to "+chain.EntityList[returnChain].Service_info, logMessage)
	return logBuf

}

func HandleLog(cmd string, chain rpcc.RPCChain) {

	if chain.IsReturnCall == false {
		logMessage := new(Msg)
		prevChain := chain.CurrentPosition - 1
		Logger.UnpackReceive(cmd+" request received from "+chain.EntityList[prevChain].Service_info, chain.Log, &logMessage)
		fmt.Println(logMessage.String())
	}

	if chain.IsReturnCall == true {
		logMessage := new(Msg)
		prevChain := chain.LastEntity()
		Logger.UnpackReceive(cmd+" return from "+prevChain.Service_info, chain.Log, &logMessage)
		fmt.Println(logMessage.String())
	}

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
func initListener(address string, typeref int) {

	switch typeref {
	case 0:
		service := new(FrontEndServiceClient)
		rpc.Register(service)
	case 1:
		service := new(FrontEndServiceMetadata)
		rpc.Register(service)
	case 2:
		// TODO: Remove or re-enable ?
		service := new(FrontEndServiceAuth)
		rpc.Register(service)
	case 3:
		service := new(FrontEndServiceFilestoreA)
		rpc.Register(service)
	case 4:
		service := new(FrontEndServiceFilestoreB)
		rpc.Register(service)
	case 9:
		service := new(FrontEndMapService)
		rpc.Register(service)
	}

	ln, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal("listen error:", e)
	} else {
		go rpc.Accept(ln)
	}
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

//Other servers use rpc methods to call this method to get their map key
func getFirstAvailableEmptyKey(argMap map[int]NodeInfo) int {
	i := 0
	for {
		if (argMap[i] == NodeInfo{}) {
			break
		}
		i++
	}

	return i
}

//generate the rpc chain components to dial to for STORE command
func generateHops(chain *rpcc.RPCChain) {

	//chain.CurrrentPosition++

	db := MetadataMap[getFirstMapKey(MetadataMap)]
	au := AuthMap[getFirstMapKey(AuthMap)]

	// Database
	chain.AddToChain(db.Addr, db.Service, "MDStore", nil, -1)

	args := chain.FirstEntity().Args.(ValArgs)

	if args.Secret_info != "" {
		// Auth
		chain.AddToChain(au.Addr, au.Service, "AStore", nil, -1)

		// Fileserver A
		fa := FilestoreMapA[getFirstMapKey(FilestoreMapA)]
		chain.AddToChain(fa.Addr, fa.Service, "FAStore", nil, -1)
	} else {
		// Fileserver B
		fb := FilestoreMapB[getFirstMapKey(FilestoreMapB)]
		chain.AddToChain(fb.Addr, fb.Service, "FBStore", nil, -1)
	}
}

func printMaps() {
	for {
		fmt.Println("MetadataMap:", MetadataMap)
		fmt.Println("AuthMap:", AuthMap)
		fmt.Println("FilestoreMapA:", FilestoreMapA)
		fmt.Println("FilestoreMapB:", FilestoreMapB)
		fmt.Println("MetadataWaitlistMap:", MetadataWaitlistMap)
		fmt.Println("FilestoreWaitlistMapA:", FilestoreWaitlistMapA)
		fmt.Println("FilestoreWaitlistMapB:", FilestoreWaitlistMapB)
		fmt.Println("ActivityMap:", ActivityMap)
		fmt.Println()
		time.Sleep(3000 * time.Millisecond)
	}
}

// send to replicas
func updateReplicas(argMap map[int]NodeInfo, argReplicaMap []map[string]string) error {
	var kvVal ValReply
	cache := CacheContent{
		Maps: argReplicaMap,
	}

	first := getFirstMapKey(argMap)

	for k, v := range argMap {
		if k != first {
			dialservice, err := dialAddr(v.Addr)
			if err == nil {
				serviceMethod := v.Service + "." + "UpdateConsistency"
				dialservice.Call(serviceMethod, cache, &kvVal)
				//checkError(err)
				//fmt.Println(service ,"Replica reply:", kvVal.Val)
			}
		}
	}

	return nil
}

func backupAuth(cache CacheContent) error {
	if len(cache.Maps[0]) != 0 {
		for _, v := range FilestoreMapA {
			dialservice, err := dialAddr(v.Addr)
			if err == nil {
				var kvVal ValReply
				serviceMethod := v.Service + "." + "AuthRecovery"
				dialservice.Call(serviceMethod, cache, &kvVal)
				//checkError(err)
			}
		}
	}

	return nil
}

func processNodeConnections(args NodeInfoCache, argType int, argMap map[int]NodeInfo, waitlistMap map[int]NodeInfo, reply *ValReply) error {
	if args.Type == argType {

		t := strconv.Itoa(argType)
		id := strconv.Itoa(args.Id)
		key := t + "-" + id

		val := time.Now().Unix()
		//val= strconv.ParseInt(val, 10, 64)

		nodeInfo := NodeInfo{
			Id:      args.Id,
			Type:    args.Type,
			Addr:    args.Addr,
			Service: args.Service,
		}

		if _, ok := argMap[args.Id]; ok {
			argMap[args.Id] = nodeInfo
		} else {
			if len(argMap) < ReplicationFactor && len(waitlistMap) == 0 {
				argMap[args.Id] = nodeInfo
			} else {
				waitlistMap[args.Id] = nodeInfo
			}
		}

		ActivityMap[key] = val

		if args.Id == getFirstMapKey(argMap) {
			if len(argMap) > 1 {
				updateReplicas(argMap, args.Maps)
			}
			//fmt.Println("Replicating first replica content...")
		}

		//fmt.Println("New node added:  type", args.Type, " info",args)
		//fmt.Println("Updated map::", argMap)
		reply.Val = "Node info recorded"

		return nil

	}
	reply.Val = "Incorrect node service"
	return nil
}

//Activity detection based on time, 3 sec = inactive, nodes send infor every second
func checkActivityStatus() {
	for k, v := range ActivityMap {
		//fmt.Println("Active Nodes:" + k)
		current := time.Now().Unix()
		dif := current - v
		if dif > 3 {
			fmt.Println(k, "unavailable!!!")
			//If a KV node died, remove its related info and copy to another node
			delete(ActivityMap, k)
			s := strings.Split(k, "-")
			nodeType := s[0]
			nodeId, _ := strconv.Atoi(s[1])
			updateFromWaitlist(nodeType, nodeId)
		}
	}
}

//Use node from waitlist to fill dead replica
func updateFromWaitlist(nodeType string, nodeId int) {
	switch nodeType {
	case "1":
		if len(MetadataWaitlistMap) != 0 {
			index := getFirstMapKey(MetadataWaitlistMap)
			MetadataMap[index] = MetadataWaitlistMap[index]
			delete(MetadataWaitlistMap, index)
		}
		delete(MetadataMap, nodeId)
	case "3":
		if len(FilestoreWaitlistMapA) != 0 {
			index := getFirstMapKey(FilestoreWaitlistMapA)
			FilestoreMapA[index] = FilestoreWaitlistMapA[index]
			delete(FilestoreWaitlistMapA, index)
		}
		delete(FilestoreMapA, nodeId)
	case "4":
		if len(FilestoreWaitlistMapB) != 0 {
			index := getFirstMapKey(FilestoreWaitlistMapB)
			FilestoreMapB[index] = FilestoreWaitlistMapB[index]
			delete(FilestoreWaitlistMapB, index)
		}
		delete(FilestoreMapB, nodeId)
	}
}

//Dial to address
func dialAddr(addr string) (*rpc.Client, error) {

	service, err := rpc.Dial("tcp", addr)
	return service, err
}
