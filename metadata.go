// Usage: go run metadata.go [metadata ip:port] [frontend ip:port] [auth ip:port] r
//
// - [metadata ip:port] : the ip and TCP port on which this metadata server is listening for front end connections.         
// - [auth ip:port] : the ip and TCP port on which auth is listening for metadata server connections. 
// - [file-store-B ip:port] : the ip and TCP port on which file storage server B is listening for metadata server connections.
// - r : replication factor
//
package main

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"time"
	"net"
	"log"
	"github.com/arcaneiceman/GoVector/govec"
)

//======================================= SERVICE =======================================
type MetadataService int

//======================================= STRUCTS =======================================
type ValArgs struct{
	File_Name string
	Text_content string
	Secret_info string
}

type ValReply struct {
	Val string // value; depends on the call
	Log LogArg
}

type EntryFunction struct{
	Store string
	Retrieve string
	List string
}

type ServerEntity struct {
	Connection_info string
	Service_info string
	Entry EntryFunction
}

type RPCChain struct {
	Id int
	CurrrentPosition int
	Chain []ServerEntity
	Args ValArgs
	Log LogArg
}
 

// Type 0 is metadata server, 1 is auth server, 2 is file storage A, 3 is file storage B
type NodeInfoCache struct {
	Id int
	Type int
	Addr string
	Service string
	Entry EntryFunction
	Maps []map[string]string
}

type NodeInfo struct {
	Id int
	Type int
	Addr string
	Service string
	Entry EntryFunction
}

type CacheContent struct{
	Maps []map[string]string
}

type LogArg struct{
	Log []byte
}

type Msg struct {
	Content string 
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
var AuthAddressGlobal string

const FileStoreAService string = "FilestoreServiceA"
const FileStoreAStoreEntryFunction string = "FAStore"
const FileStoreARetrieveEntryFunction string = "FARetrieve"
const FileStoreAListEntryFunction string = "FAList"
var FileStoreAddressAGlobal string

const FileStoreBService string = "FilestoreServiceB"
const FileStoreBStoreEntryFunction string = "FBStore"
const FileStoreBRetrieveEntryFunction string = "FBRetrieve"
const FileStoreBListEntryFunction string = "FBList"
var FileStoreAddressBGlobal string

var FilestoreMapA map[string]string
var FilestoreMapB map[string]string
var ValidationMap map[string]string

var Logger *govec.GoLog

//=========================================== LOGGER ============================================
func HandleLog(callType string, rpcc *RPCChain) {

	prevServer := (rpcc.Chain[rpcc.CurrrentPosition].Service_info)
	logFromPrev := rpcc.Log.Log
	msgFromPrev := new(Msg)

	switch callType {
		case "store":
			Logger.UnpackReceive("Store request from " + prevServer, logFromPrev[0:], &msgFromPrev)
			fmt.Println(msgFromPrev.String())
			return

		case "retrieve":
			Logger.UnpackReceive("Retrieve request from " + prevServer, logFromPrev[0:], &msgFromPrev)
			fmt.Println(msgFromPrev.String())
			return

		case "list":
			Logger.UnpackReceive("List request from " + prevServer, logFromPrev[0:], &msgFromPrev)
			fmt.Println(msgFromPrev.String())
			return
	}
}

func rpcCallLog(callType string, rpcc RPCChain, service string) []byte {

	var outBuf []byte

	if callType == "store" {
		outGoingMessage := Msg{"Hello from metadata", time.Now().String()}
		outBuf = Logger.PrepareSend("Store request to " + service, outGoingMessage)
	}

	if callType == "retrieve" {
		outGoingMessage := Msg{"Hello from metadata", time.Now().String()}
		outBuf = Logger.PrepareSend("Retrieve request to " + service, outGoingMessage)
	}

	if callType == "list" {
		outGoingMessage := Msg{"Hello from metadata", time.Now().String()}
		outBuf = Logger.PrepareSend("List request to " + service, outGoingMessage)
	}

	return outBuf

}


//======================================= SERVICE METHODS =======================================
func (ms *MetadataService) MDStore(rpcc *RPCChain, reply *ValReply) error {
	// fmt.Println("content:", rpcc.Args.Text_content)
	// fmt.Println("secret:", rpcc.Args.Secret_info)	
	var callType = "store"
	HandleLog(callType, rpcc)

	result:=composeChain("store",*rpcc,reply)
	fmt.Println("STORE RPCC:")
	fmt.Println(result)
	fmt.Println()

	return nil
}

func (ms *MetadataService) MDRetrieve(rpcc *RPCChain, reply *ValReply) error {

	var callType = "retrieve"
	HandleLog(callType, rpcc)
	fmt.Println("secret val is: ", rpcc.Args.Secret_info) //for testing
	if(rpcc.Args.Secret_info != ""){
		reply.Val = "Data sent from metadata RETRIEVE"
		result:= chooseNextHop("A",*rpcc)
		result=incCurrentPos(result)
		fmt.Println("RETRIEVE RPCC A:")
		fmt.Println(result)
		rpcCall("retrieve",result)
		
	}else{
		reply.Val = "Data sent from metadata RETRIEVE"
		result:= chooseNextHop("B",*rpcc)
		result=incCurrentPos(result)
		fmt.Println("RETRIEVE RPCC B:")
		fmt.Println(result)
		rpcCall("retrieve",result)
	}
	fmt.Println()

	return nil	
}

func (ms *MetadataService) MDList(rpcc *RPCChain, reply *ValReply) error {

	var callType = "list"
	HandleLog(callType, rpcc)

	reply.Val = "Data sent from metadata LIST filesoreA"
	result:= chooseNextHop("A",*rpcc)
	result=incCurrentPos(result)
	fmt.Println("LIST RPCC A:")
	fmt.Println(result)
	rpcCall("list",result)

	reply.Val = "Data sent from metadata LIST filesoreB"
	result2:= chooseNextHop("B",*rpcc)
	result2=incCurrentPos(result2)
	fmt.Println("LIST RPCC B:")
	fmt.Println(result2)
	rpcCall("list",result2)

	return nil	
}

func (ms *MetadataService) StoreValidation(key *ValReply, reply *ValReply) error {
	
	if v, ok := ValidationMap[key.Val]; ok {
		if(v=="A"){
			FilestoreMapA[key.Val]=v
			delete(ValidationMap,key.Val)
		}else if(v=="B"){
			FilestoreMapB[key.Val]=v
			delete(ValidationMap,key.Val)
		}
		reply.Val="Validated data in METADATA \n"
    	return nil
    }else{
   		reply.Val="Not in validation map METADATA"
		return nil
	}
}

func (ms *MetadataService) UpdateConsistency(arg *CacheContent, reply *ValReply) error {
	reply.Val = "Replica updated"
	//fmt.Println(arg)
	if (len(arg.Maps) ==2){
		FilestoreMapA=arg.Maps[0]
		FilestoreMapB=arg.Maps[1]
	}
	return nil
}
//======================================= MAIN =======================================

func main() {

	Logger = govec.Initialize("metadata", "metadata")

	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 7 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	metadataAddr := os.Args[1]
	frontendAddr := os.Args[2]
	AuthAddressGlobal = os.Args[3]
	FileStoreAddressAGlobal = os.Args[4]
	FileStoreAddressBGlobal = os.Args[5]
	replicationFactor, _ := strconv.Atoi(os.Args[6])
	fmt.Println("metadataAddr:", metadataAddr," frontendAddr:",frontendAddr, " authAddr:",AuthAddressGlobal, " fileAddrA:",FileStoreAddressAGlobal, " fileAddrB:",FileStoreAddressBGlobal," replicationFactor:", replicationFactor)

	FilestoreMapA = make(map[string]string)
	FilestoreMapB = make(map[string]string)
	ValidationMap = make(map[string]string)

	go initListener(metadataAddr)
	go printMaps()
	
	serviceFE, err := rpc.Dial("tcp", frontendAddr)
	checkError(err)
	myID := HandShake(serviceFE)
	UpdateToFrontEnd(myID, NodeType, NodeService, metadataAddr, serviceFE)
	//counter:=0
	for {
		//counter++
		//fmt.Println(counter)
		//UpdateToFrontEnd(myID, NodeType, NodeService, metadataAddr, serviceFE)
		//time.Sleep(1000*time.Millisecond)

		serviceFE, err = rpc.Dial("tcp", frontendAddr)
		if err == nil {
			UpdateToFrontEnd(myID, NodeType, NodeService, metadataAddr, serviceFE)
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

//======================================= HELPER FUNCTIONS =======================================
func (m Msg) String()  string {
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
	for{
		fmt.Println("FilestoreMapA:",FilestoreMapA)
		fmt.Println("FilestoreMapB:",FilestoreMapB)
		fmt.Println("ValidationMap:",ValidationMap)
		fmt.Println()
		time.Sleep(3000 * time.Millisecond)
	}
}


// handshake to get my id key in front end map
func HandShake(dialservice *rpc.Client) string{

	outgoingHandshake := Msg{"Hello frontend from metadata server", time.Now().String()}
	handshakeBuf := Logger.PrepareSend("Making connection to frontend", outgoingHandshake)
	handshakeLog := LogArg{handshakeBuf}

	args:= ValReply {
		Val:"hello",
		Log: handshakeLog,
	}

	var kvVal ValReply

	dialservice.Call("FrontEndServiceMetadata.Handshake", args, &kvVal)
	//checkError(err)

	return kvVal.Val
}

// send this node's information to front end 
func UpdateToFrontEnd(id string, typeArg int, service string, address string, dialservice *rpc.Client){
	ID,_:=strconv.Atoi(id)

	entryFunc:= EntryFunction{
		Store: StoreEntryFunction,
		Retrieve: RetrieveEntryFunction,
		List: ListEntryFunction,
	}

	list := make([]map[string]string,2)
	list[0] = FilestoreMapA
	list[1] = FilestoreMapB

	node := NodeInfoCache {
		Id: ID,
		Type: typeArg,
		Addr: address, 
		Service: service,
		Entry: entryFunc,
		Maps: list,
	}

	var kvVal ValReply

	dialservice.Call("FrontEndServiceMetadata.ReportServerActivity", node, &kvVal)
	//fmt.Println("ReportServerActivity err:",err)
	//checkError(err)
	//fmt.Println("Updated activity status:", node, kvVal.Val)
	
}

//Dial to address
func dialAddr(addr string) (*rpc.Client,error) {

	service, err := rpc.Dial("tcp", addr)
	
	return service, err
}

//Set current position to +=1
func incCurrentPos(rpcc RPCChain) RPCChain{
	rpcc.CurrrentPosition++
	return rpcc
}

//Get next serverentity in the chain, if next index out of bound then get client info at 0
func getNextNodeInfo(rpcc RPCChain) ServerEntity{
	
	pos:=rpcc.CurrrentPosition+1
	if(pos == len(rpcc.Chain)){
		pos=0
	}

	return rpcc.Chain[pos]
}

//construct rpc call to next server
func rpcCall(callType string, rpcc RPCChain) string{
	var kvVal ValReply
	var serviceMethod string

	nextNode := getNextNodeInfo(rpcc)
	addr := nextNode.Connection_info
	service := nextNode.Service_info
	storeFunc := nextNode.Entry.Store
	retrieveFunc := nextNode.Entry.Retrieve
	listFunc := nextNode.Entry.List

	rpcc.Log.Log = rpcCallLog(callType, rpcc, service)

	fmt.Println(service)

	dialservice,err:=dialAddr(addr)	
	if (err == nil){
		switch callType {
			case "store":
				serviceMethod = service + "." + storeFunc
				dialservice.Call(serviceMethod, rpcc, &kvVal)
				//checkError(err)
				
			case "retrieve":
				serviceMethod = service + "." + retrieveFunc
				dialservice.Call(serviceMethod, rpcc, &kvVal)
				//checkError(err)

			case "list":
				serviceMethod = service + "." + listFunc
				dialservice.Call(serviceMethod, rpcc, &kvVal)
				//checkError(err)
		}
		//fmt.Println(service ,"STORE reply:", kvVal.Val)
		return kvVal.Val
	}else{
		return "error"
	}
}

//generate the rpc chain components to dial to
func insertHops(rpcc RPCChain) RPCChain{
	rpcc.CurrrentPosition++
	pos:=rpcc.CurrrentPosition

	auEntryFunc:= EntryFunction {
		Store: AuthStoreEntryFunction,
		Retrieve: AuthRetrieveEntryFunction,
		List: AuthListEntryFunction,
	}

	auEntity := ServerEntity {
		Connection_info: AuthAddressGlobal,
		Service_info: AuthService,
		Entry:auEntryFunc,
	}	

	length:=len(rpcc.Chain)
	temp:=make([]ServerEntity,length)
	temp2:=make([]ServerEntity,length)
	copy(temp,rpcc.Chain)
	copy(temp2,rpcc.Chain)

	visited:=temp[0:pos+1]
	toBeVisited:=temp2[pos+1:]

	newRPCC:=append(visited, auEntity)
	newRPCC=append(newRPCC,toBeVisited...)

	rpcc.Chain=newRPCC	
	
	return rpcc
}

//REPLACE with API
func composeChain(callType string, rpcc RPCChain,reply *ValReply) RPCChain{
	nextNode:=getNextNodeInfo(rpcc)
	if(nextNode.Connection_info != AuthAddressGlobal || nextNode.Service_info != AuthService){
		if(rpcc.Args.Secret_info != ""){
			reply.Val = "Data sent from metadata STORE A"
			ValidationMap[rpcc.Args.File_Name]="A"
			composedRPCC:=insertHops(rpcc)
			result:=rpcCall(callType, composedRPCC)
			fmt.Println(result)
			return composedRPCC
		}else{
			reply.Val = "Data sent from metadata STORE B"
			ValidationMap[rpcc.Args.File_Name]="B"
			rpcc=incCurrentPos(rpcc)
			result:=rpcCall(callType, rpcc)
			fmt.Println(result)
			return rpcc
		}
		return rpcc	
	}else{
		reply.Val= "Data sent from metadata"
		switch callType {
			case "store":
				result:=rpcCall("store", rpcc)
				fmt.Println(result)
			case "retrieve":
				result:=rpcCall("retrieve", rpcc)
				fmt.Println(result)	
			case "list":
				result:=rpcCall("list", rpcc)
				fmt.Println(result)
		}
		return rpcc
	}

	
}

func chooseNextHop(hopType string, rpcc RPCChain) RPCChain{

	switch hopType {
		case "A":
			next:=EntryFunction{
				Store: FileStoreAStoreEntryFunction,
				Retrieve: FileStoreARetrieveEntryFunction,
				List: FileStoreAListEntryFunction,
			}

			nextEntity := ServerEntity {
				Connection_info: FileStoreAddressAGlobal,
				Service_info: FileStoreAService,
				Entry: next,
			}
			rpcc.Chain = append(rpcc.Chain, nextEntity)
			return	rpcc
		case "B":

			next:=EntryFunction{
				Store: FileStoreBStoreEntryFunction,
				Retrieve: FileStoreBRetrieveEntryFunction,
				List: FileStoreBListEntryFunction,
			}

			nextEntity := ServerEntity {
				Connection_info: FileStoreAddressBGlobal,
				Service_info: FileStoreBService,
				Entry: next,
			}
			rpcc.Chain = append(rpcc.Chain, nextEntity)
			return	rpcc
	}
	return rpcc
}