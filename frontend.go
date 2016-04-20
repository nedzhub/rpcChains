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
	"fmt"
	"net/rpc"
	"os"
	"time"
	"log"
	"net"
	"strconv"
	"strings"
	"github.com/arcaneiceman/GoVector/govec"
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
	Log LogArg
}

type ValArgs struct{
	File_Name string
	Text_content string
	Secret_info string
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

type LogArg struct {
	Log []byte
}

type Msg struct {
	Content string 
	RealTimestamp string
}

//======================================= VARIABLES =======================================
const NodeService string = "FrontEndMapService"
const StoreEntryFunction string = "FStore"
const RetrieveEntryFunction string = "FRetrieve"
const ListEntryFunction string = "FList"

const ReplicationFactor = 2

var NodeEntryFunc EntryFunction = EntryFunction {
		Store: StoreEntryFunction,
		Retrieve: RetrieveEntryFunction,
		List: ListEntryFunction,
	}

var MetadataMap map[int]NodeInfo
var AuthMap map[int]NodeInfo
var FilestoreMapA map[int]NodeInfo
var FilestoreMapB map[int]NodeInfo

//var ChainInfoMap map[int]NodeInfo

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
var requestType string

//======================================= LOGGER ================================================

func HandleLog(rpcc *RPCChain, requestType string) {

	logFromClient := rpcc.Log.Log
	incomingMessage := new(Msg)

	if (requestType == "store") {
		Logger.UnpackReceive("Store request from Client", logFromClient[0:], &incomingMessage)
		fmt.Println(incomingMessage.String())
		return
	}

	if (requestType == "retrieve") {
		Logger.UnpackReceive("Retrieve request from Client", logFromClient[0:], &incomingMessage)
		fmt.Println(incomingMessage.String())
		return
	}

	if (requestType == "list") {
		Logger.UnpackReceive("List request from Client", logFromClient[0:], &incomingMessage)
		fmt.Println(incomingMessage.String())
		return
	}
}

func HandleHandshakeLog(argType int, arg ValReply) {

	incomingHandshake := new(Msg)

	switch argType {
		case 0:
			fmt.Println("Handshake with client")
			return

		case 1: 
			Logger.UnpackReceive("Connection made with metadata server", arg.Log.Log, &incomingHandshake)
			fmt.Println(incomingHandshake.String())
			return

		case 2: 
			Logger.UnpackReceive("Connection made with auth server", arg.Log.Log, &incomingHandshake)
			fmt.Println(incomingHandshake.String())
			return

		case 3: 
			Logger.UnpackReceive("Connection made with FilestoreA", arg.Log.Log, &incomingHandshake)
			fmt.Println(incomingHandshake.String())
			return

		case 4: 
			Logger.UnpackReceive("Connection made with FilestoreB", arg.Log.Log, &incomingHandshake)
			fmt.Println(incomingHandshake.String())
			return
	}

}

func rpcCallLog(callType string, rpcc RPCChain, service string) []byte {

	var outBuf []byte

	if callType == "store" {
		outGoingMessage := Msg{"Hello from frontend", time.Now().String()}
		outBuf = Logger.PrepareSend("Store request to " + service, outGoingMessage)
	}

	if callType == "retrieve" {
		outGoingMessage := Msg{"Hello from frontend", time.Now().String()}
		outBuf = Logger.PrepareSend("Retrieve request to " + service, outGoingMessage)
	}

	if callType == "list" {
		outGoingMessage := Msg{"Hello from frontend", time.Now().String()}
		outBuf = Logger.PrepareSend("List request to " + service, outGoingMessage)
	}

	return outBuf

}

//======================================= SERVICE METHODS =======================================

func (fsc *FrontEndServiceClient) Handshake(arg *ValReply, reply *ValReply) error {
	return handleHandshake(0, *arg, reply)
}

func (fsc *FrontEndServiceClient) FStore(rpcc *RPCChain, reply *ValReply) error {

	//fmt.Println("secret:", rpcc.Args.Secret_info)
	requestType = "store"
	HandleLog(rpcc, requestType)

	fmt.Println("continuing on")

	fmt.Println(len(FilestoreMapA))

	if (len(MetadataMap)!=0 && len(FilestoreMapA)!=0 && len(FilestoreMapB)!=0){
		
		//reply before synchronous call
		reply.Val= "Data sent from front-end for STORE"
		newRPCC:=generateHops(*rpcc)
		fmt.Println(newRPCC)	
		rpcCall("store", newRPCC)
		
	}else{
		fmt.Println("System chain disrupted, error")
		reply.Val = "System chain disrupted, error"
	}
	
	fmt.Println()

	return nil
}

func (fsc *FrontEndServiceClient) FRetrieve(rpcc *RPCChain, reply *ValReply) error {

	requestType = "retrieve"
	HandleLog(rpcc, requestType)

	if (len(MetadataMap)!=0 && len(FilestoreMapA)!=0 && len(FilestoreMapB)!=0){

		reply.Val= "Data sent from front-end for RETRIEVE"
		newRPCC:=addThisNode(*rpcc)
		newRPCC=addNextHop(newRPCC,MetadataMap)
		newRPCC=incCurrentPos(newRPCC)
		fmt.Println(newRPCC)	
		rpcCall("retrieve", newRPCC)	
	}else{
		fmt.Println("System chain disrupted, error")
		reply.Val = "System chain disrupted, error"
	}
	
		
	return nil
}

func (fsc *FrontEndServiceClient) FList(rpcc *RPCChain, reply *ValReply) error {

	requestType = "list"
	HandleLog(rpcc, requestType)
	
	if (len(MetadataMap)!=0 && len(FilestoreMapA)!=0 && len(FilestoreMapB)!=0){
		reply.Val= "Data sent from front-end for LIST"
		newRPCC:=addThisNode(*rpcc)
		newRPCC=addNextHop(newRPCC,MetadataMap)
		newRPCC=incCurrentPos(newRPCC)
		fmt.Println(newRPCC)	
		rpcCall("list", newRPCC)	
	
	}else{
		fmt.Println("System chain disrupted, error")
		reply.Val = "System chain disrupted, error"
	}
	
	fmt.Println()

	return nil
}


func (fsmd *FrontEndServiceMetadata) Handshake(arg *ValReply, reply *ValReply) error {

	return handleHandshake(1, *arg, reply)
}

// when servers join assign a map to keep track of their activity
func (fsmd *FrontEndServiceMetadata) ReportServerActivity(args *NodeInfoCache, reply *ValReply) error {
	return processNodeConnections(*args, 1, MetadataMap, MetadataWaitlistMap, reply)
}

// func (fsa *FrontEndServiceAuth) Handshake(arg *ValReply, reply *ValReply) error {

// 	return handleHandshake(2, *arg, reply)
// }

// func (fsa *FrontEndServiceAuth) ReportServerActivity(args *NodeInfoCache, reply *ValReply) error {
// 	return processNodeConnections(*args, 2, AuthMap, reply)
// }

func (fsfa *FrontEndServiceFilestoreA) Handshake(arg *ValReply, reply *ValReply) error {

	return handleHandshake(3, *arg, reply)
}

func (fsfa *FrontEndServiceFilestoreA) ReportServerActivity(args *NodeInfoCache, reply *ValReply) error {
	return processNodeConnections(*args, 3, FilestoreMapA, FilestoreWaitlistMapA, reply)
}

func (fsfb *FrontEndServiceFilestoreB) Handshake(arg *ValReply, reply *ValReply) error {

	return handleHandshake(4, *arg, reply)
}

func (fsfb *FrontEndServiceFilestoreB) ReportServerActivity(args *NodeInfoCache, reply *ValReply) error {
	return processNodeConnections(*args, 4, FilestoreMapB, FilestoreWaitlistMapB, reply)
}

func (fm *FrontEndMapService) Extra(rpcc *RPCChain, reply *ValReply) error{
	return nil
}

func (fm *FrontEndMapService) AuthRecovery(arg *CacheContent, reply *ValReply) error{
	backupAuth(*arg)
	return nil
}
//======================================= MAIN =======================================

func main() {

	Logger = govec.Initialize("frontend", "frontendLog")

	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port\n", os.Args[0])
	if len(os.Args) != 8 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	clientAddr := os.Args[1]
	metadataAddr := os.Args[2]
	authAddr := os.Args[3]
	filestoreAAddr := os.Args[4]
	filestoreBAddr := os.Args[5]
	extraAddr := os.Args[6]
	ReplicationFactor := os.Args[7]

	fmt.Println("clientAddr:", clientAddr, " metadataAddr:",metadataAddr, " authAddr:",authAddr, " filestoreAAddr:",filestoreAAddr, " filestoreBAddr:",filestoreBAddr,"ReplicationFactor:",ReplicationFactor)
	
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
	go initListener(metadataAddr,1)
	go initListener(authAddr,2)
	go initListener(filestoreAAddr,3)
	go initListener(filestoreBAddr,4)
	go initListener(extraAddr,9)
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
func initListener(address string, typeref int) {

	switch typeref {
		case 0:
			service := new(FrontEndServiceClient)
			rpc.Register(service)
		case 1:
			service := new(FrontEndServiceMetadata)
			rpc.Register(service)
		case 2:
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

	i:=1
	for{
		// e:=EntryFunction{
		// 		Store:"",
		// 		Retrieve:"",
		// 		List:"",}
		// emptyNode:=NodeInfo{
		// 	Id:0,
		// 	Type: 0,
		// 	Addr: "",
		// 	Service: "",
		// 	Entry: e,
		// }
		if(argMap[i] != NodeInfo{}){
			break
		}
		i++
	}

	return i
}

//Other servers use rpc methods to call this method to get their map key
func getFirstAvailableEmptyKey(argMap map[int]NodeInfo) int{
	i:=0
	for{
		if(argMap[i] == NodeInfo{}){
			break
		}
		i++
	}

	return i
}


//generate the rpc chain components to dial to for STORE command
func generateHops(rpcc RPCChain) RPCChain{

	rpcc.CurrrentPosition++

	feEntity := ServerEntity {
		Connection_info: extraADDR,
		Service_info: NodeService,
		Entry: NodeEntryFunc,
	}

	db := MetadataMap[getFirstMapKey(MetadataMap)]
	//au := AuthMap[getFirstMapKey(AuthMap)]
	fa := FilestoreMapA[getFirstMapKey(FilestoreMapA)]
	fb := FilestoreMapB[getFirstMapKey(FilestoreMapB)]

	dbEntity := ServerEntity {
		Connection_info: db.Addr,
		Service_info: db.Service,
		Entry: db.Entry,
	}
	// auEntity := ServerEntity {
	// 	Connection_info: au.Addr,
	// 	Service_info: au.Service,
	// 	Entry: au.Entry,
	// }
	faEntity := ServerEntity {
		Connection_info: fa.Addr,
		Service_info: fa.Service,
		Entry: fa.Entry,
	}
	fbEntity := ServerEntity {
		Connection_info: fb.Addr,
		Service_info: fb.Service,
		Entry: fb.Entry,
	}

	if(rpcc.Args.Secret_info != ""){

		rpcc.Chain=append(rpcc.Chain, feEntity)
		rpcc.Chain=append(rpcc.Chain, dbEntity)
		//rpcc.Chain=append(rpcc.Chain, auEntity)
		rpcc.Chain=append(rpcc.Chain, faEntity) 

		return rpcc
	}else{
		rpcc.Chain=append(rpcc.Chain, feEntity)
		rpcc.Chain=append(rpcc.Chain, dbEntity)
		rpcc.Chain=append(rpcc.Chain, fbEntity) 
		
		return rpcc
	}	
}

func printMaps(){
	for{
		fmt.Println("MetadataMap:",MetadataMap)
		// fmt.Println("AuthMap:",AuthMap)
		fmt.Println("FilestoreMapA:",FilestoreMapA)
		fmt.Println("FilestoreMapB:",FilestoreMapB)
		fmt.Println("MetadataWaitlistMap:",MetadataWaitlistMap)
		fmt.Println("FilestoreWaitlistMapA:",FilestoreWaitlistMapA)
		fmt.Println("FilestoreWaitlistMapB:",FilestoreWaitlistMapB)
		fmt.Println("ActivityMap:",ActivityMap)
		fmt.Println()
		time.Sleep(3000 * time.Millisecond)
	}
}

func addThisNode(rpcc RPCChain) RPCChain{

	thisEntity := ServerEntity {
		Connection_info: extraADDR,
		Service_info: NodeService,
		Entry: NodeEntryFunc,
	}
	rpcc.Chain = append(rpcc.Chain, thisEntity)

	return	rpcc
}

func addNextHop(rpcc RPCChain, argMap map[int]NodeInfo) RPCChain{

	next := argMap[getFirstMapKey(argMap)]
	
	nextEntity := ServerEntity {
		Connection_info: next.Addr,
		Service_info: next.Service,
		Entry: next.Entry,
	}
	rpcc.Chain = append(rpcc.Chain, nextEntity)

	return	rpcc
}

func handleHandshake(argType int, arg ValReply, reply *ValReply) error{
	if (arg.Val == "hello"){

		HandleHandshakeLog(argType, arg)
		
		switch argType {
		// Type 1 is metadata server, 2 is auth server, 3 is file storage A, 4 is file storage B
		case 0:
			nextChainID++
			fmt.Println("Received client connection, new key:", nextChainID)
			//fmt.Println("ChainInfoMap:",ChainInfoMap)
			reply.Val = strconv.Itoa(nextChainID)
			return nil

		case 1:
			nextMetadataID++
			fmt.Println("Received metadata connection, new key:", nextMetadataID)
			fmt.Println("New MetadataMap:",MetadataMap)
			reply.Val = strconv.Itoa(nextMetadataID)
			return nil		
	
		case 2:
			nextAuthID++
			fmt.Println("Received auth connection, new key:", nextAuthID)
			fmt.Println("New AuthMap:",AuthMap)	
			reply.Val = strconv.Itoa(nextAuthID)
			return nil

		case 3:
			nextFilestoreIDA++
			fmt.Println("Received filestoreA connection, new key:", nextFilestoreIDA)
			fmt.Println("New FilestoreMapA:",FilestoreMapA)	
			reply.Val = strconv.Itoa(nextFilestoreIDA)
			return nil

		case 4:
			nextFilestoreIDB++
			fmt.Println("Received filestoreB connection, new key:", nextFilestoreIDB)
			fmt.Println("New FilestoreMapB:",FilestoreMapB)	
 			reply.Val = strconv.Itoa(nextFilestoreIDB)
			return nil
		}
	}	
	reply.Val = "Bad handshake request"

	return nil
}

// send to replicas
func updateReplicas(argMap map[int]NodeInfo, argReplicaMap []map[string]string) error {
	var kvVal ValReply
	cache := CacheContent{
		Maps: argReplicaMap,
	}

	first:=getFirstMapKey(argMap)

	for k,v := range argMap{
		if(k != first){
			dialservice, err:=dialAddr(v.Addr)
			if (err == nil){
				serviceMethod := v.Service + "." + "UpdateConsistency"
				dialservice.Call(serviceMethod, cache, &kvVal)
				//checkError(err)
				//fmt.Println(service ,"Replica reply:", kvVal.Val)
			}
		}
	}

	return nil	
}

func backupAuth(cache CacheContent) error{
	if(len(cache.Maps[0]) !=0){
		for _,v := range FilestoreMapA{
			dialservice, err:=dialAddr(v.Addr)
			if (err == nil){
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
	if (args.Type == argType){

		t:=strconv.Itoa(argType)
		id:=strconv.Itoa(args.Id)
		key:=t + "-" + id

		val:=time.Now().Unix()
		//val= strconv.ParseInt(val, 10, 64)
			
		nodeInfo := NodeInfo{
			Id: args.Id,
			Type: args.Type,
			Addr: args.Addr,
			Service: args.Service,
			Entry: args.Entry,
		}		

		if _, ok :=  argMap[args.Id]; ok{
			argMap[args.Id]=nodeInfo	

		}else{
			if(len(argMap)<ReplicationFactor && len(waitlistMap)==0){
				argMap[args.Id]=nodeInfo
			}else{
				waitlistMap[args.Id]=nodeInfo
			}
		}

		ActivityMap[key]=val

		

		if(args.Id == getFirstMapKey(argMap)){
		
			if(len(argMap)>1){
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
	for k, v := range ActivityMap{
		//fmt.Println("Active Nodes:" + k)
		current:=time.Now().Unix()
		dif:= current - v
		if (dif > 3) {
			fmt.Println(k, "unavailable!!!")
			//If a KV node died, remove its related info and copy to aother node
			delete(ActivityMap, k)
			s:=strings.Split(k,"-")
			nodeType:=s[0]
			nodeId,_:=strconv.Atoi(s[1])
			updateFromWaitlist(nodeType,nodeId)
		}
	}
}

//Use node from waitlist to fill dead replica
func updateFromWaitlist(nodeType string, nodeId int){
	switch nodeType {
		case "1":
			if(len(MetadataWaitlistMap) != 0){
				index :=getFirstMapKey(MetadataWaitlistMap)
				MetadataMap[index]=MetadataWaitlistMap[index]
				delete(MetadataWaitlistMap,index)
			}
			delete(MetadataMap,nodeId)
		case "3":
			if(len(FilestoreWaitlistMapA) != 0){
				index :=getFirstMapKey(FilestoreWaitlistMapA)
				FilestoreMapA[index]=FilestoreWaitlistMapA[index]
				delete(FilestoreWaitlistMapA,index)
			}
			delete(FilestoreMapA,nodeId)
		case "4":
			if(len(FilestoreWaitlistMapB) != 0){
				index :=getFirstMapKey(FilestoreWaitlistMapB)
				FilestoreMapB[index]=FilestoreWaitlistMapB[index]
				delete(FilestoreWaitlistMapB,index)
			}
			delete(FilestoreMapB,nodeId)
	}
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

	dialservice, err:=dialAddr(addr)	
	if (err == nil){
		switch callType {
		case "store":

			serviceMethod = service + "." + storeFunc

			err := dialservice.Call(serviceMethod, rpcc, &kvVal)
			checkError(err)
			fmt.Println(service ,"STORE reply:", kvVal.Val)
			
		case "retrieve":
			serviceMethod = service + "." + retrieveFunc

			err := dialservice.Call(serviceMethod, rpcc, &kvVal)
			checkError(err)
			fmt.Println(service, "RETRIEVE reply:", kvVal.Val)

		case "list":
			serviceMethod = service + "." + listFunc

			err := dialservice.Call(serviceMethod, rpcc, &kvVal)
			checkError(err)
			fmt.Println(service, "LIST reply:", kvVal.Val)
		}
		return kvVal.Val
	}else{
		return "error"
	}
	
	
}



