package rpcc

import (
	"fmt" 
	"net/rpc"
	"time"
    "math/rand"
    "strconv"
    "os"
    "sync"
    "encoding/gob"
)

/* === Headers === */

// The server entity in the chain
type ServerEntity struct {
	Connection_info string
	Service_info    string
	Entry           string
    Args            interface{}    // The arguments passed to the first server
}

type RPCChain struct {
	Id              string            // The unique ID of the chain, generated
	CurrentPosition int            // The current position/server in the chain
	EntityList      []ServerEntity // The full chain of the servers
	Success         int
    mutex           *sync.Mutex
    Log             []byte 
    IsReturnCall    bool // false: forwards, true: backwards
}

type ErrorFunc func(chain *RPCChain, error string)
type TimeoutFunc func(chain *RPCChain)

/* === Globals === */
var lastIdUsed int
var errorHandler ErrorFunc = nil
var timeoutHandler TimeoutFunc = nil

/* === Functions === */
// RPCC's public functions

/* == Core Chain Functionality == */
// Creates and Initializes a Chain
func CreateChain() *RPCChain {
	entitylist := make([]ServerEntity, 0)

    rpcc := new(RPCChain)
	rpcc.Id = generateUniqueId()
    rpcc.CurrentPosition = 0
    rpcc.EntityList = entitylist
    rpcc.Success = -1
    rpcc.mutex = &sync.Mutex{}
    rpcc.IsReturnCall = false

	// TODO: Possible null pointer
	return rpcc
}

// Adds an entity to a chain
func (chain *RPCChain) AddToChain(connectionInfo string, serviceInfo string, entryFunction string, args interface{}, index int) {
    // Acquire lock and setup for release
    chain.MutexLock()
    defer chain.mutex.Unlock()

    // Create the server entity based on info provided
	entity := ServerEntity{
		Connection_info: connectionInfo,
		Service_info:    serviceInfo,
		Entry:           entryFunction,
        Args:            args,
	}

    // Append the entity to our chain
	// TODO: entity or 0
	chain.EntityList = append(chain.EntityList, entity)
    if (index != -1) {
        copy(chain.EntityList[index+1:], chain.EntityList[index:])
        chain.EntityList[index] = entity
    }
}

func (chain *RPCChain) AddLogToChain(logBuf []byte) {
    chain.Log = logBuf
}

func (chain *RPCChain) ChangeDirection() {
    chain.IsReturnCall = true
}


func (chain *RPCChain) CallNext(timeout int) error {
    // Forward to CallIndex function
    nextEntity := (chain.CurrentPosition+1) % len(chain.EntityList)
    return chain.CallIndex(nextEntity, timeout)
}

func (chain *RPCChain) CallIndex(index int, timeout int) error {
    // Acquire lock and setup for release
    chain.MutexLock()
    defer chain.mutex.Unlock()

	// Prepare RPCC
    chain.CurrentPosition = index
	entity := chain.EntityList[chain.CurrentPosition]
    
    // Register our interface types
    for _, serverEntry := range chain.EntityList {
        if (serverEntry.Args != nil) {
            gob.Register(serverEntry.Args)
            //fmt.Println(reflect.TypeOf(serverEntry.Args))
        }
    }
    
    // Setup timeout procedure
	go chain.timeoutTrigger(timeout)
    
    // Dial via RPC
    var returnVal bool
    service, err := rpc.Dial("tcp", entity.Connection_info)
    checkError(chain, err)
    
    // Call
	err = service.Call(entity.Service_info+"."+entity.Entry, chain, &returnVal)
    checkError(chain, err)
    
    if (service != nil) {
        service.Close()
    }
    
    // Check success
    if (returnVal) {
        chain.Success = 1
    } else {
        chain.Success = 0
        
        if (errorHandler != nil) {
            errorHandler(chain, "Remote server returned failure")
        }
    }

	return err
}

/* == Error Handling == */
func RegisterErrorHandler(handler ErrorFunc) {
    errorHandler = handler
}

func RegisterTimeoutHandler(handler TimeoutFunc) {
    timeoutHandler = handler
}

/* == Entity Helpers == */
func (chain *RPCChain) FirstEntity() *ServerEntity {
    return &chain.EntityList[0];
}

func (chain *RPCChain) CurrentEntity() *ServerEntity {
    return &chain.EntityList[chain.CurrentPosition];
}

func (chain *RPCChain) LastEntity() *ServerEntity {
    return &chain.EntityList[len(chain.EntityList)-1];
}

func (chain *RPCChain) FindEntity(connectionInfo string) *ServerEntity {
    for i := 0; i < len(chain.EntityList); i++ {
        if (chain.EntityList[i].Service_info == connectionInfo) {
            return &chain.EntityList[i]
        }
    }
    
    return nil
}

func Dial(entity ServerEntity) (*rpc.Client,error) {
    service, err := rpc.Dial("tcp", entity.Connection_info)
    return service, err
}

func (chain *RPCChain) CheckDial(entity ServerEntity) bool {
    service, err := rpc.Dial("tcp", entity.Connection_info)
    
    if (service != nil) {
        service.Close()
    }
    
    return checkError(chain, err)
}

func (chain *RPCChain) MutexLock() {
    if (chain.mutex == nil) {
        chain.mutex = &sync.Mutex{};
    }
    chain.mutex.Lock()
}

func (chain *RPCChain) MutexUnlock() {
    chain.mutex.Unlock()
}

/* == Private Functions == */
// RPCC's internal functions
func generateUniqueId() string {
    uniqueId := ""
	lastIdUsed++
    
    randSource := rand.NewSource(time.Now().UnixNano())
    randSeed := rand.New(randSource)
    
    unixTimestamp := strconv.FormatInt(time.Now().Unix(), 10)
    uniqueId = strconv.Itoa(lastIdUsed) + unixTimestamp + strconv.Itoa(randSeed.Intn(10000))
	return uniqueId
}

func checkError(chain *RPCChain, err error) bool {
	if err != nil {
        if (errorHandler != nil) {
            // Custom Handler
            errorHandler(chain, err.Error())
        } else {
            // Default handler
            fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
            os.Exit(1)
        }
        return false
	}
    return true
}

func (chain *RPCChain) timeoutTrigger(timeout int) {
    // Acquire lock and setup for release
    chain.MutexLock()
    defer chain.mutex.Unlock()

	time.Sleep(time.Duration(timeout) * time.Millisecond)

	if chain.Success == 1 {
		return
	}

	if (timeoutHandler != nil) {
        // Custom Handler
        timeoutHandler(chain)
    } else {
        // Default Handler
        fmt.Println("Timeout on ID: ", chain.Id)
    }
}
