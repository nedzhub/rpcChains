package rpcc

import (
    "net"
    "net/rpc"
    "math/rand"
)

/* === Headers === */
type Client struct {
    // Parent RPC Client
    RPCClient rpc.Client
    
    // RPCC Specific info
}

type RpcChain struct {
    Id int                  // The unique ID of the chain, generated
    CurrrentPosition int    // The current position/server in the chain
    Args interface{}        // The arguments passed to the first server
    Reply interface{}       // The reply from the last server
    Chain serverEntity[]    // The full chain of the servers
}

// Function header of the entry function
type EntryFunction func(interface{}}interface{}

// The server entity in the chain
type ServerEntity struct {
    Connection_info string
    Server_info string
    Entry entryFunction
}

/* === Globals === */
var lastIdUsed int

/* === Functions === */
// RPCC's public functions
func Dial(network, address string) (*Client, error) {
    // Initialize RPCC if needed

    // Dial as RPC
}

func (client *Client) Call(chain rpcChain, timeout int) error {
    // Prepare RPCC
    
    // Setup timeout procedure
    
    // Send as RPC
    //client.RPCClient.Call("FrontEndServiceAuth.Handshake", args, &kvVal)
}

// TODO: Do we really need this? Maybe for stopping time-out timer
func Callback(chain rpcChain) {

}

// TODO: 
func (client *Client) Close() error {
    client.RPCClient.Close()
}

// RPCC's internal functions
function generateUniqueId() int {
    randSource := rand.NewSource(time.Now().UnixNano())
    randSeed := rand.New(s1)
    
    lastIdUsed++
    return r1.Intn(10000) + lastIdUsed
}