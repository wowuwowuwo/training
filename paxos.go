package main

import (
    "fmt"
    "math/rand"
    "sync"
    "time"
)

type PrepareResquest struct {
    instance_num  int
    from          string
    prepare_num   int
    response_chan chan PrepareResponse
}

type PrepareResponse struct {
    instance_num    int
    ok              string // "ok" or "reject"
    past_accept_num int
    past_accept_log LogEntry
    from            string
}

type AcceptRequest struct {
    instance_num  int
    from          string
    prepare_num   int
    log           LogEntry
    response_chan chan AcceptResponse
}

type AcceptResponse struct {
    instance_num int
    ok           string // "ok" or "reject"
    from         string
}

type ProposerContext struct {
    // instance num
    instance_num int
    // proposer num
    prepare_num int
    // value, max vote value or proposer value
    log LogEntry
    // max vote num
    past_max_accept_num int
    // prepare response msg
    prepare_response_count        int
    prepare_response_ok_count     int
    prepare_response_reject_count int
    prepare_response_list         []PrepareResponse
    // accept response msg
    accept_response_count        int
    accept_response_ok_count     int
    accept_response_reject_count int
    accept_response_list         []AcceptResponse
    // result
    done bool
}

func (pctx *ProposerContext) Init() {
    pctx.prepare_response_list = make([]PrepareResponse, 0, 10)
    pctx.accept_response_list = make([]AcceptResponse, 0, 10)
    pctx.done = false
    pctx.log.id = g_null
    pctx.log.data = g_null
    pctx.log.instance_num = g_null
}

func (pctx *ProposerContext) SetInstanceNum(instance_num int) {
    pctx.instance_num = instance_num
}

type AcceptorContext struct {
    instance_num int
    // as acceptor
    promise_num int
    accept_num  int
    // value
    log LogEntry
}

func (actx *AcceptorContext) Init() {
    actx.promise_num = 0
    actx.accept_num = 0
    actx.log.id = g_null
    actx.log.data = g_null
    actx.log.instance_num = g_null
}

func (actx *AcceptorContext) SetInstanceNum(instance_num int) {
    actx.instance_num = instance_num
}

type LogEntry struct {
    id           int
    data         int
    instance_num int
}

func (log *LogEntry) Init(data int) {
    num := allocate_instance_num()
    log.id = num
    log.data = data
    log.instance_num = num
}

func (log *LogEntry) IsNull() bool {
    return log.id == g_null && log.data == g_null && log.instance_num == g_null
}

type ClientRequest struct {
    data          int
    response_chan chan ClientResponse
}

type ClientResponse struct {
    from    string
    success bool
    data    int
}

type Node struct {
    name string
    // channels for paxos
    begin_paxos_chan      chan LogEntry
    end_paxos_chan        chan ClientResponse
    prepare_request_chan  chan PrepareResquest
    prepare_response_chan chan PrepareResponse
    accept_request_chan   chan AcceptRequest
    accept_response_chan  chan AcceptResponse
    // log entries
    logs []LogEntry
    // channels for client
    client_request_chan chan ClientRequest
}

func (node *Node) Init(n string) {
    node.name = n

    node.begin_paxos_chan = make(chan LogEntry)
    node.end_paxos_chan = make(chan ClientResponse)
    node.prepare_request_chan = make(chan PrepareResquest)
    node.prepare_response_chan = make(chan PrepareResponse)
    node.accept_request_chan = make(chan AcceptRequest)
    node.accept_response_chan = make(chan AcceptResponse)

    node.logs = make([]LogEntry, 0, 200)

    node.client_request_chan = make(chan ClientRequest)
}

func (node *Node) start_message() {
    fmt.Printf("node: %s has started !\n", node.name)
    go func() {
        node.do_paxos()
    }()
    req_map := make(map[int]*ClientRequest) // data --> req, use data as req id
    for {
        select {
        case req := <-node.client_request_chan:
            {
                var log *LogEntry = new(LogEntry)
                log.Init(req.data)
                req_map[req.data] = &req
                go func() {
                    node.begin_paxos_chan <- *log
                }()
            }
        case res := <-node.end_paxos_chan:
            {
                if req, exists := req_map[res.data]; exists {
                    go func() {
                        req.response_chan <- res
                    }()
                } else {
                    // impossible
                }
            }
        }
    }
}

func (node *Node) do_paxos() {
    go node.do_as_proposer()
    go node.do_as_acceptor()
}

func (node *Node) do_as_proposer() {
    // instance_num --> pctx
    var pctx_map map[int]*ProposerContext = make(map[int]*ProposerContext)
    for {
        select {
        // as proposer, prepare
        case log := <-node.begin_paxos_chan:
            {
                var pctx *ProposerContext
                if _, exists := pctx_map[log.instance_num]; !exists {
                    pctx = new(ProposerContext)
                    pctx.Init()
                    pctx.SetInstanceNum(log.instance_num)
                    pctx_map[pctx.instance_num] = pctx
                } else {
                    pctx = new(ProposerContext)
                    pctx.Init()
                    pctx.SetInstanceNum(log.instance_num)
                    pctx_map[pctx.instance_num] = pctx
                }
                // get prepare_num
                var prepare_request PrepareResquest
                prepare_request.instance_num = pctx.instance_num
                prepare_request.from = node.name
                prepare_request.prepare_num = allocate_prepare_num(pctx.instance_num)
                prepare_request.response_chan = node.prepare_response_chan
                // logentry
                pctx.log = log
                pctx.prepare_num = prepare_request.prepare_num
                // send prepare_request to quorum nodes in parallel
                for _, target := range g_node_map {
                    go func() {
                        target.prepare_request_chan <- prepare_request
                    }()
                }
            }
        // as proposer, on prepare response
        case prepare_response := <-node.prepare_response_chan:
            {
                var pctx *ProposerContext
                if _, exists := pctx_map[prepare_response.instance_num]; exists {
                    pctx = pctx_map[prepare_response.instance_num]
                } else {
                    // impossible !
                }
                if pctx.done {
                    break
                }
                if prepare_response.ok == "ok" {
                    if prepare_response.past_accept_num > pctx.past_max_accept_num &&
                        !prepare_response.past_accept_log.IsNull() {
                        // get past max vote's num & log value
                        pctx.past_max_accept_num = prepare_response.past_accept_num
                        pctx.log = prepare_response.past_accept_log
                    }
                    pctx.prepare_response_ok_count++
                } else {
                    // receive reject
                    pctx.prepare_response_reject_count++
                }
                pctx.prepare_response_list = append(pctx.prepare_response_list, prepare_response)
                if pctx.prepare_response_ok_count >= len(g_node_map)/2+1 {
                    // quorum nodes say ok, so accept it
                    var accept_request AcceptRequest
                    accept_request.instance_num = pctx.instance_num
                    accept_request.from = node.name
                    accept_request.prepare_num = pctx.prepare_num
                    accept_request.log = pctx.log
                    accept_request.response_chan = node.accept_response_chan
                    // send accept request to quorum nodes in parallel
                    for _, response := range pctx.prepare_response_list {
                        if response.ok == "ok" {
                            if target, exists := g_node_map[response.from]; exists {
                                go func() {
                                    target.accept_request_chan <- accept_request
                                }()
                            }
                        }
                    }
                }
                if pctx.prepare_response_reject_count >= len(g_node_map)/2+1 {
                    // need prepare again
                    go func() {
                        node.begin_paxos_chan <- pctx.log
                    }()
                }
            }
        // as proposer, on accept response
        case accept_response := <-node.accept_response_chan:
            {
                var pctx *ProposerContext
                if _, exists := pctx_map[accept_response.instance_num]; exists {
                    pctx = pctx_map[accept_response.instance_num]
                } else {
                    // impossible !
                }
                if pctx.done {
                    break
                }
                if accept_response.ok == "ok" {
                    pctx.accept_response_ok_count++
                } else {
                    // receive reject
                    pctx.accept_response_reject_count++
                }
                pctx.accept_response_list = append(pctx.accept_response_list, accept_response)
                if pctx.accept_response_ok_count >= len(g_node_map)/2+1 {
                    // basic paxos is finally done here !
                    pctx.done = true
                    go func() {
                        var client_response ClientResponse
                        client_response.from = node.name
                        client_response.success = true
                        client_response.data = pctx.log.data
                        node.end_paxos_chan <- client_response
                    }()
                }
                if pctx.accept_response_reject_count >= len(g_node_map)/2+1 {
                    // need prepare again
                    go func() {
                        node.begin_paxos_chan <- pctx.log
                    }()
                }
            }
        }
    }
}

func (node *Node) do_as_acceptor() {
    // instance_num --> actx
    var actx_map map[int]*AcceptorContext = make(map[int]*AcceptorContext)
    for {
        select {
        // as acceptor, on prepare
        case prepare_request := <-node.prepare_request_chan:
            {
                var actx *AcceptorContext
                if _, exists := actx_map[prepare_request.instance_num]; !exists {
                    actx = new(AcceptorContext)
                    actx.Init()
                    actx.SetInstanceNum(prepare_request.instance_num)
                    actx_map[actx.instance_num] = actx
                } else {
                    actx = actx_map[prepare_request.instance_num]
                }
                var prepare_response PrepareResponse
                prepare_response.instance_num = actx.instance_num
                if prepare_request.prepare_num >= actx.promise_num {
                    actx.promise_num = prepare_request.prepare_num
                    prepare_response.ok = "ok"
                    prepare_response.past_accept_num = actx.accept_num
                    prepare_response.past_accept_log = actx.log
                    prepare_response.from = node.name
                } else {
                    prepare_response.ok = "reject"
                }
                // send response to proposer
                go func() {
                    prepare_request.response_chan <- prepare_response
                }()
            }
        // as acceptor, on accept
        case accept_request := <-node.accept_request_chan:
            {
                var actx *AcceptorContext
                if _, exists := actx_map[accept_request.instance_num]; exists {
                    actx = actx_map[accept_request.instance_num]
                } else {
                    // impossible !
                }
                var accept_response AcceptResponse
                accept_response.instance_num = actx.instance_num
                if accept_request.prepare_num == actx.promise_num {
                    actx.accept_num = accept_request.prepare_num
                    actx.log = accept_request.log
                    accept_response.ok = "ok"
                } else {
                    accept_response.ok = "reject"
                }
                // send response to proposer
                go func() {
                    accept_request.response_chan <- accept_response
                }()
            }
        }
    }
}

type Client struct {
    node_map  map[string]*Node
    shard_num int
}

func (c *Client) Init(shard_num int) {
    c.node_map = g_node_map
    c.shard_num = shard_num
}

func (c *Client) start() {
    start := time.Now()
    rand.Seed(time.Now().UnixNano())
    data_num := 0
    for i := 0; i < g_data_num; i++ {
        if i%g_shard_num != c.shard_num {
            continue
        }
        // choose one random node as 'leader'
        cur_leader := g_node_name[rand.Intn(len(g_node_name))]
        n := c.node_map[string(cur_leader)]
        // write one data to paxos system
        req := new(ClientRequest)
        req.data = i
        req.response_chan = make(chan ClientResponse)
        go func() {
            n.client_request_chan <- *req
            fmt.Printf("client for shard: %d, send data: %d to node: %s\n", c.shard_num, req.data, n.name)
        }()
        // wait until res
        over := false
        for !over {
            select {
            case res := <-req.response_chan:
                {
                    if res.success {
                        if res.data == i {
                            fmt.Printf("info : client for shard: %d, receive res: %d, for data: %d, from node: %s, success \n",
                                c.shard_num, res.data, i, res.from)
                        } else {
                            fmt.Printf("error: client for shard: %d, receive res: %d, for data: %d, from node: %s, success but non consistency !\n",
                                c.shard_num, res.data, i, res.from)
                        }
                    } else {
                        fmt.Printf("client: receive res for data: %d, from node: %s, failed\n", res.data, res.from)
                    }
                    over = true
                }
            }
        }
        data_num++
    }
    d := time.Since(start)
    fmt.Printf("info: client for shard: %d, time consume: %s, data count: %d, speed: %d/s, paxos node count: %d\n",
        c.shard_num, d.String(), data_num, data_num/int(d.Seconds()), len(g_node_name))
    //time.Sleep(time.Second)
}

const g_null int = -1024
const g_data_num int = 1000000
const g_node_name string = "ABCDE"
const g_shard_num int = 10 // shard parallel

var g_prepare_num int = 0
var g_instance_num int = 0

var g_node_map map[string]*Node = make(map[string]*Node)
var g_client_map map[int]*Client = make(map[int]*Client)

var g_instance_map map[int]int = make(map[int]int) // instance_num --> prepare_num
var g_mutex *sync.Mutex = new(sync.Mutex)

func allocate_instance_num() int {
    g_mutex.Lock()
    num := g_instance_num
    g_instance_map[num] = 0
    g_instance_num++
    g_mutex.Unlock()
    return num
}

func allocate_prepare_num(instance_num int) int {
    g_mutex.Lock()
    num := 0
    if prepare_num, exists := g_instance_map[instance_num]; exists {
        num = prepare_num
        g_instance_map[instance_num]++
    } else {
        // impossible
    }
    g_mutex.Unlock()
    return num
}

func main() {
    for i := 0; i < len(g_node_name); i++ {
        node := new(Node)
        node.Init(string(g_node_name[i]))
        g_node_map[node.name] = node
        go node.start_message()
    }
    time.Sleep(time.Second * 5)
    for i := 0; i < g_shard_num; i++ {
        client := new(Client)
        client.Init(i)
        g_client_map[client.shard_num] = client
        go client.start()
    }
    for {
        time.Sleep(time.Second)
    }
}
