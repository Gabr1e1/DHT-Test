//TODO: LINKED LIST

package DHT

import (
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const M = 160
const RoutingLimit = 32

var expM = new(big.Int).Exp(big.NewInt(2), big.NewInt(M), nil)

type KVPair struct {
	Key   string
	Value string
}

type InfoType struct {
	IPAddr  string
	NodeNum *big.Int
}

type Node struct {
	Finger      [M]InfoType
	Successors  [M]InfoType
	Predecessor InfoType
	Info        InfoType
	data        map[string]KVPair
	server      *rpc.Server
	mux         sync.Mutex
	status      int
	listener    net.Listener
	wg          sync.WaitGroup
}

//check whether c in [a,b)
func checkBetween(a, b, mid *big.Int) bool {
	if a.Cmp(b) >= 0 {
		return checkBetween(a, expM, mid) || checkBetween(big.NewInt(0), b, mid)
	}
	return mid.Cmp(a) >= 0 && mid.Cmp(b) < 0
}

//the successor list might not be effective due to force quitting nodes
func (n *Node) FindFirstSuccessorAlive(tmp *int, reply *InfoType) error {
	n.mux.Lock()
	for i, node := range n.Successors {
		if !n.Ping(node.IPAddr) {
			n.Successors[i] = InfoType{"", big.NewInt(0)}
			continue
		}
		*reply = copyInfo(node)

		n.mux.Unlock()
		return nil
	}
	n.mux.Unlock()
	return errors.New("NoSuccessor")
}

func (n *Node) GetSuccessors(_ *int, reply *[M]InfoType) error {
	*reply = n.Successors
	return nil
}

func (n *Node) ModifySuccessors(succ *InfoType, _ *int) error {
	if succ.NodeNum.Cmp(n.Info.NodeNum) == 0 {
		return nil
	}
	client, err := n.Connect(*succ)
	if err != nil {
		return err
	}
	n.mux.Lock()
	var newSucList [M]InfoType
	err = client.Call("Node.GetSuccessors", 0, &newSucList)
	if err != nil {
		return err
	}
	_ = client.Close()
	n.Finger[0], n.Successors[0] = copyInfo(*succ), copyInfo(*succ)
	for i := 1; i < M; i++ {
		n.Successors[i] = newSucList[i-1]
	}
	n.mux.Unlock()
	return nil
}

func (n *Node) ModifyPredecessor(pred *InfoType, reply *int) error {
	n.mux.Lock()
	n.Predecessor = copyInfo(*pred)
	n.mux.Unlock()
	return nil
}

func (n *Node) GetPredecessor(_ *int, reply *InfoType) error {
	if n.Predecessor.NodeNum.Cmp(big.NewInt(0)) != 0 && !n.Ping(n.Predecessor.IPAddr) {
		n.Predecessor = InfoType{"", big.NewInt(0)}
	}
	*reply = copyInfo(n.Predecessor)
	return nil
}

func (n *Node) findPredecessor(id *big.Int) InfoType {
	var cnt = 0

	p := copyInfo(n.Info)
	var successor InfoType
	var tmp int
	err := n.FindFirstSuccessorAlive(&tmp, &successor)
	if err != nil {
		fmt.Println("Can't find predecessor")
		return InfoType{"", big.NewInt(0)}
	}

	for (!checkBetween(big.NewInt(1).Add(p.NodeNum, big.NewInt(1)), successor.NodeNum, id)) && cnt <= RoutingLimit {
		cnt++
		var err error
		if p.NodeNum.Cmp(n.Info.NodeNum) != 0 {
			client, err := n.Connect(p)
			if err != nil {
				return InfoType{"", big.NewInt(0)}
			}
			err = client.Call("Node.ClosestPrecedingNode", id, &p)
			if err != nil {
				_ = client.Close()
				return InfoType{"", big.NewInt(0)}
			}
			_ = client.Close()
		} else {
			err = n.ClosestPrecedingNode(id, &p)
			//fmt.Println(n.Info, p, id)
		}
		if err != nil {
			fmt.Println("Can't find Predecessor: ", err)
			return InfoType{"", big.NewInt(0)}
		}
		client, err := n.Connect(p)
		if err != nil {
			return InfoType{"", big.NewInt(0)}
		}
		err = client.Call("Node.FindFirstSuccessorAlive", 0, &successor)
		if err != nil {
			return InfoType{"", big.NewInt(0)}
		}
		err = client.Close()

	}
	//fmt.Printf("Found Predecessor using %d jumps\n", cnt)
	//fmt.Println(n.Info, " Found predecessor", p)
	return p
}

func (n *Node) ClosestPrecedingNode(id *big.Int, reply *InfoType) error {
	//first check finger table
	for i := M - 1; i >= 0; i-- {
		if checkBetween(big.NewInt(1).Add(n.Info.NodeNum, big.NewInt(1)), id, n.Finger[i].NodeNum) {
			// possible fail node
			if !n.Ping(n.Finger[i].IPAddr) {
				n.Finger[i] = InfoType{"", big.NewInt(0)}
				continue
			}
			*reply = copyInfo(n.Finger[i])
			return nil
		}
	}

	//then check successor list
	for i := M - 1; i >= 0; i-- {
		if checkBetween(big.NewInt(1).Add(n.Info.NodeNum, big.NewInt(1)), id, n.Successors[i].NodeNum) {
			// possible fail node
			if !n.Ping(n.Successors[i].IPAddr) {
				n.Successors[i] = InfoType{"", big.NewInt(0)}
				continue
			}
			*reply = copyInfo(n.Successors[i])
			return nil
		}
	}

	*reply = copyInfo(n.Info)
	return nil
}

func (n *Node) FindSuccessor(id *big.Int, reply *InfoType) error {
	t := n.findPredecessor(id)
	client, err := n.Connect(t)
	if err != nil {
		return err
	}
	err = client.Call("Node.FindFirstSuccessorAlive", 0, reply)
	_ = client.Close()

	if err != nil {
		fmt.Println("Can't get successor: ", err)
		return err
	}
	return nil
}

func (n *Node) DirectGet_(k *string, reply *string) error {
	id := GetHash(*k)
	n.mux.Lock()
	val, ok := n.data[id.String()]
	n.mux.Unlock()
	if !ok {
		return errors.New("can't get")
	}
	*reply = val.Value
	return nil
}

func (n *Node) Get_(k *string, reply *string) error {
	id := GetHash(*k)
	if val, ok := n.data[id.String()]; ok {
		*reply = val.Value
		return nil
	}
	var p InfoType
	err := n.FindSuccessor(id, &p)
	if err != nil {
		return err
	}

	if p.NodeNum.Cmp(n.Info.NodeNum) != 0 {
		client, err := n.Connect(p)
		if err != nil {
			return err
		}
		var res string
		err = client.Call("Node.DirectGet_", k, &res)
		if err != nil {
			_ = client.Close()
			//fmt.Println("Can't get Node: ", err)
			return err
		}
		_ = client.Close()
		*reply = res
	}
	return nil
}

func (n *Node) DirectPut_(kv *KVPair, reply *bool) error {
	id := GetHash(kv.Key)
	n.mux.Lock()
	n.data[id.String()] = *kv
	n.mux.Unlock()
	*reply = true
	return nil
}

func (n *Node) Put_(kv *KVPair, reply *bool) error {
	id := GetHash(kv.Key)
	var p InfoType
	err := n.FindSuccessor(id, &p)

	if err != nil {
		return err
	}

	var succ InfoType
	if p.NodeNum.Cmp(n.Info.NodeNum) == 0 {
		n.data[id.String()] = *kv
		*reply = true
		succ = n.Successors[0]
	} else {
		client, err := n.Connect(p)
		if err != nil {
			return err
		}
		err = client.Call("Node.DirectPut_", kv, reply)
		if err != nil {
			_ = client.Close()
			fmt.Println("Can't Put data in another node: ", err)
			return err
		}
		_ = client.Call("Node.FindFirstSuccessorAlive", 0, &succ)
		_ = client.Close()
	}

	//replicate
	client, err := n.Connect(succ)
	if err != nil {
		return err
	}
	var tmp bool
	_ = client.Call("Node.DirectPut_", kv, &tmp)
	_ = client.Close()
	return nil
}

func (n *Node) DirectDel_(k *string, reply *bool) error {
	n.mux.Lock()
	id := GetHash(*k)
	_, ok := n.data[id.String()]
	if ok {
		delete(n.data, id.String())
	}
	*reply = ok
	n.mux.Unlock()
	return nil
}

func (n *Node) Del_(k *string, reply *bool) error {
	id := GetHash(*k)
	var p InfoType
	err := n.FindSuccessor(id, &p)
	if err != nil {
		return err
	}

	if p.NodeNum.Cmp(n.Info.NodeNum) == 0 {
		_, ok := n.data[id.String()]
		if ok {
			delete(n.data, id.String())
		}
		*reply = ok
	} else {
		client, err := n.Connect(p)
		if err != nil {
			return err
		}
		err = client.Call("Node.DirectDel_", k, reply)
		if err != nil {
			_ = client.Close()
			fmt.Println("Can't Delete data in another node: ", err)
			return err
		}
		_ = client.Close()
	}
	return nil
}

func (n *Node) GetNodeInfo(_ *int, reply *InfoType) error {
	*reply = copyInfo(n.Info)
	return nil
}

func (n *Node) TransferData(replace *InfoType, reply *int) error {
	//fmt.Println("Transfer Data", n.Info.IPAddr)
	if replace.IPAddr == "" {
		return nil
	}

	client, err := n.Connect(*replace)
	if err != nil {
		return err
	}

	n.mux.Lock()
	for hashKey, KV := range n.data {
		var t big.Int
		t.SetString(hashKey, 10)
		if checkBetween(n.Info.NodeNum, replace.NodeNum, &t) {
			var tmp bool
			err := client.Call("Node.DirectPut_", &KV, &tmp)
			if err != nil {
				n.mux.Unlock()
				_ = client.Close()
				fmt.Println("Transfer Failed", err)
				return err
			}
			delete(n.data, hashKey)
		}
	}
	n.mux.Unlock()
	_ = client.Close()
	return nil
}

func (n *Node) TransferDataForce(replace *InfoType, reply *int) error {
	client, err := n.Connect(*replace)
	if err != nil {
		return err
	}

	n.mux.Lock()
	for _, KV := range n.data {
		var tmp bool
		err := client.Call("Node.DirectPut_", &KV, &tmp)
		if err != nil {
			n.mux.Unlock()
			_ = client.Close()
			fmt.Println("Transfer Failed", err)
			return err
		}
	}
	n.mux.Unlock()
	_ = client.Close()
	return nil
}

//verify(and possibly change) n's successor
func (n *Node) stabilize() {
	for {
		if n.status == 0 {
			break
		}
		var tmp int
		var x = InfoType{"", big.NewInt(0)}

		err := n.FindFirstSuccessorAlive(nil, &n.Successors[0])
		if err != nil {
			continue
		}

		client, err := n.Connect(n.Successors[0])
		if err != nil {
			continue
		}
		err = client.Call("Node.GetPredecessor", 0, &x)
		if err != nil {
			fmt.Println("Can't get predecessor: ", err)
			continue
		}

		n.mux.Lock()
		if x.NodeNum.Cmp(big.NewInt(0)) != 0 && checkBetween(big.NewInt(1).Add(n.Info.NodeNum, big.NewInt(1)), n.Successors[0].NodeNum, x.NodeNum) {
			n.Successors[0], n.Finger[0] = copyInfo(x), copyInfo(x)
			//fmt.Printf("STABILIZE: %s's successor is %s\n", n.Info.IPAddr, x.IPAddr)
		}
		n.mux.Unlock()
		_ = client.Close()

		err = n.ModifySuccessors(&n.Successors[0], &tmp)
		if err != nil {
			continue
		}
		client, err = n.Connect(n.Successors[0])
		if err != nil {
			continue
		}

		err = client.Call("Node.Notify", &n.Info, &tmp)
		_ = client.Close()
		if err != nil {
			fmt.Println("Can't Notify: ", err)
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//n(self) is notified of the existence of another node which is a candidate for Predecessor
func (n *Node) Notify(other *InfoType, reply *int) error {
	n.mux.Lock()
	if n.Predecessor.IPAddr == "" || checkBetween(big.NewInt(1).Add(n.Predecessor.NodeNum, big.NewInt(1)), n.Info.NodeNum, other.NodeNum) {
		n.Predecessor = copyInfo(*other)
		n.mux.Unlock()
		//fmt.Printf("NOTIFY: %s's predecessor is %s\n", n.Info.IPAddr, other.IPAddr)
		if n.Predecessor.IPAddr != n.Info.IPAddr {
			client, err := n.Connect(n.Predecessor)
			if err != nil {
				return err
			}
			err = client.Call("Node.Maintain", 0, nil)
			if err != nil {
				fmt.Println(n.Info, "Can't call maintain", err)
			}
			_ = client.Close()
		}
	} else {
		n.mux.Unlock()
	}
	return nil
}

func (n *Node) checkPredecessor() {
	for {
		n.mux.Lock()
		if n.Predecessor.IPAddr != "" {
			if !n.Ping(n.Predecessor.IPAddr) {
				n.Predecessor = InfoType{"", big.NewInt(0)}
			}
		}
		n.mux.Unlock()
		_ = n.Maintain(nil, nil)
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) fixFingers() {
	for {
		if n.status == 0 {
			break
		}
		i := rand.Intn(M-1) + 1 //random numbers in [1, M - 1]
		var id big.Int
		id.Add(n.Info.NodeNum, id.Exp(big.NewInt(2), big.NewInt(int64(i)), nil))
		if id.Cmp(expM) >= 0 {
			id.Sub(&id, expM)
		}

		err := n.FindSuccessor(&id, &n.Finger[i])
		if err != nil {
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) Maintain(_ *int, _ *int) error {
	err := n.FindFirstSuccessorAlive(nil, &n.Successors[0])
	if err != nil {
		return nil
	}

	n.mux.Lock()
	t := make(map[string]KVPair)
	for k, v := range n.data {
		t[k] = v
	}
	n.mux.Unlock()

	client, err := n.Connect(n.Successors[0])
	if err != nil {
		return nil
	}

	for hash, kv := range t {
		tmp, _ := new(big.Int).SetString(hash, 10)
		if !checkBetween(n.Predecessor.NodeNum, n.Info.NodeNum, tmp) {
			continue
		}
		var reply bool
		go client.Call("Node.DirectPut_", &kv, &reply)
	}
	_ = client.Close()
	return nil
}
