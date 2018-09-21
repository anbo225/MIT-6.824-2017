package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"


type Clerk struct {
	mu	sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	seqId int
	possibleLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seqId = 0
	ck.possibleLeader = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientID: ck.clientId,
		SeqID:      ck.getAndIncSeq(),
	}
	var i int

	for{
		i = ck.getPossibleLeader()
		reply := GetReply{}

		if ok := ck.servers[i].Call("RaftKV.Get", &args, &reply); !ok || reply.WrongLeader {
			ck.setPossibleLeader((i+1) %len(ck.servers))
			continue
		}

		if reply.Err != OK{
			continue
		}

		ck.setPossibleLeader(i)
		return reply.Value
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.clientId,
		SeqID:    ck.getAndIncSeq(),
	}

	var i int
	for {
		i = ck.getPossibleLeader()
		reply := PutAppendReply{}
		if ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply); !ok || reply.WrongLeader{
			ck.setPossibleLeader((i+1)%len(ck.servers))
			continue
		}

		if reply.Err != OK{
			continue
		}
		if op == Put {
			DPrintf("Put %s : %s" ,key, value)
		}else{
			DPrintf("Append %s : %s" ,key, value)
		}
		ck.setPossibleLeader(i)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
/*
 * Some help function here
 */
func (ck *Clerk) getPossibleLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.possibleLeader
}
func (ck *Clerk) setPossibleLeader(leader int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.possibleLeader = leader
}

func (ck *Clerk) getAndIncSeq() int{
	ck.mu.Lock()
	ck.mu.Unlock()
	tmp := ck.seqId
	ck.seqId += 1
	return tmp
}
