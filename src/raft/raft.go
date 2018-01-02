package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	votedFor     int
	status       int
	voteAcquired int

	heartBeatCh chan struct{}
	giveVoteCH  chan struct{}

	electionTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	isleader = false
	if rf.status == 2 {
		isleader = true
	}
	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidatedId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if rf.currentTerm < args.Term {
		DPrintf("Term less : node %d term %d give vote to  node %d term %d\n", rf.me, rf.currentTerm, args.CandidatedId, args.Term)
		//rf.mu.Lock()
		rf.currentTerm = args.Term
		//rf.mu.Unlock()
		rf.updateStateTo(FOLLOWER)
		//	rf.mu.Lock()
		reply.VoteGranted = true
		rf.votedFor = args.CandidatedId
		//	rf.mu.Unlock()
		// rf.status = 0
		// rf.votedFor = -1
		// reply.VoteGranted = true
	} else {
		//	rf.mu.Lock()
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidatedId
			reply.VoteGranted = true
			DPrintf("Term equal : %d give vote to %d\n", rf.me, args.CandidatedId)
		} else {
			reply.VoteGranted = false
		}
		//	rf.mu.Unlock()
	}

	if reply.VoteGranted == true {
		go func() { rf.giveVoteCH <- struct{}{} }()
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		reply.Success = true
	} else {
		reply.Success = true
	}
	if reply.Success == true {
		go func() {
			rf.heartBeatCh <- struct{}{}
		}()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) beginElection() {
	//rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteAcquired = 1
	//rf.mu.Unlock()
	DPrintf("[Node %d] begin election in term %d\n", rf.me, rf.currentTerm)
	rf.resetTimer()
	rf.broadcastVoteReq()
}

func (rf *Raft) broadcastVoteReq() {
	args := RequestVoteArgs{Term: rf.currentTerm, CandidatedId: rf.me}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply

			if rf.status == 1 && rf.sendRequestVote(server, &args, &reply) {

				//rf.mu.Lock()

				if reply.VoteGranted == true {
					DPrintf("[Node %d] receive vote from %d\n", rf.me, server)
					rf.voteAcquired++
					//	rf.mu.Unlock()
				} else {
					DPrintf("[Node %d] did not receive vote from %d\n", rf.me, server)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						//		rf.mu.Unlock()
						rf.updateStateTo(FOLLOWER)
					}
				}
			} else {
				DPrintf("Server %d send vote req failed.\n", rf.me)
			}
		}(i)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply AppendEntriesReply
			if rf.status == 2 && rf.sendAppendEntries(server, &args, &reply) {
				//	rf.mu.Lock()
				//	defer rf.mu.Unlock()
				if reply.Success == true {

				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.updateStateTo(FOLLOWER)
					}
				}
			}
			DPrintf("node %d broadcast heartBeat to %d \n", rf.me, server)
		}(i)
	}
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

func (rf *Raft) updateStateTo(state int) {
	// should always been protected by lock

	stateDesc := []string{"FOLLOWER", "CANDIDATE", "LEADER"}
	preState := rf.status

	switch state {
	case FOLLOWER:
		rf.mu.Lock()
		rf.status = FOLLOWER
		rf.votedFor = -1    // prepare for next election
		rf.voteAcquired = 0 // prepare for next election
		rf.mu.Unlock()
		rf.resetTimer()
		DPrintf("[Node %d]: In term %d:  transfer from %s to %s\n",
			rf.me, rf.currentTerm, stateDesc[preState], stateDesc[rf.status])
	case CANDIDATE:
		rf.mu.Lock()
		rf.status = CANDIDATE
		rf.mu.Unlock()
		DPrintf("[Node %d]: In term %d:  transfer from %s to %s\n",
			rf.me, rf.currentTerm, stateDesc[preState], stateDesc[rf.status])
		rf.beginElection()
	case LEADER:
		rf.mu.Lock()
		rf.status = LEADER
		rf.mu.Unlock()
		DPrintf("[Node %d]: In term %d:  transfer from %s to %s\n",
			rf.me, rf.currentTerm, stateDesc[preState], stateDesc[rf.status])
	default:
		DPrintf("Warning: invalid state %d, do nothing.\n", state)
	}

}

func (rf *Raft) resetTimer() {
	//rf.mu.Lock()
	//	defer rf.mu.Unlock()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := (600 + r.Intn(1000)) * 1000000
	DPrintf("[Node %d]: In term %d reset election timeout %d ms\n", rf.me, rf.currentTerm, duration/1000000)
	rf.electionTimer.Reset(time.Duration(duration))
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.voteAcquired = 0
	rf.status = 0
	rf.currentTerm = 0
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := time.Duration((600 + r.Intn(1000)) * 1000000)
	rf.electionTimer = time.NewTimer(duration)
	rf.giveVoteCH = make(chan struct{}, 1)
	rf.heartBeatCh = make(chan struct{}, 1)
	stateDesc := []string{"FOLLOWER", "CANDIDATE", "LEADER"}
	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for {
			switch rf.status {
			case 0:
				select {
				case <-rf.giveVoteCH:
					rf.resetTimer()
				case <-rf.heartBeatCh:
					rf.resetTimer()
				case <-rf.electionTimer.C:
					// rf.mu.Lock()
					rf.updateStateTo(CANDIDATE)
					// rf.mu.Unlock()
				}

			case 1:
				select {
				case <-rf.heartBeatCh:
					DPrintf("[Node %d]: status [%s] reveive heartBeat，reset election timer\n", rf.me, stateDesc[rf.status])
					rf.updateStateTo(FOLLOWER)
				case <-rf.electionTimer.C:
					rf.beginElection()
				default:
					// DPrintf("In term %d: server %d get %d\n",
					// 	rf.currentTerm, rf.me, rf.voteAcquired)
					if rf.voteAcquired > len(rf.peers)/2 {
						DPrintf("In term %d: server %d get %d\n",
							rf.currentTerm, rf.me, rf.voteAcquired)
						rf.updateStateTo(LEADER)
					}
				}

			case 2:
				// rf.mu.Lock()
				broadtimeout := time.NewTimer(150 * time.Millisecond)
				select {
				case <-rf.heartBeatCh:
					DPrintf("[Node %d]: status [%s] reveive heartBeat，reset election timer\n", rf.me, stateDesc[rf.status])
					rf.updateStateTo(FOLLOWER)
				case <-broadtimeout.C:
					rf.broadcastAppendEntries()
				default:
				}

				// rf.mu.Unlock()
			}

		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
