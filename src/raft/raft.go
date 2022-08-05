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
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "fmt"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// var electionTimeout = time.Duration(200) * time.Millisecond	// 心跳超时时间

// type State []byte

const heartTime = 100 // 定期发送心跳的时间

const (
	follower    = 1
	candidate = 2
	leader  = 3
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	// Command string
	Command interface{}
	Term    int
	Index   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	log []LogEntry

	currentTerm  int // 当前的任期
	currentState int // 服务器当前的状态
	votedFor     int // 在当前任期内投票给了谁
	voteCount    int // 当前获取到的选票的数目

	commitIndex   int       // 最新提交的log的位置
	lastApplied   int       // 最后一次提交的log的位置
	lastLogIndex  int		// 最近一次log的index
	lastLogTerm   int       // 最近一次log的term
	lastHeartBeat time.Time // 最近一次收到 HeartBeat 的时间

	nextIndex  []int // 每个服务器需要发送给它的下一条日志的索引(初始化为leader最后一条日志索引+1)
	matchIndex []int // 每个服务器已复制给它的日志的最高索引值

	leader int // 当前leader的位置

	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	term := rf.currentTerm
	isleader := rf.leader == rf.me
	// Your code here (2A).
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate当前的term
	CondidateId  int // candidate的ID
	LastLogIndex int // candidate上一条日志的Index
	LastLogTerm  int // candidate上一条日志的term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 如果term < currentTerm 返回 currentTerm，否则返回term
	Term           int
	IntvoteGranted bool
}

func (rf *Raft) agreeVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// log.Printf("follower %d 在term %d 同意给 %d 投票", rf.me, rf.currentTerm, args.CondidateId)
	rf.votedFor = args.CondidateId
	rf.currentTerm = args.Term
	reply.IntvoteGranted = true
	reply.Term = rf.currentTerm
	// log.Printf("server %d aggre vote to server %d at term %d, server lastApplied %d, lastLogTerm %d", rf.me, args.CondidateId, rf.currentTerm, rf.lastApplied, rf.lastLogTerm)
}

func (rf *Raft) disagreeVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// log.Printf("follower %d 在term %d 拒绝给 %d 投票", rf.me, rf.currentTerm, args.CondidateId)
	reply.IntvoteGranted = false
	reply.Term = rf.currentTerm
	// log.Printf("server %d disaggre vote to server %d at term %d, server lastApplied %d, lastLogTerm %d", rf.me, args.CondidateId, rf.currentTerm, rf.lastApplied, rf.lastLogTerm)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 一个任期只能投一票
	// 如果任期小于当前的任期，直接否决
	// TODO 在候选者任期比自己大的情况下还的比较日志的新旧程度
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		rf.disagreeVote(args, reply)
		log.Printf("server %d 拒绝在term %d 给server %d 投票, 因为term太小", rf.me, rf.currentTerm, args.CondidateId)
		return
	} else {
		if args.Term == rf.currentTerm && rf.votedFor != -1 {
			rf.disagreeVote(args, reply)
			log.Printf("server %d 拒绝在term %d 给server %d 投票, 因为已经投给了 %d", rf.me, rf.currentTerm, args.CondidateId, rf.votedFor)
			return
		}
		rf.votedFor = -1
		rf.currentTerm = args.Term
		if rf.lastLogTerm > args.LastLogTerm {
			rf.disagreeVote(args, reply)
			log.Printf("server %d 拒绝在term %d 给server %d 投票, 因为lastLogTerm太小", rf.me, rf.currentTerm, args.CondidateId)
			return
		} else if rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex {
			rf.disagreeVote(args, reply)
			log.Printf("server %d 拒绝在term %d 给server %d 投票, 因为lastApplied太小", rf.me, rf.currentTerm, args.CondidateId)
			return
		}


		rf.agreeVote(args, reply)
		log.Printf("server %d 同意在term %d 给server %d 投票", rf.me, rf.currentTerm, args.CondidateId)
		return
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.leader == rf.me {
		// rf.lastHeartBeat = time.Now()
		// index := rf.lastLogIndex + 1
		//if len(rf.log) == 0 {
		//	index = 0
		//} else {
		//	index = rf.log[len(rf.log)-1].Index + 1
		//}
		
		index := rf.lastLogIndex + 1
		appendLog := LogEntry{Command: command, Term: rf.currentTerm, Index: index}
		args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.lastLogIndex, rf.lastLogTerm, appendLog, rf.commitIndex}
		// rf.mu.Unlock()
		rf.lastLogIndex = index
		rf.lastLogTerm = rf.currentTerm
		rf.log = append(rf.log, appendLog)
		log.Printf("log is %v", appendLog)
		log.Printf("args is %v", args)

		for server := range rf.peers {
			if server != rf.me {
				rf.sendAppendEntries(server, args)
			}
		}
		
		log.Printf("leader %d logs are %v", rf.me, rf.log)

		

		
		// fmt.Println("entries args is", args)

		// rf.sendAppendEntries(args)
		// TODO

		// log.Printf("leader %d send prevLogIndex is %d , the prevLogTerm is %d", rf.me, rf.lastApplied, rf.lastLogTerm)

		return rf.lastLogIndex, rf.currentTerm, rf.leader == rf.me

	}

	return -1, -1, rf.leader == rf.me
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	if !rf.killed() {
		atomic.StoreInt32(&rf.dead, 1)
	}
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 先sleep一段时间，然后判断上一次收到心跳的时间是否超时
		// TODO 这里的过期时间是要随机的
		lastHeartBeat := rf.getLastHeartBeat()
		i := 200 + rand.Intn(150)
		// fmt.Println("sleep: ", i)
		time.Sleep(time.Duration(i) * time.Millisecond)
		if lastHeartBeat != rf.getLastHeartBeat() {
			continue
		}
		rf.election()

	}
}

func (rf *Raft) election() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.changeState(candidate)
	rf.voteCount++
	// fmt.Println("voteCount: ", rf.voteCount)
	rf.votedFor = rf.me
	rf.leader = -1	// 这里一定要变成 -1, 否则会误认为自己还是leader
	rf.currentTerm++
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLogIndex, rf.lastLogTerm}
	log.Printf("server %d 在term %d 请求投票, lastLogIndex %d, lastLogTerm %d", rf.me, rf.currentTerm, rf.lastLogIndex, rf.lastLogTerm)
	// rf.mu.Unlock()
	go rf.checkState() // 启动一个线程检查一定时间之后是否赢得选举
	// fmt.Println("peers:", len(rf.peers))
	for serverNum := 0; serverNum < len(rf.peers); serverNum++ {
		if serverNum != rf.me {
			// fmt.Println(i)
			go func(server int, args RequestVoteArgs) {
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply) {
					rf.handleRequse(reply)
				}
			}(serverNum, args)
		}
	}

}

func (rf *Raft) changeState(state int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// log.Printf("server %d change state at term %d, %d -> %d", rf.me, rf.currentTerm, rf.currentState, state)
	rf.currentState = state
	// log.Printf("server %d change state to %d at term %d", rf.me, state, rf.currentTerm)
}

func (rf *Raft) getLastHeartBeat() time.Time {
	return rf.lastHeartBeat
}

// 到达时间之后还未成为leader的话，直接变为follower
func (rf *Raft) checkState() {
	time.Sleep(time.Duration(450+rand.Intn(50)) * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != leader {
		log.Printf("server %d 在term %d 因为超时竞选失败, 变为follower", rf.me, rf.currentTerm)
		rf.changeState(follower)
		rf.leader = -1
		rf.voteCount = 0
		rf.votedFor = -1
	}
}

func (rf *Raft) sendHeartBeat() {
	// args := AppendEntriesArgs{true, rf.currentTerm, rf.me, rf.commitIndex, rf.currentTerm, LogEntry{}}
	for {
		time.Sleep(heartTime * time.Microsecond)
		if !rf.killed() && rf.leader == rf.me {
			if time.Since(rf.getLastHeartBeat()) > heartTime * time.Microsecond {
				// log.Printf("leader %d send heartBeat", rf.me)
				rf.lastHeartBeat = time.Now()
				rf.heartBeat()
			}
		} else {
			break
		}
	}
}

func (rf *Raft) handleRequse(reply RequestVoteReply) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// log.Printf("server %d 的voteCount: %d", rf.me, rf.voteCount)
	if rf.voteCount == 0 {
		// log.Printf("server %d 在term %d 当前的票数为 %d", rf.me, rf.currentTerm, rf.voteCount)
		// fmt.Println("rf.voteCount == 0")
		// rf.mu.Unlock()
		return
	}
	if reply.Term > rf.currentTerm {
		log.Printf("server %d 在term %d 因为term较小竞选失败, 变为follower", rf.me, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.changeState(follower)
		// fmt.Println("rf.currentTerm < reply.Term")
		rf.voteCount = 0
		rf.votedFor = -1
		// rf.mu.Unlock()
	} else if reply.IntvoteGranted && rf.currentState == candidate {
		rf.voteCount++
		// log.Printf("server %d 在term %d 当前的票数为 %d", rf.me, rf.currentTerm, rf.voteCount)
		// fmt.Println("voteCount: ", rf.voteCount)
		// 如果已经达到了多数，就直接变为leader, 并将票数归零
		if rf.voteCount > (len(rf.peers)>>1) && rf.currentState == candidate {
			log.Printf("server %d 在term %d 当选为leader, 得票为 %d, 总数为 %d", rf.me, rf.currentTerm, rf.voteCount, len(rf.peers))
			rf.changeState(leader)
			rf.leader = rf.me
			rf.voteCount = 0
			rf.votedFor = -1
			// args := HeartBeatArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.currentTerm}
			// reply := AppendEntriesReply
			rf.leaderSend()
			// rf.mu.Unlock()
			// 开始定期给follower发送心跳
			go rf.sendHeartBeat()
			go rf.updateCommitIndex()
		}
	} else {
		log.Printf("server %d 在term %d 竞选出bug, 当前票数为 %d", rf.me, rf.currentTerm, rf.voteCount)
	}
}

type HeartBeatArgs struct {
	Term         int  // leader的任期
	LeaderId     int  // leader的ID
	CommitIndex int   // 日志提交的起始位置
}

type HeartBeatReply struct {
	Term    int
	Success bool
}

type AppendEntriesArgs struct {
	// Your data here (2A).
	// 如果term < currentTerm 返回 currentTerm，否则返回term
	// Vote         bool // 是否是心跳或者当选之后的信息
	Term         int  // leader的任期
	LeaderId     int  // leader的ID
	PrevLogIndex int  // 追加日志的其实Index
	PrevLogTerm  int  // 追加日志的任期
	Entries      LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	// 如果term < currentTerm 返回 currentTerm，否则返回term
	Term    int
	Success bool
}

func (rf *Raft) updateCommitIndex() {
	for !rf.killed() && rf.leader == rf.me {
		time.Sleep(5 * time.Microsecond)
		// rf.mu.Lock()
		for n := rf.lastLogIndex; n > rf.commitIndex; n-- {
			// if rf.log[n].Term != rf.currentTerm {
			// 	rf.commitIndex = n
			// 	break
			// }

			count := 0
			for i := range rf.peers {
				if rf.matchIndex[i] >= n {
					count++
				}
				if count > len(rf.peers) >> 1 {
					log.Printf("leader %d count is %d commitIndex is %d", rf.me, count, n)
					rf.commitIndex = n
					break
				}
			}
		}
		// rf.mu.Unlock()
	}
}


func (rf *Raft) checkCommit() {
	for !rf.killed() {
		time.Sleep(5 * time.Microsecond)
		// rf.mu.Lock()
		// defer rf.mu.Unlock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			// TODO 超出索引范围
			msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.log[rf.lastApplied].Index}
			rf.applyCh <- msg
			log.Printf("%d 在 term %d 往上层状态机成功发送 Msg %+v", rf.me, rf.currentTerm, msg)
		}
		// rf.mu.Unlock()
	}
}

func (rf *Raft) leaderSend() {
	args := HeartBeatArgs{rf.currentTerm, rf.me, rf.commitIndex}
	reply := HeartBeatReply{}
	for server := range rf.peers {
		rf.peers[server].Call("Raft.ReceiveHeartBeat", &args, &reply)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.changeState(follower)
		}
	}
}

func (rf *Raft) heartBeat() {
	for server := range rf.peers {
		if server != rf.me {
			if rf.matchIndex[server] < rf.lastLogIndex {
				args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.log[rf.matchIndex[server]].Index, rf.log[rf.matchIndex[server]].Term, rf.log[rf.nextIndex[server]], rf.commitIndex}
				log.Printf("heart at matchIndex %d send args %v", rf.matchIndex[server], args)
				rf.sendAppendEntries(server, args)
			} else {
				go func(server int) {
					args := HeartBeatArgs{rf.currentTerm, rf.me, rf.commitIndex}
					reply := HeartBeatReply{}
					rf.peers[server].Call("Raft.ReceiveHeartBeat", &args, &reply)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.changeState(follower)
					}
				}(server)
			}
		}
	}
}

func (rf *Raft) ReceiveHeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) {
	if rf.currentTerm <= args.Term {
		rf.lastHeartBeat = time.Now() // 更新leader心跳的时间
		rf.leader = args.LeaderId     // 更新leader的ID
		rf.votedFor = -1
		rf.voteCount = 0
		rf.changeState(follower)
		rf.currentTerm = args.Term
		rf.commitIndex = args.CommitIndex
		reply.Term = rf.currentTerm
		reply.Success = true
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
// 		return
}


// TODO 给每个服务器发送log是要leader自己控制还是交给此函数来控制？
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) bool {
	// rf.mu.Lock() 
	// defer rf.mu.Unlock()
	// TODO 在这里判断一下是否需要发送log

	go func(server int, args AppendEntriesArgs) {
		// rf.mu.Lock()
		length := len(rf.log)
		// logLength := len(rf.log)
		reply := AppendEntriesReply{}
		for {
			if rf.leader == rf.me {
				log.Printf("leader %d send server %d args: %v at term %d", rf.me, server, args, rf.currentTerm)
				rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				// log.Printf("call %d appendentries", server)

				if !reply.Success && reply.Term > rf.currentTerm {
					log.Printf("server %d 退下leader", rf.me)
					rf.currentTerm = reply.Term
					rf.changeState(follower)
					break
				} else if !reply.Success && length > 2 && reply.Term != 0 {
					log.Printf("server %d matchIndex后移", server)
					args.Entries = rf.log[length-2]
					args.PrevLogIndex = rf.log[length-3].Index
					args.PrevLogTerm = rf.log[length-3].Term
					args.Term = rf.currentTerm
					length--
				} else if reply.Success {
					rf.matchIndex[server] = args.Entries.Index
					rf.nextIndex[server] = args.Entries.Index + 1
					log.Printf("server %d append成功, matchIndex %d, nextIndex %d", server, rf.matchIndex[server], rf.nextIndex[server])
					break
				}
			} else {
				break
			}
			// rf.mu.Unlock()
		}
	}(server, args)
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	log.Printf("server %d receive args %v from leader %d at term %d, lastLogIndex %d, lastLogTerm %d", rf.me, args, args.LeaderId, rf.currentTerm, rf.lastLogIndex, rf.lastLogTerm)
	if args.Term < rf.currentTerm {
		log.Printf("rf term: %d > args term: %d", rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	} else if args.PrevLogIndex > rf.lastLogIndex {
		log.Printf("args.PrevLogIndex: %d, rf.lastLogIndex: %d, args.PrevLogTerm: %d, rf.lastLogTerm: %d", args.PrevLogIndex, rf.lastLogIndex, args.PrevLogTerm, rf.lastLogTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	} else {
		// 先判断是否需要删除log
		if args.PrevLogIndex < rf.lastLogIndex {
			rf.log = rf.log[:args.PrevLogIndex + 1]
			log.Printf("server %d logs is %v", rf.me, rf.log)
			rf.lastLogIndex = rf.log[len(rf.log) - 1].Index
			rf.lastLogTerm = rf.log[len(rf.log) - 1].Term
		}
		if rf.lastLogIndex == args.PrevLogIndex && rf.lastLogTerm == args.PrevLogTerm {
			log.Printf("args -> rf")
			rf.lastLogIndex = args.Entries.Index
			rf.lastLogTerm = args.Entries.Term
			rf.log = append(rf.log, args.Entries)
			rf.currentTerm = args.Term
			rf.commitIndex = args.LeaderCommitIndex
			reply.Term = rf.currentTerm
			reply.Success = true
			log.Printf("server %d logs are %v", rf.me, rf.log)
			rf.mu.Unlock()
			return
		}
		log.Printf("args.PrevLogIndex: %d, rf.lastLogIndex: %d, args.PrevLogTerm: %d, rf.lastLogTerm: %d", args.PrevLogIndex, rf.lastLogIndex, args.PrevLogTerm, rf.lastLogTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}
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

	rf.currentState = follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.lastLogTerm = 0
	rf.lastLogIndex = 0
	rf.applyCh = applyCh
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.log = make([]LogEntry, 1)
	rf.lastHeartBeat = time.Now()

	for index := range peers {
		rf.matchIndex[index] = 0
		rf.nextIndex[index] = 1
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 初始化之后立马进行选举
	rf.election()
	go rf.checkCommit()
	go rf.ticker()
	fmt.Print()
	log.Print()

	return rf
}
