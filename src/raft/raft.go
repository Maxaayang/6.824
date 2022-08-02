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

const heartTime = 200 // 定期发送心跳的时间

const (
	None      = -1
	leader    = 0
	candidate = 1
	follower  = 2
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

	commitIndex   int       // 最后一次提交的log的位置
	lastApplied   int       // 最近一次的log
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
	rf.currentTerm = args.Term
	reply.IntvoteGranted = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) disagreeVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// log.Printf("follower %d 在term %d 拒绝给 %d 投票", rf.me, rf.currentTerm, args.CondidateId)
	reply.IntvoteGranted = false
	reply.Term = rf.currentTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 一个任期只能投一票
	// 如果任期小于当前的任期，直接否决
	if args.Term < rf.currentTerm {
		rf.disagreeVote(args, reply)
		return
	} else if args.Term > rf.currentTerm {
		rf.agreeVote(args, reply)
		return
	} else if rf.votedFor == -1 {
		if args.LastLogTerm > rf.lastLogTerm {
			rf.agreeVote(args, reply)
			return
		} else if args.LastLogTerm < rf.lastLogTerm {
			rf.disagreeVote(args, reply)
			return
		} else {
			if args.LastLogIndex < rf.lastApplied {
				rf.disagreeVote(args, reply)
				return
			} else {
				rf.agreeVote(args, reply)
				return
			}
		}
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
	if rf.leader == rf.me {
		rf.lastHeartBeat = time.Now()
		index := rf.lastApplied + 1
		//if len(rf.log) == 0 {
		//	index = 0
		//} else {
		//	index = rf.log[len(rf.log)-1].Index + 1
		//}

		appendLog := LogEntry{Command: command, Term: rf.currentTerm, Index: index}
		log.Printf("log is %v", appendLog)

		args := AppendEntriesArgs{false, rf.currentTerm, rf.me, rf.lastApplied, rf.lastLogTerm, appendLog}
		// fmt.Println("entries args is", args)

		// rf.sendAppendEntries(args)
		// TODO
		rf.log = append(rf.log, appendLog)
		rf.lastApplied = index
		rf.lastLogTerm = rf.currentTerm
		rf.sendAppendEntries(args)
		log.Printf("leader %d logs are %v", rf.me, rf.log)
		// log.Printf("leader %d send prevLogIndex is %d , the prevLogTerm is %d", rf.me, rf.lastApplied, rf.lastLogTerm)

		return index, rf.currentTerm, rf.leader == rf.me

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
		i := 350 + rand.Intn(150)
		// fmt.Println("sleep: ", i)
		time.Sleep(time.Duration(i) * time.Millisecond)
		if lastHeartBeat != rf.getLastHeartBeat() {
			continue
		}
		rf.election()

	}
}

func (rf *Raft) election() {
	// log.Printf("server %d 在term %d 请求投票", rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.changeState(candidate)
	rf.voteCount++
	// fmt.Println("voteCount: ", rf.voteCount)
	rf.votedFor = rf.me
	rf.currentTerm++
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, rf.lastLogTerm}
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
		rf.changeState(follower)
		rf.voteCount = 0
		rf.votedFor = -1
	}
}

func (rf *Raft) sendHeartBeat() {
	args := AppendEntriesArgs{true, rf.currentTerm, rf.me, rf.commitIndex, rf.currentTerm, LogEntry{}}
	for {
		time.Sleep(heartTime * time.Microsecond)
		if !rf.killed() && rf.currentState == leader {
			if time.Since(rf.getLastHeartBeat()) > time.Duration(heartTime)*time.Microsecond {
				rf.sendAppendEntries(args)
			}
		} else {
			break
		}
	}
}

func (rf *Raft) handleRequse(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("server %d 的voteCount: %d", rf.me, rf.voteCount)
	if rf.voteCount == 0 {
		// fmt.Println("rf.voteCount == 0")
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.changeState(follower)
		// fmt.Println("rf.currentTerm < reply.Term")
		rf.voteCount = 0
		rf.votedFor = -1
	} else if reply.IntvoteGranted {
		rf.voteCount++
		// fmt.Println("voteCount: ", rf.voteCount)
		// 如果已经达到了多数，就直接变为leader, 并将票数归零
		if rf.voteCount > (len(rf.peers)>>1) && rf.currentState == candidate {
			rf.changeState(leader)
			rf.leader = rf.me
			rf.voteCount = 0
			rf.votedFor = -1
			args := AppendEntriesArgs{true, rf.currentTerm, rf.me, rf.commitIndex, rf.currentTerm, LogEntry{}}
			// reply := AppendEntriesReply
			rf.sendAppendEntries(args)
			log.Printf("server %d 在term %d 当选为leader", rf.me, rf.currentTerm)
			// 开始定期给follower发送心跳
			go rf.sendHeartBeat()
		}
	}
}

type AppendEntriesArgs struct {
	// Your data here (2A).
	// 如果term < currentTerm 返回 currentTerm，否则返回term
	Vote         bool // 是否是心跳或者当选之后的信息
	Term         int  // leader的任期
	LeaderId     int  // leader的ID
	PrevLogIndex int  // 追加日志的其实Index
	PrevLogTerm  int  // 追加日志的任期
	Entries      LogEntry
	// LeaderCommit bool
}

type AppendEntriesReply struct {
	// Your data here (2A).
	// 如果term < currentTerm 返回 currentTerm，否则返回term
	Term    int
	Success bool
}

type LogCommitArgs struct {
	Commit bool
	Index int
}

type LogCommitReply struct {
	Commit bool
}

func (rf *Raft) checkCommit(commits *int) {
	for {
		if *commits >= len(rf.peers) >> 1 {
			args := LogCommitArgs{Commit: true, Index: rf.lastApplied}
			for server := range rf.peers {
				go func(server int, args *LogCommitArgs) {
					rf.peers[server].Call("Raft.CommitLog", args, &LogCommitReply{})
					// log.Printf("call server %d to commit", server)
				}(server, &args)
			}
			break
		}
	}
}

func (rf *Raft) CommitLog(args *LogCommitArgs, reply *LogCommitReply) {
	log.Printf("server %d receive commit", rf.me)
	if args.Commit {
		for {
			// rf.mu.Lock()
			// defer rf.mu.Unlock()
			if rf.commitIndex < args.Index && len(rf.log) > args.Index {
				log.Printf("server %d log length is %d, commit index is %d, args index is %d", rf.me, len(rf.log), rf.commitIndex, args.Index)
				// TODO out of range
				msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex + 1].Command, CommandIndex: rf.log[rf.commitIndex + 1].Index}
				rf.applyCh <- msg
				// log.Printf("%d 在 term %d 往上层状态机成功发送 Msg %+v", rf.me, rf.currentTerm, msg)
				rf.commitIndex++
			} else {
				break
			}
		}
	}
}

// TODO 给每个服务器发送log是要leader自己控制还是交给此函数来控制？
func (rf *Raft) sendAppendEntries(args AppendEntriesArgs) bool {
	if args.Vote {
		for server := range rf.peers {
			if server != rf.me {
				go func(server int, args AppendEntriesArgs) {
					// log.Printf("server %d send heart to follower %d at term %d", rf.me, server, rf.currentTerm)
					reply := AppendEntriesReply{}
					rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
					if reply.Term > rf.currentTerm {
						rf.changeState(follower)
					}
				}(server, args)
			}
		}
		return true
	}

	commits := 0
	// log.Printf("commits: %d", commits)
	for server := range rf.peers {
		if server != rf.me {
			// log.Printf("server is: %d", server)
			go func(server int, args AppendEntriesArgs, commits *int) {
				// log.Printf("server is: %d", server)
				rf.matchIndex[server] = args.PrevLogIndex
				// log.Printf("PrevLogIndex: %d", args.PrevLogIndex)
				rf.nextIndex[server] = args.Entries.Index
				length := len(rf.log)
				logLength := len(rf.log)
				reply := AppendEntriesReply{}
				for {
					// log.Printf("send server %d args: %v", server, args)
					rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
					// log.Printf("call %d appendentries", server)
					// 这里得区分是不是复制的同一个日志
					if reply.Success && length == logLength{
						rf.matchIndex[server] = rf.lastApplied
						rf.nextIndex[server] = rf.lastApplied + 1
						*commits++
						break
					} else if reply.Success {
						length++
						rf.matchIndex[server]++
						rf.nextIndex[server]++
						args.Entries = rf.log[length - 1]
						args.PrevLogIndex = rf.log[length - 2].Index
						args.PrevLogTerm = rf.log[length - 2].Term
						// length++
					} else if length > 2 && reply.Term != 0 {
						rf.matchIndex[server]--
						rf.nextIndex[server]--
						args.Entries = rf.log[length-2]
						args.PrevLogIndex = rf.log[length-3].Index
						args.PrevLogTerm = rf.log[length-3].Term
						length--
					}
				}
			}(server, args, &commits)
		}
	}
	go rf.checkCommit(&commits)
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Vote {
		if rf.currentTerm > args.Term {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		} else {
			// log.Printf("follower %d recive heart from leader %d at term %d", rf.me, args.LeaderId, rf.currentTerm)
			rf.lastHeartBeat = time.Now() // 更新leader心跳的时间
			rf.leader = args.LeaderId     // 更新leader的ID
			rf.votedFor = -1
			rf.voteCount = 0
			rf.changeState(follower)
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.Success = true
			return
		}
	}
	// log.Printf("server %d receive args %v from leader %d", rf.me, args, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.PrevLogIndex > rf.lastApplied || args.PrevLogTerm > rf.lastLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		// rf.lastApplied = args.Entries.Index
		// TODO 这里如果leader的log短了的话需要进行覆盖
		if args.PrevLogIndex < rf.lastApplied {
			rf.log[args.Entries.Index].Command = args.Entries.Command
			rf.log[args.Entries.Index].Term = args.Entries.Term
		} else {
			rf.lastApplied++
			rf.lastLogTerm = args.Term
			rf.log = append(rf.log, args.Entries)
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		log.Printf("server %d logs are %v", rf.me, rf.log)
		return
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
	rf.applyCh = applyCh
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.log = make([]LogEntry, 1)
	rf.lastHeartBeat = time.Now()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 初始化之后立马进行选举
	rf.election()
	go rf.ticker()
	fmt.Print()
	log.Print()

	return rf
}
