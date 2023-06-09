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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
import "sort"
// import "fmt"

// import "bytes"
// import "../labgob"

type LogEntry struct {
    LogTerm int
    Command interface{}
}


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int
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

    // Your data here (2A, 2B, 2C).
    timeoutInterval  time.Duration
    lastActiveTime   time.Time

    n                int          //number of servers
    role             MemberRole
    leaderId         int

    currentTerm      int
    votedFor         int
    logs             []*LogEntry

    commitIndex      int
    lastApplied      int

    nextIndex        []int
    matchIndex       []int
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

}

type MemberRole int

const (
    Leader    MemberRole = 1
    Follower  MemberRole = 2
    Candidate MemberRole = 3

    RoleNone = -1
    None     = 0
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    var term int
    var isleader bool
    // Your code here (2A).
    term = rf.currentTerm
    isleader = (rf.role == Leader)
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// The tester requires that the leader send heartbeat RPCs no more than ten times per second.
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term          int
    CandidateId   int 
    LastLogIndex  int
    LastLogTerm   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
    Term         int
    VoteGranted  bool
}


type AppendEntryArgs struct {
    // Your data here (2A, 2B).
    Term          int
    LeaderId      int
    PrevLogIndex  int
    PrevLogTerm   int 
    LogEntries    []*LogEntry
    LeaderCommit  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntryReply struct {
    // Your data here (2A).
    Term         int
    Success      bool

    TargetTerm   int
    TargetCnt    int
}

//
// example RequestVote RPC handler.
//
// Implement the RequestVote() RPC handler so that servers will vote for one another.
// follower: 判断term是否满足即可，满足的话提升term，并投票；不满足就不投票
// candidate: 判断term是否满足，满足就降级为follower，并投票，否则不投票
// leader: 直接不投票（×）
//
// 如果当前term小于requestvote的term，就一定变成follower并投票
// 如果这一轮已经投过票就不再投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // Your code here (2A, 2B).
    if rf.logs[len(rf.logs) - 1].LogTerm < args.LastLogTerm || (rf.logs[len(rf.logs) - 1].LogTerm == args.LastLogTerm && len(rf.logs)-1 <= args.LastLogIndex){
        if rf.currentTerm < args.Term {  //旧的term被修改为新的
            rf.currentTerm = args.Term
            rf.role = Follower
            rf.votedFor = args.CandidateId
            rf.lastActiveTime = time.Now()
            reply.Term = args.Term
            reply.VoteGranted = true
            Debug(dClient,"S%d , term%d: vote for node[%d]",rf.me,rf.currentTerm,rf.votedFor)
            return 
        }
        
        //这个部分好像没有必要？
        // if rf.currentTerm == args.Term && rf.votedFor == -1 || rf.votedFor == args.CandidateId {  //此时说明这一轮可以投票
        //     reply.Term = rf.currentTerm
        //     reply.VoteGranted = true
        //     rf.lastActiveTime = time.Now()
        //     rf.votedFor = args.CandidateId
        //     rf.currentTerm = args.Term
        //     Debug(dClient,"S%d  vote for node[%d]",rf.me,rf.votedFor)
        // } else {  //这一轮不能投票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		Debug(dClient,"S%d  term%d currentTerm大一些？%t rf.votedFor %d",rf.me,rf.currentTerm,rf.currentTerm > args.Term,rf.votedFor)
        // }
    }else{   //candidate不更up-to-date
        Debug(dClient,"S%d 当前candidate不更up-to-date",rf.me)
        reply.VoteGranted = false
        return
    }
    
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // Debug(dClient,"node[%d] term%d: Getting heartbeat",rf.me,rf.currentTerm)
    // Your code here (2A, 2B).
    Debug(dClient,"S%d 收到心跳!!!!",rf.me)

    if rf.currentTerm <= args.Term {  //旧的term被修改为新的
        if rf.role == Leader || rf.role == Candidate{
            Debug(dClient,"S%d  我变成follower了",rf.me)
        }
        rf.currentTerm = args.Term
        rf.role = Follower

        //log相关处理
        if args.PrevLogIndex >= len(rf.logs) {  //PrevLogIndex，PrevLogTerm的log是否匹配
            Debug(dClient,"S%d appendentry失败, len(rf.logs):%d args.PrevLogIndex:%d",rf.me,len(rf.logs),args.PrevLogIndex)
            reply.Success = false
            reply.TargetTerm = -1
            reply.TargetCnt = 0
        } else if rf.logs[args.PrevLogIndex].LogTerm != args.PrevLogTerm{
            Debug(dClient,"S%d appendentry失败",rf.me)
            reply.Success = false
            reply.TargetTerm = rf.logs[args.PrevLogIndex].LogTerm
            reply.TargetCnt = 0
            for j:=args.PrevLogIndex ;j>=0 && rf.logs[j].LogTerm!= reply.TargetTerm;j--{
                reply.TargetCnt++
            }
            rf.logs = rf.logs[:args.PrevLogIndex+1]
        } else {
            Debug(dClient,"S%d appendentry成功，args.LogEntries：%d",rf.me,len(args.LogEntries))
            reply.Success = true
            rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.LogEntries...)
            if len(args.LogEntries)>0{
                // Debug(dClient,"S%d log复制完毕，args.PrevLogIndex:%d-----LogEntries[0].LogTerm:%d-------------",rf.me,args.PrevLogIndex,args.LogEntries[0].LogTerm)
                logTerms := make([]int, len(rf.logs))
                for i, logEntry := range rf.logs {
                    logTerms[i] = logEntry.LogTerm
                }
                Debug(dClient,"S%d log terms: %v", rf.me, logTerms)
            }
            if args.LeaderCommit > rf.commitIndex{
                // rf.commitIndex =min(leaderCommit, len(rf.logs)-1)  //TODO:需要改为这个嘛？
                rf.commitIndex = args.LeaderCommit
            }
        }
    }else{  //term的问题
        reply.Success = false
    }

    rf.lastActiveTime = time.Now()
    rf.timeoutInterval = randElectionTimeout()
    reply.Term = rf.currentTerm
    
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


func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
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
    rf.mu.Lock()
    defer rf.mu.Unlock()

    index := -1
    term := -1
    isLeader := true

    // Your code here (2B).
    if rf.role != Leader{
        return -1, -1, false
    }

    rf.logs = append(rf.logs,&LogEntry{rf.currentTerm,command})
    index = len(rf.logs)-1
    term = rf.currentTerm

    Debug(dClient,"S%d term[%d] role[%v] 加一条log, logIndex[%d],len: %d", rf.me, rf.currentTerm, rf.role, index, len(rf.logs))
    return index, term, isLeader
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
    atomic.StoreInt32(&rf.dead, 1)
    // Your code here, if desired.
}

func (rf *Raft) killed() bool {
    z := atomic.LoadInt32(&rf.dead)
    return z == 1
}

const (
    ElectionTimeout = 250 * time.Millisecond
    HeartBeatTimeout = 150 * time.Millisecond
)

func randElectionTimeout() time.Duration {
    return ElectionTimeout + time.Duration(rand.Uint32())%ElectionTimeout
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
    rf.currentTerm = 0
    rf.votedFor = RoleNone
    rf.commitIndex = 0
    rf.lastApplied = 0
    rf.role = 2
    rf.lastActiveTime = time.Now()
    rf.timeoutInterval = randElectionTimeout()
    rf.n = len(peers)
    rf.leaderId = RoleNone
    rf.logs = make([]*LogEntry, 0)
    rf.nextIndex = make([]int, rf.n)
    rf.matchIndex = make([]int, rf.n)
    for i := 0; i < rf.n; i++ {
        rf.nextIndex[i] = 1
        rf.matchIndex[i] = 0
    }
    rf.logs=append(rf.logs,&LogEntry{LogTerm:0})
    // Your initialization code here (2A, 2B, 2C).
    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())
    //implement goroutines that will kick off leader election periodically 
    //by sending out RequestVote RPCs when it hasn't heard from another peer 
    //for a while. This way a peer will learn who is the leader, if there is 
    //already a leader, or become the leader itself.
    //
    //Make sure the election timeouts in different peers don't always fire at the 
    //same time, or else all peers will vote only for themselves and no one will become the leader.
    type RequestVoteResult struct {
        peerId int
        resp   *RequestVoteReply
    }
    go func(){  //election
        // Debug(dClient,"S%d election thread start",rf.me)
        // Debug(dClient,"S%d  %d nodes in total",rf.me,rf.n)
        // if rf.killed() == true {
        //  Debug(dClient,"S%d asdfsafasdfasfasfasfasdf---------------",rf.me)
        // }
        for rf.killed() == false {
            time.Sleep(time.Millisecond * 40)
            // Debug(dClient,"S%d fucking helllllllllllllllllllll",rf.me)
            
            // if rf.role == Leader {
            //  Debug(dClient,"S%d fucking helllllllllllllllllllll",rf.me)
            // }
            // Debug(dClient,"S%d fucking helllllllllllllllllllll",rf.me)

            if time.Now().Sub(rf.lastActiveTime) < rf.timeoutInterval {
                continue
            }

            func(){  //为了避免在执行这个函数的过程中发生因为接收到其它结点的信息而改变当前结点的state
                rf.mu.Lock()   //1
                defer rf.mu.Unlock()  //1

                if rf.role == Follower {
                    Debug(dClient,"S%d raft node[%d] becomes candidate, term%d",rf.me,rf.me,rf.currentTerm)
                    rf.role = Candidate
                    rf.votedFor = rf.me
                    // Debug(dClient,"S%d raft node[%d] becomes candidate, term%d",rf.me,rf.me,rf.currentTerm)
                }
                
                if rf.role == Candidate {    //有可能在里面变成follower
                    rf.currentTerm++
                    // rf.mu.Unlock()  //2
                    go func(){     //每过electiontimeout一定要发送一次requestvote而不是傻傻的等待上一个rpc结束
                        rf.mu.Lock()   //2
                        defer rf.mu.Unlock() //3
                        
                        Debug(dClient,"S%d 我是candidate,准备发送requestvote--------------",rf.me)
                        voteChan := make(chan *RequestVoteResult, rf.n-1)
                        term := rf.currentTerm //保存发送投票前的term

                        args := &RequestVoteArgs{
                            Term:                rf.currentTerm,
                            CandidateId:         rf.me,
                            LastLogIndex:        len(rf.logs) - 1,
                            LastLogTerm:         rf.logs[len(rf.logs) - 1].LogTerm,
                        }
                        for i:=0; i<rf.n; i++{
                            if i == rf.me{
                                continue
                            }

                            go func(server int, args *RequestVoteArgs, voteChan chan *RequestVoteResult){  //发送requestvote与处理接收的过程不能并发，因为共同修改了当前server
                                reply := &RequestVoteReply{}
                                Debug(dClient,"S%d term%d: sending requestvote to server[%d]",rf.me,rf.currentTerm,server)
                                
                                result := rf.sendRequestVote(server,args,reply)

                                rf.mu.Lock()  //3
                                defer rf.mu.Unlock()  //5
                                Debug(dClient,"S%d term%d: getting vote from node[%d] %t",rf.me,rf.currentTerm, server,result)
                                if(rf.currentTerm > term || rf.role == Leader || rf.role == Follower){  //有可能在等待返回的票时收到term更高的投票请求，导致candidate->follower
                                    voteChan <- &RequestVoteResult{
                                        peerId: server,
                                        resp:   nil,
                                    }
                                    return
                                }
    
                                if result == true{
                                    if reply.Term > rf.currentTerm{
                                        rf.currentTerm = reply.Term
                                        rf.votedFor = RoleNone
                                        rf.leaderId = RoleNone
                                    }
                                    voteChan <- &RequestVoteResult{   //协程安全，同时可能阻塞，不应该用锁包起来
                                        peerId: server,
                                        resp:   reply,
                                    }
                                } else {
                                    voteChan <- &RequestVoteResult{   //协程安全，同时可能阻塞，不应该用锁包起来
                                        peerId: server,
                                        resp:   nil,
                                    }
                                }
                            }(i,args,voteChan)
                        }
                        rf.mu.Unlock() //4
                        voteCount := 1
                        notvoteCount := 0
                        for i := 0; i < rf.n-1; i++ {
                            if rf.role == Leader || rf.role == Follower{
                                break
                            }
                            // Debug(dClient,"有本事就阻塞我")
                            if rf.currentTerm > term{
                                break
                            }
                            voteResult := <-voteChan  //这里应该不会发生永久阻塞？
                            if voteResult.resp == nil {
                                notvoteCount++;
                                if notvoteCount > rf.n/2 || rf.role == Leader || rf.role == Follower{  //丢失的报文超过半数了还等个毛毛
                                    Debug(dClient,"S%d 丢失的报文超过半数/role发生变化",rf.me)
                                    break
                                }
                                continue
                            }
                            Debug(dClient,"S%d getting vote message VoteGranted=%t",rf.me,voteResult.resp.VoteGranted)
                            if voteResult.resp.VoteGranted {
                                voteCount++;
                                Debug(dClient,"S%d  vote count for node[%d] is %d",rf.me,rf.me,voteCount)
                            }
                            if voteCount > rf.n/2 && rf.role == Candidate{
                                rf.leaderId = rf.me
                                rf.role = Leader
    
                                //初始化log相关内容
                                for j := 0; j < rf.n; j++ {
                                    rf.nextIndex[i] = len(rf.logs)
                                    rf.matchIndex[i] = 0
                                }

                                Debug(dClient,"S%d I become leader!!!!",rf.me)
                                break
                            } 
                        }
                        rf.mu.Lock() //4
                        // Debug(dClient,"S%d 投票部分执行完了，没阻塞",rf.me)
                        if rf.role != Leader { //只有不变成leader的时候才会改变lastActiveTime/timeoutInterval，否则心跳发送要等一个heartbeatinterval就很难顶
                            rf.timeoutInterval = randElectionTimeout()
                            rf.lastActiveTime = time.Now()
                            rf.votedFor = -1   //可恶啊，这里卡了半天
                            // rf.role = Follower  //这里有问题，不应该变回follower
                            Debug(dClient,"S%d term%d: 我从candidate变成follower了",rf.me,rf.currentTerm)
                        }
                        // rf.mu.Lock()  //5
                    }()
                }
            }()
        }
    }()
    
    //To implement heartbeats, define an AppendEntries RPC struct (though you 
    //may not need all the arguments yet), and have the leader send them out periodically. 
    //Write an AppendEntries RPC handler method that resets the election timeout so that 
    //other servers don't step forward as leaders when one has already been elected.
    //
    //The tester requires that the leader send heartbeat RPCs no more than ten times per second.
    go func(){    //heartbeat
        // Debug(dClient,"S%d heartbeat thread start",rf.me)
        for rf.killed() == false {
            time.Sleep(time.Millisecond * 40)

            if time.Now().Sub(rf.lastActiveTime) < HeartBeatTimeout {
                continue
            }

            func(){
                rf.mu.Lock()
                defer rf.mu.Unlock()
                if rf.role == Leader{
                    
                    Debug(dClient,"S%d 开始心跳!!!!  耗时：%d",rf.me, time.Now().Sub(rf.lastActiveTime)/time.Millisecond)

                    rf.lastActiveTime = time.Now()

                    for i:=0; i<rf.n; i++{
                        if rf.me == i{
                            rf.nextIndex[i]=len(rf.logs)
                            rf.matchIndex[i]=len(rf.logs)-1
                            continue
                        }
                        args := &AppendEntryArgs{
                            Term:                rf.currentTerm,
                            LeaderId:            rf.me,
                            PrevLogIndex:        rf.nextIndex[i]-1,
                            PrevLogTerm:         rf.logs[rf.nextIndex[i]-1].LogTerm,
                            LeaderCommit:        rf.commitIndex,     
                        }

                        args.LogEntries = make([]*LogEntry, 0)
                        // Debug(dClient,"S%d nextIndex[i]:%d,len(rf.logs):%d 000000000000000000000000",rf.me,rf.nextIndex[i],len(rf.logs))
                        args.LogEntries = append(args.LogEntries, rf.logs[rf.nextIndex[i]:len(rf.logs)]...)  //索引为0的槽位不用
                        // Debug(dClient,"S%d nextIndex[i]:%d,len(args.LogEntries):%d 000000000000000000000000",rf.me,rf.nextIndex[i],len(args.LogEntries))
                        go func(server int, args *AppendEntryArgs){

                            reply := &AppendEntryReply{}
                            result := rf.sendAppendEntry(server,args,reply)
                            rf.mu.Lock()
                            defer rf.mu.Unlock()
                            if rf.role == Follower || rf.role == Candidate{
                                Debug(dClient,"S%d Leader在发送AppendEntry途中变为Follower/Candidate",rf.me)
                                return
                            }
                            if reply.Term>rf.currentTerm{
                                rf.role = Follower
                                rf.votedFor = -1
                                rf.currentTerm = reply.Term
                                Debug(dClient,"S%d 我作为leader被某一个term更高的结点转化为follower",rf.me)
                                return
                            }
                            if result == false{
                                return
                            }
                            if reply.Success == false{
                                // if(rf.nextIndex[server]==1){
                                //     panic(fmt.Sprintf("结果为%d prevlogindex:%d PrevLogTerm:%d",rf.nextIndex[server],args.PrevLogIndex,args.PrevLogTerm))
                                // }
                                if rf.logs[args.PrevLogIndex].LogTerm < reply.TargetTerm{
                                    rf.nextIndex[server]-=reply.TargetCnt
                                } else{
                                    j:=args.PrevLogIndex
                                    for ;j>=0&&rf.logs[j].LogTerm == rf.logs[args.PrevLogIndex].LogTerm;j--{}
                                    rf.nextIndex[server]=j+1
                                }
                                return
                            }else if len(args.LogEntries)!=0{   //说明不是heartbeat

                                rf.nextIndex[server]=args.PrevLogIndex+len(args.LogEntries)+1
                                rf.matchIndex[server]=rf.nextIndex[server]-1  //TODO:这里就很怪，搞得matchindex好像没用
                                Debug(dClient,"S%d args.PrevLogIndex %d len(args.LogEntries) %d matchindex:%v",rf.me,args.PrevLogIndex,len(args.LogEntries),rf.matchIndex)
                                matchIndexSlice := make([]int, rf.n)
                                for index, matchIndex := range rf.matchIndex {
                                    matchIndexSlice[index] = matchIndex
                                }
                                sort.Slice(matchIndexSlice, func(i, j int) bool {
                                    return matchIndexSlice[i] < matchIndexSlice[j]
                                })
                                newCommitIndex := matchIndexSlice[rf.n/2]
                                //不能提交不属于当前term的日志
                                //TODO：这里有必要判断这一步吗？如果不满足这些条件，应该没办法被选为leader吧
                                // if newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex].LogTerm == rf.currentTerm {
                                if rf.commitIndex != newCommitIndex{
                                    Debug(dClient,"S%d 能够更改commitindex？matchindex:%v",rf.me,rf.matchIndex)
                                    Debug(dClient,"S%d role[%v] commitIndex %v update to newcommitIndex %v, command: %v", rf.me, rf.role, rf.commitIndex, newCommitIndex, rf.logs[newCommitIndex])
                                }
                                rf.commitIndex = newCommitIndex
                                // }
                            }

                            if reply.Term > rf.currentTerm{
                                rf.currentTerm = reply.Term
                                rf.votedFor = RoleNone
                                rf.leaderId = RoleNone
                                rf.role = Follower
                                // rf.persist()
                            }
                        }(i,args)
                        
                    }
                }
            }()
        }
    }()

    go func(applyCh chan ApplyMsg){   //apply the logs to application level
        for rf.killed() == false {
            time.Sleep(time.Millisecond * 40)
            if rf.lastApplied < rf.commitIndex{
                go func(){
                    for rf.lastApplied<rf.commitIndex{
                        rf.mu.Lock()
                        rf.lastApplied++
                        rf.mu.Unlock()
                        Debug(dClient,"S%d sending logs[%d] to application level", rf.me,rf.lastApplied)
                        applyCh<-ApplyMsg{true,rf.logs[rf.lastApplied].Command,rf.lastApplied}
                    }
                }()
            }
        }
    }(applyCh)
    
    Debug(dClient,"S%d starting raft node[%d]", rf.me,rf.me)
    return rf
}