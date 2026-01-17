#ifndef RAFT_H
#define RAFT_H

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <boost/any.hpp>
#include <boost/serialization/serialization.hpp>

#include "ApplyMsg.h"
#include "Persister.h"
#include "config.h"
#include "monsoon.h"
#include "raftRpcUtil.h"
#include "util.h"
// 网络状态表示  todo：可以在rpc中删除该字段，实际生产中是用不到的.  Disconnected (0)：表示网络异常。
constexpr int Disconnected = 0;  // 方便网络分区的时候debug，网络异常的时候为disconnected，只要网络正常就为AppNormal，防止matchIndex[]数组异常减小
constexpr int AppNormal = 1;

// 投票状态
constexpr int Killed = 0; // 节点已被关闭或杀掉，不再参与投票。
constexpr int Voted = 1;   // 本轮已经投过票了
constexpr int Expire = 2;  // 投票（消息、竞选者）过期
constexpr int Normal = 3; // 正常的投票状态，通常代表投票成功或符合投票条件。

class Raft : public raftRpcProctoc::raftRpc {
private:
    // 1. 基础配置与通信 这部分变量定义了节点“是谁”以及“如何说话”：
    std::mutex m_mtx;
    std::vector<std::shared_ptr<RaftRpcUtil>> m_peers; // 一个列表，存储了指向集群中其他所有节点的 RPC 句柄。
    std::shared_ptr<Persister> m_persister; // 持久化管理器，负责将核心状态存入磁盘，防止宕机后“失忆”。
    int m_me; // 当前节点的编号（ID），用于在集群中标识自己。

    // 2. 核心状态 这是 Raft 协议最核心的三个持久化变量。只要它们变了，就必须存盘
    int m_currentTerm; // 当前任期。它是集群的逻辑时钟，每开启一轮选举，这个数字就会增加。
    int m_votedFor; // 记录当前任期内，我把票投给了哪个候选人。
    std::vector<raftRpcProctoc::LogEntry> m_logs;  // 日志数组。里面保存了所有待执行的指令和产生这些指令时的任期号
            
    // 3. 进度与角色管理 这些变量决定了节点在集群中的位置和任务进度：
    int m_commitIndex; // 进度条 A。已知已经被大多数节点确认、可以安全提交的最高日志索引。
    int m_lastApplied;  // 进度条 B。已经被应用到 KV 数据库中的最高日志索引。
    std::vector<int> m_nextIndex;  //（仅 Leader 维护）。记录 Leader 认为其他每个 Follower 应该接收哪条日志，以及它们已经同步到了哪条日志。
    std::vector<int> m_matchIndex;
    enum Status { Follower, Candidate, Leader };
    Status m_status; // 身份标识（Follower, Candidate, Leader）。

    // 4. 定时器与执行引擎
    // 与上层 KV 数据库沟通的管道。当日志被提交后，Raft 会把指令塞进这个管道，告诉 KV 数据库：“可以执行这个操作了
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan;  
    // 记录上一次重置选举计时器的时间点。如果现在离这个时间太久，节点就会发起选举。
    std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
    // 心跳超时，用于leader。记录上一次发送心跳的时间点。
    std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;

    // 2D中用于传入快照点
    // 储存了快照中的最后一个日志的Index和Term
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;

  // 协程
  std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

public:
    // 1. “驱动心跳”：Ticker 循环类
    // 选举计时器。Follower 在这里“睡觉”，如果闹钟响了还没收到 Leader 的消息，它就会发起 doElection()。
    void electionTimeOutTicker();
    void leaderHearBeatTicker(); // 心跳计时器。只有 Leader 会运行它，负责定时触发doHeartBeat() 向小弟们报平安。
    void applierTicker(); // 应用计时器。不断检查哪些日志已经达成共识（Committed），然后把它们通过管道发给 KVServer 执行。

    // 2. “核心协议”：选举与心跳逻辑类
    void doElection(); // Candidate 发起选举的动作。它会给自己投一票，并调用 sendRequestVote 向全网拉票。
    // 真正通过 RPC 向某个特定节点发送投票请求，并处理返回的选票结果。
    bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                        std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);
    void doHeartBeat(); // Leader 维护统治的手段。它会构造 AppendEntriesArgs 并调用 sendAppendEntries。
    // 通过 RPC 向某个 Follower 发送日志或心跳，并根据对方的反馈更新自己的 m_nextIndex 和 m_matchIndex
    bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums);
    
    // 3. “消息处理”：RPC 回调处理类
    // 处理别人发来的投票请求。它会判断对方的任期和日志是否比自己新，从而决定是否投出赞成票。
    void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);
    
    // 处理 Leader 发来的日志同步或心跳。它负责重置选举计时器，并核对日志是否一致。
    void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);
    // 处理来自 Leader 的快照数据。当 Follower 掉队太远时，Leader 直接发快照而不是一条条发日志。
    bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);
    
    // 4. “内部管理”：状态与日志辅助类
    int getLastLogIndex(); // 获取最后一条日志的坐标，这是判断日志“新旧程度”的标准。
    int getLastLogTerm();
    int getSlicesIndexFromLogIndex(int logIndex); // 就是我们之前讨论过的，把逻辑索引转换为 vector 物理下标的转换器。
    void persist(); // 将状态保存到磁盘或从磁盘恢复，防止“失忆”。
    void readPersist(std::string data); 
    bool matchLog(int logIndex, int logTerm); // 核对某个位置的日志是否与 Leader 发来的一致。
    
    // 5. “对外接口”：与上层 KVServer 交互类
    // 上层业务调用的入口。当客户端发来一个 Put 请求，KVServer 就会调用 Start 将其塞进 Raft 日志中。
    void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);
    void Snapshot(int index, std::string snapshot); // 业务层主动告诉 Raft：“我已经把前 1000 条日志做成快照存好了，你可以把它们删了”。

    // 6. 日志与状态查询（信息的“获取与计算”）
    int getLogTermFromLogIndex(int logIndex); // 根据逻辑索引获取对应的任期号。
    void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm); // 一次性获取最后一条日志的索引和任期。
    int getNewCommandIndex(); // 计算新指令应该分配的索引（通常是 lastLogIndex + 1）。
    // Leader 专用。计算要发给某个 Follower 的 AppendEntries RPC 中所需的“前一条日志”的信息。
    void getPrevLogInfo(int server, int *preIndex, int *preTerm);
    void GetState(int *term, bool *isLeader); // 对外（如给 KVServer）提供当前节点的任期和身份状态。
    bool UpToDate(int index, int term); // 选举安全检查。判断候选人的日志是否至少和自己一样新。
    
    // 7. 领导者的管理职责（Leader 的“日常工作”）
    void leaderUpdateCommitIndex(); // Leader 尝试推进 m_commitIndex。
    void leaderSendSnapShot(int server); // 当 Follower 掉队太远（其 nextIndex 已被快照覆盖）时，Leader 直接发送整个快照。

    // 8. 持久化与数据流动（系统的“记忆与传递”）
    std::string persistData(); // 将 Raft 的核心状态（Term, VotedFor, Snapshot元数据, Logs）序列化为字符串。
    int GetRaftStateSize(); // 返回当前持久化数据的字节大小。KVServer 会根据这个大小判断是否需要制作快照以减小日志体积。
    std::vector<ApplyMsg> getApplyLogs(); // 收集所有已经提交（Committed）但尚未应用（Applied）的日志。
    void pushMsgToKvServer(ApplyMsg msg); // 将消息推送到 applyChan 队列中。
    
    // 9. RPC 接口重写（与“外界”的通信桥梁）
    // 重写基类方法,因为rpc远程调用真正调用的是这个方法
    //序列化，反序列化等操作rpc框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可。
    void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request,
                      ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;
    void InstallSnapshot(google::protobuf::RpcController *controller,
                        const ::raftRpcProctoc::InstallSnapshotRequest *request,
                        ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;
    void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                    ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;

    // 当一个 Follower 掉队太远（比如断网太久），Leader 手里的日志已经通过快照清理掉了，
    // 无法通过普通的 AppendEntries（一条条补日志）来救它时，Leader 就会直接把整个快照发给它。
    void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                        raftRpcProctoc::InstallSnapshotResponse *reply);
 
    void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

private:
    // 它是 Raft 节点存盘时的“快照镜像”。
    // 在 Raft 协议中，为了保证数据安全，只要 Term（任期）、VotedFor（投给谁） 和 Logs（日志） 发生了变化，
    // 就必须立刻存盘。
    class BoostPersistRaftNode {
    public:
        friend class boost::serialization::access;
        
        /*
          保存时 (persistData)：创建一个 BoostPersistRaftNode 对象，把 Raft 的当前状态填进去，然后用 oa << 触发此函数，将数据写入字符串。
          恢复时 (readPersist)：先用 ia >> 触发此函数，从文件读回数据填满这个临时对象，然后再把值还给 Raft 节点的各个成员变量。
        */
        template <class Archive>
        void serialize(Archive &ar, const unsigned int version) {
          ar &m_currentTerm;
          ar &m_votedFor;
          ar &m_lastSnapshotIncludeIndex;
          ar &m_lastSnapshotIncludeTerm;
          ar &m_logs;
        }

        int m_currentTerm; // （任期）
        int m_votedFor; // 投标记录
        int m_lastSnapshotIncludeIndex; // 快照元数据
        int m_lastSnapshotIncludeTerm; // 快照元数据

        std::vector<std::string> m_logs;
        std::unordered_map<std::string, int> umap;
    };
};

#endif  // RAFT_H