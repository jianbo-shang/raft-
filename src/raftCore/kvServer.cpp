#include "kvServer.h"
#include <rpcprovider.h>
#include "mprpcconfig.h"

// 打印跳表
void KvServer::DprintfKVDB() {
    if (!Debug) {
        return;
    }
    std::lock_guard<std::mutex> lg(m_mtx);
    DEFER {
        m_skipList.display_list();
    };
}

// 把数据正式写入跳表，并记下这个客人的编号，防止他重复下单。
void KvServer::ExecuteAppendOpOnKVDB(Op op) {
    m_mtx.lock();
    m_skipList.insert_set_element(op.Key, op.Value);

    // 记录请求id，防止重复
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();
    DprintfKVDB(); // 打印跳表
}

// 从数据库（跳表）中查找客户想要的数据
void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist) {
    m_mtx.lock();
    *value = "";
    *exist = false;
    if (m_skipList.search_element(op.Key, *value)) {
        *exist = true;
    }
    // 记录请求id
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();
    if (*exist) {

    } else {

    }
    DprintfKVDB();
}

// 在数据库中执行“存放”操作（Put）。和ExecuteAppendOpOnKVDB不同的是若存在就更新
void KvServer::ExecutePutOpOnKVDB(Op op) {
    m_mtx.lock();
    m_skipList.insert_set_element(op.Key, op.Value);

    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();

    DprintfKVDB();
}

// 处理来自clerk的 Get RPC（包装需求、提交给 Raft 审批、坐下等结果。）
void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply) {
    // 包装请求
    Op op;
    op.Operation = "Get";
    op.Key = args->key();
    op.Value = "";
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();

    // Raft 给这个任务排了一个号（比如：这是第 100 号任务）。
    int raftIndex = -1;
    int _ = -1;
    bool isLeader = false;
    // 是把任务交给 Raft 去开会。
    m_raftNode->Start(op, &raftIndex, &_, &isLeader); 
    // 只有 Leader 才能发起提案
    if (!isLeader) {
        reply->set_err(ErrWrongLeader);
        return;
    }

    m_mtx.lock();
    // 没有则创建一个等待队列
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    // 取号
    auto chForRaftIndex = waitApplyCh[raftIndex];
    m_mtx.unlock();  //直接解锁，等待任务执行完成，不能一直拿锁等待
    Op raftCommitOp;

    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
        int _ = -1;
        bool isLeader = false;
        m_raftNode->GetState(&_, &isLeader);
        // 已做完且依然是leader则给结果
        if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKVDB(op, &value, &exist); // 去仓库（SkipList）拿货
            if (exist) {
                reply->set_err(OK); // 告诉客人：成功！
                reply->set_value(value);
            } else {
                reply->set_err(ErrNoKey); // 告诉客人：没这货
                reply->set_value("");
            }
          } else {
              reply->set_err(ErrWrongLeader);  // 返回这个，其实就是让clerk换一个节点重试
          }
    } else {
        // 核对身份
        if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if (exist) {
                reply->set_err(OK);
                reply->set_value(value);
            } else {
                reply->set_err(ErrNoKey);
                reply->set_value("");
            }
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }
    m_mtx.lock();
    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    delete tmp;
    m_mtx.unlock();
}

/*
  客户端请求 -> 进入 Get 函数。
  Get 函数 -> 提交给 Raft，并在 waitApplyCh 留了个“信箱”后开始睡觉。
  Raft 同步 -> 完成共识。
  GetCommandFromRaft（当前函数） -> 收到 Raft 消息，改完数据库，往“信箱”里塞入结果。
  Get 函数 -> 被唤醒，从“信箱”拿走结果，回复客户端。
*/
// 产生结果并分发结果
void KvServer::GetCommandFromRaft(ApplyMsg message) {
    Op op;
    // 反序列化
    op.parseFromString(message.Command);
    DPrintf(
        "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
        "Opreation {%s}, Key :{%s}, Value :{%s}",
        m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    // 太久了
    if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
        return;
    }
    // 如果是写操作。则进行写
    if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
        if (op.Operation == "Put") {
            ExecutePutOpOnKVDB(op);
        }
        if (op.Operation == "Append") {
            ExecuteAppendOpOnKVDB(op);
        }
    }
    // 检查日志是否太多了，多就进行快照
    if (m_maxRaftState != -1) {
        IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    }
    // “往专属信箱里塞结果”。
    SendMessageToWaitChan(op, message.CommandIndex);
}

// 它的作用是：识别出那些因为网络重传而导致的“重复订单”，并拦截它们，防止系统把一件事做两遍。
bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
        return false;
    }
    /*
      如果当前请求的 RequestId 小于等于 我们记录的编号，说明：这个活儿我以前干过了！（返回 true，表示是重复请求）。
      如果当前请求的 RequestId 大于 我们记录的编号，说明：这是一个新任务！（返回 false）。
    */
    return RequestId <= m_lastRequestId[ClientId];
}


void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
    Op op;
    op.Operation = args->op();
    op.Key = args->key();
    op.Value = args->value();
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();

    int raftIndex = -1;
    int _ = -1;
    bool isleader = false;
    m_raftNode->Start(op, &raftIndex, &_, &isleader);

    if (!isleader) {
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
            "not leader",
            m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
        reply->set_err(ErrWrongLeader); // 换一个节点
        return;
    }
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
        "leader ",
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
    m_mtx.lock();
    // 等待结果
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftIndex];
    m_mtx.unlock();  //直接解锁，等待任务执行完成，不能一直拿锁等待
    Op raftCommitOp;
    // 超时处理
    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
            "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
            m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

        if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
            reply->set_err(OK);  // 超时了,但因为是重复的请求，返回ok，实际上就算没有超时，在真正执行的时候也要判断是否重复
        } else {
            reply->set_err(ErrWrongLeader);  ///这里返回这个的目的让clerk重新尝试
        }
    } else {
      DPrintf(
          "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- Index:%d , "
          "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
          m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
      if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
          //可能发生leader的变更导致日志被覆盖，因此必须检查
          reply->set_err(OK);
      } else {
          reply->set_err(ErrWrongLeader);
      }
    }
    m_mtx.lock();
    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    delete tmp;
    m_mtx.unlock();
}

// “Raft 日志共识” 到 “KV 数据库状态机执行”
void KvServer::ReadRaftApplyCommandLoop() {
    while (true) {
        // 如果只操作applyChan不用拿锁，因为applyChan自己带锁
        auto message = applyChan->Pop();  // 阻塞弹出
        DPrintf(
            "-----tmp-----[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息", m_me);
        if (message.CommandValid) { // 普通操作指令 (CommandValid)
            GetCommandFromRaft(message);
        }
        if (message.SnapshotValid) { // 全量状态快照 (SnapshotValid)
            GetSnapShotFromRaft(message);
        }
    }
}

//  raft会与persist层交互，kvserver层也会，因为kvserver层开始的时候需要恢复kvdb的状态
//  关于快照raft层与persist的交互：保存kvserver传来的snapshot；生成leaderInstallSnapshot RPC的时候也需要读取snapshot；
//  因此snapshot的具体格式是由kvserver层来定的，raft只负责传递这个东西
//  snapShot里面包含kvserver需要维护的persist_lastRequestId 以及kvDB真正保存的数据persist_kvdb
void KvServer::ReadSnapShotToInstall(std::string snapshot) {
    if (snapshot.empty()) {
        return;
    }
    parseFromString(snapshot);
}

//  发送通知给等待队列拿消息
bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex) {
    std::lock_guard<std::mutex> lg(m_mtx);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        return false;
    }
    // 把消息丢给等待队列
    waitApplyCh[raftIndex]->Push(op);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    return true;
}

// 监控日志大小，并在合适的时候将其压缩成一个“快照”（Snapshot）。
void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
    // 若大于某个值则触发快照
    if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {
        // Send SnapShot Command
        auto snapshot = MakeSnapShot();
        m_raftNode->Snapshot(raftIndex, snapshot);
    }
}

// 掉队节点恢复同步数据
void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
        // 它会把 message.Snapshot 这个大字符串拆开，重新填满 m_skipList（跳表）并恢复 m_lastRequestId（办结记录）
        ReadSnapShotToInstall(message.Snapshot);
        m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
    }
}

// 制作快照
std::string KvServer::MakeSnapShot() {
    std::lock_guard<std::mutex> lg(m_mtx);
    std::string snapshotData = getSnapshotData();
    return snapshotData;
}

void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) {
    KvServer::PutAppend(request, response);
    done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) {
    KvServer::Get(request, response);
    done->Run();
}

KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port) : m_skipList(6) {
    std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);

    m_me = me;
    m_maxRaftState = maxraftstate;
    applyChan = std::make_shared<LockQueue<ApplyMsg> >();
    m_raftNode = std::make_shared<Raft>();

    std::thread t([this, port]() -> void {
        // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
        RpcProvider provider;
        provider.NotifyService(this); // 注册 KV 服务
        provider.NotifyService(this->m_raftNode.get());  // 注册 Raft 服务
        // 阻塞运行，等待别人调用
        provider.Run(m_me, port);
    });
    t.detach();

    std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
    sleep(6); // 这里的 sleep 是为了等待其他节点也都启动 RPC 服务
    std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;
    // 获取所有raft节点ip、port ，并进行连接  ,要排除自己
    MprpcConfig config;
    config.LoadConfigFile(nodeInforFileName.c_str());
    std::vector<std::pair<std::string, short> > ipPortVt;
    for (int i = 0; i < INT_MAX - 1; ++i) {
        std::string node = "node" + std::to_string(i);
        std::string nodeIp = config.Load(node + "ip");
        std::string nodePortStr = config.Load(node + "port");
        if (nodeIp.empty()) {
            break;
        }
        ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));  // 沒有atos方法，可以考慮自己实现
    }
    std::vector<std::shared_ptr<RaftRpcUtil> > servers;
    //进行连接
    for (int i = 0; i < ipPortVt.size(); ++i) {
        if (i == m_me) {
            servers.push_back(nullptr);
            continue;
        }
        std::string otherNodeIp = ipPortVt[i].first;
        short otherNodePort = ipPortVt[i].second;
        auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
        servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));

        std::cout << "node" << m_me << " 连接node" << i << "success!" << std::endl;
    }
    sleep(ipPortVt.size() - me);  // 等待所有节点相互连接成功，再启动raft
    m_raftNode->init(servers, m_me, persister, applyChan); // 正式启动 Raft

    m_skipList;
    waitApplyCh;
    m_lastRequestId;
    m_lastSnapShotRaftLogIndex = 0;  // 
    auto snapshot = persister->ReadSnapshot();
    if (!snapshot.empty()) {
        ReadSnapShotToInstall(snapshot);
    }
    std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this);  // 马上向其他节点宣告自己就是leader
    t2.join();  // 由於ReadRaftApplyCommandLoop一直不會結束，达到一直卡在这的目的
}
