#ifndef SKIP_LIST_ON_RAFT_KVSERVER_H
#define SKIP_LIST_ON_RAFT_KVSERVER_H

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <mutex>
#include <unordered_map>

#include "kvServerRPC.pb.h"
#include "raft.h"
#include "skipList.h"


/*
  客户端（Clerk）：下达 Put（存）、Get（取）指令。
  KvServer（本项目）：接收指令，它是 Raft 的上层应用。
  Raft 节点：负责在多个服务器之间同步这些指令，确保大家存的数据都一样。
*/

/*
  我们来模拟一次 Put("name", "Gemini") 的过程：
  客户端调用 KvServer 的 PutAppend 接口。
  KvServer 把这个操作包装成 Op，调用 m_raftNode->Start(op) 发送给 Raft。
  KvServer 在 waitApplyCh 里创建一个小管道，然后阻塞等待。
  Raft 在集群内部开会，半数以上同意了。
  Raft 把这个 Op 塞进 applyChan。
  ReadRaftApplyCommandLoop 发现了消息，把它取出来。
  KvServer 调用 ExecutePutOpOnKVDB，把 "Gemini" 存入 m_skipList。
  KvServer 通过 waitApplyCh 里的管道发个信号，告诉第 3 步里等待的线程：“好了，存上了！”
  KvServer 终于可以给客户端回信了：“存好了，老板！”
*/
class KvServer : raftKVRpcProctoc::kvServerRpc {
private:
    std::mutex m_mtx;
    int m_me; // 当前服务器的编号（比如 0 号机）。
    std::shared_ptr<Raft> m_raftNode;
    std::shared_ptr<LockQueue<ApplyMsg> > applyChan;  // kvServer和raft节点的通信管道
    int m_maxRaftState;                               // snapshot if log grows this big

    std::string m_serializedKVData; // 序列化后的二进制数据，专门用于制作快照（存档）。
    SkipList<std::string, std::string> m_skipList; // 跳表。它是真正存储 key-value 数据的结构，速度极快
    std::unordered_map<std::string, std::string> m_kvDB;

    // 等待队列映射表。当一个请求发给 Raft 后，KvServer 会在这里开个“小窗”等着。
    // 一旦 Raft 确认成功，就从这里通知对应的请求：“可以回复客户端了！”
    std::unordered_map<int, LockQueue<Op> *> waitApplyCh;
    // 去重表。记录每个客户端最后一次请求的 ID。防止因为网络波动，导致同一个“加 100 块”的操作被执行两次。
    std::unordered_map<std::string, int> m_lastRequestId;  // 一个kV服务器可能连接多个client
    int m_lastSnapShotRaftLogIndex; 

public:
    KvServer() = delete;
    KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

    void StartKVServer();
    void DprintfKVDB();

    // 作用：这些是真正的执行者。当 Raft 确认了指令后，调用这些函数去修改 m_skipList 里的数据。
    void ExecuteAppendOpOnKVDB(Op op);
    void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);
    void ExecutePutOpOnKVDB(Op op);

    void Get(const raftKVRpcProctoc::GetArgs *args,  raftKVRpcProctoc::GetReply *reply);  
    void GetCommandFromRaft(ApplyMsg message);
    bool ifRequestDuplicate(std::string ClientId, int RequestId);

    void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

    void ReadRaftApplyCommandLoop();
    void ReadSnapShotToInstall(std::string snapshot);
    bool SendMessageToWaitChan(const Op &op, int raftIndex);

    void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

    void GetSnapShotFromRaft(ApplyMsg message);
    std::string MakeSnapShot();

public: 
    void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                  ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) override;

    void Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
            ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) override;

private:
    friend class boost::serialization::access;

    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)  //这里面写需要序列话和反序列化的字段
    {
        ar &m_serializedKVData;
        ar &m_lastRequestId;
    }

    std::string getSnapshotData() {
        m_serializedKVData = m_skipList.dump_file();
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);
        oa << *this;
        m_serializedKVData.clear();
        return ss.str();
    }

    void parseFromString(const std::string &str) {
        std::stringstream ss(str);
        boost::archive::text_iarchive ia(ss);
        ia >> *this;
        m_skipList.load_file(m_serializedKVData);
        m_serializedKVData.clear();
    }
};

#endif  // SKIP_LIST_ON_RAFT_KVSERVER_H
