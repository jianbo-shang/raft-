//
// Created by swx on 23-6-4.
//
#ifndef SKIP_LIST_ON_RAFT_CLERK_H
#define SKIP_LIST_ON_RAFT_CLERK_H
#include <arpa/inet.h>
#include <netinet/in.h>
#include <raftServerRpcUtil.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include <vector>

#include "kvServerRPC.pb.h"
#include "mprpcconfig.h"
class Clerk {
private:
    // 保存所有raft节点的fd //todo：全部初始化为-1，表示没有连接上
    std::vector<std::shared_ptr<raftServerRpcUtil>> m_servers;  
    // 每个客户端一个唯一 ID，用来做 请求去重
    std::string m_clientId;
    // 客户端对每次操作递增一个序号 组合起来 (clientId, requestId) 唯一标识一次操作 写操作（Put/Append）尤其需要这个，防止重试导致重复写。
    int m_requestId;
    // 这是一个 性能优化：客户端优先把请求发给“上一次猜测的 leader”
    int m_recentLeaderId;  //只是有可能是领导

    // 用于返回随机的clientId
    std::string Uuid() {
        return std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand());
    }
    //    MakeClerk  todo
    void PutAppend(std::string key, std::string value, std::string op);

public:
    // 读取 mprpcconfig.h 配置，初始化 m_servers，并生成 m_clientId、重置 m_requestId、设置初始 m_recentLeaderId 等。
    void Init(std::string configFileName);
    // 
    std::string Get(std::string key);
    // 
    void Put(std::string key, std::string value);
    // 
    void Append(std::string key, std::string value);

public:
    Clerk();
};

#endif  // SKIP_LIST_ON_RAFT_CLERK_H
