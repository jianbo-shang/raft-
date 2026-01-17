#include "raftRpcUtil.h"

#include <mprpcchannel.h>
#include <mprpccontroller.h>

/*
  （附加日志条目）
  1. 维持心跳 (Heartbeat) —— “我还在位”
  2. 日志复制 (Log Replication) —— “同步数据”
  3. 一致性检查与修复 —— “对齐历史”
  4. 推进提交进度 (Commit Index) —— “执行指令”
*/
bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response) {
    MprpcController controller;
    stub_->AppendEntries(&controller, args, response, nullptr);
    return !controller.Failed();
}

bool RaftRpcUtil::InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
                                  raftRpcProctoc::InstallSnapshotResponse *response) {
    MprpcController controller;
    stub_->InstallSnapshot(&controller, args, response, nullptr);
    return !controller.Failed();
}

bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response) {
    MprpcController controller;
    stub_->RequestVote(&controller, args, response, nullptr);
    return !controller.Failed();
}

//先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动
RaftRpcUtil::RaftRpcUtil(std::string ip, short port) {
    // 发送rpc设置
    stub_ = new raftRpcProctoc::raftRpc_Stub(new MprpcChannel(ip, port, true));
}

RaftRpcUtil::~RaftRpcUtil() { delete stub_; }
