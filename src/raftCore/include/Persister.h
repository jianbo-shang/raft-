#ifndef SKIP_LIST_ON_RAFT_PERSISTER_H
#define SKIP_LIST_ON_RAFT_PERSISTER_H
#include <fstream>
#include <mutex>

// 负责将 Raft 的核心状态和数据库快照持久化到磁盘上。
class Persister {
private:
    std::mutex m_mtx;
    // Raft 状态的内存缓存。存储当前的 Term（任期）、VotedFor（投给谁了）和 Logs（日志）。
    std::string m_raftState;
    // 状态机快照的内存缓存。
    std::string m_snapshot;

    // Raft 状态的文件路径
    const std::string m_raftStateFileName;
    // 快照的文件路径
    const std::string m_snapshotFileName;
    // 输出流
    std::ofstream m_raftStateOutStream;
    std::ofstream m_snapshotOutStream;
    // 状态文件大小
    long long m_raftStateSize;

public:
    // 原子级地同时保存 Raft 状态和快照
    void Save(std::string raftstate, std::string snapshot);
    // 仅保存raft状态
    void SaveRaftState(const std::string& data);

    // 这是**“复活”**逻辑。当节点重启执行构造函数时，
    // 它会通过这两个方法从磁盘加载数据，让 SkipList 恢复到崩溃前的样子
    std::string ReadRaftState();
    std::string ReadSnapshot();

    // 获取当前日志状态的字节数。
    long long RaftStateSize();

    explicit Persister(int me);
    ~Persister();

private:
    // 清空内存中的 m_raftState 字符串，并将磁盘文件截断（清空内容）。
    void clearRaftState();
    void clearSnapshot();
    void clearRaftStateAndSnapshot();
};

#endif  // SKIP_LIST_ON_RAFT_PERSISTER_H
