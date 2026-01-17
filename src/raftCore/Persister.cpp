#include "Persister.h"
#include "util.h"

// 存raft和快照数据
void Persister::Save(const std::string raftstate, const std::string snapshot) {
    std::lock_guard<std::mutex> lg(m_mtx);
    // 清空现有的文件内容
    clearRaftStateAndSnapshot();
    // 将raftstate和snapshot写入本地文件
    m_raftStateOutStream << raftstate;
    m_snapshotOutStream << snapshot;
}

// 从硬盘文件中读取之前存好的“快照”数据。
std::string Persister::ReadSnapshot() {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_snapshotOutStream.is_open()) {
        m_snapshotOutStream.close();
    }

    DEFER {
        // 打开快照输出流
        m_snapshotOutStream.open(m_snapshotFileName);  //默认是追加
    };
    // 以读方式打开m_snapshotFileName
    std::fstream ifs(m_snapshotFileName, std::ios_base::in);
    if (!ifs.good()) {
        return "";
    }
    // 读取并回传数据
    std::string snapshot;
    ifs >> snapshot;
    ifs.close();
    return snapshot;
}

// 只存rfat状态
void Persister::SaveRaftState(const std::string &data) {
    std::lock_guard<std::mutex> lg(m_mtx);
    // 将raftstate和snapshot写入本地文件
    clearRaftState();
    m_raftStateOutStream << data;
    m_raftStateSize += data.size();
}

long long Persister::RaftStateSize() {
    std::lock_guard<std::mutex> lg(m_mtx);
    return m_raftStateSize;
}

std::string Persister::ReadRaftState() {
    std::lock_guard<std::mutex> lg(m_mtx);

    std::fstream ifs(m_raftStateFileName, std::ios_base::in);
    if (!ifs.good()) {
        return "";
    }
    std::string snapshot;
    ifs >> snapshot;
    ifs.close();
    return snapshot;
}

Persister::Persister(const int me)
    : m_raftStateFileName("raftstatePersist" + std::to_string(me) + ".txt"),
      m_snapshotFileName("snapshotPersist" + std::to_string(me) + ".txt"),
      m_raftStateSize(0) {

    bool fileOpenFlag = true;
    // out：我要往里面写。trunc：截断（Truncate）。如果这个文件已经存在，把它里面的内容全部清空，变成一个空文件。
    std::fstream file(m_raftStateFileName, std::ios::out | std::ios::trunc);
    // 立马关闭，只是为了这个文档存在，后面m_raftStateOutStream可以对其进行输入
    if (file.is_open()) {
        file.close();
    } else {
        fileOpenFlag = false;
    }
    file = std::fstream(m_snapshotFileName, std::ios::out | std::ios::trunc);
    if (file.is_open()) {
        file.close();
    } else {
        fileOpenFlag = false;
    }
    if (!fileOpenFlag) {
        DPrintf("[func-Persister::Persister] file open error");
    }
    m_raftStateOutStream.open(m_raftStateFileName);
    m_snapshotOutStream.open(m_snapshotFileName);
}

Persister::~Persister() {
    if (m_raftStateOutStream.is_open()) {
        m_raftStateOutStream.close();
    }
    if (m_snapshotOutStream.is_open()) {
        m_snapshotOutStream.close();
    }
}

void Persister::clearRaftState() {
    m_raftStateSize = 0;
    // 关闭文件流
    if (m_raftStateOutStream.is_open()) {
        m_raftStateOutStream.close();
    }
    // 重新打开文件流并清空文件内容
    m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearSnapshot() {
    if (m_snapshotOutStream.is_open()) {
        m_snapshotOutStream.close();
    }
    m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearRaftStateAndSnapshot() {
  clearRaftState();
  clearSnapshot();
}
