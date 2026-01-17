#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "util.h"

// 超时触发，选举开始
void Raft::electionTimeOutTicker() {
  // Check if a Leader election should be started.
  while (true) {
      // 当前是leader，就没必要计算后面选举的一些东西
      while (m_status == Leader) {
          // 通过 usleep(HeartBeatTimeout)，该线程（或协程）会进入睡眠状态，释放 CPU 资源，直到下一次检查。
          usleep(HeartBeatTimeout);
      }
      // std::ratio<1, 1000000000> 代表 $1/10^9$ 秒，也就是 1 纳秒。
      std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
      // 一个“时间点” 它代表一个具体的**“时间刻度”**（类似于说“现在是 14:00:00”），而不是一段时长。
      std::chrono::system_clock::time_point wakeTime{};
      {
          m_mtx.lock();
          wakeTime = now();
          // 睡眠时间 = (随机超时时间 + 上次重置时间) - 当前时间
          // 这表示从上次收到心跳开始，往后数一段随机时间。这就是你应该“发作”发起选举的最晚时刻。
          suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
          m_mtx.unlock();
      }

      // 这是一种性能优化手段。它告诉程序：如果剩下的时间太短，就别折腾去睡觉了，直接准备干活吧！
      // 如果剩余时间不到 1 毫秒，说明选举计时器几乎已经到了触发点。与其花时间去睡眠再醒来，
      // 不如直接进入后续的逻辑处理（检查是否真的超时并触发选举）。
      if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
          // 获取当前时间点
          auto start = std::chrono::steady_clock::now();
          // 将当前协程/线程挂起，单位是微秒（$10^{-6}$ 秒）
          usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

          // 获取函数运行结束后的时间点
          auto end = std::chrono::steady_clock::now();

          // 计算时间差并输出结果（单位为毫秒）
          std::chrono::duration<double, std::milli> duration = end - start;

          // 使用ANSI控制序列将输出颜色修改为紫色
          std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                    << std::endl;
          std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                    << std::endl;
      }
      // 如果发现 m_lastResetElectionTime（最新心跳时间）比你开始计算睡眠的时间点（wakeTime）还要新，
      // 说明 Leader 还活着，只是心跳刚好在你计算时赶到了。此时节点会通过 continue 放弃本次选举，重新开始计时。
      if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0) {
          //说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
          continue;
      }
      doElection();
  }
}

// 选举，发送自己的信息投票
/*
  变身：Follower -> Candidate。
  更新：任期 +1，投自己一票，保存到磁盘。
  准备：获取自己最新的日志信息。
  拉票：并发地（开多个线程）向所有小伙伴发送“求票”信息，然后自己不等结果，直接结束函数，把剩下的事交给后台线程处理。
*/
void Raft::doElection() {
    std::lock_guard<std::mutex> g(m_mtx);

    if (m_status == Leader) {
        // fmt.Printf("[       ticker-func-rf(%v)              ] is a Leader,wait the  lock\n", rf.me)
    }
    if (m_status != Leader) {
        DPrintf("[ ticker-func-rf(%d) ]  选举定时器到期且不是leader，开始选举 \n", m_me);
        //当选举的时候定时器超时就必须重新选举，不然没有选票就会一直卡主
        //重竞选超时，term也会增加的
        m_status = Candidate;
        ///开始新一轮的选举
        m_currentTerm += 1;
        m_votedFor = m_me;  //即是自己给自己投，也避免candidate给同辈的candidate投
        persist(); // 持久化，存入磁盘
        std::shared_ptr<int> votedNum = std::make_shared<int>(1);  // 使用 make_shared 函数初始化 !! 亮点
        //	重新设置定时器
        m_lastResetElectionTime = now();
        //	发布RequestVote RPC  向所有节点发拉票广播
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                continue;
            }
            // Raft 规定，只有日志“足够新”的节点才能当选。
            // 你需要告诉别人你最后一条日志的索引（Index）和任期（Term），方便别人判断是否投给你。
            int lastLogIndex = -1, lastLogTerm = -1;
            getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  //获取最后一个log的term和下标

            std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs = std::make_shared<raftRpcProctoc::RequestVoteArgs>();
            requestVoteArgs->set_term(m_currentTerm);     // 我的当前任期
            requestVoteArgs->set_candidateid(m_me);      // 我是谁
            requestVoteArgs->set_lastlogindex(lastLogIndex); // 我最后一条日志的索引
            requestVoteArgs->set_lastlogterm(lastLogTerm);   // 我最后一条日志的任期

            auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

            // 使用匿名函数执行避免其拿到锁
            // 创建新线程并执行b函数，并传递参数
            std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply, votedNum);
            // 将新创建的线程与主线程脱离，让它在后台独立运行。
            t.detach();
        }
    }
}

// 获取选举结果
/*
  等回信（RPC）。
  比任期（认怂或继续）。
  数人头（过半原则）。
  理家产（初始化 nextIndex）。
  发通告（立即心跳）。
*/
bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {
    //这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
    // ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    auto start = now();
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 发送 RequestVote 开始", m_me, m_currentTerm, getLastLogIndex());
    
    bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
    
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 发送 RequestVote 完成，耗时:{%d} ms", m_me, m_currentTerm,
            getLastLogIndex(), now() - start);

    if (!ok) {
        return ok;  // 如果 RPC 返回 false，说明网络不通或者目标服务器宕机了。
    }

    std::lock_guard<std::mutex> lg(m_mtx);
    // 情况 A：对方任期比我高
    if (reply->term() > m_currentTerm) {
        m_status = Follower;  //三变：身份，term，和投票
        m_currentTerm = reply->term();
        m_votedFor = -1;
        persist(); // 存盘
        return true;
    } else if (reply->term() < m_currentTerm) { // 情况 B：对方任期比我小
        return true; // 忽略这个过时的回复
    }

    myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));
  
    // 对方没把票投给我
    if (!reply->votegranted()) {
        return true;
    }
    // 投票数 + 1
    *votedNum = *votedNum + 1;
    if (*votedNum >= m_peers.size() / 2 + 1) { // 获得大多数票
        //变成leader
        *votedNum = 0;
        if (m_status == Leader) {
          //如果已经是leader了，那么是就是了，不会进行下一步处理了
            myAssert(false, format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me, m_currentTerm));
        }
        //	第一次变成leader，初始化状态和nextIndex、matchIndex
        m_status = Leader;

        DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm, getLastLogIndex());

        // Leader 记录“我要发给每个 Follower 的下一条日志从哪开始”。
        // 刚当选时，Leader 很乐观，认为大家都跟自己一样新，所以设为自己的末尾。
        int lastLogIndex = getLastLogIndex();
        for (int i = 0; i < m_nextIndex.size(); i++) {
            m_nextIndex[i] = lastLogIndex + 1;  // 有效下标从1开始，因此要+1
            m_matchIndex[i] = 0;                // 每换一个领导都是从0开始，见fig2
        }

        std::thread t(&Raft::doHeartBeat, this);  // 马上向其他节点宣告自己就是leader
        t.detach();
        persist(); // 持久化 存入磁盘
    }
    return true;
}

// 接收者处理投票
/*
  任期小？ -> 拒绝（Expire）
  任期大？ -> 更新自己，继续往下走。
  日志旧？ -> 拒绝（不合格）
  已投别人？ -> 拒绝（Voted）
  没投或投过它？ -> 成功（Normal + Grant）
*/
void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply) {
    std::lock_guard<std::mutex> lg(m_mtx);

    DEFER {
        // 应该先持久化，再撤销lock
        persist();
    };

    // 任期对比（拒绝过时的候选人）
    if (args->term() < m_currentTerm) {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Expire); // 标记为：你过时了
        reply->set_votegranted(false);
        return;
    }
    // fig2:右下角，如果任何时候rpc请求或者响应的term大于自己的term，更新term，并变成follower
    if (args->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1;
        // 重置定时器：收到leader的ae，开始选举，投出票
        // 这时候更新了term之后，votedFor也要置为-1
    }
    myAssert(args->term() == m_currentTerm, format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));
   
    int lastLogTerm = getLastLogTerm();
    //只有没投票，且candidate的日志的新的程度 ≥ 接受者的日志新的程度 才会授票
    /*
      UpToDate 的判断准则：
      如果对方最后一条日志的任期号更大，对方更新。
      如果任期号相同，但对方的日志更长（Index更大），对方更新。
    */
    if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
      reply->set_term(m_currentTerm);
      reply->set_votestate(Voted);
      reply->set_votegranted(false);
      return;
    }
    
    // 投票记录检查（一人一票）
    // 如果 m_votedFor 不是 -1，且不是投给当前这个 Candidate，说明我已经支持别人了。在同一个任期内，我不能反悔。
    if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted); // 已经投给别人了
        reply->set_votegranted(false);
        return;
    } else { // 授予选票
        m_votedFor = args->candidateid(); // 登记投票
        m_lastResetElectionTime = now();  // 认为必须要在投出票的时候才重置定时器，
        reply->set_term(m_currentTerm);
        reply->set_votestate(Normal);
        reply->set_votegranted(true); // 正式授予！
        return;
    }
}

// 心跳连接
/*
  分流：你是该补快照，还是补日志？
  打包：根据每个 Follower 的进度（m_nextIndex），从日记本（m_logs）里裁下对应的一段。
  宣示：无论有没有新日志，都通过 leaderCommit 告诉大家现在的全局进度
*/
void Raft::doHeartBeat() {
    std::lock_guard<std::mutex> g(m_mtx);

    if (m_status == Leader) {
        DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
        auto appendNums = std::make_shared<int>(1);  // 正确返回的节点的数量 ，初始化1是自己的

        // 对Follower（除了自己外的所有节点发送AE）
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                continue;
            }
            DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
            myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
            //  该节点掉线太久，直接把快照发给他，因为有大量的io，所以开新线程
            if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
                std::thread t(&Raft::leaderSendSnapShot, this, i);  // 创建新线程并执行b函数，并传递参数
                t.detach();
                continue;
            }
            // 准备正常的日志
            int preLogIndex = -1;
            int PrevLogTerm = -1;
            getPrevLogInfo(i, &preLogIndex, &PrevLogTerm); // 获取“上一条”日志的信息

            // 追加日志函数
            std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs = std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
            appendEntriesArgs->set_term(m_currentTerm);
            appendEntriesArgs->set_leaderid(m_me);
            appendEntriesArgs->set_prevlogindex(preLogIndex);
            appendEntriesArgs->set_prevlogterm(PrevLogTerm);
            appendEntriesArgs->clear_entries();
            appendEntriesArgs->set_leadercommit(m_commitIndex); // 告诉大家：我已经提交到哪了

            // 第一种情况：preLogIndex 在内存日志中
            if (preLogIndex != m_lastSnapshotIncludeIndex) {
                // getSlicesIndexFromLogIndex(preLogIndex)：这个函数非常关键！它的作用是：
                // “告诉我在逻辑索引为 preLogIndex 的日志，在我的 m_logs 数组里到底是第几个元素。”
                for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {
                    raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
                    *sendEntryPtr = m_logs[j];
                }
            } else { // preLogIndex 恰好是快照边界
                for (const auto& item : m_logs) {
                    // 把所有的日志都给follower，因为刚好是边界，
                    raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
                    *sendEntryPtr = item;
                  }
            }
            int lastLogIndex = getLastLogIndex();
            // leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
            myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
                    format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                            appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
            
            // 构造返回值
            const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
                std::make_shared<raftRpcProctoc::AppendEntriesReply>();
            appendEntriesReply->set_appstate(Disconnected); // 默认disconnected

            std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply,
                          appendNums);  // 创建新线程并执行b函数，并传递参数
            t.detach();
        }
        m_lastResetHearBeatTime = now();  // 一轮广播发完后，重置心跳计时器
    }
}

/*
  权威高于一切：高 Term 具有绝对压制力。
  一致性检查：必须保证 prevLogIndex 和 prevLogTerm 完全匹配，才允许追加日志。
  安全更新：通过逐条核对而非暴力覆盖，应对复杂的网络延迟环境。
*/
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply) {
    // 一审身份（Term 检查）
    std::lock_guard<std::mutex> locker(m_mtx);
    reply->set_appstate(AppNormal);  // 能接收到代表网络是正常的
    // 如果有“前朝”的 Leader（其 Term 比我小）发来消息，必须拒绝。
    if (args->term() < m_currentTerm) {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(-100);  // 这是一个信号，让旧 Leader 赶紧更新自己
        DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
                args->term(), m_me, m_currentTerm);
        return;  // 注意从过期的领导人收到消息不要重设定时器
    }
    
    DEFER { persist(); };  // 无论成功失败，只要状态变了，退出前都要存盘

    // 第二阶段：角色对齐与持久化
    if (args->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1;  // 这里设置成-1有意义，如果突然宕机然后上线理论上是可以投票的
    }
    myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));
    
    m_status = Follower;  // 即使 Term 相等，Candidate 也要变回 Follower
    m_lastResetElectionTime = now(); // 既然认可了 Leader 的身份，就重置计时器
   
    // 第三阶段：二审一致性（日志索引检查） 那么就比较日志，日志有3种情况
    // 对方发得太快（日志空洞）中间断了
    if (args->prevlogindex() > getLastLogIndex()) {
        reply->set_success(false); // 我不接受这批日志，因为我的历史记录和你对不上。
        reply->set_term(m_currentTerm); // 让 Leader 知道你现在的任期，防止你是由于任期更高才拒绝的
        /*
          普通 Raft 做法：如果拒绝了，Leader 会很笨地把索引减 1（从 100 变成 99），再来问你。如果差了 20 条，就要往返 20 次 RPC。
          你的代码做法：你直接给 Leader 一个**“提示（Hint）”**。
          计算方式：你告诉 Leader：“别瞎猜了，我这里最后一条是 80，你下一次直接从 81 (getLastLogIndex() + 1) 开始试吧！”
          效果：Leader 收到后，会直接把发给你的 nextIndex 跳到 81。这样一次 RPC 就解决了 20 次才能解决的问题。
        */
        reply->set_updatenextindex(getLastLogIndex() + 1);
        return;
    } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) { // 对方发得太慢（已被快照覆盖）
          // 如果prevlogIndex还没有更上快照
          reply->set_success(false);
          reply->set_term(m_currentTerm);
          reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
    }
    // 若匹配成功
    if (matchLog(args->prevlogindex(), args->prevlogterm())) {
        // 不能直接把 Leader 发来的整个数组直接“啪”一下贴在末尾。需要一条一条检查
        for (int i = 0; i < args->entries_size(); i++) {
            auto log = args->entries(i);
            if (log.logindex() > getLastLogIndex()) {
                //超过就直接添加日志
                m_logs.push_back(log);
            } else { // 重叠部分
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
                    m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
                    //相同位置的log ，其logTerm相等，但是命令却不相同，不符合raft的前向匹配，异常了！
                    myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                          " {%d:%d}却不同！！\n",
                                          m_me, log.logindex(), log.logterm(), m_me,
                                          m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                          log.command()));
                }
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) {
                    // 不匹配就更新
                    m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
                }
            }
        }

        // 因为可能会收到过期的log！！！ 因此这里是大于等于
        myAssert(
            getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
            format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
                  m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));

        if (args->leadercommit() > m_commitIndex) {
            // 这个地方不能无脑跟上getLastLogIndex()，因为可能存在args->leadercommit()落后于 getLastLogIndex()的情况
            m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());  
        }

        // 领导会一次发送完所有的日志
        // 提交指针（m_commitIndex）绝对不能越过你日志数组的物理终点（getLastLogIndex）。
        myAssert(getLastLogIndex() >= m_commitIndex,
                format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                        getLastLogIndex(), m_commitIndex));
        reply->set_success(true);
        reply->set_term(m_currentTerm);
        return;
    } else {
        reply->set_updatenextindex(args->prevlogindex());

        for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
            if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
                reply->set_updatenextindex(index + 1);
                break;
            }
        }
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        return;
    }
}

/*
  看任期：如果别人比我大，我立刻下台。
  看成败：
    失败：把 nextIndex 往前挪，下次多发点。
    成功：更新该节点的 matchIndex。
  看票数：如果同步成功的节点过半，且最后一条日志是本任期的，那就推进 commitIndex。
*/
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums) {
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc开始 ， args->entries_size():{%d}", m_me,
            server, args->entries_size());
    bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

    if (!ok) {
        DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失败", m_me, server);
        return ok;
    }
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);
    if (reply->appstate() == Disconnected) {
        return ok;
    }
    std::lock_guard<std::mutex> lg1(m_mtx);
    // 对于rpc通信，无论什么时候都要检查term
    if (reply->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = reply->term();
        m_votedFor = -1;
        return ok;
    } else if (reply->term() < m_currentTerm) { // 处理“过期任期”（忽略迟到的回信）
        DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(),
                m_me, m_currentTerm);
        return ok;
    }
    if (m_status != Leader) {
        return ok;
    }

    myAssert(reply->term() == m_currentTerm,
            format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));
    if (!reply->success()) {
        if (reply->updatenextindex() != -100) {
            DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me,
                    server, reply->updatenextindex());
            // 既然当前位置不匹配，Leader 就听从 Follower 的建议，把发送起点往回挪到 updatenextindex。
            /*
              nextIndex：Leader 准备发给 Follower 的下一个索引（是尝试性的，可能失败）。
              matchIndex：Leader 确定 Follower 已经拥有的最高日志索引（是确定性的，只能增加）。
              逻辑：因为这次同步失败了，我们并没有增加 Follower 的已知进度，所以绝对不能动 m_matchIndex。
            */
            m_nextIndex[server] = reply->updatenextindex();  // 失败是不更新mathIndex的
        }
    } else {
        *appendNums = *appendNums + 1;
        DPrintf("---------tmp------------ 节点{%d}返回true,当前*appendNums{%d}", server, *appendNums);
        // m_matchIndex这是 Leader 记录的“该 Follower 肯定已经拥有的最高日志索引”。
        // 因为 RPC 回复可能由于网络原因乱序到达。如果先收到索引 100 的成功回执，后收到索引 90 的，我们不能让进度倒退。
        m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
        // 下次给它发日志时，从哪里开始发。既然它已经有了到 N 的日志，下次当然从 N+1 开始。
        m_nextIndex[server] = m_matchIndex[server] + 1;
        int lastLogIndex = getLastLogIndex();

        myAssert(m_nextIndex[server] <= lastLogIndex + 1,
                format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d", server,
                        m_logs.size(), server, lastLogIndex));
        if (*appendNums >= 1 + m_peers.size() / 2) {
            *appendNums = 0;
            if (args->entries_size() > 0) {
                DPrintf("args->entries(args->entries_size()-1).logterm(){%d}   m_currentTerm{%d}",
                        args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
            }
            if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
                DPrintf(
                    "--------tmp------- 当前term有log成功提交，更新leader的m_commitIndex "
                    "from{%d} to{%d}",
                    m_commitIndex, args->prevlogindex() + args->entries_size());

                m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
            }
            myAssert(m_commitIndex <= lastLogIndex,
                  format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n", m_me, lastLogIndex,
                          m_commitIndex));
        }
    }
    return ok;
}

/*
  观察：对比 commitIndex 和 lastApplied。
  提取：把中间差掉的日志拿出来。
  交付：通过 applyChan 把任务丢给 KVServer。
  休息：等待下一次检查
*/
void Raft::applierTicker() {
  // 它是一个死循环，只要 Raft 节点没挂，它就一直盯着进度条。
    while (true) {
        m_mtx.lock();
        if (m_status == Leader) {
            DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied, m_commitIndex);
        }
        /*
          m_commitIndex：Leader 告诉你的，“大家都确认了，可以执行了” 的上限。
          m_lastApplied：你本地已经 “执行给数据库看” 的上限。

          m_commitIndex：Leader 告诉我们已经“安全”的最高索引。
          m_lastApplied：我们本地已经执行到了哪一条。
          getApplyLogs() 的内部逻辑：它会把 (m_lastApplied + 1) 到 m_commitIndex 之间的所有日志包装成 ApplyMsg 对象，并更新 m_lastApplied 的值。
        */
        auto applyMsgs = getApplyLogs();
        m_mtx.unlock();
        if (!applyMsgs.empty()) {
            DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver报告的applyMsgs长度为：{%d}", m_me, applyMsgs.size());
        }
        // applyChan：这是 Raft 层与 KV 数据库层之间的“传送带”。
        // Push(message)：把每一条日志指令（比如 set key1 value1）扔进传送带。
        // KV 服务器在另一端会有一个死循环，不断从这个传送带里取指令，然后写进自己的哈希表。
        for (auto& message : applyMsgs) {
           applyChan->Push(message);
        }
        // 为了避免这个循环跑得太快、过度消耗 CPU，我们设置了一个 ApplyInterval（通常是几十毫秒）
        sleepNMilliseconds(ApplyInterval);
    }
}

/*
  挡驾：不是 Leader 就不接活。
  编号：给新指令分配一个全局唯一的索引位置（Index）。
  入库：把指令写进自己的内存日志并存盘（Persist）。
*/
void Raft::Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader) {
    std::lock_guard<std::mutex> lg1(m_mtx);
    // 如果客户端找错了人（找了一个 Follower），节点会果断拒绝，并告诉对方 isLeader = false。客户端收到后会去寻找真正的 Leader。
    if (m_status != Leader) {
        DPrintf("[func-Start-rf{%d}]  is not leader");
        *newLogIndex = -1;
        *newLogTerm = -1;
        *isLeader = false;
        return;
    }

    raftRpcProctoc::LogEntry newLogEntry; // 这是准备写进账本的新页。
    newLogEntry.set_command(command.asString()); // 指令
    newLogEntry.set_logterm(m_currentTerm); // 任期
    // 通过 getNewCommandIndex() 算出下一条日志应该排在哪个序号（比如上一条是 100，这一条就是 101）。
    newLogEntry.set_logindex(getNewCommandIndex());
    m_logs.emplace_back(newLogEntry); // 将这条新日志先放进 Leader 自己的内存数组 m_logs 里。注意：此时这条日志还是 “未提交” 状态。

    int lastLogIndex = getLastLogIndex();
    DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, &command);
    persist(); // 在告诉客户端结果之前，必须先将新日志持久化到磁盘
    // 把分配好的“页码”和“任期”填回给客户端，并确认身份。
    *newLogIndex = newLogEntry.logindex();
    *newLogTerm = newLogEntry.logterm();
    *isLeader = true;
}

/*
  把“内存记忆”锁进“硬盘保险柜”的动作。
*/
void Raft::persist() {
    // 第一步：序列化（打包）
    auto data = persistData();
    // 第二步：保存到磁盘（入柜）
    m_persister->SaveRaftState(data);
}

/*
  第一次变身（Protobuf -> String）： 把复杂的日志结构体（包含 Index, Term, Command 等）序列化成一个简单的字节串。
  第二次变身（Object -> Stream）： 把所有核心变量（Term, VotedFor 等）和刚才那些日志字节串一起，由 Boost 打包成一个大的数据流。
*/
std::string Raft::persistData() {
    // 创建临时的“打包对象”
    BoostPersistRaftNode boostPersistRaftNode;
    boostPersistRaftNode.m_currentTerm = m_currentTerm;
    boostPersistRaftNode.m_votedFor = m_votedFor;
    boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
    boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
    // m_logs 里的每一项是一个 Protobuf 对象。
    for (auto& item : m_logs) {
        // item.SerializeAsString()：这是 Protobuf 的内置方法，将一条日志条目（包含索引、任期、指令）转换成一串二进制字符串。
        boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
    }

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << boostPersistRaftNode;
    return ss.str();
}

void Raft::leaderHearBeatTicker() {
    while (true) {
        // 不是leader的话就没有必要进行后续操作，况且还要拿锁，很影响性能，目前是睡眠，后面再优化优化
        while (m_status != Leader) {
            usleep(1000 * HeartBeatTimeout);
            // std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
        }
        static std::atomic<int32_t> atomicCount = 0; // 静态原子变量，用于日志计数
        // 预计需要睡眠的时间
        std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
        std::chrono::system_clock::time_point wakeTime{}; // 当前唤醒（检查）的时间点
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            wakeTime = now();
            suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
        }

        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
            std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                      << std::endl;
            // 获取当前时间点
            auto start = std::chrono::steady_clock::now();

            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
            // std::this_thread::sleep_for(suitableSleepTime);

            // 获取函数运行结束后的时间点
            auto end = std::chrono::steady_clock::now();

            // 计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double, std::milli> duration = end - start;

            // 使用ANSI控制序列将输出颜色修改为紫色
            std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: " << duration.count()
                      << " 毫秒\033[0m" << std::endl;
            ++atomicCount;
        }

        if (std::chrono::duration<double, std::milli>(m_lastResetHearBeatTime - wakeTime).count() > 0) {
            //睡眠的这段时间有重置定时器，没有超时，再次睡眠
            continue;
        }
        doHeartBeat();
    }
}

bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
  return true;
  // Your code here (2D).
  // rf.mu.Lock()
  // defer rf.mu.Unlock()
  // DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex {%v} to check
  // whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)
  //// outdated snapshot
  // if lastIncludedIndex <= rf.commitIndex {
  //	return false
  // }
  //
  // lastLogIndex, _ := rf.getLastLogIndexAndTerm()
  // if lastIncludedIndex > lastLogIndex {
  //	rf.logs = make([]LogEntry, 0)
  // } else {
  //	rf.logs = rf.logs[rf.getSlicesIndexFromLogIndex(lastIncludedIndex)+1:]
  // }
  //// update dummy entry with lastIncludedTerm and lastIncludedIndex
  // rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
  //
  // rf.persister.Save(rf.persistData(), snapshot)
  // return true
}

int Raft::getLastLogIndex() {
    int lastLogIndex = -1;
    int _ = -1;
    getLastLogIndexAndTerm(&lastLogIndex, &_);
    return lastLogIndex;
}

int Raft::getLastLogTerm() {
    int _ = -1;
    int lastLogTerm = -1;
    getLastLogIndexAndTerm(&_, &lastLogTerm);
    return lastLogTerm;
}

int Raft::getSlicesIndexFromLogIndex(int logIndex) {
    // m_lastSnapshotIncludeIndex 是快照覆盖的最后一条日志。如果 logIndex 小于或等于它，说明这条日志已经被做成快照并从内存里删掉了。
    myAssert(logIndex > m_lastSnapshotIncludeIndex,
            format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                    logIndex, m_lastSnapshotIncludeIndex));
    int lastLogIndex = getLastLogIndex();
    // 检查找的日志是否超出了当前已有的最大范围。
    myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                              m_me, logIndex, lastLogIndex));
    // 计算下标  如果你想找第 505 条日志：505 - 500 - 1 = 4（对应数组下标 4）。
    int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
    return SliceIndex;
}

/*
  从磁盘到内存流：data $\rightarrow$ iss。
  从文本到结构体：通过 ia >> 还原出 boostPersistRaftNode。
  从字符串到日志对象：通过 ParseFromString 还原出每一条 m_logs。
*/
void Raft::readPersist(std::string data) {
    if (data.empty()) {
        return;
    }
    std::stringstream iss(data);
    boost::archive::text_iarchive ia(iss);

    BoostPersistRaftNode boostPersistRaftNode;
    ia >> boostPersistRaftNode;

    m_currentTerm = boostPersistRaftNode.m_currentTerm;
    m_votedFor = boostPersistRaftNode.m_votedFor;
    m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
    // 先清空当前内存里的旧日志
    m_logs.clear();
    for (auto& item : boostPersistRaftNode.m_logs) {
        raftRpcProctoc::LogEntry logEntry;
        logEntry.ParseFromString(item);
        m_logs.emplace_back(logEntry);
    }
}

// 匹配日志
bool Raft::matchLog(int logIndex, int logTerm) {
    myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
            format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                    logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
    return logTerm == getLogTermFromLogIndex(logIndex);
}

// 做成快照并存盘
void Raft::Snapshot(int index, std::string snapshot) {
    std::lock_guard<std::mutex> lg(m_mtx);

    if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
        DPrintf(
            "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
            "smaller ",
            m_me, index, m_lastSnapshotIncludeIndex);
        return;
    }
    auto lastLogIndex = getLastLogIndex();

    int newLastSnapshotIncludeIndex = index; // 记录快照包含的最后位置
    // 通过 getSlicesIndexFromLogIndex 找到那条日志，取出它的任期（Term）。
    int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
    
    // 保存除快照之后的日志（这些日志不被做成快照）
    std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
    for (int i = index + 1; i <= getLastLogIndex(); i++) {
        trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
    }
    m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
    m_logs = trunckedLogs;
    m_commitIndex = std::max(m_commitIndex, index);
    m_lastApplied = std::max(m_lastApplied, index);

    m_persister->Save(persistData(), snapshot);

    DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
            m_lastSnapshotIncludeTerm, m_logs.size());
    myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
            format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                    m_lastSnapshotIncludeIndex, lastLogIndex));
}

// 取最后索引及任期
void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
    if (m_logs.empty()) {
        *lastLogIndex = m_lastSnapshotIncludeIndex;
        *lastLogTerm = m_lastSnapshotIncludeTerm;
        return;
    } else {
        // 因为m_logs是vector，所以直接取最后一个就行
        *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
        *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
        return;
    }
}

int Raft::getLogTermFromLogIndex(int logIndex) {
    myAssert(logIndex >= m_lastSnapshotIncludeIndex,
            format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                    logIndex, m_lastSnapshotIncludeIndex));
    int lastLogIndex = getLastLogIndex();
    myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                              m_me, logIndex, lastLogIndex));

    if (logIndex == m_lastSnapshotIncludeIndex) {
        return m_lastSnapshotIncludeTerm;
    } else {
        return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
    }
}

// 获取新命令应该分配的Index
int Raft::getNewCommandIndex() {
    //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
    auto lastLogIndex = getLastLogIndex();
    return lastLogIndex + 1;
}

// 获取当前日志的前一条日志以匹配是否对的上
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
    // logs长度为0返回0, 不是0就根据nextIndex数组的数值返回
    if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
        // 要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
        *preIndex = m_lastSnapshotIncludeIndex;
        *preTerm = m_lastSnapshotIncludeTerm;
        return;
    }
    auto nextIndex = m_nextIndex[server];
    *preIndex = nextIndex - 1;
    *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

// 获取当前的任期及地位
void Raft::GetState(int* term, bool* isLeader) {
    m_mtx.lock();
    DEFER {
        m_mtx.unlock();
    };
    // Your code here (2A).
    *term = m_currentTerm;
    *isLeader = (m_status == Leader);
}

// 选举的资格对比
bool Raft::UpToDate(int index, int term) {
    int lastIndex = -1;
    int lastTerm = -1;
    getLastLogIndexAndTerm(&lastIndex, &lastTerm);
    // 比任期 (term > lastTerm) 或 日志长度
    return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

// follower落后太多，直接发送快照
void Raft::leaderSendSnapShot(int server) {
    m_mtx.lock();
    raftRpcProctoc::InstallSnapshotRequest args;
    args.set_leaderid(m_me);
    args.set_term(m_currentTerm);
    args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
    args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
    args.set_data(m_persister->ReadSnapshot());

    raftRpcProctoc::InstallSnapshotResponse reply;
    m_mtx.unlock();
    bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
    m_mtx.lock();
    DEFER { m_mtx.unlock(); };
    if (!ok) {
        return;
    }
    // 拿到回复后重新审视身份
    if (m_status != Leader || m_currentTerm != args.term()) {
        return;  // 中间释放过锁，可能状态已经改变了
    }
    //	无论什么时候都要判断term
    if (reply.term() > m_currentTerm) {
        // 三变
        m_currentTerm = reply.term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
        m_lastResetElectionTime = now();
        return;
    }
    // Leader 记录的、确定已经同步到该 Follower 的最高日志索引。
    m_matchIndex[server] = args.lastsnapshotincludeindex();
    // Leader 下一次准备发给该 Follower 的日志索引。
    m_nextIndex[server] = m_matchIndex[server] + 1;
}

// 统计全集群的进度，决定哪一页账本可以被正式标记为“已提交（Committed）”。
void Raft::leaderUpdateCommitIndex() {
    // 首先将提交进度重置到快照的截止位置。
    m_commitIndex = m_lastSnapshotIncludeIndex;
    for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--) {
        int sum = 0;
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                sum += 1; // Leader 自己肯定有这条日志，先给自己加一票
                continue;
            }
            if (m_matchIndex[i] >= index) {
                sum += 1; // 如果 Follower i 的进度已经达到或超过了当前的 index，票数加一
            }
        }

        if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
            m_commitIndex = index;
            break;
        }
    }
}

// 测量当前 Raft 节点持久化数据（日志、任期等）在磁盘/内存中占用了多少字节。
int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

// 找出那些已经提交但还没执行的日志，把它们打包送往数据库去执行。
std::vector<ApplyMsg> Raft::getApplyLogs() {
    std::vector<ApplyMsg> applyMsgs;
    // 确保“已提交”的指针没有超过“最后一条日志”的指针。这属于逻辑底线检查
    myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                        m_me, m_commitIndex, getLastLogIndex()));
    
    while (m_lastApplied < m_commitIndex) {
        m_lastApplied++;
        myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
                format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                        m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
        // 从 m_logs 中取出具体的指令内容，塞进 ApplyMsg 对象中，放进待交付列表。
        ApplyMsg applyMsg;
        applyMsg.CommandValid = true; // 标记这是一条普通的命令（不是快照）
        applyMsg.SnapshotValid = false;
        applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
        applyMsg.CommandIndex = m_lastApplied;
        applyMsgs.emplace_back(applyMsg);
    }
    return applyMsgs;
}

// 是 Raft 协议栈与上层业务逻辑（KVServer）之间的**“交货滑梯
void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }

// AppendEntries 负责 RPC 协议对接（拿数据、跑回调）
// AppendEntries1 负责 Raft 算法逻辑（比对任期、对暗号、写日志）
void Raft::AppendEntries(google::protobuf::RpcController* controller,
                         const ::raftRpcProctoc::AppendEntriesArgs* request,
                         ::raftRpcProctoc::AppendEntriesReply* response, ::google::protobuf::Closure* done) {
    AppendEntries1(request, response);
    done->Run();
}

// 是 Follower 节点的**“强制同步处理器”**。当 Follower 发现自己落后 Leader 太多
void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) {
    m_mtx.lock();
    DEFER { m_mtx.unlock(); };
    // 如果发快照的人任期比我还小，说明他是个“过气领导”，直接拒绝。
    if (args->term() < m_currentTerm) {
        reply->set_term(m_currentTerm);
        return;
    }
    if (args->term() > m_currentTerm) {
        // 后面两种情况都要接收日志
        m_currentTerm = args->term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
    }
    m_status = Follower;
    m_lastResetElectionTime = now();
    // 如果这个快照还没我本地现有的快照新，那就没必要安装了。
    if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
        return;
    }
    auto lastLogIndex = getLastLogIndex();

    if (lastLogIndex > args->lastsnapshotincludeindex()) {
        // 情况 A：快照只覆盖了我的一部分日志
        m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
    } else {
        // 情况 B：快照比我整个账本都新
        m_logs.clear();
    }
    // 更新自己的进度
    m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
    m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
    m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
    m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

    reply->set_term(m_currentTerm);

    // ApplyMsg 是Raft 层与 KVServer 层沟通的标准数据结构。
    ApplyMsg msg;
    // 在普通的日志同步中，我们会设置 CommandValid = true。这里显式标记为“快照有效”，
    // 是告诉接收方的 KVServer：“这是一个特殊的包裹，里面装的是整个数据库的完整镜像，
    // 而不是一条条增量指令”。
    msg.SnapshotValid = true;
    msg.Snapshot = args->data();
    msg.SnapshotTerm = args->lastsnapshotincludeterm();
    msg.SnapshotIndex = args->lastsnapshotincludeindex();

    std::thread t(&Raft::pushMsgToKvServer, this, msg);  // 创建新线程并执行b函数，并传递参数
    t.detach();
    m_persister->Save(persistData(), args->data());
}

void Raft::InstallSnapshot(google::protobuf::RpcController* controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest* request,
                           ::raftRpcProctoc::InstallSnapshotResponse* response, ::google::protobuf::Closure* done) {
    InstallSnapshot(request, response);
    done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController* controller, const ::raftRpcProctoc::RequestVoteArgs* request,
                       ::raftRpcProctoc::RequestVoteReply* response, ::google::protobuf::Closure* done) {
    RequestVote(request, response);
    done->Run();
}

void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
    m_peers = peers; // 集群中其他节点的服务列表
    m_persister = persister; // 持久化存储器（用于读写磁盘）
    m_me = me; // 当前节点在集群中的编号
    // Your initialization code here (2A, 2B, 2C).
    m_mtx.lock();

    this->applyChan = applyCh;
 
    m_currentTerm = 0;  // 初始任期为 0
    m_status = Follower; // 刚启动时所有节点都是 Follower
    m_commitIndex = 0; // 已提交索引初始化为 0
    m_lastApplied = 0; // 已应用索引初始化为 0
    m_logs.clear(); // 日志初始化为空
    for (int i = 0; i < m_peers.size(); i++) {
        m_matchIndex.push_back(0);
        m_nextIndex.push_back(0);
    }
    m_votedFor = -1;

    m_lastSnapshotIncludeIndex = 0;
    m_lastSnapshotIncludeTerm = 0;
    m_lastResetElectionTime = now(); // 初始化选举计时器起点
    m_lastResetHearBeatTime = now(); // 初始化心跳计时器起点

    // initialize from state persisted before a crash
    // 崩溃恢复（Crash Recovery）和状态同步
    // 从持久化存储（硬盘）中读取 Raft 的核心状态并恢复到内存中。
    readPersist(m_persister->ReadRaftState());
    if (m_lastSnapshotIncludeIndex > 0) {
        m_lastApplied = m_lastSnapshotIncludeIndex;
    }

    DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
            m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

    m_mtx.unlock();

    // 协程调度器（IOManager）和独立线程（std::thread）。
    m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);
    // 将“心跳定时器”加入调度队列。
    m_ioManager->scheduler([this]() -> void { this->leaderHearBeatTicker(); });
    // 将“选举超时定时器”加入调度队列。
    m_ioManager->scheduler([this]() -> void { this->electionTimeOutTicker(); });
    // applierTicker：它的任务是监听 m_commitIndex 的变化。
    // 一旦发现有新的日志被提交（达成共识），它就负责把这些日志内容推送到 applyChan 管道中，
    // 交给上层的 KVServer（数据库）去执行。
    std::thread t3(&Raft::applierTicker, this);
    t3.detach();
}


