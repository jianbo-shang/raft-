#include "util.h"
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <iomanip>

void myAssert(bool condition, std::string message) {
    if (!condition) {
        std::cerr << "Error: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

// 获取时间
std::chrono::_V2::system_clock::time_point now() { return std::chrono::high_resolution_clock::now(); }

// 生成随机选举超时时间
std::chrono::milliseconds getRandomizedElectionTimeout() {
    // 这是一个真随机数发生器。它通常利用硬件噪声（比如电脑内部的电磁波动）来获取一个初始种子。
    // 它的作用是确保每次程序启动时，得到的随机序列都是不一样的。
    std::random_device rd;
    // 这是著名的 梅森旋转算法（Mersenne Twister）。它是一个极其优秀的伪随机数引擎，生成的数字随机性极强，且在长周期内不会重复。我们将 rd() 产生的种子喂给它。
    std::mt19937 rng(rd());
    // 这是一个均匀分布器。每一个整数被抽到的概率都是完全相等的。
    std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime);

    return std::chrono::milliseconds(dist(rng));
}

/*
  std::this_thread：指向当前正在执行这段代码的线程。sleep_for：让当前线程暂停执行一段时间。
  std::chrono::milliseconds(N)：将整数 $N$ 转化为标准的时间长度单位（毫秒）
*/
void sleepNMilliseconds(int N) { std::this_thread::sleep_for(std::chrono::milliseconds(N)); };

/*
  在本地运行 Raft 集群时（一台电脑启动多个节点），如果每个节点都硬编码同一个端口（如 8080），
  会导致端口冲突，只有第一个节点能启动成功。这个函数解决了这个问题。
*/
bool getReleasePort(short &port) {
    short num = 0;
    // 循环条件：如果当前端口不可用，且尝试次数还没超过 30 次
    while (!isReleasePort(port) && num < 30) {
        ++port; // 尝试下一个端口
        ++num; // 计数器加 1
    }
    // // 如果尝试了 30 次都没找到空闲端口
    if (num >= 30) {
        port = -1;
        return false;
    }
    // return true; // 找到了可用的端口，并通过引用参数 port 返回
    return true;
}

bool isReleasePort(unsigned short usPort) {
    int s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    sockaddr_in addr;
    // htons 和 htonl 是为了把数字转换成网络能听懂的“大端字节序”
    addr.sin_family = AF_INET;
    addr.sin_port = htons(usPort);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int ret = ::bind(s, (sockaddr *)&addr, sizeof(addr));
    if (ret != 0) {
        close(s);
        return false;
    }
    close(s);
    return true;
}

void DPrintf(const char *format, ...) {
    if (Debug) {
        // 获取当前的日期，然后取日志信息，写入相应的日志文件当中 a+
        time_t now = time(nullptr);
        tm *nowtm = localtime(&now);
        va_list args; // 1. 定义一个参数列表变量
        va_start(args, format); // 2. 初始化列表，使其指向 format 后的第一个参数
        // tm_year + 1900：C 语言的 tm 结构体中，年份是从 1900 年开始计算的偏移量，所以需要加上 1900。
        // tm_mon + 1：月份是从 0 开始的（0 代表 1 月），所以需要加上 1。类似 [2026-1-5-20-45-30] 的标记
        std::printf("[%d-%d-%d-%d-%d-%d] ", nowtm->tm_year + 1900, nowtm->tm_mon + 1, nowtm->tm_mday, nowtm->tm_hour,
                    nowtm->tm_min, nowtm->tm_sec);
        std::vprintf(format, args); // 3. 核心：将格式字符串和参数列表交给 vprintf 处理
        std::printf("\n");
        va_end(args); // 4. 清理现场
    }
}
