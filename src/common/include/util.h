#ifndef UTIL_H
#define UTIL_H

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <condition_variable>  // pthread_condition_t
#include <functional>
#include <iostream>
#include <mutex>  // pthread_mutex_t
#include <queue>
#include <random>
#include <sstream>
#include <thread>
#include "config.h"

template <class F>
class DeferClass {
public:
    // 右值引用。 完美转发
    DeferClass(F&& f) : m_func(std::forward<F>(f)) {}
    DeferClass(const F& f) : m_func(f) {}
    ~DeferClass() { m_func(); }

    DeferClass(const DeferClass& e) = delete;
    DeferClass& operator=(const DeferClass& e) = delete;

private:
    F m_func;
};
// ## 符号：在宏定义中，## 被称为“记号粘贴”操作符。它的作用是把两个不相干的文字“粘”在一起，变成一个名字。
#define _CONCAT(a, b) a##b
#define _MAKE_DEFER_(line) DeferClass _CONCAT(defer_placeholder, line) = [&]()

#undef DEFER
#define DEFER _MAKE_DEFER_(__LINE__)

void DPrintf(const char* format, ...);

void myAssert(bool condition, std::string message = "Assertion failed!");

// 它的目的是将一个 C 风格的字符串指针（const char*）安全地转换成 C++ 的 std::string 对象。
inline std::string format(const char* s) {
    return std::string(s ? s : "");
}

// 可变参数模板。它表示这个函数可以接收任意数量、任意类型的参数（比如一个整数、一个字符串、一个浮点数同时传进来）。
template <typename... Args>
std::string format(const char* format_str, Args... args) {
    // 通常 snprintf 是用来写字符串的。但当你把目标设为 nullptr（空）且长度设为 0 时，
    // 它不会真正写入，而是会计算并返回：如果真的要拼这个字符串，到底需要多少个字节。
    int size_s = std::snprintf(nullptr, 0, format_str, args...) + 1; // c风格字符串"\0"

    if (size_s <= 0) { throw std::runtime_error("Error during formatting."); }

    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, format_str, args...);
    return std::string(buf.data(), buf.data() + size - 1);  // remove '\0'
}

std::chrono::_V2::system_clock::time_point now();

std::chrono::milliseconds getRandomizedElectionTimeout();
void sleepNMilliseconds(int N);

// 异步写日志的日志队列  线程安全队列，它是实现“生产者-消费者”模型的核心组件
// 多个业务线程（生产者）不停地往里面塞日志，而一个专门的日志线程（消费者）负责从里面取日志并写入磁盘。
// read is blocking!!! LIKE  go chan
template <typename T>
class LockQueue {
public:
    // 多个worker线程都会写日志queue
    void Push(const T& data) {
        std::lock_guard<std::mutex> lock(m_mutex);  //使用lock_gurad，即RAII的思想保证锁正确释放
        m_queue.push(data);
        m_condvariable.notify_one();
    }

    // 一个线程读日志queue，写日志文件
    T Pop() {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (m_queue.empty()) {
          // 日志队列为空，线程进入wait状态
          m_condvariable.wait(lock);  //这里用unique_lock是因为lock_guard不支持解锁，而unique_lock支持
        }
        T data = m_queue.front();
        m_queue.pop();
        return data;
    }

    bool timeOutPop(int timeout, T* ResData)  // 添加一个超时时间参数，默认为 50 毫秒
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        // 获取当前时间点，并计算出超时时刻
        auto now = std::chrono::system_clock::now();
        auto timeout_time = now + std::chrono::milliseconds(timeout);

        // 在超时之前，不断检查队列是否为空
        while (m_queue.empty()) {
            // 如果已经超时了，就返回一个空对象
            if (m_condvariable.wait_until(lock, timeout_time) == std::cv_status::timeout) {
                return false;
            } else {
                continue;
            }
        }

        T data = m_queue.front();
        m_queue.pop();
        *ResData = data;
        return true;
    }

private:
    std::queue<T> m_queue;
    std::mutex m_mutex;
    std::condition_variable m_condvariable;
};
// 两个对锁的管理用到了RAII的思想，防止中途出现问题而导致资源无法释放的问题！！！
// std::lock_guard 和 std::unique_lock 都是 C++11 中用来管理互斥锁的工具类，它们都封装了 RAII（Resource Acquisition Is
// Initialization）技术，使得互斥锁在需要时自动加锁，在不需要时自动解锁，从而避免了很多手动加锁和解锁的繁琐操作。
// std::lock_guard 是一个模板类，它的模板参数是一个互斥量类型。当创建一个 std::lock_guard
// 对象时，它会自动地对传入的互斥量进行加锁操作，并在该对象被销毁时对互斥量进行自动解锁操作。std::lock_guard
// 不能手动释放锁，因为其所提供的锁的生命周期与其绑定对象的生命周期一致。 
// std::unique_lock也是一个模板类，同样的，其模板参数也是互斥量类型。不同的是，std::unique_lock 提供了更灵活的锁管理功能。可以通过
// lock()、unlock()、try_lock() 等方法手动控制锁的状态。当然，std::unique_lock 也支持 RAII
// 技术，即在对象被销毁时会自动解锁。另外， std::unique_lock 还支持超时等待和可中断等待的操作。

// 这个Op是kv传递给raft的command
class Op {
public:
    // Your definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
    std::string Operation;  // "Get" "Put" "Append"
    std::string Key;
    std::string Value;
    std::string ClientId;  //客户端号码
    int RequestId;         //客户端号码请求的Request的序列号，为了保证线性一致性
                         // IfDuplicate bool // Duplicate command can't be applied twice , but only for PUT and APPEND

public:
  // todo
  //为了协调raftRPC中的command只设置成了string,这个的限制就是正常字符中不能包含|
  //当然后期可以换成更高级的序列化方法，比如protobuf
    std::string asString() const {
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss); // 创建一个“文本存档”对象

        // write class instance to archive
        oa << *this;  // 将当前对象的所有内容“流”进存档
        // close archive

        return ss.str(); // 返回转换后的文本字符串
    }

    bool parseFromString(std::string str) {
        std::stringstream iss(str); // 把传入的二进制/文本字符串 str 包装成一个输入流 iss。
        boost::archive::text_iarchive ia(iss); // 创建 Boost 的“文本输入存档”对象 ia。
        // read class state from archive
        ia >> *this; // 这是最核心的一行。它会再次触发 Op 类私有成员里的 serialize 函数模板。
        return true;  
    }

public:
    friend std::ostream& operator<<(std::ostream& os, const Op& obj) {
        os << "[MyClass:Operation{" + obj.Operation + "},Key{" + obj.Key + "},Value{" + obj.Value + "},ClientId{" +
                  obj.ClientId + "},RequestId{" + std::to_string(obj.RequestId) + "}";  // 在这里实现自定义的输出格式
        return os;
    }

private:
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar& Operation;
        ar& Key;
        ar& Value;
        ar& ClientId;
        ar& RequestId;
    }
};

// kvserver reply err to clerk

const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";

// 获取可用端口

bool isReleasePort(unsigned short usPort);

bool getReleasePort(short& port);

#endif  //  UTIL_H