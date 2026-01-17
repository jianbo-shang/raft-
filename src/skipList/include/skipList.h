#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>

#define STORE_FILE "store/dumpFile"
static std::string delimiter = ":";

// 跳表节点
template <typename K, typename V>
class Node {
public:
    Node() {}
    Node(K k, V v, int);
    ~Node();

    K get_key() const;
    V get_value() const;
    void set_value(V);

    // 前进指针阵列
    Node<K, V> **forward;
    // 节点高度 记录这个节点当前拥有多少层索引
    int node_level;

private:
    K key;
    V value;
};

// 构造函数
template <typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level) {
    this->key = k;
    this->value = v;
    this->node_level = level;

    // 因为是从0开始的，所以0-level有level+1层
    this->forward = new Node<K, V> *[level + 1];
    // 初始化，内存清零，防止指针乱指，出现（Segmentation Fault（段错误/程序崩溃）。）
    memset(this->forward, 0, sizeof(Node<K, V> *) * (level + 1));
};

// 析构函数
template <typename K, typename V>
Node<K, V>::~Node() {
    delete[] forward;
};

// 获取键
template <typename K, typename V>
K Node<K, V>::get_key() const {
    return key;
};

// 获取值
template <typename K, typename V>
V Node<K, V>::get_value() const {
    return value;
};

// 设置值
template <typename K, typename V>
void Node<K, V>::set_value(V value) {
    this->value = value;
};

// 将“复杂链表”拍平为“简单数组”
template <typename K, typename V>
class SkipListDump {
public:
    friend class boost::serialization::access;

    // 序列化及反序列化
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar &keyDumpVt_;
        ar &valDumpVt_;
    }
    // 用来存储跳表中所有节点的 Key 和 Value
    std::vector<K> keyDumpVt_;
    std::vector<V> valDumpVt_;

public:
    void insert(const Node<K, V> &node);
};

// 跳表
template <typename K, typename V>
class SkipList {
public:
    // 构造函数。初始化大厦。你需要告诉它最大允许多少层（比如 16 层）。
    SkipList(int);
    ~SkipList();

    int get_random_level(); // 掷硬币。每插入一个新节点，就调用它。它可能返回 0，也可能返回很高的层数。
    Node<K, V> *create_node(K, V, int); // 创建节点
    int insert_element(K, V); // 插入元素
    bool search_element(K, V &value); // 从最高层开始“大步跳”，如果跳过了就降一层继续找，直到找到或确定不存在。
    void delete_element(K); // 删除元素
    void insert_set_element(K &, V &); // 插入或更新。如果 Key 已存在，就改它的 Value；不存在就新建。
    
    std::string dump_file(); // 导出。把内存里的数据全部变成一个字符串，准备存盘。
    void load_file(const std::string &dumpStr); // 加载。从磁盘读取字符串，把数据重新塞回跳表。
    void display_list(); // :打印。把每一层的样子画在屏幕上，方便你调试。
    void clear(Node<K, V> *); // 递归删除节点
    int size(); // 返回当前存了多少条数据。

private:
    // 从字符串中获取key value
    void get_key_value_from_string(const std::string &str, std::string *key, std::string *value);
    bool is_valid_string(const std::string &str); // 验证一个字符串是否合格

private:
    int _max_level; // 跳表允许的最大层数（天花板高度）
    int _skip_list_level; // 当前跳表里最高层数
    Node<K, V> *_header;  // 头节点。它是每一层索引的起点，不存真实数据
    int _element_count; // 记录现在一共存了多少个元素
    std::mutex _mtx;  // 锁

    std::ofstream _file_writer; // 输出流，代表写操作
    std::ifstream _file_reader; // 输入流，代表读操作
};

// 创建新节点
template <typename K, typename V>
Node<K, V> *SkipList<K, V>::create_node(const K k, const V v, int level) {
    Node<K, V> *n = new Node<K, V>(k, v, level);
    return n;
}

// Insert given key and value in skip list
// return 1 means element exists
// return 0 means insert successfully
/*
                           +------------+
                           |  insert 50 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |                      insert +----+
level 3         1+-------->10+---------------> | 50 |          70       100
                                               |    |
                                               |    |
level 2         1          10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 1         1    4     10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 0         1    4   9 10         30   40  | 50 |  60      70       100
                                               +----+
*/
template <typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value) {
    _mtx.lock();
    Node<K, V> *current = this->_header; // 从头节点开始找
    Node<K, V> *update[_max_level + 1]; // 辅助数组，用于维持插入的左边节点
    memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1)); 

    // 从跳表当前的最高层开始往低层找
    for (int i = _skip_list_level; i >= 0; i--) {
        // 只要右边的节点不为空，且右边节点的 Key 比我们要找的小，就一直往右跳
        while (current->forward[i] != NULL && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        // 当往右跳不动了，记录下这一层最后停下的位置
        update[i] = current;
    }
    // 降到最底层（Level 0），同时也会指向待插入节点的右边位置
    current = current->forward[0];
    // 若存在同样的节点，则直接返回
    if (current != NULL && current->get_key() == key) {
        std::cout << "key: " << key << ", exists" << std::endl;
        _mtx.unlock(); // 存在就解锁退出
        return 1;
    }
    // 这种情况则是不存在该节点
    if (current == NULL || current->get_key() != key) {
        // 给定随机的层数
        int random_level = get_random_level();
        // 新的高度，多出来的高度的左边的邻居只能是头节点
        if (random_level > _skip_list_level) {
            for (int i = _skip_list_level + 1; i < random_level + 1; i++) {
                update[i] = _header;
            }
            // 更新跳表全局高度
            _skip_list_level = random_level;
        }
        // 3. 调用“工厂”函数创建新节点
        Node<K, V> *inserted_node = create_node(key, value, random_level);
        // 
        for (int i = 0; i <= random_level; i++) {
            // 1. 新节点的右手，拉住老邻居原本的右手
            inserted_node->forward[i] = update[i]->forward[i];
            // 2. 老邻居的右手，改拉住新节点
            update[i]->forward[i] = inserted_node;
        }
        std::cout << "Successfully inserted key:" << key << ", value:" << value << std::endl;
        _element_count++;
    }
    _mtx.unlock();
    return 0;
}

// Display skip list
template <typename K, typename V>
void SkipList<K, V>::display_list() {
    std::cout << "\n*****Skip List*****" << "\n";
    for (int i = 0; i <= _skip_list_level; i++) {
        Node<K, V> *node = this->_header->forward[i];
        std::cout << "Level " << i << ": ";
        while (node != NULL) {
            std::cout << node->get_key() << ":" << node->get_value() << ";";
            node = node->forward[i];
        }
        std::cout << std::endl;
    }
}

// Dump data in memory to file
template <typename K, typename V>
std::string SkipList<K, V>::dump_file() {
    Node<K, V> *node = this->_header->forward[0]; // 1. 从最底层（Level 0）的第一个节点开始
    SkipListDump<K, V> dumper; // 2. 准备好我们之前提到的“数据打包盒”
    while (node != nullptr) {
        dumper.insert(*node);
        node = node->forward[0];
    }
    std::stringstream ss; // 创建一个字符串流（可以理解为一块临时的内存缓冲区）
    boost::archive::text_oarchive oa(ss); // 创建一个 Boost 序列化“压缩机”
    oa << dumper; // 执行压缩：把打包盒里的所有数据转成特定的文本格式并存入 ss
    return ss.str();
}

// Load data from disk
template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string &dumpStr) {
    if (dumpStr.empty()) {
        return;
    }
    SkipListDump<K, V> dumper; // 1. 准备好用来装“零件”的空箱子
    std::stringstream iss(dumpStr); // 1. 准备好用来装“零件”的空箱子
    boost::archive::text_iarchive ia(iss); // 3. 初始化文本解压机（ia 中的 i 代表 Input）
    ia >> dumper;  // 4. 正式解压：把字符串还原成 dumper 里的数组
    for (int i = 0; i < dumper.keyDumpVt_.size(); ++i) {
        insert_element(dumper.keyDumpVt_[i], dumper.keyDumpVt_[i]);
    }
}

// Get current SkipList size
template <typename K, typename V>
int SkipList<K, V>::size() {
    return _element_count;
}

template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string &str, std::string *key, std::string *value) {
    if (!is_valid_string(str)) {
        return;
    }
    *key = str.substr(0, str.find(delimiter));
    *value = str.substr(str.find(delimiter) + 1, str.length());
}

// 检查字符串是否合法
template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string &str) {
    if (str.empty()) {
        return false;
    }
    if (str.find(delimiter) == std::string::npos) {
        return false;
    }
    return true;
}

// Delete element from skip list
template <typename K, typename V>
void SkipList<K, V>::delete_element(K key) {
    _mtx.lock();
    Node<K, V> *current = this->_header;
    Node<K, V> *update[_max_level + 1]; // 辅助数组
    memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

    // 存待删除节点的前驱节点
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] != NULL && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];
    if (current != NULL && current->get_key() == key) {
        for (int i = 0; i <= _skip_list_level; i++) {
            // // 如果这一层没有该节点，后面更高层肯定也没有
            if (update[i]->forward[i] != current) break;
            update[i]->forward[i] = current->forward[i]; // 修改指针，跳过 current 节点
        }
        // 若删除的是最高的几层，则需要把跳表的高度降下来
        while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0) {
            _skip_list_level--;
        }
        std::cout << "Successfully deleted key " << key << std::endl;
        delete current;
        _element_count--;
    }
    _mtx.unlock();
    return;
}

/**
 * \brief 作用与insert_element相同类似，
 * insert_element是插入新元素，
 * insert_set_element是插入元素，如果元素存在则改变其值
 */
template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K &key, V &value) {
    V oldValue;
    // 为了保证跳表中 key 的唯一性，函数先调用我们之前分析过的 delete_element(key) 把旧节点彻底删掉。
    if (search_element(key, oldValue)) {
        delete_element(key);
    }
    insert_element(key, value);
}

// Search for element in skip list
/*
                           +------------+
                           |  select 60 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |
level 3         1+-------->10+------------------>50+           70       100
                                                   |
                                                   |
level 2         1          10         30         50|           70       100
                                                   |
                                                   |
level 1         1    4     10         30         50|           70       100
                                                   |
                                                   |
level 0         1    4   9 10         30   40    50+-->60      70       100
*/
template <typename K, typename V>
bool SkipList<K, V>::search_element(K key, V &value) {
    std::cout << "search_element-----------------" << std::endl;
    Node<K, V> *current = _header;

    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
    }
    // 经过上面的循环，current 停在了第 0 层（最底层）中“小于目标 key”的最后一个节点上。
    current = current->forward[0];
    if (current and current->get_key() == key) {
        value = current->get_value();
        std::cout << "Found key: " << key << ", value: " << current->get_value() << std::endl;
        return true;
    }
    std::cout << "Not Found Key:" << key << std::endl;
    return false;
}

template <typename K, typename V>
void SkipListDump<K, V>::insert(const Node<K, V> &node) {
    keyDumpVt_.emplace_back(node.get_key());
    valDumpVt_.emplace_back(node.get_value());
}

// construct skip list
template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level) { 
    this->_max_level = max_level; // 设置跳表允许的最大层高
    this->_skip_list_level = 0; // 初始化当前跳表的实际有效层高为 0
    this->_element_count = 0; // 初始化元素个数为 0
    // 创建头节点
    K k;
    V v;
    this->_header = new Node<K, V>(k, v, _max_level);
};

template <typename K, typename V>
SkipList<K, V>::~SkipList() {
    if (_file_writer.is_open()) {
        _file_writer.close();
    }
    if (_file_reader.is_open()) {
        _file_reader.close();
    }

    //递归删除跳表链条
    if (_header->forward[0] != nullptr) {
        clear(_header->forward[0]);
    }
    delete (_header);
}
template <typename K, typename V>
void SkipList<K, V>::clear(Node<K, V> *cur) {
    if (cur->forward[0] != nullptr) {
        clear(cur->forward[0]);
    }
    delete (cur);
}

template <typename K, typename V>
int SkipList<K, V>::get_random_level() {
    int k = 1;
    while (rand() % 2) {
        k++;
    }
    k = (k < _max_level) ? k : _max_level;
    return k;
};
#endif  // SKIPLIST_H
