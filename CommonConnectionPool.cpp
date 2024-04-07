#include "CommonConnectionPool.h"
#include "public.h"

#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <functional>

/*
在C++中，静态局部变量在程序的生命周期内只被初始化一次，即使函数被调用多次。这是因为静态局部变量的初始化只在第一次进入函数时发生，之后的函数调用会跳过初始化过程，直接使用已经初始化的值

在C++11标准之后，静态局部变量的初始化在多线程环境下是线程安全的。这意味着如果有多个线程同时访问这个函数，静态局部变量 pool 只会被初始化一次，且保证初始化的过程是线程安全的。
根据C++11标准，在多线程环境下，当一个线程首次调用包含静态局部变量的函数时，编译器会负责确保静态局部变量的初始化只会在一个线程中进行。
这通常通过使用一种称为“双检锁模式（Double-Checked Locking Pattern）”的技术来实现，其中包括了对静态局部变量初始化的同步。此后，其他线程调用同一函数时会直接使用已经初始化好的静态局部变量，无需再次初始化。
因此，根据C++11标准，静态局部变量的初始化在多线程环境下是线程安全的，且不需要额外的锁或同步机制。
*/

// 线程安全的懒汉单例函数接口
ConnectionPool *ConnectionPool::getConnectionPool()
{
    static ConnectionPool pool; // 初始化线程安全
    return &pool;
}

// 从配置文件中加载配置项
// bool ConnectionPool::loadConfigFile()
// {
//     FILE *pf = fopen("/home/zy/桌面/ConnectionPool/mysql.ini", "r");
//     if (pf == nullptr)
//     {
//         LOG("mysql.ini file is not exist!");
//         return false;
//     }

//     while (!feof(pf))
//     {
//         char line[1024] = {0};
//         fgets(line, 1024, pf);
//         string str = line;
//         int idx = str.find('=', 0);
//         if (idx == -1) // 无效的配置项
//         {
//             continue;
//         }

//         // password=123456\n
//         int endidx = str.find('\n', idx); // 从idx往后找 \n
//         string key = str.substr(0, idx);
//         string value = str.substr(idx + 1, endidx - idx - 1);

//         if (key == "ip")
//         {
//             _ip = value;
//         }
//         else if (key == "port")
//         {
//             _port = atoi(value.c_str());
//         }
//         else if (key == "username")
//         {
//             _username = value;
//         }
//         else if (key == "password")
//         {
//             _password = value;
//         }
//         else if (key == "dbname")
//         {
//             _dbname = value;
//         }
//         else if (key == "initSize")
//         {
//             _initSize = atoi(value.c_str());
//         }
//         else if (key == "maxSize")
//         {
//             _maxSize = atoi(value.c_str());
//         }
//         else if (key == "maxIdleTime")
//         {
//             _maxIdleTime = atoi(value.c_str());
//         }
//         else if (key == "connectionTimeOut")
//         {
//             _connectionTimeout = atoi(value.c_str());
//         }
//     }
//     return true;
// }

// 这样对修改更方便
bool ConnectionPool::loadConfigFile()
{
    std::ifstream file("mysql.ini");
    if (!file.is_open())
    {
        LOG("mysql.ini file is not exist!");
        return false;
    }

    std::unordered_map<std::string, std::function<void(const std::string&)>> configMap = {
        {"ip", [&](const std::string& value) { _ip = value; }},
        {"port", [&](const std::string& value) { _port = std::stoi(value); }},
        {"username", [&](const std::string& value) { _username = value; }},
        {"password", [&](const std::string& value) { _password = value; }},
        {"dbname", [&](const std::string& value) { _dbname = value; }},
        {"initSize", [&](const std::string& value) { _initSize = std::stoi(value); }},
        {"maxSize", [&](const std::string& value) { _maxSize = std::stoi(value); }},
        {"maxIdleTime", [&](const std::string& value) { _maxIdleTime = std::stoi(value); }},
        {"connectionTimeOut", [&](const std::string& value) { _connectionTimeout = std::stoi(value); }}
    };

    std::string line;
    while (std::getline(file, line))
    {
        std::istringstream iss(line);  // 确保 line 的类型是 std::string
        std::string key, value;
        if (std::getline(iss, key, '=') && std::getline(iss, value))
        {
            auto it = configMap.find(key);
            if (it != configMap.end())
            {
                it->second(value);
            }
        }
    }

    return true;
}

// 连接池的构造
ConnectionPool::ConnectionPool()
{
    // 加载配置项了
    if (!loadConfigFile())
    {
        return;
    }

    // 创建初始数量的连接
    for (int i = 0; i < _initSize; ++i)
    {
        Connection *p = new Connection();
        p->connect(_ip, _port, _username, _password, _dbname);
        p->refreshAliveTime(); // 刷新一下开始空闲的起始时间
        _connectionQue.push(p);
        _connectionCnt++;
    }

    // 启动一个新的线程，作为连接的生产者 linux thread => pthread_create
    thread produce(std::bind(&ConnectionPool::produceConnectionTask, this));
    produce.detach();

    // 启动一个新的定时线程，扫描超过maxIdleTime时间的空闲连接，进行对于的连接回收
    thread scanner(std::bind(&ConnectionPool::scannerConnectionTask, this));
    scanner.detach();
}

// 运行在独立的线程中，专门负责生产新连接
void ConnectionPool::produceConnectionTask()
{
    for (;;)
    {
        unique_lock<mutex> lock(_queueMutex);
        while (!_connectionQue.empty())
        {
            cv.wait(lock); // 队列不空，此处生产线程进入等待状态
        }

        // 连接数量没有到达上限，继续创建新的连接
        if (_connectionCnt < _maxSize)
        {
            Connection *p = new Connection();
            p->connect(_ip, _port, _username, _password, _dbname);
            p->refreshAliveTime(); // 刷新一下开始空闲的起始时间
            _connectionQue.push(p);
            _connectionCnt++;
        }

        // 通知消费者线程，可以消费连接了
        cv.notify_all();
    }
}

// 扫描超过maxIdleTime时间的空闲连接，进行对于的连接回收
void ConnectionPool::scannerConnectionTask()
{
    for (;;)
    {
        // 通过sleep模拟定时效果
        this_thread::sleep_for(chrono::seconds(_maxIdleTime));

        // 扫描整个队列，释放多余的连接
        unique_lock<mutex> lock(_queueMutex);
        while (_connectionCnt > _initSize)
        {
            Connection *p = _connectionQue.front();
            if (p->getAliveeTime() >= (_maxIdleTime * 1000))
            {
                _connectionQue.pop();
                _connectionCnt--;
                delete p; // 调用~Connection()释放连接
            }
            else
            {
                break; // 队头的连接没有超过_maxIdleTime，其它连接肯定没有
            }
        }
    }
}


// 给外部提供接口，从连接池中获取一个可用的空闲连接
shared_ptr<Connection> ConnectionPool::getConnection()
{
    unique_lock<mutex> lock(_queueMutex);
    while (_connectionQue.empty()) // 连接队列为空         消费者线程在等待连接队列中有可用的连接
    {
        // 不使用sleep
        if (cv_status::timeout == cv.wait_for(lock, chrono::milliseconds(_connectionTimeout))) // 如果等待时间超过了超时时间，条件变量的等待状态将是 cv_status::timeout，此时执行下面的代码块。
        {
            if (_connectionQue.empty()) // 检查等待后连接队列是否仍然为空，如果是，表示等待超时，记录日志并返回空指针
            {
                LOG("获取空闲连接超时了...获取连接失败!");
                return nullptr;
            }
        }
    }

    /*
    shared_ptr智能指针析构时，会把connection资源直接delete掉，相当于
    调用connection的析构函数，connection就被close掉了。
    这里需要自定义shared_ptr的释放资源的方式，把connection直接归还到queue当中
    */
    // 传入一个 lambda 表达式作为 deleter，用于在智能指针释放资源时将连接归还到连接池队列中
    shared_ptr<Connection> sp(_connectionQue.front(),
                              [&](Connection *pcon)
                              {
                                  // 这里是在服务器应用线程中调用的，所以一定要考虑队列的线程安全操作
                                  unique_lock<mutex> lock(_queueMutex);
                                  pcon->refreshAliveTime(); // 刷新一下开始空闲的起始时间
                                  _connectionQue.push(pcon);
                              });

    _connectionQue.pop();
    cv.notify_all();       // 消费完连接以后，通知生产者线程检查一下，如果队列为空了，赶紧生产连接

    return sp;
}
// 使用 `cv.wait_for` 而不是 `sleep` 的主要原因是效率和资源利用率。

// 1. **阻塞线程的效率**：`cv.wait_for` 允许线程在等待期间释放互斥锁，并进入阻塞状态，等待条件变量的通知。这样可以确保其他线程能够在等待期间获得互斥锁并继续工作，而不是等待期间持续占用锁。相比之下，使用 `sleep` 函数的话，线程会一直持有锁，导致其他线程无法进入临界区，从而降低了并发效率。
// 2. **资源利用率**：`cv.wait_for` 可以在等待期间使线程进入阻塞状态，不会消耗 CPU 资源。而使用 `sleep` 函数时，线程仍然会占用 CPU 资源进行忙等待，这样会浪费系统资源。
// 3. **条件变量的通知**：`cv.wait_for` 结合了条件变量，可以在某个条件被满足或超时时唤醒线程。这样能够更灵活地控制线程的等待，不需要在超时之前手动唤醒线程。而 `sleep` 函数只能简单地让线程休眠一段固定的时间，无法响应其他线程的动态变化。
// 因此，使用 `cv.wait_for` 更符合多线程编程的最佳实践，可以提高程序的性能和资源利用率。
