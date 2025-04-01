#include <cstdio>
#include <cassert>
#include <cstdlib>
#include <algorithm>
#include <vector>
#include <set>
#include <map>
#include <deque>
#include <random>
#include <string>
#include <functional>
#include <queue>
#include <bitset>
#include <cmath>
#include <thread>

/**
 *
 *  固定参数区
 *
 */
#define MAX_DISK_NUM (10 + 1)
#define MAX_DISK_SIZE (16384 + 1)
#define MAX_REQUEST_NUM (30000000 + 1)
#define MAX_OBJECT_NUM (100000 + 1)
#define REP_NUM (3)
#define FRE_PER_SLICING (1800)
#define EXTRA_TIME (105)
// 最大的对象类型数量
#define MAX_M (16 + 1)
// 对象包含的最大对象块数量
#define MAX_BLOCKS (5 + 1)
// 最大的时间片数量
#define MAX_TIME (86400 + 1)



/**
 *
 *  可调参数区 (按重要性排序)
 *  注意：
 *      最好满足区间 [MIN_PAGE_NUM, MAX_PAGE_NUM] 中包含 REP_NUM 的倍数，不然可能会浪费几个页的空间，导致磁盘被写爆。
 *      最终的PAGE_NUM是在 [MIN_PAGE_NUM, MAX_PAGE_NUM] 中自适应取一个浪费磁盘空间最小的值。
 *
 */
// 期望的最小页数（1 ~ 3276）
#define MIN_PAGE_NUM (18)
// 期望的最大页数（1 ~ 3276 并且 >= MIN_PAGE_NUM）
#define MAX_PAGE_NUM (18)
// K参数 (1 ~ 50)：每个磁头以 不超过K 个时间片为单位进行一次决策(第1个时间片移动到目的地 + 顺序读取，后续若干时间片以最小开销策略顺序读取)
#define MAX_K (15)
// K参数衰减系数
#define K_DECAY (-0.003)
// R参数 (1 ~ 50)：每个时间段起始时刻，向后看R个时间段的统计信息
#define R (8)
// P参数 (0 ~ 1): 区分主流对象类型和次要对象类型的累计频率分布阈值
#define P (0.9)
// 窗口大小 (0 ~ 20)：作为顺序读取时贪心策略的参数，在当前位置先向后看WINDOW_SIZE个块，再决定当前是read还是pass
// (设为0时，不同位置采用自适应计算的不同窗口大小)
#define WINDOW_SIZE (0)
// 磁盘碎片阈值(1 ~ 100): 两个同类对象块之间相隔不超过FRAG_THRETHOULD，且出现了空位置，则认为这是一个磁盘碎片
#define FRAG_THRETHOULD (32)
// 随机数种子：设置为0时调用当前系统时间作为种子
#define RANDOM_SEED (114514)
// 权重阈值（1 ~ INF）：如果当前块的权重小于该值就不读了，直接跳过
#define WEIGHT_THRESHOULD (550)
// // 早停参数 (0 ~ 1)：当磁头累计实际读取的权重大于预期权重的 EARLY_STOP_THRESHOULD 倍时，提前结束掉当前磁头 K 轮循环的动作，释放磁头
// #define EARLY_STOP_THRESHOULD (2)
// 最长存活时间 (1 ~ 105)：一个读请求的最长存活时间，限制未完成请求队列的大小，剔除已过期的读请求
#define MAX_ALIVE (105)
// 最大处理能力 (1 ~ 105)：每个时间片都以最理想的情况读取时，可以保留多少个时间片的请求，限制未完成请求队列的大小，剔除在磁头处理能力之外的读请求
#define RESERVING_ROUNDS (105)

// 读
#define ALPHA (1)
// 写
#define BETA (0)
// 删
#define GAMMA (0)

/**
 *
 *  数据结构区
 *
 */
// 定义读请求
typedef struct Request_ {
    int id;
    int object_id;
    int prev_id;
    // 这个请求所需的对象块状态 (0: 未读取, 1: 已读取)
    std::bitset<MAX_BLOCKS> block_status;
    // 这个请求到来的时间片
    int timestamp;
} Request;

// 定义页结构
typedef struct Page_
{
    int disk_id = 0;        // 所在磁盘的id
    int position = 0;       // 第一个块在磁盘中的位置 (1 ~ V)
    int page_size = 0;      // 实际页大小
    int page_index = 0;     // 实际页号（1 ~ PAGE_NUM)
    int usable_size = 0;    // 剩余可用空间
    int cnt[MAX_M] = {0};   // 各个类型的对象块数量
    int frag[MAX_M] = {0};   // 各个类型的磁盘碎片数量
    bool is_ok = false;     // 标识位，初始化后仍为false的页直接被弃用

    // Page_() = default;
    // Page_(int _disk_id, int _page_index):
    //     disk_id(_disk_id), page_index(_page_index) {}
    // Page_(int _disk_id, int _position, int _pagesize, int _page_index, int _usable_size):
    //     disk_id(_disk_id), position(_position), page_size(_pagesize), page_index(_page_index), usable_size(_usable_size) {}

    bool friend operator < (const Page_ &aa, const Page_ &bb) {
        if (aa.disk_id == bb.disk_id)
        {
            return aa.page_index < bb.page_index;
        }
        return aa.disk_id < bb.disk_id;
    }
}Page;


// 定义磁盘间页的绑定方式
// TODO: 后期可以和Page结构体合并，代码更简洁
typedef struct PageBinding_
{
    // 当前页的磁盘号、页号
    int disk_id;
    int page_index;
    // 当前页是绑定关系中的第rep_id个副本
    int rep_id;
    // 绑定的REP_NUM个页共享同一个b_id;
    int b_id;

    // 指向绑定关系中的值
    PageBinding_ *units[REP_NUM + 1];
}PageBinding;

// 定义对象
typedef struct Object_ {
    int replica[REP_NUM + 1];
    int* unit[REP_NUM + 1];
    PageBinding_ *binding;
    int size;
    int tag;
    int last_request_point;
    bool is_delete;

    // 对象id
    int id;
    // 排序规则
    bool friend operator <(const Object_ &aa, const Object_ &bb)
    {
        return aa.tag < bb.tag;
    }
} Object;

// 定义链表节点
typedef struct Node_ {
    // 要保存的数据索引
    int id;
    Node_ *prev;
    Node_ *next;
    Node_() = default;
    explicit  Node_(int _id): id(_id), prev(nullptr), next(nullptr){}
}Node;

// 定义双向链表
typedef struct Deque_
{
    // 头节点和尾节点均不保存数据
    Node_ *head;
    Node_ *tail;
    // 定义从id到链表节点指针的映射，实现O(1)删除
    // id范围：1 ~ cnt
    Node_ **mp = nullptr;
    // 节点数量（不含头、尾节点）
    int cnt;
    Deque_()
    {
        this -> head = new Node_();
        this -> tail = new Node_();
        this -> head -> next = this -> tail;
        this -> tail -> prev = this -> head;
        this -> cnt = 0;
    }

    // 每个Deque创建完后都要
    void init(int size)
    {
        this -> mp = new Node_*[size + 1]();
    }

    // 删除id的节点
    void erase(int id)
    {
        if (mp[id] == nullptr)
        {
            return;
        }
        Node_ *p = mp[id];
        Node_ *prev = p -> prev, *next = p -> next;
        prev -> next = next;
        next -> prev = prev;
        mp[id] = nullptr;

        this -> cnt --;
    };

    // 插入表尾部
    void append(int id)
    {
        mp[id] = new Node_(id);
        Node_ *p = mp[id], *prev = tail -> prev;

        prev -> next = p;
        p -> prev = prev;
        p -> next = this -> tail;
        this -> tail -> prev = p;

        this -> cnt ++;
    }

    // 获取表头节点的id
    int front()
    {
        Node_ *front = this -> head -> next;
        return front -> id;
    }

    // 获取表尾节点的id
    int back()
    {
        Node_ *back = this -> tail -> prev;
        return back -> id;
    }

    // 弹出表头节点
    void pop_front()
    {
        Node_ *front = this -> head -> next;
        Node_ *next = front -> next;
        this -> head -> next = next;
        next -> prev = this -> head;

        mp[front -> id] = nullptr;

        this -> cnt --;
    }

    // 是否有编号为id的节点
    bool exist(int id)
    {
        return this -> mp[id] != nullptr;
    }

    // 当前链表保存的id数
    int size()
    {
        return this -> cnt;
    }
}Deque;

struct CmpPair1 {
    bool operator()(const std::pair<double, int>& a, const std::pair<double, int>& b) {
        if (a.first == b.first) return a.second > b.second;
        return a.first > b.first;
    }
};

// 预估的磁头扫描区域
struct DiskPointSchedule
{
    int start;  // 扫描的起始位置
    int end;    // 扫描的结束位置
    int K;      // 预估完成这次扫描所需的时间片数量
    DiskPointSchedule() = default;
    DiskPointSchedule(int _start, int _end, int _K): start(_start), end(_end), K(_K){}
};


/**
 *
 *  需要全局维护的变量
 *
 */
int T, M, N, V, G;
int fre_del[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
int fre_write[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
int fre_read[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
Request requests[MAX_REQUEST_NUM];
Object objects[MAX_OBJECT_NUM];
int now_time;
int now_slice;

// 一个磁盘划分的页数量
int PAGE_NUM;
// 页大小（每个磁盘最后一页的大小可能不等于PAGE_SIZE）
int PAGE_SIZE;
// 磁盘存储状态
int b_disk[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 2];
// 位置的预划分
int b_prefer[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 2];
// 磁头当前位置
int disk_point[MAX_DISK_NUM];
// 磁头当前的状态 (k>0: 磁头已经连续读了k次, k=0: 磁头上次pass了, k=-1: 磁头上次是jump过来的)
int disk_point_last_status[MAX_DISK_NUM];
// 磁头上次消耗的令牌数
int disk_point_last_cost[MAX_DISK_NUM];
// 磁头这次K轮循环的预期的收益
int disk_point_last_profit[MAX_DISK_NUM];
// 磁头这次K轮循环的已积累收益
int disk_point_last_accumulated[MAX_DISK_NUM];
// 当前的磁头还需要接着读K个时间片(0 ~ K, 0代表磁头不做任何动作)
int disk_point_k_status[MAX_DISK_NUM];
// 当前磁头disk_id 期望的起点
int disk_point_start[MAX_DISK_NUM];
// 当前磁头disk_id 期望的终点
int disk_point_end[MAX_DISK_NUM];
// 每个硬盘的实际有效总存储空间（V对PAGE_SIZE取余的部分 和 is_ok = false的页所占空间被剔除了）
int valid_capacy[MAX_DISK_NUM];
// 每个磁盘的剩余可用空间
int usable_capacy[MAX_DISK_NUM];
// 每个磁盘存储的各类对象块数量
int disk_tag_count[MAX_DISK_NUM][MAX_M];

// 一次JUMP消耗的令牌数
int JUMP_COST;
// 一次PASS消耗的令牌数
int PASS_COST;
// 允许的最大unfinished大小，可以控制代码运行时间
int MAX_DEQUE_SIZE;
// 最大的时间段数量
int MAX_SLICE_NUM;

// 维护磁盘i中第j个页
Page pages[MAX_DISK_NUM][MAX_PAGE_NUM + 1];
// 维护磁盘i中为空的页号
Deque st_empty[MAX_DISK_NUM];
// 维护磁盘i中半空半满的页号
Deque st_half[MAX_DISK_NUM];
// 维护磁盘i中全满的页号
Deque st_full[MAX_DISK_NUM];
// 维护页的绑定关系
PageBinding bindings[MAX_DISK_NUM][MAX_PAGE_NUM + 1];
// 指向每个唯一页绑定关系的指针数组 (每个绑定关系中的第一个)
std::vector<PageBinding*> unique_bindings;
// unique_bindings的数量 （等于unique_bindings.size() - 1）
int UNIQUE_NUM;
// 维护当前仍有效的读请求
Deque unfinished;
// 维护历史所有请求的完成状态 (0:未完成, 1:已完成)
bool request_finished[MAX_REQUEST_NUM];
// 维护历史所有请求的删除状态 (0:未删除, 1:已删除)
bool request_deleted[MAX_REQUEST_NUM];
// 为了省时间，保存从(disk_id, position)到(b_id, b_position)的映射
std::pair<int, int> b_map[MAX_DISK_NUM][MAX_DISK_SIZE];
// 有效页b_id的锁情况 (位锁)
int b_locks[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 2];




/**
 *
 *  需要在每个时间片初始化的变量
 *
 */
// 维护磁盘i中第j块的权重
int b_weights[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 2];
// 权重的前缀和
int b_s_weights[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 2];
// // 期望的令牌消耗量前缀和
// int b_s_expected_tokens[MAX_N * MAX_PAGE_NUM + 1][MAX_DISK_SIZE];
// 维护磁盘i中第j块，在当前时间片是否被读出来
int b_visited[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 2];

// 每个时间片删除的对象id列表
std::vector<int> deleted_object_ids;
// 每个时间片新增的对象id列表
std::vector<int> added_object_ids;
// 每个时间片新增的请求id列表
std::vector<int> added_request_ids;
// 每个时间片完成的请求id列表
std::vector<int> finished_request_ids;
// // 每个时间片中，当前页的偏移量（防止多个磁头竞争同一页）
// int offsets[MAX_DISK_NUM][MAX_PAGE_NUM + 1];
// 当前时间片的磁头移动结果 (用于输出)
std::string result[MAX_DISK_NUM];




/**
 *
 *  需要在每个时间段初始化的变量
 *
 */
// 每个类的重要性权重
// float importance[MAX_M];
// 类i和类j在未来R个时间段内，读行为上的相似性
double sim_read[MAX_M][MAX_M];
// 类i和类j在未来R个时间段内，写行为上的相似性
double sim_write[MAX_M][MAX_M];
// 类i和类j在未来R个时间段内，删行为上的相似性
double sim_del[MAX_M][MAX_M];
// 类i和类j在未来R个时间段内，总体上的相似性
double sim[MAX_M][MAX_M];
// 类j在类i在未来R个时间段内，总体相似性上的排序
int sim_rank[MAX_M][MAX_M];


/**
 *
 *  初始化与工具函数
 *
 */
// 工具函数声明
void init();
void init_per_timestamp();
void update_unfinished();
Page_* find_page_(int disk_id, int position);
int find_now_slice_();
Page_* next_page_(Page_ *now_page);
int time_decay(int timestamp);
int read_cost_(int last_status, int last_cost);
bool is_valid(int disk_id);
std::pair<int, int> map_disks_(int disk_id, int position);
bool is_disk_point_in_lock(int disk_id);
void update_page_frags(int disk_id, int page_index, int tag);
bool is_object_suitable_for_page(int disk_id, int page_index, Object *obj);


// 预输入与全局初始化
void init()
{
    scanf("%d%d%d%d%d", &T, &M, &N, &V, &G);
    for (int i = 1; i <= M; i++) {
        for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
            scanf("%d", &fre_del[i][j]);
        }
    }
    for (int i = 1; i <= M; i++) {
        for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
            scanf("%d", &fre_write[i][j]);
        }
    }
    for (int i = 1; i <= M; i++) {
        for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
            scanf("%d", &fre_read[i][j]);
        }
    }
    printf("OK\n");
    fflush(stdout);
    // 初始化随机种子
    std::srand(RANDOM_SEED);

    // 初始化常量
    JUMP_COST = G;
    PASS_COST = 1;
    MAX_SLICE_NUM = (T + FRE_PER_SLICING - 1) / FRE_PER_SLICING;

    // 自适应计算最优的PAGE_NUM和PAGE_SIZE
    assert(MIN_PAGE_NUM >= 1 && MIN_PAGE_NUM <= V);
    assert(MAX_PAGE_NUM >= 1 && MAX_PAGE_NUM <= V);
    assert(MIN_PAGE_NUM <= MAX_PAGE_NUM);


    // 找到浪费空间最少的PAGE_NUM
    int wasted = 1e9;
    for (int page_num = MIN_PAGE_NUM; page_num <= MAX_PAGE_NUM; page_num ++)
    {
        int page_size = V / page_num;
        int total_valid_pages =(N * page_num) - (N * page_num) % REP_NUM;
        int useful = total_valid_pages * page_size;
        if (V * N - useful < wasted)
        {
            wasted = V * N - useful;
            PAGE_NUM = page_num;
        }
    }
    PAGE_SIZE = V / PAGE_NUM;

    // printf("wasted rate is:\t%.3lf%%\n", (float)wasted / (float)(V * N) * 100);
    // printf("page_size is:\t%d\n", PAGE_SIZE);
    // printf("page_num is:\t%d\n", PAGE_NUM);


    // 最大的有效请求数 = 每个时间片完成读请求的能力上限 * 预留轮数
    MAX_DEQUE_SIZE = std::max(G / 16 / 1 * N * RESERVING_ROUNDS, 1);

    // 初始化磁头
    for (int i = 1; i <= N; i++) {
        disk_point[i] = 1;
    }

    //初始化页的绑定关系
    std::deque<std::pair<int, int> > tmp;
    for (int j = 1; j <= PAGE_NUM; j ++)
    {
        for (int i = 1; i <= N; i ++)
        {
            tmp.push_back(std::make_pair(i, j));
        }
    }

    // for (int j = 1; j <= 14; j ++)
    // {
    //     for (int i = 1; i <= 9; i ++)
    //     {
    //         tmp.push_back(std::make_pair(i, j));
    //     }
    // }
    // int t_disk_id = 1;
    // int t_page_index = 15;
    // for (int j = 1; j <= 18; j ++)
    // {
    //     tmp.push_back(std::make_pair(10, j));
    //     tmp.push_back(std::make_pair(t_disk_id, t_page_index));
    //     if (t_disk_id == 9)
    //     {
    //         t_disk_id = 1;
    //         t_page_index ++;
    //     }else
    //     {
    //         t_disk_id ++;
    //     }
    //     tmp.push_back(std::make_pair(t_disk_id, t_page_index));
    //     if (t_disk_id == 9)
    //     {
    //         t_disk_id = 1;
    //         t_page_index ++;
    //     }else
    //     {
    //         t_disk_id ++;
    //     }
    // }


    int tmp_disk_id[REP_NUM + 1], tmp_page_index[REP_NUM + 1];
    while (tmp.size() >= REP_NUM)
    {
        for (int i = 1; i <= REP_NUM; i ++)
        {
            std::pair<int, int> p = tmp.front();
            tmp.pop_front();
            tmp_disk_id[i] = p.first;
            tmp_page_index[i] = p.second;
        }
        unique_bindings.push_back(&bindings[tmp_disk_id[1]][tmp_page_index[1]]);
        for (int i = 1; i <= REP_NUM; i ++)
        {
            int disk_id = tmp_disk_id[i], page_index = tmp_page_index[i];

            bindings[disk_id][page_index].disk_id = disk_id;
            bindings[disk_id][page_index].page_index = page_index;
            bindings[disk_id][page_index].rep_id = i;
            bindings[disk_id][page_index].b_id = (int)unique_bindings.size();
            pages[disk_id][page_index].is_ok = true;

            for (int j = 1; j <= REP_NUM; j ++)
            {
                int n_disk_id = tmp_disk_id[j], n_page_index = tmp_page_index[j];
                bindings[disk_id][page_index].units[j] = &bindings[n_disk_id][n_page_index];
            }
        }
    }
    UNIQUE_NUM = (int)unique_bindings.size();
    unique_bindings.insert(unique_bindings.begin(), nullptr);

    // 初始化页表
    for (int i = 1; i <= N; i ++)
    {
        int capacy = 0;
        for (int j = 1; j <= PAGE_NUM; j ++)
        {
            if (! pages[i][j].is_ok)    continue;
            int position = (j - 1) * PAGE_SIZE + 1;
            // int page_size = j * PAGE_SIZE <= V? PAGE_SIZE: V - position + 1;

            pages[i][j].disk_id = i;
            pages[i][j].position = position;
            pages[i][j].page_size = PAGE_SIZE;
            pages[i][j].page_index = j;
            pages[i][j].usable_size = PAGE_SIZE;

            capacy += PAGE_SIZE;
        }
        usable_capacy[i] = capacy;
        valid_capacy[i] = capacy;
    }

    // Deque初始化
    unfinished.init(MAX_REQUEST_NUM);

    for (int i = 1; i <= N; i ++)
    {
        st_empty[i].init(MAX_PAGE_NUM + 1);
        st_half[i].init(MAX_PAGE_NUM + 1);
        st_full[i].init(MAX_PAGE_NUM + 1);
        for (int j = 1; j <= valid_capacy[i] / PAGE_SIZE; j ++)
        {
            st_empty[i].append(j);
        }
        // printf("%d, %d, %d\n", st_empty[i].size(), st_half[i].size(), st_full[i].size());
    }
    // b_map初始化
    for (int i = 1; i <= N; i++)
    {
        for (int j = 1; j <= V; j ++)
        {
            b_map[i][j] = map_disks_(i, j);
        }
    }
    // 存储空间的预划分
    int pre_capacy[MAX_M] = {0};
    double pre_capacy_percent[MAX_M] = {0};
    int sum = 0;
    for (int i = 1; i <= M; i ++)
    {
        pre_capacy[i] = 0;
        int last = 0;
        for (int j = 1; j <= MAX_SLICE_NUM; j ++)
        // for (int j = 1; j <= 1; j ++)
        {
            pre_capacy[i] = std::max(pre_capacy[i], last + fre_write[i][j]);
            last += (fre_write[i][j] - fre_del[i][j]);
            assert(last >= 0);
        }
        sum += pre_capacy[i];
    }
    for (int i = 1; i <= M; i ++)
    {
        pre_capacy_percent[i] = pre_capacy_percent[i - 1] + (double) pre_capacy[i] / (double) sum;
    }
    for (int i = 1; i <= N; i ++)
    {
        for (int j = 1; j <= usable_capacy[i]; j ++)
        {
            int tag = 0;
            for (int k = 1; k <= M; k ++)
            {
                double now_percent = (double) j / (double) usable_capacy[i];
                if (now_percent >= pre_capacy_percent[k - 1] && now_percent <= pre_capacy_percent[k])
                {
                    tag = k;
                    break;
                }
            }

            assert(tag >= 1);
            int b_id = b_map[i][j].first, b_pos = b_map[i][j].second;
            b_prefer[b_id][b_pos] = tag;
        }
    }


}

// 每个时间片的初始化
void init_per_timestamp()
{
    added_object_ids.clear();
    deleted_object_ids.clear();
    added_request_ids.clear();
    finished_request_ids.clear();
    for (int i = 1; i <= N; i ++)
    {
        result[i].clear();
    }

    for (int i = 1; i <= UNIQUE_NUM; i ++)
    {
        // range_mutex[i].reset();
        for (int j = 1; j <= PAGE_SIZE; j ++)
        {
            b_weights[i][j] = 0;
            b_visited[i][j] = 0;
            // b_s_weights[i][j] = 0;
            // b_s_expected_tokens[i][j] = 0;
        }
    }
}


// 每个时间段的初始化
void init_per_time_slice()
{
    if (! now_time % FRE_PER_SLICING == 1)  return;

    /**
     *  初始化当前的主要类和次要类列表
     *      根据未来R个时间段的统计信息，分析出累计概率分布达到
     */
    // 初始化sim_read数组
    now_slice = find_now_slice_();
    for (int i = 1; i <= M; i ++)
    {
        for (int j = 1; j <= M; j ++)
        {
            sim_read[i][j] = 0;
            // int s_i = 0, s_j = 0;
            // 计算未来R个时间段的海明距离之和
            for (int s = now_slice; s <= std::min(MAX_SLICE_NUM, now_slice + R - 1); s ++)
            {
                // s_i += fre_read[i][s];
                // s_j += fre_read[j][s];
                sim_read[i][j] += (double)abs(fre_read[i][s] - fre_read[j][s]);
                sim_write[i][j] += (double)abs(fre_write[i][s] - fre_write[j][s]);
                sim_del[i][j] += (double)abs(fre_del[i][s] - fre_del[j][s]);
            }
            // if (s_i / (std::min(MAX_SLICE_NUM, now_slice + R - 1) - now_slice + 1) <= 200 ||
            //     s_j / (std::min(MAX_SLICE_NUM, now_slice + R - 1) - now_slice + 1) <= 200 )
            // {
            //     sim_read[i][j] = 20000;
            // }
        }
    }

    // 为了方便，进行归一化
    for (int i = 1; i <= M; i ++)
    {
        double sum_read = 1;
        double sum_write = 1;
        double sum_del = 1;
        for (int j = 1; j <= M; j ++)
        {
            sum_read += sim_read[i][j];
            sum_write += sim_write[i][j];
            sum_del += sim_del[i][j];
        }
        for (int j = 1; j <= M; j ++)
        {
            sim_read[i][j] /= sum_read;
            sim_write[i][j] /= sum_write;
            sim_del[i][j] /= sum_del;

            sim[i][j] = ALPHA * sim_read[i][j] + BETA * sim_write[i][j] + GAMMA * sim_del[i][j];
        }
    }

    static std::pair<float, int> tmp[MAX_M];
    // 计算类别i的相似性排序为j的类别号
    for (int i = 1; i <= M; i ++)
    {
        for (int j = 1; j <= M; j ++)
        {
            tmp[j] = std::make_pair(sim[i][j], j);
        }
        std::sort(tmp + 1, tmp + 1 + M);

        // 根据rank填写sim_read_rank[i]
        for (int j = 1; j <= M; j ++)
        {
            sim_rank[i][tmp[j].second] = j;
        }
    }

}

// 更新unfinished链表
void update_unfinished()
{
    // 新增的Request
    for(int request_id: added_request_ids)
    {
        unfinished.append(request_id);
    }

    // 控制unfinished大小
    while (unfinished.size() > MAX_DEQUE_SIZE)
    {
        unfinished.pop_front();
    }
    // 过期的Request
    while (unfinished.size() > 0 && (now_time - requests[unfinished.front()].timestamp > MAX_ALIVE))
    {
        unfinished.pop_front();
    }
    // 删除的Object绑定的Request
    for (int object_id: deleted_object_ids)
    {
        int request_id = objects[object_id].last_request_point;
        while (request_id > 0)
        {
            unfinished.erase(request_id);
            request_id = requests[request_id].prev_id;
        }
    }
}

// 返回磁盘disk_id上position对应的页
Page_* find_page_(int disk_id, int position)
{
    return &pages[disk_id][(position - 1) / PAGE_SIZE + 1];
}

// 返回now_page的下一页
Page_* next_page_(Page_ *now_page)
{
    int disk_id = now_page -> disk_id, page_index = now_page -> page_index;
    Page_ *p = &pages[disk_id][page_index % PAGE_NUM + 1];
    return p -> is_ok? p: &pages[disk_id][1];
}

// 当前时间片所在的时间段下标
int find_now_slice_()
{
    return (now_time + FRE_PER_SLICING - 1) / FRE_PER_SLICING + 1;
}

// 从(disk_id, position)映射到(b_id, b_position)
std::pair<int, int> map_disks_(int disk_id, int position)
{
    if (position > valid_capacy[disk_id])    return std::make_pair(0, 0);
    Page_ *p = find_page_(disk_id, position);
    int b_id = bindings[p -> disk_id][p -> page_index].b_id;
    int b_position = position - p -> position + 1;
    return std::make_pair(b_id, b_position);
}


// 打分（int代替，乘了1000倍）
int time_decay(int timestamp){
    int delta = now_time - timestamp;
    if (delta <= 10)    return -5 * delta + 1000;
    if (delta <= 105)   return -10* delta + 1050;
    return 0;
}


// 根据磁头上一次的动作和令牌消耗，确定磁头这次读一个块的令牌消耗
int read_cost_(int last_status, int last_cost)
{
    /** 根据磁头上一时刻的状态和令牌消耗量计算
     */
    // return last_status <= 0 ? 64 : std::max(16, (int)std::ceil(0.8 * last_cost));;
    if (last_status <= 0)  return 64;
    if (last_cost == 64)    return 52;
    if (last_cost == 52)    return 42;
    if (last_cost == 42)    return 34;
    if (last_cost == 34)    return 28;
    if (last_cost == 28)    return 23;
    if (last_cost == 23)    return 19;
    return 16;
}

// 当前disk_id对应的磁头是否位于有效存储空间
bool is_valid(int disk_id)
{
    return disk_point[disk_id] <= valid_capacy[disk_id];
}

// 当前disk_id对应的磁头是否在他自己的位锁区间上
bool is_disk_point_in_lock(int disk_id)
{
    if (disk_point_start[disk_id] == 0 && disk_point_end[disk_id] == 0) return false;
    if (disk_point_start[disk_id] > disk_point_end[disk_id])
    {
        return disk_point[disk_id] >= disk_point_start[disk_id] && disk_point[disk_id] <= valid_capacy[disk_id] ||
               disk_point[disk_id] <= disk_point_end[disk_id] && disk_point[disk_id] >= 1;
    }
    return disk_point[disk_id] >= disk_point_start[disk_id] && disk_point[disk_id] <= disk_point_end[disk_id];
}

// 更新页(disk_id, page_index)上类型为tag的对象块的磁盘碎片数量
void update_page_frags(int disk_id, int page_index, int tag)
{
    Page_ *p = &pages[disk_id][page_index];
    assert(p -> is_ok);

    int frags_count = 0;
    int lst_position = 0;
    bool has_empty = false;
    for (int pos = p -> position; pos <= p -> position + p -> page_size - 1; pos ++)
    {
        int b_id = b_map[disk_id][pos].first, b_pos = b_map[disk_id][pos].second;
        if (b_disk[b_id][b_pos] == 0)
        {
            has_empty = true;
        }else if (b_disk[b_id][b_pos] == tag)
        {
            if (lst_position != 0 && has_empty)
            {
                frags_count ++;
            }
            lst_position = pos;
            has_empty = false;
        }
    }
    p -> frag[tag] = frags_count;
}

// 将obj写入当前binding时的gap_score （保证当前页一定能写进对象）
double try_obj_with_binding(Object *obj, PageBinding *binding, int mode)
{
    /**
     *  mode == 0:
     *      不写入obj到磁盘，只计算gap_score并返回
     *  mode == 1:
     *      写入obj，返回0
     *
     *  计算gap_score:
     *      1. 初始化队列q为空
     *      2. 根据sim_read_rank[obj -> tag]遍历每一个相似类别 now_tag，直到对象已经写完
     *          2.2 将左右两侧有一个块类型为 now_tag 的空位置加入q
     *          2.3 每次取队头位置pos，直到队列为空或者对象已经写完
     *              2.3.1 写入对象块block到pos
     *              2.3.2 计算block与相邻块neighbor的归一化相似度alpha = sim_read[obj -> tag][objects[neighbor].tag]
     *          2.4 gap_score += alpha
     */

    double gap_score = 0;
    int b_id = binding -> b_id;
    std::priority_queue<std::pair<double, int>, std::vector<std::pair<double, int> >, CmpPair1> q;
    std::vector<int> write_positions;
    static bool visited[MAX_DISK_SIZE / MIN_PAGE_NUM + 2];

    for (int b_pos = 1; b_pos <= PAGE_SIZE; b_pos ++)
    {
        visited[b_pos] = false;
        // 当前位置不为空，则跳过
        if (b_disk[b_id][b_pos]) continue;
        // 相邻位置均为空，也跳过
        if (b_pos > 1 && b_pos < PAGE_SIZE && !b_disk[b_id][b_pos - 1] && !b_disk[b_id][b_pos + 1]) continue;

        // 计算当前这个位置的分数
        double alpha = 1e9;
        if (b_pos == 1)
        {
            // alpha = 1e-4;
            alpha = 1 + 1e-4;
        } else if (b_pos == PAGE_SIZE)
        {
            alpha = 2 + 1e-4;
            // alpha = 1 + 1e-4;
        } else
        {
            int left_tag = b_disk[b_id][b_pos - 1] != 0? objects[b_disk[b_id][b_pos - 1]].tag: 0;
            int right_tag = b_disk[b_id][b_pos + 1] != 0? objects[b_disk[b_id][b_pos + 1]].tag: 0;
            if (left_tag)   alpha = std::min(alpha, 1 + sim[obj -> tag][left_tag]);
            if (right_tag)  alpha = std::min(alpha, 1 + sim[obj -> tag][right_tag]);
            // if (left_tag)   alpha = std::min(alpha, (double)sim_rank[obj -> tag][left_tag] - 1);
            // if (right_tag)  alpha = std::min(alpha, (double)sim_rank[obj -> tag][right_tag] - 1);

        }
        // if (b_prefer[b_id][b_pos] == obj -> tag && now_time <= 900)
        // {
        //     alpha = 0;
        // }
        q.push(std::make_pair(alpha, b_pos));
        visited[b_pos] = true;
    }

    while (! q.empty() && write_positions.size() < obj -> size)
    {
        double alpha = q.top().first;
        int b_pos = q.top().second;
        q.pop();

        // 记录写入的位置和gap_score
        write_positions.push_back(b_pos);
        gap_score += alpha;

        // 搜索相邻的位置 (新增的位置分数都为0， 因为已经和一个obj的对象块相邻了)
        if (b_pos > 1 && !visited[b_pos - 1] && !b_disk[b_id][b_pos - 1])
        {
            // if (b_prefer[b_id][b_pos - 1] == obj -> tag && now_time <= 900)
            // {
            //     q.push(std::make_pair(0, b_pos - 1));
            // }else
            // {
            //     q.push(std::make_pair(1, b_pos - 1));
            // }
            q.push(std::make_pair(1, b_pos - 1));
            visited[b_pos - 1] = true;
        }
        if (b_pos < PAGE_SIZE && !visited[b_pos + 1] && !b_disk[b_id][b_pos + 1])
        {
            // if (b_prefer[b_id][b_pos + 1] == obj -> tag && now_time <= 900)
            // {
            //     q.push(std::make_pair(0, b_pos + 1));
            // }else
            // {
            //     q.push(std::make_pair(1, b_pos + 1));
            // }
            q.push(std::make_pair(1, b_pos + 1));
            visited[b_pos + 1] = true;
        }
    }

    assert(write_positions.size() == obj -> size);

    // mode == 0    只返回分数
    if (mode == 0) return gap_score;

    // mode == 1    按顺序写入对象块
    if (mode == 1)
    {
        int write_num = 0;
        for (int b_pos: write_positions)
        {
            write_num ++;
            b_disk[b_id][b_pos] = obj -> id;

            for (int rep_id = 1; rep_id <= REP_NUM; rep_id ++)
            {
                int offset = pages[binding -> units[rep_id] -> disk_id][binding -> units[rep_id] -> page_index].position - 1;

                // 维护对象信息
                obj -> unit[rep_id][write_num] = offset + b_pos;
            }
        }
    }
    return 0;
}


/**
 *
 *  时间片对齐事件
 *
 */
void timestamp_action()
{
    int timestamp;
    scanf("%*s%d", &timestamp);
    printf("TIMESTAMP %d\n", timestamp);

    fflush(stdout);
}



/**
 *
 *  删除事件
 *
 */
// 删除obj，维护obj的相关信息
void do_object_delete(Object *obj)
{
    /** 1. 在硬盘中删除obj的3个副本，更新硬盘信息disk
     *  2. 更新obj信息
     *  3. 更新可用空间usable_capacy
     *  4. 更新页信息pages
     *  5. 更新st
     */


    // 更新obj
    // 只用删除一次就行了
    PageBinding_ *binding = obj -> binding;
    assert(binding -> rep_id == 1);

    for (int j = 1; j <= obj -> size; j ++) {
        // 更新disk
        int pos = obj -> unit[1][j] - pages[binding -> disk_id][binding -> page_index].position + 1;
        b_disk[binding -> b_id][pos] = 0;
    }

    for (int i = 1; i <= REP_NUM; i ++)
    {
        // 更新pages
        // Page_ *p = find_page_(disk_id, obj -> unit[i][1]);
        Page_ *p = &pages[binding -> units[i] ->disk_id][binding -> units[i] ->page_index];
        // int lst_max_tag = p -> max_tag;
        p -> cnt[obj -> tag] -= obj -> size;
        p -> usable_size += obj -> size;

        // 更新usable_capacy
        usable_capacy[p -> disk_id] += obj -> size;
        disk_tag_count[p -> disk_id][obj -> tag] -= obj -> size;
        update_page_frags(p -> disk_id, p -> page_index, obj -> tag);

        // 更新st
        if (p -> usable_size - obj -> size == 0)
        {
            // 原本处于full
            st_full[p -> disk_id].erase(p -> page_index);
            if (p -> usable_size == p -> page_size)
            {
                // 现在为empty
                st_empty[p -> disk_id].append(p -> page_index);
            } else
            {
                // 现在为half
                st_half[p -> disk_id].append(p -> page_index);
            }
        }else
        {
            // 原本处于half
            if (p -> usable_size == p -> page_size)
            {
                // 现在为empty
                st_half[p -> disk_id].erase(p -> page_index);
                st_empty[p -> disk_id].append(p -> page_index);
            }
        }

    }
    obj -> is_delete = true;
}


void delete_action()
{
    int n_delete;
    int abort_num = 0;
    static int _id[MAX_OBJECT_NUM];

    scanf("%d", &n_delete);
    for (int i = 1; i <= n_delete; i++) {
        scanf("%d", &_id[i]);
        deleted_object_ids.push_back(_id[i]);
    }

    for (int i = 1; i <= n_delete; i++) {
        int id = _id[i];
        int current_id = objects[id].last_request_point;
        while (current_id != 0) {
            if (request_finished[current_id] == false) {
                abort_num++;
            }
            current_id = requests[current_id].prev_id;
        }
    }

    printf("%d\n", abort_num);
    for (int i = 1; i <= n_delete; i++) {
        int id = _id[i];
        Object *obj = &objects[id];
        do_object_delete(obj);

        // 输出所有取消的请求编号，并设置标识位
        int current_id = obj -> last_request_point;
        while (current_id != 0) {
            if (request_finished[current_id] == false) {
                printf("%d\n", current_id);
                request_deleted[current_id] = true;
            }
            current_id = requests[current_id].prev_id;
        }
    }

    fflush(stdout);
}



/**
 *
 *  写事件
 *
 */
// 把对象obj写进磁盘，并更新存储信息和页信息（自动找到最合适的磁盘和页）
void object_write_(Object *obj)
{
    /** 1. 先随机选择一个初始位置，从这个位置开始遍历unique_bindings （随机选择起始位置是为了各个磁盘的负载均衡）
     *  2. 找到一个gap_score最小的binding，将该binding作为要写入的位置 （如果找到一个gap_score == 0 的，则不用向后遍历了）
     *  3. 在逻辑上，将对象obj写入binding对应的三个页；在实际存储上，将obj写入binding对应的b_disk
     *  4. 更新页信息pages
     *  5. 更新可用空间usable_capacy
     *  6. 更新st
     */


    // 随机生成一个遍历的起始位置
    PageBinding *binding = nullptr;
    int count_retry = 0;
    int start_b_id = std::rand() % UNIQUE_NUM + 1;
    int start_disk_id = unique_bindings[start_b_id] -> disk_id;
    double min_gap_score = 1e9;
    while (count_retry < UNIQUE_NUM)
    {
        int b_id = (start_b_id + count_retry - 1) % UNIQUE_NUM + 1;
        count_retry ++;
        PageBinding_ *now_binding = unique_bindings[b_id];
        Page_ *p_first = &pages[now_binding -> disk_id][now_binding -> page_index];
        if (p_first -> usable_size < obj -> size)   continue;
        // if (now_binding -> disk_id != start_disk_id)   continue;

        // 先计算gap_score，模式为0
        double now_gap_score = try_obj_with_binding(obj, now_binding, 0);
        if (now_gap_score < min_gap_score)
        {
            min_gap_score = now_gap_score;
            binding = now_binding;

            if (min_gap_score == 0) break;
        }
    }
    assert(binding != nullptr);
    obj -> binding = binding;


    // 设置obj第rep_id个副本的磁盘号，分配对象块到磁盘位置映射的内存
    for (int rep_id = 1; rep_id <= REP_NUM; rep_id ++)
    {
        // 设置对象obj第rep_id个副本所在的磁盘号
        obj -> replica[rep_id] = pages[binding -> units[rep_id] -> disk_id][binding -> units[rep_id] -> page_index].disk_id;
        // 为对象obj的第rep_id个副本信息 申请存储空间
        obj -> unit[rep_id] = static_cast<int*>(malloc(sizeof(int) * (obj -> size + 1)));
    }
    // 写入，模式改为1
    try_obj_with_binding(obj, binding, 1);



    // 更新页表信息、磁盘信息
    for (int rep_id = 1; rep_id <= REP_NUM; rep_id ++)
    {
        Page_ *q = &pages[binding -> units[rep_id] -> disk_id][binding -> units[rep_id] -> page_index];

        // 维护页信息
        q -> cnt[obj -> tag] += obj -> size;
        q -> usable_size -= obj -> size;

        // 维护磁盘剩余容量
        usable_capacy[q -> disk_id] -= obj -> size;
        disk_tag_count[q -> disk_id][obj -> tag] += obj -> size;
        update_page_frags(q -> disk_id, q -> page_index, obj -> tag);

        // 更新st
        if (q -> page_size - q -> usable_size == obj -> size)
        {
            // 原本处于empty
            st_empty[q -> disk_id].erase(q -> page_index);
            if (q -> usable_size == 0)
            {
                // 现在为full
                st_full[q -> disk_id].append(q -> page_index);
            } else
            {
                // 现在为half
                st_half[q -> disk_id].append(q -> page_index);
            }
        }else
        {
            // 原本处于half
            if (q -> usable_size == 0)
            {
                // 现在为full
                st_half[q -> disk_id].erase(q -> page_index);
                st_full[q -> disk_id].append(q -> page_index);
            }
        }
    }
}


// 写这个时间片的全部对象
void do_objects_write()
{
    /** 写策略
     *  1. 选择一个硬盘含同类对象最少的硬盘
     *  2. 在这个硬盘上找到一个合适的页page，并找到它对应的binding -> b_id -> unique_binding
     *  3. 逻辑上将这个对象obj写入binding对应的3个页
     *  4. 存储上将obj写入unique_binding对应的1个页
     */

    for (int object_id: added_object_ids)
    {
        Object *obj = &objects[object_id];
        object_write_(obj);
    }
}

void write_action()
{
    int n_write;
    scanf("%d", &n_write);
    // 同一次写请求中的对象先暂存起来，排序后再写

    for (int i = 1; i <= n_write; i++) {
        int id, size, tag;
        scanf("%d%d%d", &id, &size, &tag);
        objects[id].last_request_point = 0;
        objects[id].size = size;
        objects[id].tag = tag;
        objects[id].is_delete = false;
        objects[id].id = id;
        added_object_ids.push_back(id);
    }

    // 批量写入
    do_objects_write();

    for (int id: added_object_ids) {
        int size = objects[id].size;

        printf("%d\n", id);
        for (int j = 1; j <= REP_NUM; j++) {
            printf("%d", objects[id].replica[j]);
            for (int k = 1; k <= size; k++) {
                int x_tmp = objects[id].unit[j][k];
                printf(" %d", objects[id].unit[j][k]);
            }
            printf("\n");
        }
    }

    fflush(stdout);
}



/**
 *
 *  读事件
 *
 */
// 给r还未完成的块添加权重
void add_weights(Request *r)
{
    Object *obj = &objects[r -> object_id];
    for (int j = 1; j <= obj -> size; j ++)
    {
        if (r -> block_status.test(j))
        {
            // 这一块已经读过了
            continue;
        }
        // 添加贡献
        int b_pos = b_map[obj -> binding -> disk_id][obj -> unit[1][j]].second;
        b_weights[obj -> binding -> b_id][b_pos] += time_decay(r -> timestamp);
    }
}

// 给定状态的磁头，正位于(disk_id, position)，为了读到下一个有效块，找到下一步的最优行动方式
// (disk_id, position)当前如果不在有效页上，则直接pass
int best_way_to_move(int last_status, int last_cost, int disk_id, int start_position, int &tokens, bool ignore_locks=false)
{
    /**
     *  策略: 当前块的读写操作，应当取决于接下来几个块（包括当前块）中，是否存在有效块。
     *  理论推导：
    *          假设当前位置是1，那么：
     *              now_cost * (1 - 0.8^{k}) / (1 - 0.8) + 16 * (x - k) =
     *              (x - 8) * PASS_COST + 64 * (1 - 0.8^{8}) / (1 - 0.8)
     *
     *          常数8等于从64每次乘0.8衰减到16所需的次数
     *          x的含义是，接着一直读和先pass再读，这两种策略在第x轮的总花费相等
     *          k的含义是，如果接着一直读，那么第k轮的令牌花费能降低到16
     *          数值计算结果：
     *              now_cost * (1 - 0.8^{k}) * 5 + 15 * x - 16 * k = 270
     *              278 + 15x - 16 * 8 = 270   x=8.00
     *              214 +15x - 16 * 7 = 270    x=11.20
     *              162 +15x - 16 * 6 = 270    x=13.60
     *              120 +15x - 16 * 5 = 270    x=15.33
     *              86 +15x - 16 * 4 = 270     x=16.53
     *              58 +15x - 16 * 3 = 270     x=17.33
     *              35 +15x - 16 * 2 = 270     x=17.80
     *              16 +15x - 16 * 1 = 270     x=18.00
     *
     *          那么最后一个需要读的块，至少应该出现在[1, 下取整x - WINDOW_SIZE + 1]（闭区间）。
     *          计算过程中有取整，last_cost和k, x, 检查区间长度的对应关系是：
     *               now_cost   k       x       区间长度
     *                  64      8       23          1
     *                  52      7       18          4
     *                  42      6       12          6
     *                  34      5       9           8
     *                  28      4       8           9
     *                  23      3       6           10
     *                  19      2       3           10
     *                  16      1       1           11
     *          先带入k，再计算得到x，再看后面x个块的状态，决定是否改变
     *
     *  返回值
     *      0:  下一步选择pass   并扣减tokens
     *      1:  下一步选择read   并扣减tokens
     *      -1: 令牌数不够
     */

    // 理论最优的窗口大小
    // 实际情况并不理想，因此设置为一个可调参数WINDOW_SIZE
    int length;
    if (WINDOW_SIZE != 0)       length = WINDOW_SIZE;
    else if (last_status <= 0)  length = 1;
    else if (last_cost > 52)    length = 4;
    else if (last_cost > 42)    length = 6;
    else if (last_cost > 34)    length = 8;
    else if (last_cost > 28)    length = 9;
    else if (last_cost > 23)    length = 10;
    else if (last_cost > 19)    length = 10;
    else                        length = 11;

    if (tokens < PASS_COST) return -1;
    if (start_position > valid_capacy[disk_id])
    {
        tokens -= PASS_COST;
        return 0;
    }

    int move_type = 0;
    int end_position = std::min(start_position + length - 1, V);
    int window_weight;


    int start_b_id = b_map[disk_id][start_position].first;
    int start_b_pos = b_map[disk_id][start_position].second;
    int end_b_id = b_map[disk_id][end_position].first;
    int end_b_pos = b_map[disk_id][end_position].second;

    if (ignore_locks)
    {
        if (start_b_id == end_b_id)
        {
            // start_position到end_position没有跨页，可以直接前缀和计算
            window_weight = b_s_weights[end_b_id][end_b_pos] - b_s_weights[start_b_id][start_b_pos - 1];
        }else
        {
            window_weight = b_s_weights[end_b_id][end_b_pos] - 0 +
                            b_s_weights[start_b_id][PAGE_SIZE] - b_s_weights[start_b_id][start_b_pos - 1];
        }
        if (window_weight >= WEIGHT_THRESHOULD)
        {
            move_type = 1;
        }
        // for (int now_position = position; now_position <= end_position; now_position ++)
        // {
        //     int b_id = b_map[disk_id][now_position].first, b_pos = b_map[disk_id][now_position].second;
        //     if (b_weights[b_id][b_pos] >= WEIGHT_THRESHOULD)
        //     {
        //         move_type = 1;
        //         break;
        //     }
        // }
    }else
    {
        for (int now_position = start_position; now_position <= end_position; now_position ++)
        {
            int b_id = b_map[disk_id][now_position].first, b_pos = b_map[disk_id][now_position].second;
            if (b_weights[b_id][b_pos] >= WEIGHT_THRESHOULD && ! b_locks[b_id][b_pos])
            {
                move_type = 1;
                break;
            }
        }
    }


    if (move_type == 1)
    {
        if (tokens >= read_cost_(last_status, last_cost))
        {
            tokens -= read_cost_(last_status, last_cost);
            return 1;
        }
        return  -1;
    }
    tokens -= PASS_COST;
    return 0;
}


// 找到磁盘disk_id上，预期收益率最大的段(起始位置 + 结束位置)
DiskPointSchedule search_best_position(int disk_id)
{
    /** 策略
     *  1. 使用滑动窗口找到收益最高的段
     *  2. 窗口大小 = 磁头移动到窗口开头的成本 + 这个窗口扫完的预估成本
     *  3. 收益率 = 目标页的权重之和
     *
     */
    int res_start = 0;
    int res_end = 0;
    // int res_K = G * (int)((float)MAX_K * (1 - K_DECAY * now_slice));
    int res_K = 35;
    int max_score = 0;
    int last_status = disk_point_last_status[disk_id];
    int last_cost = disk_point_last_cost[disk_id];
    int cost_limit = G * (int)((float)MAX_K * (1 - K_DECAY * now_slice));
    // int position = 1;

    // 辅助数组，记录下每个位置的花费
    static int *pos_cost = new int[V + 1];

    // 滑动窗口
    int start = 1, end = 0;
    int now_score = 0;
    int window_cost = 0;
    int tokens_last = 1e9, tokens = 1e9;
    // int now_cost = std::min(JUMP_COST, ((position - disk_point[disk_id] + V) % V) * PASS_COST);


    while (end < valid_capacy[disk_id])
    {
        end ++;

        int move_type = best_way_to_move(last_status, last_cost, disk_id, end, tokens);
        assert(move_type >= 0);

        last_status = move_type;
        last_cost = (tokens_last - tokens);
        tokens_last = tokens;

        pos_cost[end] = last_cost;
        window_cost += last_cost;


        int b_id_end = b_map[disk_id][end].first, b_pos_end = b_map[disk_id][end].second;
        if (! b_locks[b_id_end][b_pos_end])
        {
            now_score += b_weights[b_id_end][b_pos_end];
            // now_score = now_score *
        }

        // int jump_cost = std::min(JUMP_COST, ((start - disk_point[disk_id] + V) % V) * PASS_COST);
        // int jump_cost = G;
        // int now_cost = window_cost + jump_cost;

        int b_id_start = b_map[disk_id][start].first, b_pos_start = b_map[disk_id][start].second;
        while (start < end && (window_cost > cost_limit ||
            b_weights[b_id_start][b_pos_start] < WEIGHT_THRESHOULD ||
            b_locks[b_id_start][b_pos_start]) )
        {
            // now_cost -= pos_cost[start];
            window_cost -= pos_cost[start];
            if (! b_locks[b_id_start][b_pos_start])
            {
                now_score -= b_weights[b_id_start][b_pos_start];
            }
            start ++;
            b_id_start = b_map[disk_id][start].first;
            b_pos_start = b_map[disk_id][start].second;
        }

        // 指针移动成本可能不是G，这种情况下需要修正
        // int tmp_window_cost = window_cost;
        // int tmp_last_status = last_status;
        // int tmp_last_cost = last_cost;

        int tmp_start = start;
        int tmp_end = end;
        int tmp_now_score = now_score;
        // 修正的令牌数
        int additional = G - std::min((tmp_start - disk_point[disk_id] + V) % V * PASS_COST, G);
        if (tmp_start > 0)
        {
            tmp_now_score += (int)((float)tmp_now_score * ((float)additional / (float)window_cost));
        }

        if (tmp_now_score > max_score)
        {
            max_score = tmp_now_score;
            res_start = tmp_start;
            res_end = tmp_end;
        }
    }

    disk_point_last_profit[disk_id] = max_score;
    disk_point_last_accumulated[disk_id] = 0;
    return DiskPointSchedule(res_start, res_end, res_K);
}



// 向前移动磁头(已经判断过剩余token足够)
void move_point(int disk_id, int move_type)
{
    if (move_type == 0)
    {
        // 下一块的操作为pass
        disk_point_last_cost[disk_id] = PASS_COST;
        disk_point_last_status[disk_id] = 0;
        result[disk_id].append("p");
    } else if (move_type == 1)
    {
        // 下一块的操作为read
        int b_id = b_map[disk_id][disk_point[disk_id]].first, b_pos = b_map[disk_id][disk_point[disk_id]].second;
        disk_point_last_cost[disk_id] = read_cost_(disk_point_last_status[disk_id], disk_point_last_cost[disk_id]);
        disk_point_last_status[disk_id] = std::max(1, disk_point_last_status[disk_id] + 1);
        b_visited[b_id][b_pos] ++;
        result[disk_id].append("r");
    }

    disk_point[disk_id] = disk_point[disk_id] % V + 1;
}

// 更新请求r的状态
bool update_request(Request *r)
{
    Object_ *obj = &objects[r -> object_id];
    std::bitset<MAX_BLOCKS> bits;
    PageBinding_ *binding = obj -> binding;
    for (int j = 1; j <= obj -> size; j ++)
    {
        int b_pos = b_map[binding -> disk_id][obj -> unit[1][j]].second;
        if (b_visited[binding -> b_id][b_pos])
        {
            bits.set(j);
        }
    }
    r -> block_status |= bits;
    if (r -> block_status.count() == obj -> size)
    {
        // r已经完成
        request_finished[r -> id] = true;
        return true;
    }
    return false;
}

// 1个时间片内，移动所有磁头，返回已完成的请求列表
void do_objects_read()
{
    /**
     *  TODO:优化策略
     *  1. 初始化
     *  2. 计算每个磁盘，每个块的权重
     *  3. 每个磁盘上根据权重选出当前磁盘中最优的页
     *  4. 根据策略移动磁头
     *  5. 更新状态信息
     *  6. 统计读取结果
     */

    // 遍历每个已有请求，贡献权重
    for (Node_ *p = unfinished.head ->next; p != unfinished.tail; p = p -> next)
    {
        add_weights(&requests[p -> id]);
    }
    // 维护权重的前缀和，维护指针跳转的开销
    for (int i = 1; i <= UNIQUE_NUM; i ++)
    {
        // // 假定每个有效页的起始位置状态为0或-1，先模拟走完这一页算出开销
        // PageBinding_ *binding = unique_bindings[i];
        // int last_status = 0;
        // int last_cost = 0;
        // int disk_id = binding -> disk_id;
        // int position = pages[binding -> disk_id][binding -> page_index].position;
        // int last_tokens = 1e9, tokens = 1e9;

        for (int j = 1; j <= PAGE_SIZE; j ++)
        {
            // int obj_tag = objects[b_disk[i][j]].tag;
            // b_weights[i][j] = (int)((float)b_weights[i][j] * importance[obj_tag]);
            b_s_weights[i][j] = b_s_weights[i][j - 1] + (b_weights[i][j] >= WEIGHT_THRESHOULD? b_weights[i][j]: 0);
            // int move_type = best_way_to_move(last_status, last_cost, disk_id, position, tokens);
            // if
            // b_s_expected_tokens[i][j] = last_tokens - tokens;
        }
    }

    // 移动各个磁头
    for (int i = 1; i <= N; i ++)
    {
        /**
         *  磁头移动策略
         *  1. 如果disk_point_k_status == 0, 说明磁头i的上个流程已经完成，可以去找新的位置读，找到位置后先跳转再把剩余令牌消耗完。
         *  2. 如果disk_point_k_status > 0, 那么磁头当前只需要接着读把令牌耗完就行。
         */
        int tokens = G;

        if (disk_point_k_status[i] == 0)
        {

            // 先释放磁头i的位锁
            if (disk_point_start[i] != 0 && disk_point_end[i] != 0)
            {
                for (int j = disk_point_start[i]; j <= std::min(disk_point_end[i], V); j ++)
                {
                    int b_id = b_map[i][j].first, b_pos = b_map[i][j].second;
                    b_locks[b_id][b_pos] --;
                }
                if (disk_point_start[i] > disk_point_end[i])
                {
                    for (int j = 1; j <= disk_point_end[i]; j ++)
                    {
                        int b_id = b_map[i][j].first, b_pos = b_map[i][j].second;
                        b_locks[b_id][b_pos] --;
                    }
                }
            }

            // 找到一个最优的段，磁头接下来K个时间片内的任务就是读这个段
            DiskPointSchedule schedule = search_best_position(i);
            disk_point_start[i] = schedule.start;
            disk_point_end[i] = schedule.end;
            disk_point_k_status[i] = (int)((float)schedule.K * (1 - K_DECAY * now_slice));
            // disk_point_k_status[i] = (int)((float)K * (1 - K_DECAY * now_slice));

            // 如果当前磁盘没有需要读的对象块，则把状态置0
            if (disk_point_start[i] == 0 || disk_point_end[i] == 0)
            {
                disk_point_k_status[i] = 0;
                continue;
            }

            // 给这个段加位锁
            for (int j = disk_point_start[i]; j <= std::min(disk_point_end[i], V); j ++)
            {
                int b_id = b_map[i][j].first, b_pos = b_map[i][j].second;
                b_locks[b_id][b_pos] ++;
            }
            if (disk_point_start[i] > disk_point_end[i])
            {
                for (int j = 1; j <= disk_point_end[i]; j ++)
                {
                    int b_id = b_map[i][j].first, b_pos = b_map[i][j].second;
                    b_locks[b_id][b_pos] --;
                }
            }

            // // 目标位置太远，直接跳
            // disk_point[i] = disk_point_start[i];
            // disk_point_last_status[i] = -1;
            // disk_point_last_cost[i] = JUMP_COST;
            // result[i].append("j ").append(std::to_string(disk_point[i]));
            // tokens = 0;

            // 磁头尝试先跳转到这个段
            if (JUMP_COST <= (disk_point_start[i] - disk_point[i] + V) % V * PASS_COST)
            {
                // 目标位置太远，直接跳
                disk_point[i] = disk_point_start[i];
                disk_point_last_status[i] = -1;
                disk_point_last_cost[i] = JUMP_COST;
                result[i].append("j ").append(std::to_string(disk_point[i]));
                tokens = 0;
            } else
            {
                // 目标页比较近，磁头自行决定怎么移到目标页的开头 （可能是全pass，也可能是全read）
                if ((disk_point_start[i] - disk_point[i] + V) % V == 1)
                {
                    int move_type = best_way_to_move(disk_point_last_status[i], disk_point_last_cost[i],
                        i, disk_point[i], tokens);
                    // assert(move_type >= 0);
                    if (move_type == 1)
                    {
                        int tmp_b_id = b_map[i][disk_point[i]].first, tmp_b_pos = b_map[i][disk_point[i]].second;
                        disk_point_last_accumulated[i] += b_weights[tmp_b_id][tmp_b_pos];
                    }
                    move_point(i, move_type);
                }
                while (disk_point[i] != disk_point_start[i])
                {
                    move_point(i, 0);
                    tokens -= PASS_COST;
                }
            }
        } else
        {
            disk_point_k_status[i] --;
        }


        if (! is_disk_point_in_lock(i))
        {
            disk_point_k_status[i] = 0;
        }
        // 把剩下的令牌全消耗完，如果中途超出锁范围就把disk_point_k_status 置0；
        while (true)
        {
            int move_type = best_way_to_move(disk_point_last_status[i], disk_point_last_cost[i],
                i, disk_point[i], tokens, true);
            if (move_type != -1)
            {
                if (move_type == 1)
                {
                    int tmp_b_id = b_map[i][disk_point[i]].first, tmp_b_pos = b_map[i][disk_point[i]].second;
                    disk_point_last_accumulated[i] += b_weights[tmp_b_id][tmp_b_pos];
                }
                move_point(i, move_type);
                if (! is_disk_point_in_lock(i))
                {
                    disk_point_k_status[i] = 0;
                }
            }else
            {
                break;
            }
        }
        // // 如果已经完成的分数占比超过 EARLY_STOP_THRESHOULD，则允许提前停止
        // if (((float) disk_point_last_accumulated[i] + 1)/ ((float) disk_point_last_profit[i] + 1) >= EARLY_STOP_THRESHOULD)
        // {
        //     disk_point_k_status[i] = 0;
        // }
    }


    // 把unfinished队列的每个请求拿出来，检查是否完成
    for (Node_ *p = unfinished.tail -> prev; p != unfinished.head; p = p -> prev)
    {
        Request_ *r = &requests[p -> id];
        if (update_request(r))
        {
            finished_request_ids.push_back(r -> id);
        }
    }
    for (int request_id: finished_request_ids)
    {
        unfinished.erase(request_id);
    }
}


void read_action()
{
    int n_read;
    int request_id, object_id;

    scanf("%d", &n_read);
    for (int i = 1; i <= n_read; i++) {
        scanf("%d%d", &request_id, &object_id);
        requests[request_id].id = request_id;
        requests[request_id].object_id = object_id;
        requests[request_id].prev_id = objects[object_id].last_request_point;
        requests[request_id].timestamp = now_time;
        objects[object_id].last_request_point = request_id;

        added_request_ids.push_back(request_id);
    }

    // 更新unfinished队列
    update_unfinished();

    // 在这一时间片上完成读操作
    do_objects_read();

    // 上报所有已完成的请求
    for (int i = 1; i <= N; i++) {
        if(result[i].empty() || result[i][0] != 'j')
        {
            result[i].append("#");
        }
        printf("%s\n", result[i].c_str());
    }
    printf("%d\n", (int)finished_request_ids.size());
    for (int _request_id: finished_request_ids)
    {
        printf("%d\n", _request_id);
    }
    fflush(stdout);
}

void clean()
{
    for (auto& obj : objects) {
        for (int i = 1; i <= REP_NUM; i++) {
            if (obj.unit[i] == nullptr)
                continue;
            free(obj.unit[i]);
            obj.unit[i] = nullptr;
        }
    }
}


void test()
{
    if (now_time == 1)
    {
        for(int i = 1; i <= N; i ++)
        {
            printf("\n_______________________________________________________________________________________________");
            printf("\nDISK %d:\n", i);
            printf("empty:\t");
            for (Node_ *node = st_empty[i].head -> next; node != st_empty[i].tail; node = node -> next)
            {
                printf(" %d", node -> id);
            }
            printf("\nhalf:\t");
            for (Node_ *node = st_half[i].head -> next; node != st_half[i].tail; node = node -> next)
            {
                printf(" %d", node -> id);
            }
            printf("\nfull:\t");
            for (Node_ *node = st_full[i].head -> next; node != st_full[i].tail; node = node -> next)
            {
                printf(" %d", node -> id);
            }
            fflush(stdout);
        }
        std::this_thread::sleep_for(std::chrono::seconds(1000000));

    }
}

void printDisks(){
    printf("Printing disks...\n");
    for (int i = 1; i <= N; i ++) {
        for(int j = 1; j <= V; j ++) {
            std::pair<int, int> v = b_map[i][j];
            printf("%d ", b_disk[v.first][v.second]);
        }
        printf("\n");
    }
}

int main()
{
    // freopen("../../data/sample_official.in","r",stdin);
    // freopen("../../data/sample.in","r",stdin);
    init();

    for (now_time = 1; now_time <= T + EXTRA_TIME; now_time++) {

        timestamp_action();

        init_per_timestamp();
        init_per_time_slice();

        delete_action();
        write_action();

        // printDisks();

        read_action();

        // test();
    }
    clean();

    return 0;
}