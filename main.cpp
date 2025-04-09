#include <cstdio>
#include <cassert>
#include <cstdlib>
#include <algorithm>
#include <vector>
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
#define MAX_POINT_NUM (2 + 1)
#define MAX_DISK_SIZE (16384 + 1)
#define MAX_REQUEST_NUM (30000000 + 1)
#define MAX_OBJECT_NUM (100000 + 1)
#define REP_NUM (3)
#define POINT_NUM (2)
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
#define MIN_PAGE_NUM (12)
// 期望的最大页数（1 ~ 3276 并且 >= MIN_PAGE_NUM）
#define MAX_PAGE_NUM (12)
// K参数 (1 ~ 256)：每个磁头以 不超过K 个时间片为单位进行一次决策(第1个时间片移动到目的地 + 顺序读取，后续若干时间片以最小开销策略顺序读取)
#define BASE_K (40)
// K参数衰减系数
#define K_DECAY (-0.004)
// R参数 (1 ~ 50)：每个时间段起始时刻，向后看R个时间段的统计信息
#define R (8)
// 窗口大小 (0 ~ 20)：作为顺序读取时贪心策略的参数，在当前位置先向后看WINDOW_SIZE个块，再决定当前是read还是pass
// (设为0时，不同位置采用自适应计算的不同窗口大小)
#define WINDOW_SIZE (0)
// 随机数种子：设置为0时调用当前系统时间作为种子
#define RANDOM_SEED (114514)
// 权重阈值（1 ~ INF）：如果当前块的权重小于该值就不读了，直接跳过
#define WEIGHT_THRESHOULD (3501)
// // 早停参数 (0 ~ 1)：当磁头累计实际读取的权重大于预期权重的 EARLY_STOP_THRESHOULD 倍时，提前结束掉当前磁头 K 轮循环的动作，释放磁头
// #define EARLY_STOP_THRESHOULD (2)
// 最长存活时间 (1 ~ 105)：一个读请求的最长存活时间，限制未完成请求队列的大小，剔除已过期的读请求
#define MAX_ALIVE (105)
// 最大处理能力 (1 ~ 105)：每个时间片都以最理想的情况读取时，可以保留多少个时间片的请求，限制未完成请求队列的大小，剔除在磁头处理能力之外的读请求
#define RESERVING_ROUNDS (105)
// 可用空间修正系数，用来修正对象写入时的评分，使对象倾向于写到剩余空间大的页里
#define SIGMA (1000.0)
// 分割长度，当存在连续SPLIT_LENGTH个权重<WEIGHT_THRESHOULD的块时，重置当前的滑动窗口
#define BASE_SPLIT_LENGTH (32)
// 分割长度的变化率
#define SPLIT_LENGTH_DECAY (0.000)
// 分数的修正值
#define FIX_SCORE (3500)
// 每个读请求块每过一个时间片，损失的分数
#define DECAY_SCORE (9.52381)


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
    // int cnt[MAX_M] = {0};   // 各个类型的对象块数量
    bool is_ok = false;     // 标识位，初始化后仍为false的页直接被弃用

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
typedef struct Node_
{
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

    // 每个Deque创建完后都要初始化，分配底层数组
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


// 预估的磁头扫描区域
struct DiskPointSchedule
{
    int start;  // 扫描的起始位置
    int end;    // 扫描的结束位置
    int K;      // 预估完成这次扫描所需的时间片数量
    DiskPointSchedule() = default;
    DiskPointSchedule(int _start, int _end, int K_): start(_start), end(_end), K(K_){}
};

// 定义段
struct Segment
{
    int start;
    int end;
    int tag;
    int b_id;
    int idx;
    Segment() = default;
    Segment(int _start, int _end, int _tag, int _b_id): start(_start), end(_end), tag(_tag), b_id(_b_id), idx(0){}
    Segment(int _start, int _end, int _tag, int _b_id, int _idx): start(_start), end(_end), tag(_tag), b_id(_b_id), idx(_idx){}
};


// 定义将对象块写入空段时，产生收益的排序关系
struct CmpWrite
{
    // 给堆用，顺序反过来
    bool operator()(const std::pair<float, Segment>& a, const std::pair<float, Segment>& b) {
        if (abs(a.first - b.first) < 1e-6)
        {
            if (a.second.end - a.second.start == b.second.end - b.second.start)
            {
                // 返回下标小的
                return a.second.start > b.second.start;
            }
            // 返回length大的
            return a.second.end - a.second.start < b.second.end - b.second.start;
        }
        // 返回profit最大的
        return a.first < b.first;
    }
};

// 定义交换两个段时，产生收益的排序关系
struct CmpGC
{
    // 给堆用，顺序反过来
    bool operator()(const std::pair<float, Segment>& a, const std::pair<float, Segment>& b) {
        if (abs(a.first - b.first) < 1e-6)
        {
            // 返回位置最小的
            return a.second.start > b.second.start;
        }
        // 返回profit最大的
        return a.first < b.first;
    }
};

/**
 *
 *  需要全局维护的变量
 *
 */
int T, M, N, V, G, C;
int fre_del[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
int fre_write[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
int fre_read[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
float percent_del[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
float percent_write[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
float percent_read[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
Request requests[MAX_REQUEST_NUM];
Object objects[MAX_OBJECT_NUM];
int now_time;
int now_slice;

// 一个磁盘划分的页数量
int PAGE_NUM;
// 页大小（每个磁盘最后一页的大小可能不等于PAGE_SIZE）
int PAGE_SIZE;
// 磁盘存储状态
int b_disk[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 3];
// // 位置的预划分
// int b_prefer[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 2];
// 磁头当前位置
int disk_point[MAX_DISK_NUM][MAX_POINT_NUM];
// 磁头当前的状态 (k>0: 磁头已经连续读了k次, k=0: 磁头上次pass了, k=-1: 磁头上次是jump过来的)
int disk_point_last_status[MAX_DISK_NUM][MAX_POINT_NUM];
// 磁头上次消耗的令牌数
int disk_point_last_cost[MAX_DISK_NUM][MAX_POINT_NUM];
// 当前的磁头还需要接着读K个时间片(0 ~ K, 0代表磁头不做任何动作)
int disk_point_k_status[MAX_DISK_NUM][MAX_POINT_NUM];
// 当前磁头disk_id 期望的起点
int disk_point_start[MAX_DISK_NUM][MAX_POINT_NUM];
// 当前磁头disk_id 期望的终点
int disk_point_end[MAX_DISK_NUM][MAX_POINT_NUM];

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
// // 允许的最大unfinished大小，可以控制代码运行时间
// int MAX_DEQUE_SIZE;
// 最大的时间段数量
int MAX_SLICE_NUM;

// 维护磁盘i中第j个页
Page pages[MAX_DISK_NUM][MAX_PAGE_NUM + 1];
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
int b_locks[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 3];
// 每个块上的有效读请求数，上次更新距离现在的时间片数量
int b_dirty[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 3];
// 每个块上的有效读请求数量
int b_active_num[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 3];


/**
 *
 *  需要在每个时间片初始化的变量
 *
 */
// 维护磁盘i中第j块的权重
float b_weights[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 3];
// // 权重的前缀和
// int b_s_weights[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 3];
// // 期望的令牌消耗量前缀和
// int b_s_expected_tokens[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE];
// 维护磁盘i中第j块，在当前时间片是否被读出来
int b_visited[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 3];

// 每个时间片删除的对象id列表
std::vector<int> deleted_object_ids;
// 每个时间片新增的对象id列表
std::vector<int> added_object_ids;
// 每个时间片新增的请求id列表
std::vector<int> added_request_ids;
// 每个时间片删除的请求id列表
std::vector<int> deleted_request_ids;
// 每个时间片完成的请求id列表
std::vector<int> finished_request_ids;
// 每个时间片放弃（繁忙）的请求id列表
std::vector<int> dropped_request_ids;
// 当前时间片的磁头移动结果 (用于输出)
std::string result[MAX_DISK_NUM][MAX_POINT_NUM];

// 该时间片被读到过的对象块对应object_id
std::vector<int> has_read;



/**
 *
 *  需要在每个时间段初始化的变量
 *
 */
// 每个类的重要性权重
// float importance[MAX_M];
// 类i和类j在未来R个时间段内，读行为上的相似性
float gap_read[MAX_M][MAX_M];
// 类i和类j在未来R个时间段内，写行为上的相似性
float gap_write[MAX_M][MAX_M];
// 类i和类j在未来R个时间段内，删行为上的相似性
float gap_del[MAX_M][MAX_M];
// 类i和类j在未来R个时间段内，总体上的相似性
float gap[MAX_M][MAX_M];
// 类j在类i在未来R个时间段内，总体相似性上的排序
int gap_rank[MAX_M][MAX_M];
// 每个磁盘清理的结果
std::vector<std::pair<int, int> > gc_result[MAX_DISK_NUM];
// 各个磁盘的gc次数限制
int gc_limits[MAX_DISK_NUM];
// gc过程中，标记某个块是否被gc
int gc_visited[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_DISK_SIZE / MIN_PAGE_NUM + 3];
// gc过程中，标记页上某个类的最后一个块的位置
int gc_m_pos[MAX_DISK_NUM * MAX_PAGE_NUM + 1][MAX_M];

/**
 *
 *  初始化与工具函数
 *
 */
// 工具函数声明
void init();
void init_per_timestamp();
void update_unfinished();
void update_active(int object_id);
Page_* find_page_(int disk_id, int position);
int find_now_slice_();
Page_* next_page_(Page_ *now_page);
float time_decay(int current_time, int past_time);
int read_cost_(int last_status, int last_cost);
bool is_valid(int disk_id, int p_id);
std::pair<int, int> map_disks_(int disk_id, int position);
bool is_disk_point_in_lock(int disk_id, int p_id);
std::vector<Segment> get_disk_segments(int b_id);


// 预输入与全局初始化
void init()
{
    scanf("%d%d%d%d%d%d", &T, &M, &N, &V, &G, &C);
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


    // // 最大的有效请求数 = 每个时间片完成读请求的能力上限 * 预留轮数
    // MAX_DEQUE_SIZE = std::max(G / 16 / 1 * N * RESERVING_ROUNDS, 1);

    // 初始化磁头
    for (int i = 1; i <= N; i++) {
        for (int j = 1; j <= POINT_NUM; j ++)
        {
            disk_point[i][j] = 1;
        }
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

    // b_map初始化
    for (int i = 1; i <= N; i++)
    {
        for (int j = 1; j <= V; j ++)
        {
            b_map[i][j] = map_disks_(i, j);
        }
    }

    // 计算各个时间段，个类别各种操作的占比
    for (int s = 1; s <= MAX_SLICE_NUM; s ++)
    {
        int sum_del = 0, sum_write = 0, sum_read = 0;
        for (int i = 1; i <= M; i ++)
        {
            sum_del += fre_del[i][s];
            sum_write += fre_write[i][s];
            sum_read += fre_read[i][s];
        }
        for (int i = 1; i <= M; i ++)
        {
            percent_del[i][s] = (float)fre_del[i][s] / (float)sum_del;
            percent_write[i][s] = (float)fre_write[i][s] / (float) sum_write;
            percent_read[i][s] = (float)fre_read[i][s] / (float) sum_read;
        }
    }
    // 初始化b_dirty数组
    for(int i = 1; i <= UNIQUE_NUM; i ++)
    {
        for (int j = 1; j <= PAGE_SIZE; j ++)
        {
            b_dirty[i][j] = 1e9;
        }
    }

}

// 每个时间片的初始化
void init_per_timestamp()
{
    added_object_ids.clear();
    deleted_object_ids.clear();
    added_request_ids.clear();
    deleted_request_ids.clear();
    finished_request_ids.clear();
    dropped_request_ids.clear();
    has_read.clear();
    for (int i = 1; i <= N; i ++)
    {
        for (int j = 1; j <= POINT_NUM; j ++)
        {
            result[i][j].clear();
        }
    }

    for (int i = 1; i <= UNIQUE_NUM; i ++)
    {
        // range_mutex[i].reset();
        for (int j = 1; j <= PAGE_SIZE; j ++)
        {
            // b_weights[i][j] = 0;
            b_visited[i][j] = 0;
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
    now_slice = find_now_slice_();
    for (int i = 1; i <= M; i ++)
    {
        for (int j = 1; j <= M; j ++)
        {
            gap_read[i][j] = 0;
            // int s_i = 0, s_j = 0;
            // 计算未来R个时间段的海明距离之和
            for (int s = now_slice; s <= std::min(MAX_SLICE_NUM, now_slice + R - 1); s ++)
            {
                // s_i += fre_read[i][s];
                // s_j += fre_read[j][s];
                gap_read[i][j] += (float)abs(fre_read[i][s] - fre_read[j][s]);
                gap_write[i][j] += (float)abs(fre_write[i][s] - fre_write[j][s]);
                gap_del[i][j] += (float)abs(fre_del[i][s] - fre_del[j][s]);
            }
        }
    }

    // 为了方便，进行归一化
    for (int i = 1; i <= M; i ++)
    {
        float sum_read = 1;
        float sum_write = 1;
        float sum_del = 1;
        for (int j = 1; j <= M; j ++)
        {
            sum_read += gap_read[i][j];
            sum_write += gap_write[i][j];
            sum_del += gap_del[i][j];
        }
        for (int j = 1; j <= M; j ++)
        {
            gap_read[i][j] /= sum_read;
            gap_write[i][j] /= sum_write;
            gap_del[i][j] /= sum_del;

            gap[i][j] = ALPHA * gap_read[i][j] + BETA * gap_write[i][j] + GAMMA * gap_del[i][j];
            if (i != j)
            {
                gap[i][j] += 10;
                // gap[i][j] = 1;
            }
        }
        // gap[i][0] = gap[0][i] = 0.09;
        gap[i][0] = gap[0][i] = 0.01;
    }

    static std::pair<float, int> tmp[MAX_M];
    // 计算类别i的相似性排序为j的类别号
    for (int i = 1; i <= M; i ++)
    {
        for (int j = 1; j <= M; j ++)
        {
            tmp[j] = std::make_pair(gap[i][j], j);
        }
        std::sort(tmp + 1, tmp + 1 + M);

        // 根据rank填写sim_read_rank[i]
        for (int j = 1; j <= M; j ++)
        {
            gap_rank[i][tmp[j].second] = j;
        }
    }
    // 初始化gc相关变量
    for (int i = 1; i <= N; i ++)
    {
        gc_result[i].clear();
        gc_limits[i] = C;
    }
    for (int i = 1; i <= UNIQUE_NUM; i ++)
    {
        for (int j = 1; j <= V; j ++)
        {
            gc_visited[i][j] = 0;
        }
        for (int j = 1; j <= M; j ++)
        {
            gc_m_pos[i][j] = PAGE_SIZE;
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
        update_active(requests[request_id].object_id);
    }

    // 删除的Object绑定的Request
    for (int object_id: deleted_object_ids)
    {
        update_active(object_id);
        int request_id = objects[object_id].last_request_point;
        while (request_id > 0)
        {
            unfinished.erase(request_id);

            request_id = requests[request_id].prev_id;
        }
    }
}

// 更新b_active_num
void update_active(int object_id)
{
    /**
     *  direction:
     *      1:  新增
     *      -1:  删除
     */
    Object *obj = &objects[object_id];
    for (int i = 1; i <= obj -> size; i ++)
    {
        int b_id = b_map[obj -> replica[1]][obj -> unit[1][i]].first, b_pos = b_map[obj -> replica[1]][obj -> unit[1][i]].second;
        b_dirty[b_id][b_pos] = 1e9;
        b_active_num[b_id][b_pos] = 0;
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
float time_decay(int current_time, int past_time){
    float delta = (float)current_time - (float)past_time;
    if (delta <= 10)    return -5 * delta + 1000 + FIX_SCORE;
    if (delta <= 105)   return -10* delta + 1050 + FIX_SCORE;

    // int delta = current_time - past_time;
    // if (delta <= 105)   return - (int)(9.52381 * delta) + 1000;
    return 0;
}


// 根据磁头上一次的动作和令牌消耗，确定磁头这次读一个块的令牌消耗
int read_cost_(int last_status, int last_cost)
{
    /** 根据磁头上一时刻的状态和令牌消耗量计算
     */
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
bool is_valid(int disk_id, int p_id)
{
    return disk_point[disk_id][p_id] <= valid_capacy[disk_id];
}


// 当前disk_id对应的磁头是否在他自己的位锁区间上
bool is_disk_point_in_lock(int disk_id, int p_id)
{
    if (disk_point_start[disk_id][p_id] == 0 && disk_point_end[disk_id][p_id] == 0) return false;
    if (disk_point_start[disk_id][p_id] > disk_point_end[disk_id][p_id])
    {
        return disk_point[disk_id][p_id] >= disk_point_start[disk_id][p_id] && disk_point[disk_id][p_id] <= valid_capacy[disk_id] ||
               disk_point[disk_id][p_id] <= disk_point_end[disk_id][p_id] && disk_point[disk_id][p_id] >= 1;
    }
    return disk_point[disk_id][p_id] >= disk_point_start[disk_id][p_id] && disk_point[disk_id][p_id] <= disk_point_end[disk_id][p_id];
}


// 将页b_id进行分段
std::vector<Segment> get_disk_segments(int b_id)
{
    std::vector<Segment> res;
    // 开头段
    res.push_back(Segment(0, 0, -1, b_id, 0));

    int l = 1, r = 2, last_tag = objects[b_disk[b_id][1]].tag;
    for (; r <= PAGE_SIZE + 1; r ++)
    {
        int tag = r <= PAGE_SIZE? objects[b_disk[b_id][r]].tag: -1;
        if (tag == last_tag) continue;
        Segment now = Segment(l, r - 1, last_tag, b_id, (int)res.size());
        res.push_back(now);
        l = r;
        last_tag = tag;
    }
    // 结尾段
    res.push_back(Segment(PAGE_SIZE + 1, PAGE_SIZE + 1, -1, b_id, (int)res.size()));
    return res;
}

int get_tag(int b_id, int b_pos)
{
    return objects[b_disk[b_id][b_pos]].tag;
}


int find_expected_tag(Segment &segment)
{
    /**
     *  找到空段segment期望填充的标签id
     *  如果两侧标签不同，返回-1
     */
    int b_id = segment.b_id;

    // assert(segment.tag == 0);
    int left_tag = segment.start > 1? objects[b_disk[b_id][segment.start - 1]].tag: -1;
    int right_tag = segment.end < PAGE_SIZE? objects[b_disk[b_id][segment.end + 1]].tag: -1;
    assert(! (left_tag == -1 && right_tag == -1));

    if (left_tag == right_tag)  return left_tag;
    if (left_tag == -1)     return right_tag;
    if (right_tag == -1)    return left_tag;
    return -1;

    // int b_id = segment.b_id;
    //
    // assert(segment.tag == 0);
    // int left_tag = segment.start > 1? objects[b_disk[b_id][segment.start - 1]].tag: -1;
    // int right_tag = segment.end < PAGE_SIZE? objects[b_disk[b_id][segment.end + 1]].tag: -1;
    //
    // if (left_tag == right_tag && left_tag != -1)  return left_tag;
    // return -1;
}


// 计算以b为中心的局部gap_score
float cal_gap_score(Segment &a, Segment &b, Segment &c)
{
    // return (a.start != 0 && a.tag != b.tag) + (b.end != PAGE_SIZE + 1 && b.tag != c.tag);
    float res = 0;
    if (a.start != 0)   res += gap[a.tag][b.tag];
    if (c.end != PAGE_SIZE + 1) res += gap[b.tag][c.tag];
    return res;
}


// 计算将obj写入当前binding的gap_score （保证当前页一定能写进对象）
std::pair<float, std::vector<int> > try_obj_with_binding(Object *obj, PageBinding *binding)
{
    /**
     *  返回gap_score和要写入的位置
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

    float total_profit = 0;
    int b_id = binding -> b_id;
    Page *p = &pages[binding -> disk_id][binding -> page_index];
    std::vector<Segment> segments = get_disk_segments(b_id);
    std::priority_queue<std::pair<float, Segment>, std::vector<std::pair<float, Segment> >, CmpWrite> q;
    // std::priority_queue<std::pair<int, Segment>, std::vector<std::pair<int, Segment> >, CmpWrite> q;
    std::vector<int> write_positions;

    for (int i = 1; i <= segments.size() - 2; i ++)
    {
        if (segments[i].tag != 0)    continue;
        Segment left = segments[i - 1], mid = segments[i], right = segments[i + 1];
        float gap_before = cal_gap_score(left, mid, right);

        // 写入后的段
        mid.tag = obj -> tag;

        float gap_after = cal_gap_score(left, mid, right);
        float profit = gap_before - gap_after;
        q.push(std::make_pair(profit, segments[i]));
    }


    while (! q.empty() && write_positions.size() < obj -> size)
    {
        float profit = q.top().first;
        Segment segment = q.top().second;
        q.pop();

        // 记录写入的位置和gap_score
        int left_size = obj -> size - (int)write_positions.size();
        int length = segment.end - segment.start + 1;
        int idx = segment.idx;
        if (left_size < length)
        {
            // 实际上没有填满，这时需要重新计算profit
            Segment left = segments[idx - 1], mid = segments[idx], right = segments[idx];
            mid.end = mid.start + left_size - 1;
            right.start = mid.end + 1;
            float gap_before = cal_gap_score(left, mid, right);

            mid.tag = obj -> tag;

            float gap_after = cal_gap_score(left, mid, right);
            profit = gap_before - gap_after;
        }
        for (int j = 1; j <= std::min(left_size, length); j ++)
        {
            write_positions.push_back(segment.start + j - 1);
        }

        // // 修正profit
        // if (profit > 0) profit += PAGE_SIZE;
        // else if (profit == 0)
        // {
        //     // profit =  profit + (profit) * 2 / (float)((length + 1));
        //     profit = length;
        // }else
        // {
        //     profit = profit + (profit) * 2 / (float)((length + 1));
        // }

        total_profit += profit;
        // total_profit += profit + (profit) * 2 / ((length + 1));


        // if (profit < -1)
        // {
        //     bool flag = false;
        //     // 新增与其它某个类型的块相邻，从空段的中间开始填
        //     for (int j = 1; j <= std::min(left_size, length); j ++)
        //     {
        //         int b_pos = segment.start + (j - 1 + length / 2) % length;
        //         if (b_pos == segment.start || b_pos == segment.end) flag = true;
        //         write_positions.push_back(b_pos);
        //     }
        //     if (! flag) profit = - 2 * gap[obj -> tag][0];
        // }else
        // {
        //     for (int j = 1; j <= std::min(left_size, length); j ++)
        //     {
        //         write_positions.push_back(segment.start + j - 1);
        //     }
        // }
        // total_profit += profit;

    }

    // 修正项：写入块和不同类型块相邻时，剩余空间大小是一个加分项
    if (total_profit < -1)
    {
        total_profit = -10 + (total_profit) / ((float)(p -> usable_size + 1));
        // total_profit = total_profit + (total_profit) / ((p -> usable_size + 1)) + (total_profit) / ((p -> cnt[obj -> tag] + 1));
    }else if (total_profit < 0)
    {
        // total_profit = total_profit + (total_profit) / ((p -> usable_size + 1));
    }
    // total_profit = total_profit + (total_profit) * 2 / ((p -> usable_size + 1));


    assert(write_positions.size() == obj -> size);
    return std::make_pair(total_profit, write_positions);

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
        Page_ *p = &pages[binding -> units[i] ->disk_id][binding -> units[i] ->page_index];
        p -> usable_size += obj -> size;

        // 更新usable_capacy
        usable_capacy[p -> disk_id] += obj -> size;
        disk_tag_count[p -> disk_id][obj -> tag] -= obj -> size;

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
            if (request_finished[current_id] || request_deleted[current_id])    break;
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
            if (request_finished[current_id] || request_deleted[current_id])    break;
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

void write_obj_in_binding(Object *obj, PageBinding *binding, std::vector<int> &write_positions)
{
    int b_id = binding -> b_id;
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
    float max_profit = -1e9;
    std::vector<int> write_positions;
    while (count_retry < UNIQUE_NUM)
    {
        int b_id = (start_b_id + count_retry - 1) % UNIQUE_NUM + 1;
        count_retry ++;
        PageBinding_ *now_binding = unique_bindings[b_id];
        Page_ *p_first = &pages[now_binding -> disk_id][now_binding -> page_index];
        if (p_first -> usable_size < obj -> size)   continue;

        // 先计算profit
        auto tmp = try_obj_with_binding(obj, now_binding);
        float now_profit = tmp.first;
        if (now_profit > max_profit)
        {
            max_profit = now_profit;
            binding = now_binding;
            write_positions = tmp.second;
            // 剪枝 （profit必然 <=0）
            // if (max_profit >= 0) break;
        }
        // assert(now_profit <= 0);
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
    // 写入
    assert(write_positions.size() == obj -> size);
    write_obj_in_binding(obj, binding, write_positions);



    // 更新页表信息、磁盘信息
    for (int rep_id = 1; rep_id <= REP_NUM; rep_id ++)
    {
        Page_ *q = &pages[binding -> units[rep_id] -> disk_id][binding -> units[rep_id] -> page_index];

        // 维护页信息
        // q -> cnt[obj -> tag] += obj -> size;
        q -> usable_size -= obj -> size;

        // 维护磁盘剩余容量
        usable_capacy[q -> disk_id] -= obj -> size;
        disk_tag_count[q -> disk_id][obj -> tag] += obj -> size;

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
        b_weights[obj -> binding -> b_id][b_pos] += time_decay(now_time, r -> timestamp);
    }
}


// 给定状态的磁头，正位于(disk_id, position)，为了读到下一个有效块，找到下一步的最优行动方式
// (disk_id, position)当前如果不在有效页上，则直接pass
int best_way_to_move(int last_status, int last_cost, int disk_id, int start_position, int &tokens, bool ignore_locks=false, int decay_num=0)
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

    if (ignore_locks)
    {
        for (int now_position = start_position; now_position <= end_position; now_position ++)
        {
            int b_id = b_map[disk_id][now_position].first, b_pos = b_map[disk_id][now_position].second;
            if (b_weights[b_id][b_pos] >= WEIGHT_THRESHOULD && !b_visited[b_id][b_pos])
            {
                move_type = 1;
                break;
            }
        }
    }else
    {
        for (int now_position = start_position; now_position <= end_position; now_position ++)
        {
            int b_id = b_map[disk_id][now_position].first, b_pos = b_map[disk_id][now_position].second;
            if (b_weights[b_id][b_pos] - decay_num * DECAY_SCORE >= WEIGHT_THRESHOULD && ! b_locks[b_id][b_pos] && !b_visited[b_id][b_pos])
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

// 向前移动磁头(已经判断过剩余token足够)
void move_point(int disk_id, int p_id, int move_type)
{
    if (move_type == 0)
    {
        // 下一块的操作为pass
        disk_point_last_cost[disk_id][p_id] = PASS_COST;
        disk_point_last_status[disk_id][p_id] = 0;
        result[disk_id][p_id].append("p");
    } else if (move_type == 1)
    {
        // 下一块的操作为read
        int b_id = b_map[disk_id][disk_point[disk_id][p_id]].first, b_pos = b_map[disk_id][disk_point[disk_id][p_id]].second;
        disk_point_last_cost[disk_id][p_id] = read_cost_(disk_point_last_status[disk_id][p_id], disk_point_last_cost[disk_id][p_id]);
        disk_point_last_status[disk_id][p_id] = std::max(1, disk_point_last_status[disk_id][p_id] + 1);
        b_visited[b_id][b_pos] ++;
        result[disk_id][p_id].append("r");
        if (b_disk[b_id][b_pos] > 0)
        {
            has_read.push_back(b_disk[b_id][b_pos]);
        }
    }

    disk_point[disk_id][p_id] = disk_point[disk_id][p_id] % V + 1;
}

// 检查请求r的状态
int check_request(Request *r)
{
    /**
     *  返回值:
     *      1   读完成
     *      0   未完成
     *      -1  放弃（报繁忙）
     */
    Object_ *obj = &objects[r -> object_id];
    std::bitset<MAX_BLOCKS> bits;
    PageBinding_ *binding = obj -> binding;

    // 检查r是否已经完成
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
        return 1;
    }

    // 如果没完成，决定是否需要舍弃
    // TODO: 实现舍弃的策略
    if (now_time - r -> timestamp + 1 >= MAX_ALIVE)
    {
        request_finished[r -> id] = true;
        return -1;
    }

    return 0;
}




// // 找到磁盘disk_id上，预期收益率最大的段(起始位置 + 结束位置 + 预期消耗的K)
// DiskPointSchedule search_best_position(int disk_id, int p_id)
// {
//     /** 策略
//      *  1. 将磁盘上的位置根据收益分区段，读完每个区段的花费不应当超过 MAX_K 个时间片
//      *  2. 遍历各个区段的平均收益，找到收益最大的那一个并返回
//      */
//
//     // // 辅助数组，记录下每个位置的花费
//     // static int *pos_cost = new int[V + 1];
//
//     // 按衰减率计算出当前允许的最大K
//     int MAX_K = (int)((float)BASE_K * (1 - K_DECAY * now_slice));
//     int SPLIT_LENGTH = (int)((float)BASE_SPLIT_LENGTH * (1 - SPLIT_LENGTH_DECAY * now_slice));
//     DiskPointSchedule res(disk_point[disk_id][p_id], disk_point[disk_id][p_id], 1);
//     // DiskPointSchedule res(1, 1, 1);
//     // 记录最大的收益率
//     float max_profit = -1e9;
//
//     int count_read = 0;
//     // 用 (last_l, last_r) 记录一个可读的区段
//     int last_l = -1, last_r = -1;
//     for (int r = 1; r <= valid_capacy[disk_id] + 1; r ++)
//     {
//         // 如果当前扫完磁盘，或者距离上次的隔断数超过SPLIT_LENGTH，则把(last_l, last_r)视为一个可处理的区段，计算其收益
//         if ((r == valid_capacy[disk_id] + 1 || r - last_r >= SPLIT_LENGTH) && last_l != -1)
//         {
//             // 初始化磁头状态，计算 (last_l, last_r) 的收益
//             int last_status = disk_point_last_status[disk_id][p_id];
//             int last_cost = disk_point_last_cost[disk_id][p_id];
//             int point = disk_point[disk_id][p_id];
//             float now_profit = 0;
//             int tokens = G;
//             int cost_K = 0;
//
//             // 先将磁头移动到 last_l, 重新调整磁头当前状态和令牌数
//             int distance = (last_l - point + V) % V;
//             if (JUMP_COST <= distance * PASS_COST)
//             {
//                 // 目标位置太远，则选择JUMP
//                 last_status = -1;
//                 last_cost = 64;
//                 point = last_l;
//                 tokens = 0;
//             } else
//             {
//                 // 目标位置比较近，磁头可以PASS到相应位置
//                 last_status = 0;
//                 last_cost = PASS_COST;
//                 point = last_l;
//                 tokens -= distance * PASS_COST;
//             }
//
//             // 开始计算最大K个时间片窗口内的收益
//             for (int i = 1; i <= MAX_K && point <= last_r; i ++)
//             {
//                 float tmp_profit = 0;
//                 // 相较于起始点，当前所在时间片的偏移量
//                 int decay_num = i - 1;
//                 // int decay_num = 0;
//                 // 每个K单独计算收益，涉及到时间衰减
//                 while (point <= last_r)
//                 {
//                     int b_id = b_map[disk_id][point].first, b_pos = b_map[disk_id][point].second;
//
//                     int last_tokens = tokens;
//                     int move_type = best_way_to_move(
//                         last_status,
//                         last_cost,
//                         disk_id,
//                         point,
//                         tokens,
//                         false,
//                         decay_num);
//
//                     if (move_type == 0)
//                     {
//                         // 当前需要PASS
//                         last_status = 0;
//                         last_cost = PASS_COST;
//                         point ++;
//                     }else if (move_type == 1)
//                     {
//                         // 当前需要READ
//                         last_status = 1;
//                         last_cost = last_tokens - tokens;
//                         point ++;
//
//                         // float fixed_weight = b_weights[b_id][b_pos] - (float)(decay_num * DECAY_SCORE * b_active_num[b_id][b_pos]);
//                         // float fixed_weight = b_weights[b_id][b_pos] - (float)(decay_num * DECAY_SCORE);
//                         float fixed_weight = b_weights[b_id][b_pos];
//                         // if (b_disk[b_id][b_pos] > 0 && !)
//
//                         // if (fixed_weight - FIX_SCORE < decay_num * DECAY_SCORE)
//                         // if (fixed_weight - FIX_SCORE <= 1)
//                         // {
//                         //     // 读到当前块时，该块已经过期
//                         //     fixed_weight = 0;
//                         //     last_status = 0;
//                         //     tokens = last_tokens - 1;
//                         //     last_cost = PASS_COST;
//                         //
//                         // }
//                         tmp_profit += fixed_weight;
//                         now_profit += fixed_weight;
//                         // now_profit += b_weights[b_id][b_pos];
//
//                     }else
//                     {
//                         // 令牌不够接着移动了，停下即可
//                         break;
//                     }
//                 }
//                 // TODO: 早停止策略，当前这个时间段取得的总收益太少则停止
//                 // if (tmp_profit <= 0)
//                 // {
//                 //     // r = std::min(point + 1, valid_capacy[disk_id]);
//                 //     break;
//                 // }
//
//                 cost_K = i;
//                 // 刷新下一轮的令牌数
//                 tokens = G;
//             }
//             // 更新最大收益
//             now_profit /= (float)cost_K;
//             if (now_profit > max_profit)
//             {
//                 max_profit = now_profit;
//                 res.start = last_l;
//                 res.end = std::min(point, valid_capacy[disk_id]);
//                 res.K = MAX_K + 1;
//                 // res.K = cost_K + 1;
//             }
//
//             // 重置窗口(last_l, last_r)
//             last_l = -1;
//             last_r = -1;
//             count_read = 0;
//         }
//
//         // 先映射到 (b_id, b_pos)
//         int b_id_r = b_map[disk_id][r].first, b_pos_r = b_map[disk_id][r].second;
//
//         if (b_weights[b_id_r][b_pos_r] >= WEIGHT_THRESHOULD &&
//             !b_locks[b_id_r][b_pos_r] &&
//             !b_visited[b_id_r][b_pos_r]){
//             // 如果当前位置可读，那么需要更新(last_l, last_r)
//             if (last_l == -1)
//             {
//                 // 如果是第一个可读块，还需要更新last_l
//                 last_l = r;
//             }
//             last_r = r;
//             count_read ++;
//         }
//     }
//
//     return res;
// }



// 找到磁盘disk_id上，预期收益率最大的段(起始位置 + 结束位置 + 预期消耗的K)
DiskPointSchedule search_best_position(int disk_id, int p_id)
{
    /** 策略
     *  1. 将磁盘上的位置根据收益分区段，读完每个区段的花费不应当超过 MAX_K 个时间片
     *  2. 遍历各个区段的平均收益，找到收益最大的那一个并返回
     */

    // // 辅助数组，记录下每个位置的花费
    // static int *pos_cost = new int[V + 1];

    // 按衰减率计算出当前允许的最大K
    int MAX_K = (int)((float)BASE_K * (1 - K_DECAY * now_slice));
    int SPLIT_LENGTH = (int)((float)BASE_SPLIT_LENGTH * (1 - SPLIT_LENGTH_DECAY * now_slice));
    DiskPointSchedule res(disk_point[disk_id][p_id], disk_point[disk_id][p_id], 1);
    // DiskPointSchedule res(1, 1, 1);

    // 记录最大的收益率
    float max_profit = -1e9;
    // 初始化磁头状态，计算 (last_l, last_r) 的收益
    int last_status = disk_point_last_status[disk_id][p_id];
    int last_cost = disk_point_last_cost[disk_id][p_id];
    int point = disk_point[disk_id][p_id];
    float now_profit = 0;
    int tokens = G;
    int cost_K = 1;
    bool is_scanning = false;
    int last_l = -1, last_r = -1;


    for (int r = 1; r <= valid_capacy[disk_id]; r ++)
    {
        // 先映射到 (b_id, b_pos)
        int b_id = b_map[disk_id][r].first, b_pos = b_map[disk_id][r].second;

        // 判断是否需要刷新当前窗口
        if (b_weights[b_id][b_pos] >= WEIGHT_THRESHOULD &&
            !b_locks[b_id][b_pos] &&
            !b_visited[b_id][b_pos] &&
            !is_scanning)
            {

            // 如果是第一个可读块，则先刷新当前窗口
            last_status = disk_point_last_status[disk_id][p_id];
            last_cost = disk_point_last_cost[disk_id][p_id];
            point = disk_point[disk_id][p_id];
            now_profit = 0;
            tokens = G;
            cost_K = 1;
            is_scanning = true;
            last_l = r;

            // 先将磁头移动到 r, 重新调整磁头当前状态和令牌数
            int distance = (r - point + V) % V;
            if (JUMP_COST <= distance * PASS_COST)
            {
                // 目标位置太远，则选择JUMP
                last_status = -1;
                last_cost = 64;
                point = r;
                tokens = 0;
            } else if (distance > 0)
            {

                // 目标页比较近，磁头自行决定怎么移到目标页的开头收益最大
                // while (distance >= 1)
                // {
                //     // 能先读就先读
                //     int last_tokens = tokens;
                //     int move_type = best_way_to_move(last_status,
                //         last_cost,
                //         disk_id,
                //         point,
                //         tokens,
                //         false,
                //         0);
                //
                //     if (move_type != -1 && (distance - 1) * PASS_COST > tokens)
                //     {
                //         // 这一步不能再走了，应当回溯
                //         tokens = last_tokens;
                //         break;
                //     }
                //     if (move_type == -1)
                //     {
                //         break;
                //     }
                //
                //     last_cost = last_tokens - tokens;
                //     last_status = move_type;
                //
                //     point = point % V + 1;
                //     distance = (r - point + V) % V;
                // }
                while (point != r)
                {
                    tokens -= PASS_COST;
                    point = point % V + 1;
                    last_cost = PASS_COST;
                    last_status = 0;
                }


                // // 目标位置比较近，磁头可以一直PASS到相应位置
                // last_status = 0;
                // last_cost = PASS_COST;
                // point = r;
                // tokens -= distance * PASS_COST;
            }
        }


        if (is_scanning)
        {
            // 如果当前处于扫描状态，那么移动磁头就好
            int last_tokens = tokens;
            int move_type = best_way_to_move(
                last_status,
                last_cost,
                disk_id,
                point,
                tokens,
                false,
                cost_K - 1);

            if (move_type == -1)
            {
                // 令牌数不够此次移动了
                if (cost_K < MAX_K)
                {
                    // 如果还没超过最大时间片数量，则新开一个时间片，并且重新尝试移动
                    cost_K ++;
                    tokens = G;
                    last_tokens = tokens;

                    move_type = best_way_to_move(
                        last_status,
                        last_cost,
                        disk_id,
                        point,
                        tokens,
                        false,
                        cost_K - 1);
                } else
                {
                    // 如果超过了，则计算收益，并重置窗口
                    now_profit /= (float)cost_K;
                    assert(last_l != -1);
                    if (now_profit > max_profit)
                    {
                        max_profit = now_profit;
                        res.start = last_l;
                        res.end = std::min(point, valid_capacy[disk_id]);
                        res.K = MAX_K + 1;
                        // res.K = cost_K + 1;
                    }

                    is_scanning = false;
                    last_l = -1;
                    last_r = -1;

                    r --;
                    continue;
                }
            }

            assert(move_type >= 0);
            if (move_type == 0)
            {
                // 当前需要PASS
                last_status = 0;
                last_cost = PASS_COST;
                point ++;
            }else
            {
                // 当前需要READ
                last_r = r;
                last_status = 1;
                last_cost = last_tokens - tokens;
                point ++;

                float fixed_weight = b_weights[b_id][b_pos];
                // float fixed_weight = b_weights[b_id][b_pos] - (float)((cost_K - 1) * DECAY_SCORE);

                now_profit += fixed_weight;
            }
        } else
        {
            // 当前并不处于扫描状态
        }

        // 如果当前距离上次读距离过远，则初始化窗口
        if (is_scanning && (r == valid_capacy[disk_id] || (r - last_r) >= SPLIT_LENGTH))
        {
            // 如果超过了，则计算收益，并重置窗口
            now_profit /= (float)cost_K;
            if (now_profit > max_profit)
            {
                max_profit = now_profit;
                res.start = last_l;
                // res.end = std::min(point, valid_capacy[disk_id]);
                res.end = std::min(last_r, valid_capacy[disk_id]);
                res.K = MAX_K + 1;
                // res.K = cost_K + 1;
            }

            is_scanning = false;
            last_l = -1;
            last_r = -1;
        }

    }

    return res;
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

    // // 遍历式更新b_weights
    // for (Node_ *p = unfinished.head ->next; p != unfinished.tail; p = p -> next)
    // {
    //     add_weights(&requests[p -> id]);
    // }

    // 增量式更新b_weights
    for (int i = 1; i <= UNIQUE_NUM; i ++)
    {
        for (int j = 1; j <= PAGE_SIZE; j ++)
        {
            if (b_disk[i][j] == 0)
            {
                b_weights[i][j] = 0;
                continue;
            }

            if (b_dirty[i][j] <= 104)
            {
                b_weights[i][j] -= (float)9.52381 * (float)b_active_num[i][j];
                b_dirty[i][j] ++;
            }else
            {
                b_weights[i][j] = 0;
                b_dirty[i][j] = 0;
                b_active_num[i][j] = 0;

                Object *obj = &objects[b_disk[i][j]];
                int current_id = obj -> last_request_point;
                while (current_id != 0) {
                    Request *r = &requests[current_id];
                    if (request_finished[current_id] || request_deleted[current_id])    break;

                    int idx = 1;
                    while(true)
                    {
                        int b_pos = b_map[obj -> replica[1]][obj -> unit[1][idx]].second;
                        if (b_pos == j) break;
                        idx ++;
                    }
                    if (! r -> block_status.test(idx))
                    {
                        b_weights[i][j] += time_decay(now_time, r -> timestamp);
                        b_active_num[i][j] ++;
                    }
                    current_id = requests[current_id].prev_id;
                }
            }
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
        // 对于同一磁盘上的多个磁头
        for (int p_id = 1; p_id <= POINT_NUM; p_id ++)
        {
            int tokens = G;

            if (disk_point_k_status[i][p_id] == 0)
            {

                // 先释放磁头i的位锁
                if (disk_point_start[i][p_id] != 0 && disk_point_end[i][p_id] != 0)
                {
                    for (int j = disk_point_start[i][p_id]; j <= std::min(disk_point_end[i][p_id], V); j ++)
                    {
                        int b_id = b_map[i][j].first, b_pos = b_map[i][j].second;
                        b_locks[b_id][b_pos] --;
                    }
                    if (disk_point_start[i][p_id] > disk_point_end[i][p_id])
                    {
                        for (int j = 1; j <= disk_point_end[i][p_id]; j ++)
                        {
                            int b_id = b_map[i][j].first, b_pos = b_map[i][j].second;
                            b_locks[b_id][b_pos] --;
                        }
                    }
                }

                // 找到一个最优的段，磁头接下来K个时间片内的任务就是读这个段
                DiskPointSchedule schedule = search_best_position(i, p_id);
                disk_point_start[i][p_id] = schedule.start;
                disk_point_end[i][p_id] = schedule.end;
                disk_point_k_status[i][p_id] = schedule.K;

                // 如果当前磁盘没有需要读的对象块，则把状态置0
                if (disk_point_start[i][p_id] == 0 || disk_point_end[i][p_id] == 0)
                {
                    disk_point_k_status[i][p_id] = 0;
                    continue;
                }

                // 给这个段加位锁
                for (int j = disk_point_start[i][p_id]; j <= std::min(disk_point_end[i][p_id], V); j ++)
                {
                    int b_id = b_map[i][j].first, b_pos = b_map[i][j].second;
                    b_locks[b_id][b_pos] ++;
                }
                if (disk_point_start[i][p_id] > disk_point_end[i][p_id])
                {
                    for (int j = 1; j <= disk_point_end[i][p_id]; j ++)
                    {
                        int b_id = b_map[i][j].first, b_pos = b_map[i][j].second;
                        b_locks[b_id][b_pos] ++;
                    }
                }

                int distance = (disk_point_start[i][p_id] - disk_point[i][p_id] + V) % V;
                // 磁头尝试先跳转到这个段
                if (JUMP_COST <= distance * PASS_COST)
                {
                    // 目标位置太远，直接跳
                    disk_point[i][p_id] = disk_point_start[i][p_id];
                    disk_point_last_status[i][p_id] = -1;
                    disk_point_last_cost[i][p_id] = JUMP_COST;
                    result[i][p_id].append("j ").append(std::to_string(disk_point[i][p_id]));
                    tokens = 0;
                } else
                {
                    // 目标页比较近，磁头自行决定怎么移到目标页的开头收益最大

                    // while (distance >= 1)
                    // {
                    //
                    //     // 能先读就先读
                    //     int last_tokens = tokens;
                    //     int move_type = best_way_to_move(disk_point_last_status[i][p_id],
                    //         disk_point_last_cost[i][p_id],
                    //         i,
                    //         disk_point[i][p_id],
                    //         tokens,
                    //         true,
                    //         0);
                    //
                    //     if (move_type != -1 && (distance - 1) * PASS_COST > tokens)
                    //     {
                    //         // 这一步不能再走了，应当回溯
                    //         tokens = last_tokens;
                    //         break;
                    //     }
                    //     if (move_type == -1)
                    //     {
                    //         break;
                    //     }
                    //
                    //
                    //     // int move_type = best_way_to_move(disk_point_last_status[i][p_id],
                    //     //     disk_point_last_cost[i][p_id],
                    //     //     i, disk_point[i][p_id],
                    //     //     tokens, true);
                    //     move_point(i, p_id, move_type);
                    //     distance = (disk_point_start[i][p_id] - disk_point[i][p_id] + V) % V;
                    // }
                    while (disk_point[i][p_id] != disk_point_start[i][p_id])
                    {
                        move_point(i, p_id, 0);
                        tokens -= PASS_COST;
                    }
                }

                assert(disk_point[i][p_id] == disk_point_start[i][p_id]);
            } else
            {
                disk_point_k_status[i][p_id] --;
            }


            // 把剩下的令牌全消耗完，如果中途超出锁范围就把disk_point_k_status 置0；
            while (true)
            {
                int move_type = best_way_to_move(disk_point_last_status[i][p_id],
                    disk_point_last_cost[i][p_id],
                    i, disk_point[i][p_id], tokens, true);
                if (move_type != -1)
                {
                    move_point(i, p_id, move_type);
                    if (! is_disk_point_in_lock(i, p_id))
                    {
                        disk_point_k_status[i][p_id] = 0;
                    }
                }else
                {
                    break;
                }
            }
        }
    }


    // 不遍历unfinished队列，只遍历可能完成的读请求
    std::sort(has_read.begin(), has_read.end());
    auto last_unique = std::unique(has_read.begin(), has_read.end());
    has_read.erase(last_unique, has_read.end());
    for (int object_id: has_read)
    {
        Object *obj = &objects[object_id];
        int current_id = obj -> last_request_point;
        update_active(obj -> id);

        while (current_id != 0) {
            if (request_deleted[current_id] || request_finished[current_id])    break;
            Request_ *r = &requests[current_id];
            if (unfinished.exist(current_id) && check_request(r) == 1)
            {
                finished_request_ids.push_back(r -> id);
            }
            current_id = requests[current_id].prev_id;
        }
    }
    // 完成的请求需要拿出队列
    for (int request_id: finished_request_ids)
    {
        unfinished.erase(request_id);
    }

    // 遍历可能过期的读请求
    for (Node_ *iter = unfinished.head -> next; iter != unfinished.tail; iter = iter -> next)
    {
        Request_ *r = &requests[iter -> id];
        if (check_request(r) == -1)
        {
            dropped_request_ids.push_back(r -> id);
        } else
        {
            break;
        }
    }
    // 放弃的请求需要拿出队列
    for (int request_id: dropped_request_ids)
    {
        unfinished.erase(request_id);
        update_active(requests[request_id].object_id);
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

    // 上报磁头的移动方式
    for (int i = 1; i <= N; i++) {
        for (int j = 1; j <= POINT_NUM; j ++)
        {
            if(result[i][j].empty() || result[i][j][0] != 'j')
            {
                result[i][j].append("#");
            }
            printf("%s\n", result[i][j].c_str());
        }
    }
    // 上报所有的已完成请求
    printf("%d\n", (int)finished_request_ids.size());
    for (int _request_id: finished_request_ids)
    {
        printf("%d\n", _request_id);
    }
    // 上报所有的繁忙请求
    printf("%d\n", (int)dropped_request_ids.size());
    for (int _request_id: dropped_request_ids)
    {
        printf("%d\n", _request_id);
    }
    fflush(stdout);
}


/**
 *
 *  磁盘清理事件
 *
 */


// 交换b_id对应的c_1, c_2两个位置，并维护相关数据结构
void swap_block(PageBinding *binding, int c_1, int c_2)
{
    // 先记录这两个块所属的对象
    int b_id = binding -> b_id;
    Object *obj_1 = b_disk[b_id][c_1] != 0? &objects[b_disk[b_id][c_1]]: nullptr;
    Object *obj_2 = b_disk[b_id][c_2] != 0? &objects[b_disk[b_id][c_2]]: nullptr;

    // 改写磁盘上的对象块信息
    std::swap(b_disk[b_id][c_1], b_disk[b_id][c_2]);

    // 改写对象上的位置信息
    for (int i = 1; i <= REP_NUM; i ++)
    {
        Page *p = &pages[binding -> units[i] -> disk_id][binding -> units[i] -> page_index];
        int disk_id = p -> disk_id;
        int position_1 = p -> position + c_1 - 1;
        int position_2 = p -> position + c_2 - 1;

        if (obj_1 != nullptr)
        {
            // 先找到c_1是obj_1的第几个块
            int idx_1 = -1;
            for (int j = 1; j <= obj_1 -> size; j ++)
            {
                if (obj_1 -> unit[i][j] == position_1)
                {
                    idx_1 = j;
                    break;
                }
            }
            assert(idx_1 != -1);
            obj_1 -> unit[i][idx_1] = position_2;
        }

        if (obj_2 != nullptr)
        {
            // 先找到c_2是obj_2的第几个块
            int idx_2 = -1;
            for (int j = 1; j <= obj_2 -> size; j ++)
            {
                if (obj_2 -> unit[i][j] == position_2)
                {
                    idx_2 = j;
                    break;
                }
            }
            assert(idx_2 != -1);
            obj_2 -> unit[i][idx_2] = position_1;
        }
        gc_limits[disk_id] --;
        assert(gc_limits[disk_id] >= 0);
        gc_result[disk_id].push_back(std::make_pair(position_1, position_2));
    }
}


void add_candidate_segments(PageBinding *binding, std::priority_queue<std::pair<float, Segment>, std::vector<std::pair<float, Segment> >, CmpGC> &q)
{
    int b_id = binding -> b_id;
    std::vector<Segment> segments = get_disk_segments(b_id);

    for (int i = 1; i <= segments.size() - 2; i ++)
    {
        int length = segments[i].end - segments[i].start + 1;
        if (segments[i].tag != 0 || length >= 5)   continue;
        // if (length >= 5)   continue;
        int expected_tag = find_expected_tag(segments[i]);
        if (expected_tag <= 0)  continue;
        Segment mid = segments[i];
        mid.tag = expected_tag;

        float gap_score_before = cal_gap_score(segments[i - 1], segments[i], segments[i + 1]);
        float gap_score_after = cal_gap_score(segments[i - 1], mid, segments[i + 1]);
        // float profit = (gap_score_before - gap_score_after) / (float)length;
        float profit = (gap_score_before - gap_score_after);
        // profit = profit * 100 * (1 - percent_write[expected_tag][now_slice + 1]);
        // profit = profit * 100 * (2 + percent_write[expected_tag][now_slice + 1]);
        q.push(std::make_pair(profit, segments[i]));
    }
}


// 磁盘清理
void do_gc()
{
    std::priority_queue<std::pair<float, Segment>, std::vector<std::pair<float, Segment> >, CmpGC> q;

    for (int i = 1; i <= UNIQUE_NUM; i ++)
    {
        PageBinding_ *binding = unique_bindings[i];
        add_candidate_segments(binding, q);
    }

    int total_cost = 0;
    while (! q.empty())
    {
        if (total_cost >= N * C)
        {
            break;
        }

        Segment segment = q.top().second;
        q.pop();

        int minn_gc_limit = 1e9;
        int length = segment.end - segment.start + 1;
        PageBinding *binding = unique_bindings[segment.b_id];
        int b_id = binding -> b_id;
        for (int j = 1; j <= REP_NUM; j ++)
        {
            minn_gc_limit = std::min(minn_gc_limit, gc_limits[binding -> units[j] -> disk_id]);
        }
        if (minn_gc_limit <= length) continue;

        bool flag = false;
        for (int j = segment.start; j <= segment.end; j ++)
        {
            if (gc_visited[b_id][j])    flag = true;
        }
        if (flag) continue;

        std::vector<int> swap_positions;
        // segement相邻的两段，要么为边界，要么两段tag相同
        int expected_tag = find_expected_tag(segment);
        if (expected_tag <= 0)
        {
            continue;
        }

        int tmp = gc_m_pos[b_id][expected_tag];
        for (int j = 1; j <= length; j ++)
        {
            while (true)
            {
                int b_pos = gc_m_pos[b_id][expected_tag];
                if (b_pos <= 0 || (get_tag(b_id, b_pos) == expected_tag && !gc_visited[b_id][b_pos])) break;
                gc_m_pos[b_id][expected_tag] --;
            }
            int b_pos = gc_m_pos[b_id][expected_tag];
            if (b_pos > 0 && b_pos > segment.end && get_tag(b_id, b_pos) == expected_tag && !gc_visited[b_id][b_pos])
            {
                swap_positions.push_back(b_pos);
                gc_m_pos[b_id][expected_tag] --;
            }
        }
        if (swap_positions.size() == length)
        {
            for (int j = 1; j <= length; j ++)
            {
                swap_block(binding, segment.start + j - 1, swap_positions[j - 1]);
                gc_visited[b_id][segment.start + j - 1] = 1;
                gc_visited[b_id][swap_positions[j - 1]] = 1;
            }
            total_cost += length * REP_NUM;
        }else
        {
            gc_m_pos[b_id][expected_tag] = tmp;
        }
    }
}


void gc_action()
{
    if (now_time % FRE_PER_SLICING != 0)    return;

    scanf("%*s %*s");
    printf("GARBAGE COLLECTION\n");

    do_gc();

    for (int i = 1; i <= N; i ++)
    {
        printf("%d\n", (int)gc_result[i].size());
        for (auto v: gc_result[i])
        {
            printf("%d %d\n", v.first, v.second);
        }
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


int main()
{
    // freopen("../../data/sample_practice.in","r",stdin);
    // freopen("../../data/sample.in","r",stdin);
    init();

    for (now_time = 1; now_time <= T + EXTRA_TIME; now_time++) {

        timestamp_action();

        init_per_timestamp();
        init_per_time_slice();

        delete_action();
        write_action();

        read_action();

        gc_action();
    }
    clean();

    return 0;
}