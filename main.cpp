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
#include <bitset>
#include <cmath>

#define MAX_DISK_NUM (10 + 1)
#define MAX_DISK_SIZE (16384 + 1)
#define MAX_REQUEST_NUM (30000000 + 1)
#define MAX_OBJECT_NUM (100000 + 1)
#define REP_NUM (3)
#define FRE_PER_SLICING (1800)
#define EXTRA_TIME (105)

// 最大磁盘数
#define MAX_N (10 + 1)
// 最大磁盘空间
#define MAX_V (16384 + 1)
// 最大的对象类型数量
#define MAX_M (16 + 1)
// 对象包含的最大对象块数量
#define MAX_BLOCKS (5 + 1)
// 最大的时间片数量
#define MAX_TIME (86400 + 1)
// 最大重试次数（调试用）
#define MAX_RETRY (100)
// 一个读请求的最长存活时间，可以控制代码运行时间 (最大取105)
#define MAX_ALIVE (105)
// 期望的最小页数（不是最终的PAGE_NUM）
#define MIN_PAGE_NUM (16)
// 期望的最大页数（不是最终的PAGE_NUM）
#define MAX_PAGE_NUM (16)
// 权重阈值，如果当前块的权重比该值低就不读了，直接跳过
#define WEIGHT_THRESHOULD (1 * REP_NUM)
// 阈值gamma，当该页的剩余部分平均收益小于前面部分平均收益的gamma倍时，把该磁头的状态标记为已完成
#define EARLY_STOP_THRESHOULD (0)
// 每个时间片都以最理想的情况读取时，限制unfinished队列大小的参数 (用于剔除在磁头读取能力之外的读请求)
#define RESERVING_ROUNDS (60)
// 窗口尺寸，作为贪心策略的参数，决定是否要读当前对象块，或是pass (设为0时，采用自适应计算的窗口大小；非0时调参发现9附近效果最好)
#define WINDOW_SIZE (0)
// 随机数种子，设置为0代表使用当前系统时间
#define RANDOM_SEED (114514)

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
    int max_cnt = 0;        // 占比最高的对象块类型数量 (1 ~ page_size)
    int max_tag = 0;        // 占比最高的对象块类型 (1 ~ V)
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
typedef struct PageBinding_
{
    // 当前页的磁盘号、页号
    int disk_id;
    int page_index;
    // 当前页是绑定关系中的第rep_id个副本
    int rep_id;
    // 绑定的REP_NUM个页共享同一个b_id;
    int b_id;

    // 指向绑定关系中的其它页
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

    // // 插入表中合适的位置，使得链表保持按id有序
    // void insert(int id)
    // {
    //
    // }

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

    int size()
    {
        return this -> cnt;
    }

}Deque;


/**
 *
 *  需要全局维护的变量
 *
 */

int T, M, N, V, G;
/**
 *  TODO: 还没使用统计信息
 */
int fre_del[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
int fre_write[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
int fre_read[MAX_M][(MAX_TIME + FRE_PER_SLICING - 1) / FRE_PER_SLICING];
Request requests[MAX_REQUEST_NUM];
Object objects[MAX_OBJECT_NUM];
int now_time;

// 一个磁盘划分的页数量
int PAGE_NUM;
// 页大小（每个磁盘最后一页的大小可能不等于PAGE_SIZE）
int PAGE_SIZE;
// 磁盘存储状态
int b_disk[MAX_N * MAX_PAGE_NUM + 1][MAX_V / MIN_PAGE_NUM + 2];
// 磁头当前位置
int disk_point[MAX_DISK_NUM];
// 磁头当前的状态 (k>0: 磁头已经连续读了k次, k=0: 磁头上次pass了, k=-1: 磁头上次是jump过来的)
int disk_point_last_status[MAX_DISK_NUM];
// 磁头上次消耗的令牌数
int disk_point_last_cost[MAX_DISK_NUM];
// 磁头上个时间片是否读完了它的目标页
bool disk_point_last_done[MAX_DISK_NUM];
// 每个硬盘的实际有效总存储空间（V对PAGE_SIZE取余的部分 和 is_ok = false的页所占空间被剔除了）
int valid_capacy[MAX_DISK_NUM];
// 每个磁盘的剩余可用空间
int usable_capacy[MAX_DISK_NUM];

// 一次JUMP消耗的令牌数
int JUMP_COST;
// 一次PASS消耗的令牌数
int PASS_COST;

// 允许的最大unfinished大小，可以控制代码运行时间
int MAX_DEQUE_SIZE;

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
// 维护当前仍有效的读请求
Deque unfinished;
// 维护历史所有请求的完成状态 (0:未完成, 1:已完成)
bool request_finished[MAX_REQUEST_NUM];
// 维护历史所有请求的删除状态 (0:未删除, 1:已删除)
bool request_deleted[MAX_REQUEST_NUM];


/**
 *
 *  需要每个时间片都初始化的变量
 *
 */
// 维护磁盘i中第j块的权重
int b_weights[MAX_N * MAX_PAGE_NUM + 1][MAX_V / MIN_PAGE_NUM + 2];;
// 权重的前缀和
int b_s_weights[MAX_N * MAX_PAGE_NUM + 1][MAX_V / MIN_PAGE_NUM + 2];;
// // 期望的令牌消耗量前缀和
// int s_expected_tokens[MAX_DISK_NUM][MAX_DISK_SIZE];
// 维护磁盘i中第j块，在当前时间片是否被读出来
int b_visited[MAX_N * MAX_PAGE_NUM + 1][MAX_V / MIN_PAGE_NUM + 2];;

// 每个时间片删除的对象id列表
std::vector<int> deleted_object_ids;
// 每个时间片新增的对象id列表
std::vector<int> added_object_ids;
// 每个时间片新增的请求id列表
std::vector<int> added_request_ids;
// 每个时间片完成的请求id列表
std::vector<int> finished_request_ids;
// 每个时间片中，当前页的偏移量（防止多个磁头竞争同一页）
int offsets[MAX_DISK_NUM][MAX_PAGE_NUM + 1];
// 当前时间片的磁头移动结果 (用于输出)
std::string result[MAX_DISK_NUM];




/**
 *
 *  工具函数
 *
 */
// 预输入与全局初始化
void init()
{
    // 调试用
    // 86400 16 10 5792 350
    // T = 86400;
    // M = 16;
    // N = 10;
    // V = 5792;
    // G = 350;

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

    // 自适应计算最优的PAGE_NUM和PAGE_SIZE
    assert(MIN_PAGE_NUM >= 1 && MIN_PAGE_NUM <= V);
    assert(MAX_PAGE_NUM >= 1 && MAX_PAGE_NUM <= V);
    assert(MIN_PAGE_NUM <= MAX_PAGE_NUM);
    // 找到能刚好整除V的PAGE_NUM
    PAGE_NUM = MIN_PAGE_NUM;
    while (V % PAGE_NUM != 0 && PAGE_NUM <= MAX_PAGE_NUM)
    {
        PAGE_NUM ++;
    }
    if (PAGE_NUM > MAX_PAGE_NUM)
    {
        // 如果找不到这样的PAGE_NUM，就调整V的大小，使得浪费的空间最少
        int wasted = 1e9;
        for (int i = MIN_PAGE_NUM; i <= MAX_PAGE_NUM; i ++)
        {
            if (V - (V / i) * i < wasted)
            {
                PAGE_NUM = i;
                wasted = V - (V / i) * i ;
            }
        }
    }
    PAGE_SIZE = V / PAGE_NUM;
    // PAGE_SIZE = std::min(V, DEFAULT_PAGE_SIZE);
    // PAGE_SIZE = std::max(PAGE_SIZE, V / 16);
    // PAGE_NUM = (V + PAGE_SIZE - 1) / PAGE_SIZE;

    // 最大的有效请求数 = 每个时间片完成读请求的能力上限 * 预留轮数
    MAX_DEQUE_SIZE = std::max(G / 16 / 1 * N * RESERVING_ROUNDS, 1);

    // 初始化磁头
    for (int i = 1; i <= N; i++) {
        disk_point[i] = 1;
        disk_point_last_done[i] = true;
    }

    //初始化页的绑定关系
    // std::set<std::pair<int, int>, std::greater<> > tmp;
    // auto cmp = [](const std::pair<int, int> &a, const std::pair<int, int> &b)
    // {
    //     if (a.first == b.first) return a.second < b.second;
    //     return a.first > b.first;
    // };
    std::deque<std::pair<int, int> > tmp;
    // std::set<std::pair<int, int>, decltype(cmp)> tmp(cmp);

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
        for (int j = 1; j <= PAGE_NUM; j ++)
        {
            offsets[i][j] = 0;
        }
    }

    for (int i = 1; i <= unique_bindings.size(); i ++)
    {
        for (int j = 1; j <= PAGE_SIZE; j ++)
        {
            b_weights[i][j] = 0;
            b_visited[i][j] = 0;
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
std::pair<int, int> map_disks(int disk_id, int position)
{
    if (position > valid_capacy[disk_id])    return std::make_pair(0, 0);
    Page_ *p = find_page_(disk_id, position);
    int b_id = bindings[p -> disk_id][p -> page_index].b_id;
    int b_position = position - p -> position + 1;
    return std::make_pair(b_id, b_position);
}

void printDisks(){
    printf("printing disks...\n");
    for (int i = 1; i <= N; i ++) {
        for(int j = 1; j <= V; j ++) {
            std::pair<int, int> v = map_disks(i, j);
            printf("%d ", b_disk[v.first][v.second]);
        }
        printf("\n");
    }
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
    for (int j = 1; j <= obj -> size; j ++) {
        // 更新disk
        int pos = obj -> unit[1][j] - pages[binding -> disk_id][binding -> page_index].position + 1;
        b_disk[binding -> b_id][pos] = 0;
    }

    for (int i = 1; i <= REP_NUM; i ++)
    {
        int disk_id = obj -> replica[i];

        // 更新pages
        Page_ *p = find_page_(disk_id, obj -> unit[i][1]);
        int lst_max_tag = p -> max_tag;
        p -> cnt[obj -> tag] -= obj -> size;
        p -> usable_size += obj -> size;

        // 更新usable_capacy
        usable_capacy[disk_id] += obj -> size;

        // 如果占比最高的类型发生了改变
        if (p -> cnt[obj -> tag] < p -> page_size - p -> usable_size - p -> cnt[obj -> tag])
        {
            int max_cnt = -1, max_tag = 0;
            // 先判断tag类的块数量，是否小于其它所有类型的块数量之和，是的话就先更新一遍
            for (int k = 1; k <= M; k ++)
            {
                if (p -> cnt[k] > max_cnt)
                {
                    max_cnt = p -> cnt[k];
                    max_tag = k;
                }
            }
            if (max_tag != lst_max_tag)
            {
                // 如果标签变了，那么更新max_tag和max_percent，更新st
                p -> max_tag = max_tag;
                p -> max_cnt = max_cnt;

                // 更新st
                if (p -> usable_size + obj -> size == p -> page_size)
                {
                    // 原本处于full
                    st_full[disk_id].erase(p -> page_index);
                    if (p -> usable_size == 0)
                    {
                        // 现在为empty
                        st_empty[disk_id].append(p -> page_index);
                    } else
                    {
                        // 现在为half
                        st_half[disk_id].append(p -> page_index);
                    }
                }else
                {
                    // 原本处于half
                    if (p -> usable_size == 0)
                    {
                        // 现在为empty
                        st_half[disk_id].erase(p -> page_index);
                        st_empty[disk_id].append(p -> page_index);
                    }
                }

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
// 把对象obj写进磁盘disk_id，并更新存储信息和页信息（自动找到最合适的写入位置，调用函数前保证一定有足够空间）
bool object_write_(Object *obj, int disk_id)
{
    /** 1. 先在当前磁盘上找到合适的写入位置，并找到这个位置绑定的其它页
     *  2. 逻辑上写入对象到当前绑定的3个页上，更新存储信息disk
     *  3. 更新对象信息obj
     *  4. 更新页信息pages
     *  5. 更新可用空间usable_capacy
     *  6. 更新st
     */

    // 先找到一个剩余空间足够写入的页
    // 当前要写入的页
    Page_ *p = nullptr, *q = nullptr;
    for (Node_ *node = st_half[disk_id].tail -> prev; node != st_half[disk_id].head; node = node -> prev)
    {
        int page_index = node -> id;
        if (pages[disk_id][page_index].max_tag == obj -> tag &&
            pages[disk_id][page_index].usable_size >= obj -> size)
        {
            p = &pages[disk_id][page_index];
            break;
        }
        if (pages[disk_id][page_index].usable_size >= obj -> size)
        {
            q = &pages[disk_id][page_index];
        }
    }

    if (p == nullptr)
    {
        // 找空页
        if (st_empty[disk_id].size() > 0)
        {
            int page_index = st_empty[disk_id].front();
            p = &pages[disk_id][page_index];
        }
    }

    if (p == nullptr)
    {
        p = q;
    }

    // 找不到足够大的空位，则当前磁盘的写入操作失败
    if (p == nullptr)   return false;

    PageBinding *binding = bindings[p -> disk_id][p -> page_index].units[1];
    obj -> binding = binding;

    // 设置obj第rep_id个副本的磁盘号，分配对象块到磁盘位置映射的内存
    for (int rep_id = 1; rep_id <= REP_NUM; rep_id ++)
    {
        q = &pages[binding -> units[rep_id] -> disk_id][binding -> units[rep_id] -> page_index];
        obj -> replica[rep_id] = q -> disk_id;
    }

    // 写入
    int write_num = 0;
    int b_id = binding -> b_id;
    for (int position = 1; position <= PAGE_SIZE && write_num < obj -> size; position ++)
    {
        if (b_disk[b_id][position] == 0)
        {
            // 位置为空，可以写入
            write_num ++;
            b_disk[b_id][position] = obj -> id;


            for (int rep_id = 1; rep_id <= REP_NUM; rep_id ++)
            {
                q = &pages[binding -> units[rep_id] -> disk_id][binding -> units[rep_id] -> page_index];

                // 维护对象信息
                obj -> unit[rep_id][write_num] = q -> position + position - 1;
                // 维护页信息
                q -> cnt[obj -> tag] ++;
                q -> usable_size --;


                if (q -> max_tag ==0 || q -> cnt[obj -> tag] > q -> cnt[q -> max_tag])
                {
                    // 占比最大的类型发生了变化
                    q -> max_tag = obj -> tag;

                }
                q -> max_cnt = q -> cnt[q -> max_tag];
                // 维护磁盘剩余容量
                usable_capacy[q -> disk_id] --;

            }
        }
    }

    for (int rep_id = 1; rep_id <= REP_NUM; rep_id ++)
    {
        q = &pages[binding -> units[rep_id] -> disk_id][binding -> units[rep_id] -> page_index];

        // 更新st
        if (q -> page_size - q -> usable_size == obj -> size)
        {
            // 原本处于empty
            st_empty[disk_id].erase(q -> page_index);
            if (q -> usable_size == 0)
            {
                // 现在为full
                st_full[disk_id].append(q -> page_index);
            } else
            {
                // 现在为half
                st_half[disk_id].append(q -> page_index);
            }
        }else
        {
            // 原本处于half
            if (q -> usable_size == 0)
            {
                // 现在为full
                st_half[disk_id].erase(q -> page_index);
                st_full[disk_id].append(q -> page_index);
            }
        }
    }

    return true;
}

// 写这个时间片的全部对象
void do_objects_write()
{
    /** 写策略
     *  1. 随机选择一个硬盘disk_id
     *  2. 在这个硬盘上找到一个合适的页page，并找到它对应的binding -> b_id -> unique_binding
     *  3. 逻辑上将这个对象obj写入binding对应的3个页
     *  4. 实际上将obj写入unique_binding对应的1个页
     */

    for (int object_id: added_object_ids)
    {
        Object *obj = &objects[object_id];
        int count_retry = 0;

        for (int rep_id = 1; rep_id <= REP_NUM; rep_id ++)
        {
            obj -> unit[rep_id] = static_cast<int*>(malloc(sizeof(int) * (obj -> size + 1)));
        }

        while (true)
        {
            // 重试MAX_RETRY次都没能成功，说明没有足够存储空间了
            assert(count_retry < MAX_RETRY);
            count_retry ++;

            int disk_id = std::rand() % N + 1;
            if (usable_capacy[disk_id] < obj -> size)   continue;
            if (! object_write_(obj, disk_id))  continue;
            break;
        }
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
        b_weights[obj -> binding -> b_id][j] += time_decay(r -> timestamp);
    }
}

// 给定状态的磁头，正位于(disk_id, position)，为了读到下一个有效块，找到下一步的最优行动方式
int best_way_to_move(int last_status, int last_cost, int disk_id, int position, int &tokens)
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
    int move_type;
    int window_weight;
    std::pair<int, int> v = map_disks(disk_id, position);
    int b_id = v.first, pos = v.second;
    int end_pos = std::min(PAGE_SIZE, pos + length - 1);

    window_weight = b_s_weights[b_id][end_pos] - b_s_weights[b_id][pos - 1];

    if (last_status <= 0)
    {
        move_type = b_weights[b_id][pos] >= WEIGHT_THRESHOULD ? 1: 0;
    } else
    {
        move_type = window_weight > WEIGHT_THRESHOULD ? 1: 0;
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


// // 找到磁盘disk_id上，预期收益率最大的页(这个页必须是合法的页)
// std::pair<Page_*, int> search_best_page(int disk_id)
// {
//     /** 策略
//      *  1. 磁头上个时间片的任务还没完成，则不允许去扫新的一页
//      *  2. 收益率 = 目标页的权重之和 / (磁头从当前位置移动到目标页预期花费的令牌 + 扫描目标页花费的令牌)
//      *
//      */
//     Page_* now_page = is_valid(disk_id)? find_page_(disk_id, disk_point[disk_id]): nullptr;
//     Page_* res = nullptr;
//     // if (! disk_point_last_done[disk_id])
//     // {
//     //     assert(is_valid(disk_id));
//     //     // 上个时间片磁头在对应页的读取还没完成，则不允许去读新页，此时磁头一定位于有效页上
//     //     return now_page;
//     // }
//
//     float max_score = 0;
//     int move_distance = 0;
//     int last_status = disk_point_last_status[disk_id];
//     int last_cost = disk_point_last_cost[disk_id];
//
//
//     for (int i = 1; i <= PAGE_NUM; i ++)
//     {
//         // p一定是个合法页
//         Page_ *p = &pages[disk_id][i];
//         if ((! p -> is_ok) || offsets[p -> disk_id][p -> page_index])    continue;
//         // 选择磁头直接跳到页首，还是一格一格移过去
//         int position = p == now_page? disk_point[disk_id]: p -> position + offsets[p -> disk_id][p -> page_index];
//         int end_position = p -> position + p -> page_size - 1;
//         int sum_tokens = std::min(JUMP_COST, ((position - disk_point[disk_id] + V) % V) * PASS_COST);
//         int tokens_origin = G, tokens = G;
//         int tmp_move_distance = 0;
//
//         int sum_weights = s_weights[disk_id][end_position] - s_weights[disk_id][position - 1];
//         if (sum_weights <= 0)   continue;
//
//         for(int j = position ; j <= end_position; j ++)
//         {
//             /**
//              *  TODO: 当前策略需要优化
//              */
//             int move_type = best_way_to_move(last_status, last_cost, disk_id, j, tokens);
//             if (move_type == -1)
//             {
//                 break;
//             }
//
//             tmp_move_distance ++;
//             if (move_type == 0)
//             {
//                 last_status = 0;
//                 last_cost = PASS_COST;
//             } else
//             {
//                 last_cost = read_cost_(last_status, last_cost);
//                 last_status = std::max(1, last_status + 1);
//                 // sum_weights += weights[disk_id][j];
//             }
//
//             // // 如果剩余部分价值不高，则停止扫描
//             // float profit_1 = (float)(s_weights[i][disk_point[i]] -  s_weights[i][position - 1]) / (float)(disk_point[i] - position + 1);
//             // float profit_2 = (float)(s_weights[i][end_position] - s_weights[i][disk_point[i] - 1]) / (float)(end_position - disk_point[i] + 1);
//             // if (profit_2 < profit_1 * EARLY_STOP_THRESHOULD)
//             // {
//             //     break;
//             // }
//
//         }
//
//         // sum_weights += s_weights[disk_id][ending_position] - s_weights[disk_id][position - 1];
//         sum_tokens += (tokens_origin - tokens);
//         // float score = sum_weights / (static_cast<float>(sum_tokens) + 1);
//         float score = static_cast<float>(sum_weights) / static_cast<float>(sum_tokens);
//         // float score = static_cast<float>(sum_weights) / static_cast<float>(end_position - position + 1);
//         // float score = static_cast<float>(sum_weights);
//         if (score > max_score)
//         {
//             max_score = score;
//             move_distance = tmp_move_distance;
//             res = p;
//         }
//     }
//
//     if (res == nullptr)
//     {
//         // 如果实在找不到合法的页了，那么就返回第一页去读
//         // disk_point_last_done[disk_id] = true;
//         // return &pages[disk_id][1];
//         return std::make_pair(&pages[disk_id][std::rand() % (valid_capacy[disk_id] / PAGE_SIZE) + 1], 0);
//     }
//     return std::make_pair(res, move_distance);
// }
//
//
//
// // 向前移动磁头(已经判断过剩余token足够)
// void move_point(int disk_id, int move_type)
// {
//     if (move_type == 1)
//     {
//         // 下一块的操作为read
//         disk_point_last_cost[disk_id] = read_cost_(disk_point_last_status[disk_id], disk_point_last_cost[disk_id]);
//         disk_point_last_status[disk_id] = std::max(1, disk_point_last_status[disk_id] + 1);
//         visited[disk_id][disk_point[disk_id]] ++;
//         result[disk_id].append("r");
//     }else
//     {
//         // 下一块的操作为pass
//         disk_point_last_cost[disk_id] = PASS_COST;
//         disk_point_last_status[disk_id] = 0;
//         result[disk_id].append("p");
//     }
//     disk_point[disk_id] = disk_point[disk_id] % V + 1;
// }
//
// // 更新请求r的状态
// bool update_request(Request *r)
// {
//     Object_ *obj = &objects[r -> object_id];
//     std::bitset<MAX_BLOCKS> bits;
//     for (int i = 1; i <= REP_NUM; i ++)
//     {
//         for (int j = 1; j <= obj -> size; j ++)
//         {
//             int disk_id = obj -> replica[i], position = obj -> unit[i][j];
//             if (visited[disk_id][position])
//             {
//                 bits.set(j);
//             }
//         }
//     }
//     r -> block_status |= bits;
//     if (r -> block_status.count() == obj -> size)
//     {
//         // r已经完成
//         request_finished[r -> id] = true;
//         return true;
//     }
//     return false;
// }

// // 1个时间片内，移动所有磁头，返回已完成的请求列表
// void do_objects_read()
// {
//     /**
//      *  TODO:优化策略
//      *  1. 初始化
//      *  2. 计算每个磁盘，每个块的权重
//      *  3. 每个磁盘上根据权重选出当前磁盘中最优的页
//      *  4. 根据策略移动磁头
//      *  5. 更新状态信息
//      *  6. 统计读取结果
//      */
//
//     // 遍历每个已有请求，贡献权重
//     for (Node_ *p = unfinished.head ->next; p != unfinished.tail; p = p -> next)
//     {
//         add_weights(&requests[p -> id]);
//     }
//     // 维护权重的前缀和
//     for (int i = 1; i <= unique_bindings.size(); i ++)
//     {
//         for (int j = 1; j <= PAGE_SIZE; j ++)
//         {
//             b_s_weights[i][j] = b_s_weights[i][j - 1] + b_weights[i][j];
//         }
//     }
//
//     // 移动各个磁头
//     for (int i = 1; i <= N; i ++)
//     {
//         int tokens = G;
//         // 磁头当前所在的页
//         Page_ *now_page = is_valid(i)? find_page_(i, disk_point[i]): nullptr;
//         // 预期收益率最高的页 (如果上个时间片是跳转指令，那这个时间片扫描当前页就行了；否则去找一个最优的目标页)
//         auto tmp = search_best_page(i);
//         Page_ *best_page = tmp.first;
//         int move_distance = tmp.second;
//         int position = best_page -> position + offsets[best_page -> disk_id][best_page -> page_index];
//         int end_position = best_page -> position + best_page -> page_size - 1;
//
//         if (now_page != best_page)
//         {
//             // 目标页是其它页，那么先转去跳转
//             // disk_point_last_done[i] = false;
//             // if (JUMP_COST <= (position - disk_point[i] + V) % V * PASS_COST)
//             // {
//             //     // 目标页太远，直接跳
//             //     disk_point[i] = position;
//             //     disk_point_last_status[i] = -1;
//             //     disk_point_last_cost[i] = JUMP_COST;
//             //     result[i].append("j ").append(std::to_string(disk_point[i]));
//             //     tokens = 0;
//             // } else
//             // {
//             //     // 目标页比较近，可以逐步pass到目标页的开头
//             //     while (disk_point[i] != position)
//             //     {
//             //         move_point(i, 0);
//             //         tokens -= PASS_COST;
//             //     }
//             // }
//
//             // 目标页太远，直接跳
//             disk_point[i] = best_page -> position;
//             disk_point_last_status[i] = -1;
//             disk_point_last_cost[i] = JUMP_COST;
//             result[i].append("j ").append(std::to_string(disk_point[i]));
//             tokens = 0;
//         }
//
//         PageBinding_ *binding = &bindings[best_page -> disk_id][best_page -> page_index];
//         for (int rep_id = 1; rep_id <= REP_NUM; rep_id ++)
//         {
//             offsets[binding -> units[rep_id] -> disk_id][binding -> units[rep_id] -> page_index] += move_distance;
//         }
//
//         // if (tokens == 0)
//         // {
//         //     PageBinding_ *binding = &bindings[best_page -> disk_id][best_page -> page_index];
//         //     for (int rep_id = 1; rep_id <= REP_NUM; rep_id ++)
//         //     {
//         //         excepted_pages[binding -> units[rep_id] -> disk_id][binding -> units[rep_id] -> page_index] = true;
//         //     }
//         // }
//
//
//         // 把剩下的令牌全消耗完
//         while (true)
//         {
//             int move_type = best_way_to_move(disk_point_last_status[i], disk_point_last_cost[i],
//                 i, disk_point[i], tokens);
//             if (move_type != -1)
//             {
//                 move_point(i, move_type);
//                 // if (disk_point[i] == end_position)
//                 // {
//                 //     disk_point_last_done[i] = true;
//                 // }
//             }else
//             {
//                 break;
//             }
//         }
//
//         /**
//          *  优化：
//          *      如果当前页剩下部分收益不高，就把disk_point_last_done[i]置为1，这样该磁头下一轮就有机会跳出该页
//          */
//         // if (is_valid(i))
//         // {
//         //     now_page = find_page_(i, disk_point[i]);
//         //     int left = now_page -> position, right = now_page -> position + now_page -> page_size - 1;
//         //     float profit_1 = (float)(s_weights[i][disk_point[i]] -  s_weights[i][left - 1]) / (float)(disk_point[i] - left + 1);
//         //     float profit_2 = (float)(s_weights[i][right] - s_weights[i][disk_point[i] - 1]) / (float)(right - disk_point[i] + 1);
//         //     if (profit_2 < profit_1 * EARLY_STOP_THRESHOULD)
//         //     {
//         //         disk_point_last_done[i] = true;
//         //     }
//         // } else
//         // {
//         //     disk_point_last_done[i] = true;
//         // }
//     }
//
//
//     // 把unfinished队列的每个请求拿出来，检查是否完成
//     for (Node_ *p = unfinished.tail -> prev; p != unfinished.head; p = p -> prev)
//     {
//         Request_ *r = &requests[p -> id];
//         if (update_request(r))
//         {
//             finished_request_ids.push_back(r -> id);
//         }
//     }
//     for (int request_id: finished_request_ids)
//     {
//         unfinished.erase(request_id);
//     }
// }


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

    // // 更新unfinished队列
    // update_unfinished();
    //
    // // 在这一时间片上完成读操作
    // do_objects_read();

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

int main()
{
    init();

    for (now_time = 1; now_time <= T + EXTRA_TIME; now_time++) {
        timestamp_action();

        init_per_timestamp();
        delete_action();
        write_action();
        read_action();

        // printDisks();
    }
    clean();

    return 0;
}