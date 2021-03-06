#include <iostream>
#include <fstream>
#include <vector>
#include <iterator>
#include <string>
#include <algorithm>
#include <queue>

#define TICK_MAX 8930
#define CMR_MAX 40
#define EDGE_MAX 140
#define LINK_MAX 4730

using namespace std;

static string CONFIG_PATH="/data/config.ini";
static string DATA_PATH="/data/"; 
static string OUTPUT_PATH="/output/solution.txt";
int QOS_LIMIT; // 最大延迟

struct Customer;
struct Edge;
struct Link;

// 客户节点结构体
struct Customer {
    string id;
    int num;
    int demand[TICK_MAX]; // 每时刻的剩余带宽需求
    int linkNum; // 相连的链路数量
    Link *links[EDGE_MAX]; // 相连的链路的指针
};

// 边缘节点结构体
struct Edge {
    string id;
    int num;
    int limit; // 带宽上限
    int linkNum; // 相连的链路数量
    Link *links[CMR_MAX]; // 相连的链路的指针
    int bandwidth[TICK_MAX] = {}; // 每时刻分配的总带宽
    bool isFree[TICK_MAX] = {}; // 每时刻分配的带宽是否可视为能无代价分完
};

// 链路结构体
struct Link {
    Customer *cmr; // 相连的客户节点
    Edge *edge; // 相连的边缘节点
    int qos; // 时延
    int bandwidth[TICK_MAX] = {}; // 每时刻分配的带宽
};

Customer cmrs[CMR_MAX];
Edge edges[EDGE_MAX];
bool cmrFlag[CMR_MAX] = {};
bool edgeFlag[EDGE_MAX] = {};
int cmrNum; // 客户节点数
int edgeNum; // 边缘节点数
int tickNum; // 总时刻数
int fullNum; // 比95百分位高的时刻数
int demands[TICK_MAX] = {}; // 每个时刻客户的总需求

/**
 * String splict by delimiters
 */
static vector<string> split(const string &s, const string &delimiters = ",") {
    vector<string> tokens;
    size_t start = 0;
    size_t end = s.find_first_of(delimiters, start);
    while (end != string::npos) {
        tokens.emplace_back(s.substr(start, end - start));
        start = end + 1;
        end = s.find_first_of(delimiters, start);
    }
    tokens.emplace_back(s.substr(start));
    return tokens;
}

/**
 * read Config
 */
void readConf(){
    ifstream config;
    config.open(CONFIG_PATH);
    string tmp_line;
    getline(config,tmp_line);
    getline(config,tmp_line);
    QOS_LIMIT = atoi(string(tmp_line.begin() + tmp_line.find('=')+1, tmp_line.end()).c_str());
    // TODO
    config.close();
}
/**
 * read Data
 */
void readData(){
    vector<string> tmp_vec;
    ifstream data;
    string tmp_line;

    data.open(DATA_PATH+"demand.csv");//客户节点在不同时刻的带宽需求信息
    getline(data, tmp_line);
    tmp_vec = split(tmp_line, ",\n\r");
    cmrNum = tmp_vec.size() - 2;
    for (int i = 1; i <= cmrNum; i++) {
        cmrs[i-1].id = tmp_vec[i];
        cmrs[i-1].linkNum = 0;
    }
    tickNum = 0;
    while(getline(data,tmp_line)){
        tmp_vec = split(tmp_line, ",");
        for (int i = 1; i <= cmrNum; i++) {
            cmrs[i-1].demand[tickNum] = stoi(tmp_vec[i]);
            cmrs[i-1].num = i-1;
        }
        tickNum++;
    }
    fullNum = tickNum / 20;
    data.close();
    data.clear();
    
    data.open(DATA_PATH+"site_bandwidth.csv");//边缘节点列表以及每个边缘节点的带宽上限
    getline(data,tmp_line);
    edgeNum = 0;
    while(getline(data,tmp_line)){
        tmp_vec = split(tmp_line, ",");
        edges[edgeNum].id = tmp_vec[0];
        edges[edgeNum].linkNum = 0;
        edges[edgeNum].num = edgeNum;
        edges[edgeNum++].limit = stoi(tmp_vec[1]);
    }
    data.close();
    data.clear();

    data.open(DATA_PATH+"qos.csv");//客户节点和边缘节点的Qos
    getline(data, tmp_line);
    int pt = 0;
    while (getline(data, tmp_line)) {
        tmp_vec = split(tmp_line, ",");
        for (int i = 1; i <= cmrNum; i++) {
            int qos = stoi(tmp_vec[i]);
            if (qos < QOS_LIMIT) {
                Link *link = new Link();
                link->cmr = cmrs + i - 1;
                link->edge = edges + pt;
                link->qos = qos;
                cmrs[i-1].links[cmrs[i-1].linkNum++] = link;
                edges[pt].links[edges[pt].linkNum++] = link;
            }
        }
        pt++;
    }
    data.close();
    data.clear();

}

void Output(){
    ofstream output;
    output.open(OUTPUT_PATH);
    bool isFirstLine = true;
    for (int t = 0; t < tickNum; t++) {
        for (int c = 0; c < cmrNum; c++) {
            if (!isFirstLine) output << endl;
            else isFirstLine = false;
            output << cmrs[c].id << ":";
            bool isFirst = true;
            for (int l = 0; l < cmrs[c].linkNum; l++) {
                if (cmrs[c].links[l]->bandwidth[t] > 0) {
                    if (!isFirst) output << ",";
                    else isFirst = false;
                    output << "<" << cmrs[c].links[l]->edge->id << "," << cmrs[c].links[l]->bandwidth[t] << ">";
                }
            }
        }
    }
}
/**
 * 
 */
void testIO()
{
    readConf();
    readData();
}

void firstStep() {
    int fullCnt[EDGE_MAX] = {}; // 边缘节点使用掉的免费次数
    int edgeLimitDs[EDGE_MAX]; // 边缘节点序号按带宽上限降序排序
    int *edgeLimitDsPt = edgeLimitDs; // 边缘节点序号按带宽上限降序排序的指针
    int tickDemandDs[TICK_MAX]; // 时刻按总需求降序排序
    int *tickDemandDsPt = tickDemandDs; // 时刻按总需求降序排序的指针
    int tickDemands[TICK_MAX] = {}; // 每时刻的总需求

    // 按总需求量降序排序时刻，按带宽上限降序排序边缘节点
    for (int t = 0; t < tickNum; t++) {
        tickDemandDs[t] = t;
        for (int i = 0; i < cmrNum; i++) tickDemands[t] += cmrs[i].demand[t];
    }
    sort(tickDemandDs, tickDemandDs+tickNum, [&tickDemands](int &a, int &b){return tickDemands[a] > tickDemands[b];});
    for (int j = 0; j < edgeNum; j++) edgeLimitDs[j] = j;
    sort(edgeLimitDs, edgeLimitDs+edgeNum, [](int &a, int &b){return edges[a].limit > edges[b].limit;});
    
    // 优先为总需求量最大的时刻分配带宽上限不超过剩余需求量且最大的边缘节点的全部带宽
    while (edgeLimitDsPt != edgeLimitDs + edgeNum && tickDemandDsPt != tickDemandDs + tickNum) {
        int demand = tickDemands[*tickDemandDsPt];
        bool flag = false;
        for (int *p = edgeLimitDsPt; p != edgeLimitDs + edgeNum; p++) {
            if (fullCnt[*p] < fullNum && edges[*p].limit <= demand) {
                edges[*p].isFree[*tickDemandDsPt] = true;
                tickDemands[*tickDemandDsPt] -= edges[*p].limit;
                fullCnt[*p]++;
                flag = true;
                break;
            }
        }
        if (fullCnt[*edgeLimitDsPt] >= fullNum) edgeLimitDsPt++;
        if (!flag) tickDemandDsPt++;
    }
}

int adjustBnadwidth(int num, bool isEdge, int tick, int need, int depth, int depthMax) {
    if (depth > depthMax || isEdge && edgeFlag[num] || !isEdge && cmrFlag[num]) return 0;
    int bandwidthPassed = 0;
    if (!isEdge) {
        cmrFlag[num] = true;
        for (int l = 0; l < cmrs[num].linkNum; l++) {
            int bandwidthGot = adjustBnadwidth(cmrs[num].links[l]->edge->num, true, tick, need-bandwidthPassed+cmrs[num].demand[tick], depth+1, depthMax);
            int bandwidthKept = min(cmrs[num].demand[tick], bandwidthGot);
            cmrs[num].links[l]->bandwidth[tick] += bandwidthKept;
            cmrs[num].demand[tick] -= bandwidthKept;
            bandwidthPassed += max(0, bandwidthGot-bandwidthKept);
            if (bandwidthPassed >= need) return bandwidthPassed;
        }
    } else {
        edgeFlag[num] = true;
        int bandwidthGiven = min(need, edges[num].limit - edges[num].bandwidth[tick]);
        bandwidthPassed += bandwidthGiven;
        edges[num].bandwidth[tick] += bandwidthGiven;
        if (bandwidthPassed >= need) return bandwidthPassed;
        for (int l = 0; l < edges[num].linkNum; l++) {
            int bandwidthGot = adjustBnadwidth(edges[num].links[l]->cmr->num, false, tick, need-bandwidthPassed, depth+1, depthMax);
            edges[num].links[l]->bandwidth[tick] -= bandwidthGot;
            bandwidthPassed += bandwidthGot;
            if (bandwidthPassed >= need) return bandwidthPassed;
        }
    }
    return bandwidthPassed;
}

void secondStepWhen(int tick) {
    vector<int> frees; // 免费边缘节点向量
    Customer *cmrLinkNumAs[CMR_MAX]; // 客户节点指针数组，按照客户可连接的边缘节点数量排序
    // 用边缘节点填满全部客户节点
    for (int j = 0; j < edgeNum; j++) {
        if (edges[j].isFree[tick]) frees.emplace_back(j);
    }
    for (int c = 0; c < cmrNum; c++) cmrLinkNumAs[c] = cmrs + c;
    sort(cmrLinkNumAs, cmrLinkNumAs+cmrNum, [](Customer *a, Customer *b){return a->linkNum < b->linkNum;});

    for (int cp = 0; cp < cmrNum; cp++) {
        Customer *cmr = cmrLinkNumAs[cp];
        for (int l = 0; l < cmr->linkNum && cmr->demand[tick] > 0; l++) {
            Edge *edge = cmr->links[l]->edge;
            int edgeBandwidthLeft = edge->limit - edge->bandwidth[tick];
            if (edgeBandwidthLeft >= cmr->demand[tick]) {
                cmr->links[l]->bandwidth[tick] = cmr->demand[tick];
                edge->bandwidth[tick] += cmr->demand[tick];
                cmr->demand[tick] = 0;
            } else {
                cmr->links[l]->bandwidth[tick] = edgeBandwidthLeft;
                edge->bandwidth[tick] = edge->limit;
                cmr->demand[tick] -= edgeBandwidthLeft;
            }
        }
    }

    for (int c = 0; c > cmrNum; c++) {
        for (int dm = 3; cmrs[c].demand[tick] > 0; dm += 2) {
            for (int cp = 0; cp < cmrNum; cp++) cmrFlag[cp] = false;
            for (int ep = 0; ep < edgeNum; ep++) edgeFlag[ep] = false;
            adjustBnadwidth(c, false, tick, cmrs[c].demand[tick], 0, dm);
        }
    }

    // for (int j = 0; j < edgeNum; j++) {
    //     if (edges[j].isFree[tick]) frees.emplace_back(j);
    //     for (int l = 0; l < edges[j].linkNum; l++) {
    //         Customer *cmr = edges[j].links[l]->cmr;
    //         if (cmr->demand[tick] == 0) continue;
    //         if (cmr->demand[tick] < edges[j].limit - edges[j].bandwidth[tick]) {
    //             edges[j].links[l]->bandwidth[tick] = cmr->demand[tick];
    //             edges[j].bandwidth[tick] += cmr->demand[tick];
    //             cmr->demand[tick] = 0;
    //         } else {
    //             edges[j].links[l]->bandwidth[tick] = edges[j].limit - edges[j].bandwidth[tick];
    //             cmr->demand[tick] -= edges[j].limit - edges[j].bandwidth[tick];
    //             edges->bandwidth[tick] = edges[j].limit;
    //             break;
    //         }
    //     }
    // }

    // // 第二步，用免费边缘节点填满未满足客户
    // sort(frees.begin(), frees.end(), [](int &a, int &b){return edges[a].limit > edges[b].limit;});
    // for (auto it = frees.begin(); it != frees.end(); it++) {
    //     int j = *it;
    //     for (int l = 0; l < edges[j].linkNum; l++) {
    //         Customer *cmr = edges[j].links[l]->cmr;
    //         if (cmr->demand[tick] == 0) continue;
    //         if (cmr->demand[tick] < edges[j].limit - edges[j].bandwidth[tick]) {
    //             edges[j].links[l]->bandwidth[tick] = cmr->demand[tick];
    //             edges[j].bandwidth[tick] += cmr->demand[tick];
    //             cmr->demand[tick] = 0;
    //         } else {
    //             edges[j].links[l]->bandwidth[tick] = edges[j].limit - edges[j].bandwidth[tick];
    //             cmr->demand[tick] -= edges[j].limit - edges[j].bandwidth[tick];
    //             edges->bandwidth[tick] = edges[j].limit;
    //             break;
    //         }
    //     }
    // }

    // 用免费边缘节点剩余带宽分担非免费边缘节点的带宽
    sort(frees.begin(), frees.end(), [&tick](int &a, int &b){return edges[a].limit - edges[a].bandwidth[tick] > edges[b].limit - edges[b].bandwidth[tick];});
    struct Chain {
        Edge *edge;
        vector<pair<Link*, Link*>> paths;

        Chain(Edge *edge, Link *link1, Link *link2) {
            this->edge = edge;
            this->paths.emplace_back(make_pair(link1, link2));
        }
    };
    for (auto it = frees.begin(); it != frees.end(); it++) {
        vector<Chain> chains;
        for (int l1 = 0; l1 < edges[*it].linkNum; l1++) {
            Link *link1 = edges[*it].links[l1];
            Customer *cmr = link1->cmr;
            for (int l2 = 0; l2 < cmr->linkNum; l2++) {
                Link *link2 = cmr->links[l2];
                Edge *edge = link2->edge;
                if (edge->isFree[tick]) continue;
                bool flag = false;
                for (auto cit = chains.begin(); cit != chains.end(); cit++) {
                    if (cit->edge == edge) {
                        flag = true;
                        cit->paths.emplace_back(make_pair(link1, link2));
                        break;
                    }
                }
                if (!flag) {
                    chains.emplace_back(Chain(edge, link1, link2));
                }
            }
        }

        sort(chains.begin(), chains.end(), [&tick](Chain &a, Chain &b){return a.edge->bandwidth[tick] > b.edge->bandwidth[tick];});
        int *bandwidthLeft = new int[chains.size()];
        for (int c = 0; c < chains.size(); c++) {
            sort(chains[c].paths.begin(), chains[c].paths.end(), [&tick](pair<Link*, Link*> &a, pair<Link*, Link*> &b){return a.second->bandwidth[tick] > b.second->bandwidth[tick];});
            int target;
            if (c+1 >= chains.size()) target = 0;
            else target = chains[c].edge->bandwidth[tick];
            bandwidthLeft[c] = 0;
            for (int l = 0; l < chains[c].edge->linkNum; l++) bandwidthLeft[c] += chains[c].edge->links[l]->bandwidth[tick];
            
            int cnt = c + 1;
            bool isRunUp = false;
            while (cnt) {
                int minLeft = 2147483647;
                int targetNeed = -1;
                cnt = c + 1;
                for (int e = 0; e <= c; e++) {
                    if (bandwidthLeft[e] > 0) {
                        minLeft = min(minLeft, bandwidthLeft[e]);
                        if (targetNeed == -1) targetNeed = chains[e].edge->bandwidth[tick] - target;
                    } else {
                        cnt--;
                    }
                }

                if (cnt < 1) break;
                int bandwidthGiven = min(minLeft, targetNeed);
                if (edges[*it].limit - edges[*it].bandwidth[tick] <= bandwidthGiven * cnt) {
                    bandwidthGiven = (edges[*it].limit - edges[*it].bandwidth[tick]) / cnt;
                    isRunUp = true;
                }
                edges[*it].bandwidth[tick] += bandwidthGiven * cnt;
                for (int e = 0; e <= c; e++) {
                    if (bandwidthLeft[e] > 0) {
                        bandwidthLeft[e] -= bandwidthGiven;
                        chains[e].edge->bandwidth[tick] -= bandwidthGiven;
                        int bandwidthGivenLeft = bandwidthGiven;
                        auto p = chains[e].paths.begin();
                        while (bandwidthGivenLeft && p != chains[e].paths.end()) {
                            int pathGiven = min(bandwidthGivenLeft, p->second->bandwidth[tick]);
                            p->first->bandwidth[tick] += pathGiven;
                            p->second->bandwidth[tick] -= pathGiven;
                            bandwidthGivenLeft -= pathGiven;
                            p++;
                        }
                    }
                }
                if (isRunUp || targetNeed <= minLeft) break;
            }
            if (isRunUp) break;
        }
        delete bandwidthLeft;
    }
}

void secondStep() {
    for (int t = 0; t < tickNum; t++) secondStepWhen(t);
}

int main() {
    testIO();
    firstStep();
    secondStep();
    Output();
    return 0;
}
