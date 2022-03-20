import operator
from typing import List, Dict
import copy

if __name__ == "__main__":
    """
    第一部分
    读取数据
    """
    with open("data/config.ini", mode="r", encoding="utf-8") as f:
        temp1: List[str] = f.readlines()
        temp2: List[str] = temp1[1].replace("\n", "").split("=")
        MAX_QOS: int = int(temp2[1])    # 获取 Qos 最大限制
    f.close()
    # print(MAX_QOS)

    userCanUseSiteMap: Dict[str, List[str]] = {}    # 用户在 Qos 限制下能够访问的网站列表
    with open("data/qos.csv", mode="r", encoding="utf-8") as f:
        temp1: str = f.readline()
        temp2: List[str] = temp1.replace("\n", "").split(",")
        user: List[str] = temp2[1:]     # 用户列表
        site: List[str] = []    # 网站列表
        for u in user:
            userCanUseSiteMap[u]: List[str] = []
        for line in f.readlines():
            temp3: List[str] = line.replace("\n", "").split(",")
            siteName: str = temp3[0]
            site.append(siteName)
            for index in range(len(user)):
                if int(temp3[index + 1]) < MAX_QOS:
                    userCanUseSiteMap[user[index]].append(siteName)
    f.close()
    print("user:", user)
    print("site:", site)
    print("userCanUseSiteMap", userCanUseSiteMap)

    timeToInt: Dict[str, int] = {}  # 字符串时间转换成 int 时间
    timeAndUserNeed: Dict[int, List[int]] = {}  # int 时间和用户需求列表
    userNeed: Dict[str, List[int]] = {}     # 每个用户和用户需求
    lengthOfNeed: int = 0
    with open("data/demand.csv", mode="r", encoding="utf-8") as f:
        f.readline()
        temp1: int = 0
        for u in user:
            userNeed[u]: List[int] = []
        for line in f.readlines():
            temp2: List[str] = line.replace("\n", "").split(",")
            timeToInt[temp2[0]] = temp1
            timeAndUserNeed[temp1]: List[int] = []
            for index in range(len(user)):
                temp3: int = int(temp2[index + 1])
                timeAndUserNeed[temp1].append(temp3)
                userNeed[user[index]].append(temp3)
            temp1 += 1
        lengthOfNeed = temp1
    f.close()
    # print("lengthOfNeed:", lengthOfNeed)
    print("timeToInt:", timeToInt)
    print("timeAndUserNeed", timeAndUserNeed)
    # print(len(timeAndUserNeed[0]))
    print("userNeed:", userNeed)
    # print(len(userNeed["A"]))

    siteAndBand: Dict[str, int] = {}
    with open("data/site_bandwidth.csv", mode="r", encoding="utf-8") as f:
        f.readline()
        for line in f.readlines():
            temp1: List[str] = line.replace("\n", "").split(",")
            siteAndBand[temp1[0]] = int(temp1[1])
    f.close()
    print("siteAndBand:", siteAndBand)

    """
    第二部分
    计算相应值
    """
    # for s in site:
    #     siteAndBand[s] = 5000
    result: List[Dict[str, Dict[str, int]]] = []
    remainSiteAndBand: Dict[str, int] = {}  # 网站剩余容量
    for need in timeAndUserNeed.values():
        remainSiteAndBand = copy.deepcopy(siteAndBand)
        need: List[int]     # 每时刻的需求
        temp6: Dict[str, Dict[str, int]] = {}
        for index, temp1 in enumerate(need):
            temp2: str = user[index]    # 用户
            temp3: Dict[str, int] = {}
            temp4: List[str] = userCanUseSiteMap[temp2]     # 用户可用网站
            flag: int = len(temp4)  # 用于判断是否消除了全部 div
            div: int = temp1 // len(temp4)
            mod: int = temp1 % len(temp4)
            remainSiteAndBand = dict(sorted(remainSiteAndBand.items(), key=operator.itemgetter(1), reverse=True))
            if temp1 == 0:
                temp6[temp2] = temp3
                continue
            while flag != 0 or mod != 0:
                for temp5 in temp4:     # temp5 是分配的网站
                    if remainSiteAndBand[temp5] >= div + mod:
                        if temp5 in temp3.keys():
                            temp3[temp5] += div + mod
                        else:
                            temp3[temp5] = div + mod
                        remainSiteAndBand[temp5] -= (div + mod)
                        mod = 0
                    else:
                        if remainSiteAndBand[temp5] != 0:
                            if temp5 in temp3.keys():
                                temp3[temp5] += remainSiteAndBand[temp5]
                            else:
                                temp3[temp5] = remainSiteAndBand[temp5]
                            mod += (div - remainSiteAndBand[temp5])
                            remainSiteAndBand[temp5] = 0
                    if flag != 0:
                        flag -= 1
                    elif flag == 0:
                        div = 0
                    remainSiteAndBand = dict(sorted(remainSiteAndBand.items(), key=operator.itemgetter(1), reverse=True))
            temp6[temp2] = temp3
        result.append(temp6)
    """
    第三部分
    写文件

    输出样例：四个用户对应一个时刻
        CB:<S1,9700>,<SB,6>
        CA:<S1,6704>,<SB,3352>,<SD,3353>
        CE:<S1,5209>
        CX:
        CB:<S1,8000>,<SB,120>,<SD,7>
        CA:<S1,10000>,<SB,1100>,<SD,54>
        CE:<S1,2>,<SD,4260>
        CX:<S1,300>
    """
    with open("output/solution.txt", mode="w", encoding="utf-8") as f:
        for everytimeResult in result:  # 获取每时刻的带宽分配结果
            everytimeResult: Dict[str, Dict[str, int]]
            for temp1 in user:  # 获取用户
                f.write(temp1 + ":")
                userResult: Dict[str, int] = everytimeResult[temp1]
                if len(userResult) != 0:    # 如果该用户结果长度为零则直接跳过，如果不为零则输出每个分配结果
                    temp2: int = 0
                    for (tempUser, tempBand) in zip(userResult.keys(), userResult.values()):
                        f.write("<" + str(tempUser) + "," + str(tempBand) + ">")
                        temp2 += 1
                        if temp2 < len(userResult):
                            f.write(",")
                f.write("\n")
    f.close()
