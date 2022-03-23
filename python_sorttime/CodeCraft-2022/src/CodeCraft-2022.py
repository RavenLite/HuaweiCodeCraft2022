# import logging
# logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s', level=logging.INFO)

"""
This is the algorithm implement of Huawei CodeCraft 2022 contest, a edge-node bandwidth scheduling algorithm.
Contest homepage: https://competition.huaweicloud.com/codecraft2022
Remote repository: https://github.com/RavenLite/HuaweiCodeCraft2022

Online judging environment:

Hardware: 4U 4G, x86
OS: ubuntu:18.04
Python: Python3.7.3
PyPy: pypy3.7-v7.3.7
numpy: 1.19.4
"""

class DataPool:
    """
    Import data from files.

    config.ini - qos limit (all time)
    qos.csv - qos between user and edge (all time)
    site_bandwidth.csv - max bandwith of each edge (all time)
    demand.csv - demand of each user (by time)
    """
    def __init__(self):
        self.read_data()

    @staticmethod
    def read_config():
        with open("/data/config.ini", mode="r", encoding="utf-8") as f:
            temp: list[str] = f.readlines()[1].replace("\n", "").split("=")
            max_qos: int = int(temp[1])
        f.close()
        return max_qos

    def read_qos(self):
        with open("/data/qos.csv", mode="r", encoding="utf-8") as f:
            first_line: str = f.readline()
            first_line_splited: list[str] = first_line.replace("\n", "").split(",")
            user_list: list[str] = first_line_splited[1:]
            edge_list: list[str] = []
            user_edge_list_map: dict[str, list[str]] = {}
            user_edge_qos_map: dict[str, dict[bool]] = {}

            for user in user_list:
                user_edge_list_map[user] = []
                user_edge_qos_map[user] = {}

            for line in f.readlines():
                line_splited: list[str] = line.replace("\n", "").split(",")
                edge: str = line_splited[0]
                edge_list.append(edge)

                for index in range(len(user_list)):
                    if int(line_splited[index + 1]) < self.max_qos:
                        user_edge_list_map[user_list[index]].append(edge)
                        user_edge_qos_map[user_list[index]][edge] = True
                    else:
                        user_edge_qos_map[user_list[index]][edge] = False

        f.close()

        return user_list, edge_list, user_edge_list_map, user_edge_qos_map
    
    @staticmethod
    def read_bandwidth() -> dict:
        edge_bandwidth_map = {}

        with open("/data/site_bandwidth.csv", mode="r", encoding="utf-8") as f:
            f.readline()
            for line in f.readlines():
                line_splited: list[str] = line.replace("\n", "").split(",")
                edge_bandwidth_map[line_splited[0]] = int(line_splited[1])
        f.close()

        return edge_bandwidth_map
    
    def read_demand(self) -> tuple:
        timestamp_key_map: dict[str, int] = {}
        user_demand_list_map: dict[str, list[int]] = {}
        time_demand_list_map: dict[int, list[int]] = {}

        with open("/data/demand.csv", mode="r", encoding="utf-8") as f:
            f.readline()

            for user in self.user_list:
                user_demand_list_map[user] = []

            row: int = 0
            for line in f.readlines():
                line_splited: list[str] = line.replace("\n", "").split(",")
                timestamp_key_map[line_splited[0]] = row
                time_demand_list_map[row] = []

                for index in range(len(self.user_list)):
                    demand_value: int = int(line_splited[index + 1])
                    user_demand_list_map[self.user_list[index]].append(demand_value)
                    time_demand_list_map[row].append(demand_value)
                
                row += 1
        
        return timestamp_key_map, user_demand_list_map, time_demand_list_map

    def read_data(self):
        self.max_qos = self.read_config()
        self.user_list, self.edge_list, self.user_edge_list_map, self.user_edge_qos_map = self.read_qos()
        self.edge_bandwidth_map = self.read_bandwidth()
        self.timestamp_key_map, self.user_demand_list_map, self.time_demand_list_map = self.read_demand()
        self.user_count = len(self.user_list)
        self.edge_count = len(self.edge_list)
        self.timestamp_count = len(list(self.timestamp_key_map.keys()))
        self.free_count = int(self.timestamp_count * 0.05)
    
    def display_data(self):
        pass
        # logging.info("[Reading] max_qos(%s) is %d", type(self.max_qos), self.max_qos)
        # logging.info("[Reading] user_list(%s) has %d elements, the first one is %s", type(self.user_list), len(self.user_list), self.user_list[0])
        # logging.info("[Reading] edge_list(%s) has %d elements, the first one is %s", type(self.edge_list), len(self.edge_list), self.edge_list[0])
        # logging.info("[Reading] user_edge_list_map(%s) has %d pairs, the first one is %s - %s", type(self.user_edge_list_map), len(self.user_edge_list_map.keys()), list(self.user_edge_list_map.keys())[0], self.user_edge_list_map[list(self.user_edge_list_map.keys())[0]])
        # logging.info("[Reading] user_edge_qos_map(%s) has %d pairs, the first one is %s - %s", type(self.user_edge_qos_map), len(self.user_edge_qos_map.keys()), list(self.user_edge_qos_map.keys())[0], self.user_edge_qos_map[list(self.user_edge_qos_map.keys())[0]])
        # logging.info("[Reading] edge_bandwidth_map(%s) has %d pairs, the first one is %s - %s", type(self.edge_bandwidth_map), len(self.edge_bandwidth_map.keys()), list(self.edge_bandwidth_map.keys())[0], self.edge_bandwidth_map[list(self.edge_bandwidth_map.keys())[0]])
        # logging.info("[Reading] timestamp_key_map(%s) has %d pairs, the first one is %s - %s", type(self.timestamp_key_map), len(self.timestamp_key_map.keys()), list(self.timestamp_key_map.keys())[0], self.timestamp_key_map[list(self.timestamp_key_map.keys())[0]])
        # logging.info("[Reading] user_demand_list_map(%s) has %d pairs, the first one is %s - %s", type(self.user_demand_list_map), len(self.user_demand_list_map.keys()), list(self.user_demand_list_map.keys())[0], self.user_demand_list_map[list(self.user_demand_list_map.keys())[0]])
        # logging.info("[Reading] time_demand_list_map(%s) has %d pairs, the first one is %s - %s", type(self.time_demand_list_map), len(self.time_demand_list_map.keys()), list(self.time_demand_list_map.keys())[0], self.time_demand_list_map[list(self.time_demand_list_map.keys())[0]])


class DemandPool:
    """
    Initialize the left demand and resource.

    timeIndex_user_left_map - user left demand (by time)
    timeIndex_edge_left_map - edge left bandwidth (by time)
    edge_free_left - edge left free count (large bandwidth over 95th sorted by time)
    """
    def __init__(self, dataPool: DataPool):
        self.timeIndex_user_left_map = {}
        self.timeIndex_edge_left_map = {}
        self.edge_free_left = {}

        for timeIndex in range(dataPool.timestamp_count):
            self.timeIndex_edge_left_map[timeIndex] = {}
            self.timeIndex_user_left_map[timeIndex] = {}
            for edge_qos in list(dataPool.edge_bandwidth_map.keys()):
                self.timeIndex_edge_left_map[timeIndex][edge_qos] = dataPool.edge_bandwidth_map[edge_qos]
            for user_demand in list(dataPool.user_demand_list_map.keys()):
                self.timeIndex_user_left_map[timeIndex][user_demand] = dataPool.user_demand_list_map[user_demand][timeIndex]
        
        for edge in dataPool.edge_list:
            self.edge_free_left[edge] = dataPool.free_count
        
        # logging.info("[DemandPool] timeIndex_edge_left_map(%s) has %d pairs, the first one is %s - %s", type(self.timeIndex_edge_left_map), len(self.timeIndex_edge_left_map.keys()), list(self.timeIndex_edge_left_map.keys())[0], self.timeIndex_edge_left_map[list(self.timeIndex_edge_left_map.keys())[0]])
        # logging.info("[DemandPool] timeIndex_user_left_map(%s) has %d pairs, the first one is %s - %s", type(self.timeIndex_user_left_map), len(self.timeIndex_user_left_map.keys()), list(self.timeIndex_user_left_map.keys())[0], self.timeIndex_user_left_map[list(self.timeIndex_user_left_map.keys())[0]])


class Scheduler:
    """
    Arrange bandwidth and output results.

    timeIndex_edge_demand_list - edge list sorted by the maxinum arrangeable bandwidth at some time
    timeIndex_user_list_sorted_map - user list sorted by the number of available edge under qos

    meet_demand_by_sorted_edge_timestamp() - phase 1: edge could arrange more, first
    meet_demand_left() - phase 2: arrange left demand, by average
    """
    def __init__(self, dataPool: DataPool, demandPool: DemandPool) -> None:
        self.dataPool = dataPool
        self.demandPool = demandPool
        self.timeIndex_edge_demand_list: list = []
        self.timeIndex_user_list_sorted_map: dict = {}
        self.res: dict = {}

        for timeIndex in range(self.dataPool.timestamp_count):
            self.res[timeIndex] = {}
            for user in self.dataPool.user_list:
                self.res[timeIndex][user] = []
    
    def sort_demand_by_edge_timestamp(self):
        self.timeIndex_edge_demand_list.clear()

        for timeIndex in range(self.dataPool.timestamp_count):
            for edge in self.dataPool.edge_list:
                if self.demandPool.edge_free_left[edge] <= 0:
                    continue
                sum_demand = 0
                for user in list(self.demandPool.timeIndex_user_left_map[timeIndex].keys()):
                    if self.dataPool.user_edge_qos_map[user][edge]:
                        sum_demand += self.demandPool.timeIndex_user_left_map[timeIndex][user]
                self.timeIndex_edge_demand_list.append((timeIndex, edge, sum_demand))

        self.timeIndex_edge_demand_list = sorted(self.timeIndex_edge_demand_list, key=lambda tuple : tuple[2], reverse=True)
        # logging.info("[Scheduling] timeIndex_edge_demand_list(%s) has %d elements, the first one is %s", type(self.timeIndex_edge_demand_list), len(self.timeIndex_edge_demand_list), self.timeIndex_edge_demand_list[0])

    def sort_demand_by_user_timestamp(self):
        for timeIndex in list(self.demandPool.timeIndex_edge_left_map.keys()):
            user_list: list = list(self.demandPool.timeIndex_user_left_map[timeIndex].keys())
            user_edge_num_tuple_list: list = []

            for user in user_list:
                user_edge_num_tuple_list.append((user, len(self.dataPool.user_edge_list_map[user])))

            user_edge_num_tuple_list = sorted(user_edge_num_tuple_list, key=lambda tuple : tuple[1])
            user_list_sorted = []
            for user_edge_num_tuple in user_edge_num_tuple_list:
                user_list_sorted.append(user_edge_num_tuple[0])

            self.timeIndex_user_list_sorted_map[timeIndex] = user_list_sorted

    # method one: sort edges by amount of space already used
    def sort_demand_by_user_edge_list(self, timeIndex: int, user_edge_list: list) -> list:
        sorted_user_edge_list = []
        for edge in user_edge_list:
            if self.demandPool.timeIndex_edge_left_map[timeIndex][edge] <= 0:
                continue
            demand = self.dataPool.edge_bandwidth_map[edge] - self.demandPool.timeIndex_edge_left_map[timeIndex][
                edge]
            sorted_user_edge_list.append((edge, demand))
        sorted_user_edge_list = sorted(sorted_user_edge_list, key=lambda tuple: tuple[1])
        return sorted_user_edge_list

    def meet_demand_by_sorted_edge_timestamp(self, meet_count_max=4):
        meet_count = 0
        hasLeftEdge = False
        # edge could provide most at some time, first
        for timeIndex_edge_demand in self.timeIndex_edge_demand_list:
            timeIndex = timeIndex_edge_demand[0]
            edge = timeIndex_edge_demand[1]

            if self.demandPool.edge_free_left[edge] <= 0:
                continue

            hasLeftEdge = True
            hasmeet = False
            # user with fewer number of available edge under qos, fisrt be arranged
            for user in self.timeIndex_user_list_sorted_map[timeIndex]:
                if self.dataPool.user_edge_qos_map[user][edge] and self.demandPool.timeIndex_user_left_map[timeIndex][user] > 0:
                    meet_num = 0

                    if self.demandPool.timeIndex_edge_left_map[timeIndex][edge] >= self.demandPool.timeIndex_user_left_map[timeIndex][user]:
                        meet_num = self.demandPool.timeIndex_user_left_map[timeIndex][user]
                        self.demandPool.timeIndex_edge_left_map[timeIndex][edge] -= meet_num
                        self.demandPool.timeIndex_user_left_map[timeIndex][user] = 0
                    else:
                        meet_num = self.demandPool.timeIndex_edge_left_map[timeIndex][edge]
                        self.demandPool.timeIndex_user_left_map[timeIndex][user] -= meet_num
                        self.demandPool.timeIndex_edge_left_map[timeIndex][edge] = 0

                    self.res[timeIndex][user].append((edge, meet_num))
                    hasmeet = True

                    if self.demandPool.timeIndex_edge_left_map[timeIndex][edge] == 0:
                        break
            
            if hasmeet:
                self.demandPool.edge_free_left[edge] -= 1
                # simple dynamic sort: from here to the end of this function
        #         meet_count += 1
        #         if meet_count == meet_count_max:
        #             break

        # if hasLeftEdge and meet_count == meet_count_max:
        #     self.sort_demand_by_edge_timestamp()
        #     self.meet_demand_by_sorted_edge_timestamp(meet_count_max * 4)
    
    def meet_demand_left(self):
        for timeIndex in list(self.demandPool.timeIndex_user_left_map.keys()):
            # user with fewer number of available edge under qos, fisrt be arranged
            for user in self.timeIndex_user_list_sorted_map[timeIndex]:
                if self.demandPool.timeIndex_user_left_map[timeIndex][user] <= 0:
                    continue
                edge_meetnum_map = {}

                count = 0
                user_edge_list = []
                while self.demandPool.timeIndex_user_left_map[timeIndex][user] > 0:
                    # user_edge_list = self.dataPool.user_edge_list_map[user]
                    if count <= 0:
                        user_edge_list = self.sort_demand_by_user_edge_list(timeIndex, self.dataPool.user_edge_list_map[user])
                    edge = user_edge_list[count][0]
                    count = (count + 1) % len(user_edge_list)

                    # TODO: edge with fewer bandwidth at this time should provide more
                    # for edge in user_edge_list:
                    if self.demandPool.timeIndex_edge_left_map[timeIndex][edge] <= 0:
                        continue
                    else:
                        meetnum = 0
                        if self.demandPool.timeIndex_edge_left_map[timeIndex][edge] < self.demandPool.timeIndex_user_left_map[timeIndex][user]:
                            meetnum = self.demandPool.timeIndex_edge_left_map[timeIndex][edge]
                            self.demandPool.timeIndex_edge_left_map[timeIndex][edge] = 0
                            self.demandPool.timeIndex_user_left_map[timeIndex][user] -= meetnum
                        else:
                            meetnum = int(self.demandPool.timeIndex_user_left_map[timeIndex][user] * 0.5)
                            if meetnum == 0:
                                meetnum = self.demandPool.timeIndex_user_left_map[timeIndex][user]
                            self.demandPool.timeIndex_edge_left_map[timeIndex][edge] -= meetnum
                            self.demandPool.timeIndex_user_left_map[timeIndex][user] = self.demandPool.timeIndex_user_left_map[timeIndex][user] - meetnum

                        # avoid repeated addition from the same edge
                        if edge not in list(edge_meetnum_map.keys()):
                            edge_meetnum_map[edge] = meetnum
                        else:
                            edge_meetnum_map[edge] += meetnum

                        # if self.demandPool.timeIndex_user_left_map[timeIndex][user] == 0:
                        #     break
                
                for edge in list(edge_meetnum_map.keys()):
                    self.res[timeIndex][user].append((edge, edge_meetnum_map[edge]))
        
    def output(self):
        with open("/output/solution.txt", mode="w", encoding="utf-8") as f:
            for timeIndex in list(self.res.keys()):
                for user in list(self.res[timeIndex].keys()):
                    f.write(user + ":")

                    timeIndex_user_length: int = len(self.res[timeIndex][user])
                    count: int = 0
                    for edge_meetnum in self.res[timeIndex][user]:
                        f.write("<" + str(edge_meetnum[0]) + "," + str(edge_meetnum[1]) + ">")
                        count += 1
                        if count < timeIndex_user_length:
                            f.write(",")
                    
                    f.write("\n")
        f.close()


    def schedule(self):
        # prehandle
        self.sort_demand_by_edge_timestamp()
        # prehandle
        self.sort_demand_by_user_timestamp()
        # phase 1
        self.meet_demand_by_sorted_edge_timestamp()
        # phase 2
        self.meet_demand_left()
        # output
        self.output()


def main():
    # Import data from files
    dataPool = DataPool()
    dataPool.display_data()
    # Initialize the left demand and resource
    demandPool = DemandPool(dataPool)
    # Arrange bandwidth and output results
    Scheduler(dataPool, demandPool).schedule()


if __name__ == "__main__":
    main()
