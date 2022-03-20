import logging

logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s', level=logging.INFO)

class DataPool:
    def __init__(self) -> None:
        self.read_data()

    @staticmethod
    def read_config() -> str:
        """
        读取配置的QOS上限
        """
        with open("data/config.ini", mode="r", encoding="utf-8") as f:
            temp: list[str] = f.readlines()[1].replace("\n", "").split("=")
            max_qos: int = int(temp[1])
        f.close()
        return max_qos

    @staticmethod
    def read_qos(max_qos) -> tuple:
        """
        读取各custom节点和各edge节点的QOS关系，并分别初始化custom和edge节点信息
        """
        with open("data/qos.csv", mode="r", encoding="utf-8") as f:
            first_line: str = f.readline()
            first_line_splited: list[str] = first_line.replace("\n", "").split(",")
            user_list: list[str] = first_line_splited[1:]
            site_list: list[str] = []
            user_site_list: dict[str, list[str]] = {}
            user_site_map: dict[str, dict[bool]] = {}

            for user in user_list:
                user_site_list[user] = []
                user_site_map[user] = {}

            for line in f.readlines():
                line_splited: list[str] = line.replace("\n", "").split(",")
                site: str = line_splited[0]
                site_list.append(site)

                for index in range(len(user_list)):
                    if int(line_splited[index + 1]) < max_qos:
                        user_site_list[user].append(site)
                        user_site_map[user][site] = True
                    else:
                        user_site_map[user][site] = False

        f.close()

        return user_list, site_list, user_site_list, user_site_map
    
    @staticmethod
    def read_bandwidth() -> dict:
        site_bandwidth_map = {}

        with open("data/site_bandwidth.csv", mode="r", encoding="utf-8") as f:
            f.readline()
            for line in f.readlines():
                line_splited: list[str] = line.replace("\n", "").split(",")
                site_bandwidth_map[line_splited[0]] = int(line_splited[1])
        f.close()

        return site_bandwidth_map
    
    @staticmethod
    def read_demand(user_list) -> tuple:
        timestamp_key_map: dict[str, int] = {}
        user_demand_list: dict[str, list[int]] = {}
        time_demand_list: dict[int, list[int]] = {}


        with open("data/demand.csv", mode="r", encoding="utf-8") as f:
            f.readline()

            for user in user_list:
                user_demand_list[user] = []

            row: int = 0
            for line in f.readlines():
                line_splited: list[str] = line.replace("\n", "").split(",")
                timestamp_key_map[line_splited[0]] = row
                time_demand_list[row] = []

                for index in range(len(user_list)):
                    demand_value: int = int(line_splited[index + 1])
                    user_demand_list[user_list[index]].append(demand_value)
                    time_demand_list[row].append(demand_value)
                
                row += 1
        
        return timestamp_key_map, user_demand_list, time_demand_list

    def read_data(self):
        self.max_qos = self.read_config()
        self.user_list, self.site_list, self.user_site_list, self.user_site_map = self.read_qos(self.max_qos)
        self.site_bandwidth_map = self.read_bandwidth()
        self.timestamp_key_map, self.user_demand_list, self.time_demand_list = self.read_demand(self.user_list)
    
    def display_data(self):
        logging.info("[Reading] max_qos(%s) is %d", type(self.max_qos), self.max_qos)
        logging.info("[Reading] user_list(%s) has %d elements, the first one is %s", type(self.user_list), len(self.user_list), self.user_list[0])


def main():
    DataPool().display_data()


if __name__ == "__main__":
    main()
