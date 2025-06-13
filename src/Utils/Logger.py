from Utils.Singleton import Singleton
import time


class Logger(metaclass=Singleton):
    def __init__(self): 
        self.file = open("logs.log", "w")

    def benchmark(self, id: str) -> str:
        log_data = f"[{time.perf_counter()}]: BENCHMARK: {id}\n"
        self.file.write(log_data)
        return log_data

    def log(self, data: str, prefix: str) -> str:
        now = time.gmtime(time.time())
        log_data = f"[{time.asctime(now)}]: {prefix}: {data}\n"
        self.file.write(log_data)
        return log_data

    def debug(self, data: str) -> str:
        log_data = self.log(data, prefix="DEBUG")
        print(log_data)
        return log_data
    
    def error(self, data: str) -> str:
        log_data = self.log(data, prefix="ERROR")
        print(log_data)
        return log_data
