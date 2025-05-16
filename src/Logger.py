import time

class Logger:
    def __init__(self):
        self.file = open("logs.txt", "w")

    def log(self, data: str) -> str:
        now = time.gmtime(time.time())
        log_data = f"[{time.asctime(now)}]: Debug: {data}\n"
        self.file.write(log_data)
        return log_data

    def debug(self, data: str) -> str:
        log_data = self.log(data)
        print(log_data)
        return log_data
