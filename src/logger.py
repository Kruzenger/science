import time

class Logger:
    def __init__(self):
        self.file = open("logs.txt", "w")

    def debug(self, data: str = ""):
        now = time.gmtime(time.time())
        self.file.write(f"[{now.tm_year}.{now.tm_mon}.{now.tm_mday} {now.tm_hour}:{now.tm_min}:{now.tm_sec}]: Debug: {data}\n")
