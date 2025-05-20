import asyncio
from Controller import controllerTask
    

def main(): 
    loop = asyncio.new_event_loop()
    controller_task = loop.create_task(controllerTask())
    
    loop.run_forever()
    return 0

main()
