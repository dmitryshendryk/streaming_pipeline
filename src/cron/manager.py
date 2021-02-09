

import schedule
import time



class CronTab():
    def __init__(self, job, configurator) -> None:
        self.configurator = configurator
        
        amount = self.configurator['cron']['amount']
        range = self.configurator['cron']['range']

        if range == 'seconds':
            schedule.every(amount).seconds.do(job)
        elif range == 'minutes':
            schedule.every(amount).minutes.do(job)
        elif range == 'hour':
            schedule.every(amount).hour.do(job)
        elif range == 'day':
            schedule.every(amount).hour.do(job)
        
    @staticmethod
    def start():
        while 1:
            schedule.run_pending()
            time.sleep(1)