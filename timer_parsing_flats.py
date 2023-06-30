import time
import threading
import pytz
from PIK_parsing_flats import pik_parsing_flats_all_jobs
from Samolet_parsing_flats import samolet_parsing_flats_all_jobs
from A101_parsing_flats import a101_parsing_flats_all_jobs
from datetime import datetime


class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.next_call = time.time()
        self.start()
        
    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self.next_call += self.interval
            self._timer = threading.Timer(self.next_call - time.time(), self._run)
            self._timer.start()
            self.is_running = True
            
    def stop(self):
        self._timer.cancel()
        self.is_running = False


def all_jobs():
    pik_parsing_flats_all_jobs()
    time.sleep(10)
    samolet_parsing_flats_all_jobs()
    time.sleep(10)
    a101_parsing_flats_all_jobs()


if __name__ == '__main__':
    label_bool = True
    while label_bool:
        time.sleep(60)
        datetime_now = datetime.now(pytz.timezone('Europe/Moscow')).replace(tzinfo=None)
        if (datetime_now.hour==15):
            label_bool = False
    rt = RepeatedTimer(86400, all_jobs)
    all_jobs()
