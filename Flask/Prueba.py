import threading
import time
import datetime

def worker():
    print(threading.currentThread().getName(), 'Lanzado', time.localtime())
    time.sleep(2)
    print(threading.currentThread().getName(), 'Deteniendo', time.localtime())
def servicio():
    print(threading.currentThread().getName(), 'Lanzado', time.localtime())
    print(threading.currentThread().getName(), 'Deteniendo', time.localtime())

w = threading.Thread(target=worker, name='Worker')
z = threading.Thread(target=worker, name= 'Worker1')
t = threading.Thread(target=servicio, name='Servicio')

w.start()
z.start()
t.start()

