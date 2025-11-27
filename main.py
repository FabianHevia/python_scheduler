'''
La idea del desafío es programar un mini-motor de 
tareas programadas, similar al cron de Linux, que:

    - Permita registrar tareas con una regla de tiempo.
    - Interprete esas reglas.
    - Ejecute automáticamente la función cuando corresponde.
    - Maneje múltiples tareas simultáneas.
    - No bloquee el programa.

    
Penitencias:

    - No se puede usar schedule, APScheduler, etc.
    - Se debe hacer desde cero, usando hilos o async.

Desarrollo del código:

Vamos a construir un “cron” básico con:

    - Una clase CronJob
    - Un manejador CronScheduler
    - Threads para ejecutar las tareas sin bloquear
    - Reglas simples tipo "*/5" para “cada 5 segundos”
'''

# Código del Schedule

import time
import threading
from typing import Callable, List


class CronJob:


    def __init__(self, rule: str, func: Callable):

        self.interval = self._parse_rule(rule)
        self.func = func
        self.next_run = time.time() + self.interval


    def _parse_rule(self, rule: str) -> int:

        if not rule.startswith("*/"):

            raise ValueError("Regla no válida. Usa formato */N")
        
        return int(rule.split("/")[1])


    def should_run(self) -> bool:

        # Indica si la tarea debe ejecutarse ahora

        return time.time() >= self.next_run


    def run(self):

        # Ejecuta la tarea y reprograma el siguiente run

        self.func()
        self.next_run = time.time() + self.interval


class CronScheduler:


    def __init__(self):

        self.jobs: List[CronJob] = []
        self.running = False


    def add_job(self, rule: str, func: Callable):

        self.jobs.append(CronJob(rule, func))


    def start(self):

        self.running = True
        threading.Thread(target=self._loop, daemon=True).start()


    def _loop(self):
        
        # Ciclo interno que revisan las tareas a ejecutar

        while self.running:

            for job in self.jobs:

                if job.should_run():

                    threading.Thread(target=job.run, daemon=True).start()

            time.sleep(0.1)


# Ejemplo de Uso

def tarea_saludo():

    print("Se ejecutó la tarea")

scheduler = CronScheduler()
scheduler.add_job("*/3", tarea_saludo)
scheduler.start()

print("Cron iniciado, las tareas se ejecutan en el background")

# Mantener el programa vivo
while True:

    time.sleep(1)
