import time
import os
import json
import logging
from multiprocessing import Process

from Inspector import Inspector


class CPUAndNetInspector(Inspector):
    NETWORK_METRICS_NAMES = [
        'network_bytes_rx',
        'network_packets_rx',
        'network_errs_rx',
        'network_drop_rx',
        'network_fifo_errors_rx',
        'network_frame_rx',
        'network_compressed_rx',
        'network_multicast_rx',
        'network_bytes_tx',
        'network_packets_tx',
        'network_errs_tx',
        'network_drop_tx',
        'network_fifo_errors_tx',
        'network_colls_tx',
        'network_carrier_tx',
        'network_compressed_tx',
    ]

    def __init__(self):
        super().__init__()

    def inspectNetwork(self):
        netInfo = ""

        with open('/proc/net/dev', 'r') as f:
            netInfo = f.read().splitlines()
        
        # skip header lines
        netInfo = netInfo[2:]

        network_metrics = {name: dict() for name in self.NETWORK_METRICS_NAMES}
        for line in netInfo:
            interface, metrics = tuple(line.split(':', maxsplit=1))
            interface = interface.strip()
            metrics = metrics.split()
            for name, value in zip(self.NETWORK_METRICS_NAMES, metrics):
                network_metrics[name][interface] = int(value)

        self._Inspector__attributes = dict(**self._Inspector__attributes,
                                           **network_metrics)

    def inspectAll(self):
        self.inspectNetwork()
        super().inspectAll()


class MonitorBase:
    OUTPUT_FILEPATH = "/tmp/readings.txt"

    def monitor(self):
        pass

    def started_info(self):
        pass

    def finished_info(self):
        pass

    @staticmethod
    def create_result_file():
        with open(MonitorBase.OUTPUT_FILEPATH, "w"):
            pass

    @staticmethod
    def get_monitoring_results():
        lines = []
        try:
            with open(MonitorBase.OUTPUT_FILEPATH, "r") as f:
                lines = f.readlines()
        except Exception:
            logging.error('MONITORING ERROR:\n'
                          'error while getting monitoring results from file')

        return list(map(lambda x: json.loads(x), lines))


class CPUAndNetMonitor(MonitorBase):
    def __init__(self, rangeid,):
        self.rangehash = hash(rangeid)

    def inspect(self):
        inspector = CPUAndNetInspector()
        inspector.inspectAll()
        inspector.addAttribute("taskID", self.rangehash)

        return inspector.finish()

    def monitor(self):
        self.create_result_file()

        while True:
            os.nice(0)
            inspect_result = self.inspect()
            with open("/tmp/readings.txt","a") as f:
                f.write(json.dumps(inspect_result))
                f.write("\n")
            time.sleep(1)
    
    def started_info(self):
        logging.info('monitoring started!')

    def finished_info(self):
        logging.info('monitoring finished!')


class EmptyMonitor(MonitorBase):
    def __init__(self):
        super().__init__()
        self.create_result_file()

    def monitor(self):
        pass


class monitoring_thread:
    """ Context manager for running monitoring
    """
    def __init__(self, monitor):
        self.monitor = monitor
        self.thread = None

    def __enter__(self):
        self.monitor.started_info()
        self.thread = Process(target=self.monitor.monitor)
        self.thread.start()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.thread.terminate()
        self.thread.join()
        self.monitor.finished_info()
