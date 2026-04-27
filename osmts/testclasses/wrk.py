import os
import shutil
from pathlib import Path
import re,subprocess
from openpyxl import Workbook
from openpyxl.styles import Alignment

from .errors import RunError,SummaryError



class Wrk:
    def __init__(self,**kwargs):
        self.rpms = {'wrk'}
        self.directory:Path = kwargs.get('saved_directory') / 'wrk'
        self.wrk_seconds:int = kwargs.get('wrk_seconds',60)
        self.wrk_url:str = kwargs.get('wrk_url','http://www.baidu.com')
        self.run_command = f"wrk -t{os.cpu_count() or 1} -c1023 -d{self.wrk_seconds}s --latency {self.wrk_url}"
        self.test_result = ''


    # 在测试结果文本中以多行模式执行正则搜索
    def _search(self, pattern: str):
        return re.search(pattern, self.test_result, re.MULTILINE)


    def pre_test(self):
        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir(parents=True)


    def run_test(self):
        try:
            wrk = subprocess.run(
                self.run_command,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise RunError(e.returncode,'wrk命令运行报错,报错信息:' + e.stderr.decode('utf-8'))

        self.test_result = wrk.stdout.decode('utf-8')
        stderr_text = wrk.stderr.decode('utf-8')
        if stderr_text.strip():
            self.test_result = f"{self.test_result}\n\n[stderr]\n{stderr_text}"
        with open(self.directory / 'wrk.log','w') as file:
            file.write(self.test_result)


    def result2summary(self):
        wb = Workbook()
        ws = wb.active
        ws.title = 'wrk'
        running_line = self._search(r"^\s*Running\s+(.+)$")
        thread_connection = self._search(r"^\s*(\d+)\s+threads and\s+(\d+)\s+connections\s*$")

        ws.append(['Command', self.run_command, '', '', ''])
        if running_line is not None:
            ws.append(['Running', running_line.group(1), '', '', ''])
        if thread_connection is not None:
            ws.append(['Threads', thread_connection.group(1), '', '', ''])
            ws.append(['Connections', thread_connection.group(2), '', '', ''])
        ws.append(['', '', '', '', ''])
        ws.append(['Thread Stats', '', '', '', ''])
        ws.append(['metric', 'Avg(平均值)', 'Stdev(标准差)', 'Max(最大值)', '+/- Stdev(正负一个标准差所占比例)'])

        # Latency   265.57ms  382.20ms   2.00s    85.56%
        latency = self._search(r"^\s*Latency\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)%\s*$")
        if latency is None:
            raise ValueError('wrk日志中未找到Latency统计')
        ws.append(['Latency(延迟)',latency.group(1),latency.group(2),latency.group(3),latency.group(4)+'%'])

        # Req/Sec    25.06     22.19   310.00     84.21%
        req_per_sec = self._search(r"^\s*Req/Sec\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)%\s*$")
        if req_per_sec is not None:
            ws.append(['Req/Sec(每秒请求数)',req_per_sec.group(1),req_per_sec.group(2),req_per_sec.group(3),req_per_sec.group(4)+'%'])
        else:
            ws.append(['Req/Sec(每秒请求数)','未在日志中提供','','',''])

        ws.append(['', '', '', '', ''])

        # Latency Distribution
        ws.append(['Latency Distribution(延迟分布)', '', '', '', ''])
        for LD in ('50%','75%','90%','99%'):
            distribution = self._search(rf"^\s*{LD}\s+(\S+)\s*$")
            if distribution is None:
                raise ValueError(f'wrk日志中未找到{LD}延迟分布')
            ws.append([LD,distribution.group(1), '', '', ''])

        ws.append(['', '', '', '', ''])

        overall = self._search(r"^\s*(\d+)\s+requests in\s+([^,]+),\s+(\S+)\s+read\s*$")
        if overall is None:
            raise ValueError('wrk日志中未找到整体请求统计')
        ws.append([f"在{overall.group(2)} 内处理了{overall.group(1)} 个请求，读取了{overall.group(3)}数据", '', '', '', ''])

        # Socket errors: connect 3, read 131564, write 0, timeout 1836
        socket_errors = self._search(r"^\s*Socket errors:\s*connect\s+(\d+),\s*read\s+(\d+),\s*write\s+(\d+),\s*timeout\s+(\d+)\s*$")
        if socket_errors is not None:
            ws.append(['发生错误统计','connect','read','write','timeout'])
            ws.append(['',socket_errors.group(1),socket_errors.group(2),socket_errors.group(3),socket_errors.group(4)])

        ws.append(['', '', '', '', ''])

        # Requests/sec:    671.87
        requests = self._search(r"^\s*Requests/sec:\s+(\S+)\s*$")
        if requests is None:
            raise ValueError('wrk日志中未找到Requests/sec')
        ws.append([f'平均每秒处理请求数:',requests.group(1), '', '', ''])

        # Transfer/sec:     19.57MB
        transfer = self._search(r"^\s*Transfer/sec:\s+(\S+)\s*$")
        if transfer is None:
            raise ValueError('wrk日志中未找到Transfer/sec')
        ws.append([f'平均每秒读取数据:',transfer.group(1), '', '', ''])

        ws.freeze_panes = 'A2'
        for row in ws.iter_rows():
            for cell in row:
                cell.alignment = Alignment(vertical='top', wrap_text=True)

        ws.column_dimensions['A'].width = 34
        ws.column_dimensions['B'].width = 38
        ws.column_dimensions['C'].width = 18
        ws.column_dimensions['D'].width = 18
        ws.column_dimensions['E'].width = 24

        wb.save(self.directory / 'wrk.xlsx')


    def run(self):
        print('开始进行wrk测试')
        self.pre_test()
        self.run_test()
        try:
            self.result2summary()
        except Exception as e:
            logFile = self.directory / 'wrk_summary_error.log'
            with open(logFile,'w') as log:
                log.write(str(e))
            raise SummaryError(logFile)
        print('wrk测试结束')
