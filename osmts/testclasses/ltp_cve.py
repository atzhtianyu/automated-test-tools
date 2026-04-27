from pathlib import Path
import sys,subprocess,shutil,os,re
from openpyxl import Workbook




"""
ltp cve测试在ltp测试的基础上运行,是ltp其中一个测试
"""

class Ltp_cve:
    def __init__(self, **kwargs):
        self.rpms = {'automake', 'pkgconf', 'autoconf', 'bison', 'flex', 'm4', 'kernel-headers', 'glibc-headers',
                     'findutils', 'libtirpc', 'libtirpc-devel', 'pkg-config'}
        self.path = Path('/root/osmts_tmp/ltp')
        self.directory: Path = kwargs.get('saved_directory') / 'ltp_cve'
        self.results_dir = Path('/opt/ltp/results')
        self.output_dir = Path('/opt/ltp/output')


    def pre_test(self):
        if not self.directory.exists():
            self.directory.mkdir(exist_ok=True, parents=True)
        if not Path('/opt/ltp/finish.sign').exists():
            if self.path.exists():
                shutil.rmtree(self.path)

            git_clone = subprocess.run(
                "cd /root/osmts_tmp/ && git clone https://github.com/linux-test-project/ltp.git",
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            if git_clone.returncode != 0:
                print(f"ltp_cve测试出错.git clone拉取ltp源码失败:{git_clone.stderr.decode('utf-8')}")
                sys.exit(1)

            make = subprocess.run(
                "cd /root/osmts_tmp/ltp/ && make autotools && ./configure && make -j $(nproc) && make install",
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            if make.returncode != 0:
                print(f"ltp_cve测试出错.configure和make出错:报错信息:{make.stderr.decode('utf-8')}")
                sys.exit(1)
            Path('/opt/ltp/finish.sign').touch()
        else:
            if self.results_dir.exists():
                shutil.rmtree(self.results_dir)
                self.results_dir.mkdir()
            if self.output_dir.exists():
                shutil.rmtree(self.output_dir)
                self.output_dir.mkdir()


    def run_test(self):
        runltp = subprocess.run(
            "cd /opt/ltp && ./runltp -f cve",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if runltp.returncode != 0:
            print(f"ltp_cve测试出错.runltp进程报错:报错信息:{runltp.stderr.decode('utf-8')}")
            print('这是正常现象,osmts继续运行')

        # 测试结果存储在/opt/ltp/results,测试日志保存在/opt/ltp/output
        for file in os.listdir(self.results_dir):
            if 'LTP' in file:
                shutil.copy(self.results_dir / file,self.directory)
                log_file = self.directory / file
                self._parse_and_generate_excel(log_file)
                Path(self.results_dir / file).unlink()
        for file in os.listdir(self.output_dir):
            if 'LTP' in file:
                shutil.copy(self.output_dir / file,self.directory)
                Path(self.output_dir / file).unlink()

    # 解析 LTP CVE 日志，提取测试统计和各用例结果，写入 Excel
    def _parse_and_generate_excel(self, log_path: Path):
        results = []
        start_time = None
        total_tests = 0
        total_skipped = 0
        total_failures = 0
        kernel_version = ""
        machine_arch = ""
        hostname = ""

        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.rstrip()
                if line.startswith('Test Start Time:'):
                    start_time = line.split('Test Start Time:', 1)[1].strip()
                elif line.startswith('Total Tests:'):
                    total_tests = int(line.split('Total Tests:', 1)[1].strip())
                elif line.startswith('Total Skipped Tests:'):
                    total_skipped = int(line.split('Total Skipped Tests:', 1)[1].strip())
                elif line.startswith('Total Failures:'):
                    total_failures = int(line.split('Total Failures:', 1)[1].strip())
                elif line.startswith('Kernel Version:'):
                    kernel_version = line.split('Kernel Version:', 1)[1].strip()
                elif line.startswith('Machine Architecture:'):
                    machine_arch = line.split('Machine Architecture:', 1)[1].strip()
                elif line.startswith('Hostname:'):
                    hostname = line.split('Hostname:', 1)[1].strip()
                else:
                    match = re.match(r'\s*(\S+)\s+(PASS|FAIL|CONF|BROC)\s+(\d+)', line)
                    if match:
                        results.append((match.group(1), match.group(2), int(match.group(3))))

        wb = Workbook()
        ws = wb.active
        ws.title = 'LTP_CVE'
        ws.append(['Testcase', 'Result', 'Exit Value'])
        for testcase, result, exit_val in results:
            ws.append([testcase, result, exit_val])

        ws.append([])
        ws.append(['统计信息'])
        ws.append(['总测试数', total_tests])
        ws.append(['跳过数', total_skipped])
        ws.append(['失败数', total_failures])
        ws.append(['通过数', total_tests - total_skipped - total_failures])
        ws.append([])
        ws.append(['环境信息'])
        ws.append(['测试开始时间', start_time])
        ws.append(['内核版本', kernel_version])
        ws.append(['架构', machine_arch])
        ws.append(['主机名', hostname])

        excel_path = log_path.with_suffix('.xlsx')
        wb.save(excel_path)



    def run(self):
        print("开始进行ltp_cve测试")
        self.pre_test()
        self.run_test()
        print("ltp_cve测试结束")