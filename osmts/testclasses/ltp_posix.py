from pathlib import Path
import subprocess, re, os, shutil
from openpyxl import Workbook

from .errors import GitCloneError,DefaultError


"""
ltp posix测试在ltp源码的基础上单独编译运行
"""

class Ltp_posix(object):
    def __init__(self, **kwargs):
        self.rpms = {'automake', 'pkgconf', 'autoconf', 'bison', 'flex', 'm4', 'kernel-headers', 'glibc-headers',
                     'findutils', 'libtirpc', 'libtirpc-devel', 'pkg-config'}
        self.directory: Path = kwargs.get('saved_directory') / 'ltp_posix'
        self.ltp_path = Path('/root/osmts_tmp/ltp')
        self.test_result = ''
        self.test_passed = 0
        self.test_failed = 0
        self.test_skipped = 0
        self.test_list = []
        self.test_results = {}
    
    
    def pre_test(self):
        if not self.directory.exists():
            self.directory.mkdir(exist_ok=True,parents=True)
        if not Path('/opt/ltp').exists():
            try:
                subprocess.run(
                    "git clone https://github.com/linux-test-project/ltp.git",
                    cwd="/root/osmts_tmp/",
                    shell=True,check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
            except subprocess.CalledProcessError as e:
                raise GitCloneError(e.returncode,'https://github.com/linux-test-project/ltp.git',e.stderr.decode())

        try:
            make = subprocess.run(
                f" ./configure && make all -j $(nproc)",
                cwd=self.ltp_path / "testcases/open_posix_testsuite",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"ltp_posix测试出错.configure和make all出错.报错信息:{e.decode('utf-8')}")
        
        self._patch_scripts()
        self._scan_test_list()
    
    # 将定制化脚本覆盖到 ltp 目录中的对应位置
    def _patch_scripts(self):
        bin_dir = self.ltp_path / "testcases/open_posix_testsuite/bin"
        script_dir = Path(__file__).parent / 'ltp_posix_scripts'
        
        for script_name in ['run-posix-option-group-test.sh', 'run-all-posix-option-group-tests.sh']:
            src = script_dir / script_name
            dst = bin_dir / script_name
            if src.exists():
                shutil.copy2(src, dst)
                os.chmod(dst, 0o755)

    # 扫描 POSIX 接口测试目录，收集所有 .c 测试用例的文件名列表
    def _scan_test_list(self):
        interfaces_dir = self.ltp_path / "testcases/open_posix_testsuite/conformance/interfaces"
        self.test_list = []
        for interface_dir in sorted(interfaces_dir.iterdir()):
            if interface_dir.is_dir():
                for test_file in sorted(interface_dir.glob('*.c')):
                    test_name = f"{interface_dir.name}/{test_file.name}"
                    self.test_list.append(test_name)


    def run_test(self):
        runltp = subprocess.run(
            "cd /root/osmts_tmp/ltp/testcases/open_posix_testsuite/bin && ./run-all-posix-option-group-tests.sh",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, 'OUTPUT_FORMAT': 'structured'},
        )
        if runltp.returncode != 0:
            print(f"ltp_posix测试出错.run-all-posix-option-group-tests.sh脚本报错:报错信息:{runltp.stderr.decode('utf-8')}")

        self.test_result = runltp.stdout.decode('utf-8')
        with open(self.directory / 'ltp_posix.log', 'w') as file:
            file.write(self.test_result)
        
        self._parse_results()
        self._generate_excel()
        print(f"通过数量:{self.test_passed}",f"失败数量:{self.test_failed}",f"跳过数量:{self.test_skipped}",sep='\n')

    # 解析结构化测试输出，统计各用例的 PASS/FAIL/SKIP 结果
    def _parse_results(self):
        lines = self.test_result.splitlines()
        current_suite = None
        self.test_results = {}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            if line.startswith('[TESTSUITE]'):
                current_suite = line.split(']', 1)[1].strip()
                continue
            
            if line.startswith('[RESULT]'):
                parts = {}
                for part in ['TESTCASE', 'RESULT', 'REASON']:
                    match = re.search(rf'\[{part}\]\s*([^[]+)', line)
                    if match:
                        parts[part] = match.group(1).strip()
                
                if 'TESTCASE' in parts and 'RESULT' in parts:
                    test_name = parts['TESTCASE']
                    result = parts['RESULT']
                    reason = parts.get('REASON', '')
                    
                    if test_name not in self.test_results:
                        if result == 'PASS':
                            self.test_passed += 1
                        elif result == 'FAIL':
                            self.test_failed += 1
                        elif result == 'SKIP':
                            self.test_skipped += 1
                    
                    self.test_results[test_name] = {'result': result, 'reason': reason}

    # 将解析后的 POSIX 测试结果写入 Excel
    def _generate_excel(self):
        wb = Workbook()
        ws = wb.active
        ws.title = 'LTP_POSIX'
        ws.append(['测试用例', '结果', '详情'])
        
        if self.test_results:
            for test_name in sorted(self.test_results.keys()):
                item = self.test_results[test_name]
                ws.append([test_name, item['result'], item['reason']])
        else:
            ws.append(['(无)', '(无)', '日志中未找到失败测试'])
        
        ws.append([])
        ws.append(['统计信息'])
        ws.append(['通过数量', self.test_passed])
        ws.append(['失败数量', self.test_failed])
        ws.append(['跳过数量', self.test_skipped])
        ws.append(['总数', self.test_passed + self.test_failed + self.test_skipped])

        wb.save(self.directory / 'ltp_posix.xlsx')


    def run(self):
        print("开始进行ltp_posix测试")
        self.pre_test()
        self.run_test()
        print("ltp_posix测试结束")
