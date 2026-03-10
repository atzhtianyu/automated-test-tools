import os
import requests
import shutil
import subprocess
from pathlib import Path
from openpyxl import Workbook

from .errors import GitCloneError, DefaultError

"""
文档:https://blog.sina.com.cn/s/blog_7695e9f40100yjme.html
"""



class Ltp:
    def __init__(self, **kwargs):
        self.rpms = {'automake','pkgconf','autoconf','bison','flex','m4','kernel-headers','glibc-headers','findutils','libtirpc','libtirpc-devel','pkg-config'}
        self.path = Path('/root/osmts_tmp/ltp')
        self.directory: Path = kwargs.get('saved_directory') / 'ltp'
        self.results_dir = Path('/opt/ltp/results')
        self.output_dir = Path('/opt/ltp/output')

    def get_latest_ltp_version(self):
        API_URL = "https://api.github.com/repos/linux-test-project/ltp/releases/latest"
        headers = {"User-Agent": "LTP-Auto-Downloader"}
        try:
            response = requests.get(API_URL, headers=headers, timeout=10)
            response.raise_for_status()
            version = response.json()["tag_name"]
            print(f"检测到最新LTP版本: {version}")
            return version
        except Exception as e:
            print(f"获取最新LTP版本失败: {e}")
            print("使用 master 分支")
            return "master"

    def pre_test(self):
        if not self.directory.exists():
            self.directory.mkdir(exist_ok=True, parents=True)
        if self.path.exists():
            shutil.rmtree(self.path)

        version = self.get_latest_ltp_version()

        try:
            subprocess.run(
                f"git clone --depth 1 --branch {version} https://github.com/linux-test-project/ltp.git",
                cwd = "/root/osmts_tmp",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise GitCloneError(e.returncode,'https://github.com/linux-test-project/ltp.git',e.stderr)

        try:
            subprocess.run(
                "make autotools && ./configure && make -j $(nproc) && make install",
                cwd = "/root/osmts_tmp/ltp/",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"ltp测试出错.configure和make出错:报错信息:{e.stderr.decode('utf-8')}")

        # 添加标记
        Path('/opt/ltp/finish.sign').touch()

        # 确保运行前/opt/ltp/results和/opt/ltp/output为空目录
        if self.results_dir.exists():
            shutil.rmtree(self.results_dir)
            self.results_dir.mkdir()
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
            self.output_dir.mkdir()


    def run_test(self):
        runltp = subprocess.run(
            "./runltp",
            cwd="/opt/ltp",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if runltp.returncode != 0:
            print(f"ltp测试出错.runltp进程报错:报错信息:{runltp.stderr.decode('utf-8')}")
            print('这是正常现象,osmts继续运行')


        # 测试结果存储在/opt/ltp/results,测试日志保存在/opt/ltp/output
        wb = Workbook()
        ws = wb.active
        ws.title = 'ltp report'
        ws.append(['Testcase', 'Result', 'Exit Value'])
        for file in sorted(os.listdir(self.results_dir)):
            if file.endswith('.log'):
                with open(self.results_dir / file, errors="ignore") as f:
                    for line in f:
                        parts = line.split()
                        if len(parts) >= 3 and parts[1] in ('PASS','FAIL','CONF'):
                            ws.append(parts[:3])

            shutil.copy(self.results_dir / file, self.directory)

            if (self.results_dir / file).exists():
                Path(self.results_dir / file).unlink()

        wb.save(self.directory / 'ltp.xlsx')

        # 复制 summary
        for file in os.listdir(self.output_dir):
            if 'LTP' in file:
                shutil.copy(self.output_dir / file, self.directory)
                if (self.output_dir / file).exists():
                    Path(self.output_dir / file).unlink()


    def run(self):
        print("开始进行ltp测试")
        self.pre_test()
        self.run_test()
        print("ltp测试结束")