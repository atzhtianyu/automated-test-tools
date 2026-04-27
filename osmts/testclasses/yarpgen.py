import os
from pathlib import Path
import sys,subprocess,shutil
from openpyxl import Workbook
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from .errors import GitCloneError,DefaultError


class Yarpgen:
    def __init__(self, **kwargs):
        self.rpms = {'gcc-g++'}
        self.believe_tmp: bool = kwargs.get('believe_tmp')
        self.path = Path('/root/osmts_tmp/yarpgen')
        self.directory: Path = kwargs.get('saved_directory') / 'yarpgen'
        self.yarpgen = Path('/root/osmts_tmp/yarpgen/build/yarpgen')
        self.testdir = Path('/root/osmts_tmp/yarpgen/testdir')
        self.yarpgen_count = kwargs.get('yarpgen_count')

        self.passed = 0
        self.failed = 0


    def pre_test(self):
        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir(parents=True)
        if self.path.exists() and self.believe_tmp:
            pass
        else:
            shutil.rmtree(self.path,ignore_errors=True)
            try:
                subprocess.run(
                    "git clone https://gitee.com/zhtianyu/yarpgen.git",
                    cwd="/root/osmts_tmp",
                    shell=True,check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
            except subprocess.CalledProcessError as e:
                raise GitCloneError(e.returncode,'https://gitee.com/zhtianyu/yarpgen.git',e.stderr.decode('utf-8'))

        if self.testdir.exists():
            shutil.rmtree(self.testdir)
        self.testdir.mkdir(parents=True)

        # 构建yarpgen命令
        try:
            subprocess.run(
                args=f"mkdir build && cd build && cmake .. && make -j {os.cpu_count()}",
                cwd=self.path,
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"yarpgen测试出错.cmake和make失败,报错信息:{e.stderr.decode('utf-8')}")


    def create_source_code_and_run(self,id) -> dict:
        directory = self.testdir / str(id)
        directory.mkdir(parents=True)
        # 生成随机c++代码
        create_source_code = subprocess.run(
            f"{self.yarpgen} && cat init.h func.cpp driver.cpp > random.cpp",
            cwd=directory,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if create_source_code.returncode != 0:
            print(f"yarpgen测试{id}出错.创建随机c++源代码文件失败,报错信息:{create_source_code.stderr.decode('utf-8')}")
            sys.exit(1)

        # 编译c++代码
        compile = subprocess.run(
            f"g++ ./random.cpp -O0 -o g++_O0.out &&" # 不开优化编译
            f"g++ ./random.cpp -O3 -o g++_O3.out",         # O3优化编译
            cwd=directory,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if compile.returncode != 0:
            print(f"yarpgen测试{id}出错.编译c++源代码失败,具体报错信息请前往{directory}/error.log查看")
            with open(directory / 'error.log', 'w') as log:
                log.write(compile.stderr.decode('utf-8'))
            return {
                "id": id,
                "success": False,
                "reason":"编译报错"
            }

        # 执行并判断
        O1_result = subprocess.run(
            f"{directory}/g++_O0.out",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        ).stdout.decode('utf-8')
        O3_result = subprocess.run(
            f"{directory}/g++_O3.out",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        ).stdout.decode('utf-8')
        return {
            "id":id,
            "success":(O1_result == O3_result),
            "reason":"运行结果不一致"
        }


    def run_test(self):
        wb = Workbook()
        ws = wb.active
        ws.title = 'yarpgen'
        ws.append(['请进入/root/osmts_tmp/yarpgen/testdir查看所有结果'])
        ws.append(['出错项目id','出错原因'])
        failed_logs = []

        with ThreadPoolExecutor(max_workers=os.cpu_count()) as pool:
            results = list(tqdm(pool.map(self.create_source_code_and_run, range(1, self.yarpgen_count + 1)),total=self.yarpgen_count))
            for result in results:
                if result.get('success'):
                    self.passed += 1
                else:
                    self.failed += 1
                    ws.append([result.get('id',"获取id失败"),result.get('reason','原因未知')])
                    failed_logs.append(result.get('id'))
        ws.append(["计数统计",f"passwd数量:{self.passed}",f"failed数量:{self.failed}"])
        wb.save(self.directory / 'yarpgen.xlsx')

        if failed_logs:
            import zipfile
            zip_path = self.directory / 'yarpgen_error_logs.zip'
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for fid in failed_logs:
                    log_path = self.testdir / str(fid) / 'error.log'
                    if log_path.exists():
                        zf.write(log_path, f'{fid}_error.log')


    def run(self):
        print("开始运行yarpgen测试")
        self.pre_test()
        self.run_test()
        print("yarpgen测试结束")