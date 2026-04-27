from pathlib import Path
import subprocess,shutil

from .errors import DefaultError, CompileError, RunError


class Iozone:
    def __init__(self,**kwargs ):
        self.rpms = set()
        self.believe_tmp: bool = kwargs.get('believe_tmp')
        self.path = Path('/root/osmts_tmp/iozone')
        self.directory:Path = kwargs.get('saved_directory') / 'iozone'
        self.compiler:str = kwargs.get('compiler')


    def pre_test(self):
        if self.path.exists():
            shutil.rmtree(self.path)
        self.path.mkdir(parents=True, exist_ok=True)

        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir(parents=True)

        # 获取iozone的源码
        try:
            subprocess.run(
                "wget https://www.iozone.org/src/current/iozone3_508.tar && "
                "tar -xf iozone3_508.tar",
                cwd=self.path,
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"iozone测试出错:下载/解压iozone源码压缩包失败.报错信息:{e.stderr.decode('utf-8')}")


        #编译iozone
        try:
            compile = subprocess.run(
                f"make clean && make CC={self.compiler} CFLAGS=-fcommon linux",
                cwd="/root/osmts_tmp/iozone/iozone3_508/src/current",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE
            )
        except subprocess.CalledProcessError as e:
            raise CompileError(e.returncode,self.compiler,e.stderr.decode('utf-8'))


    def run_test(self):
        try:
            subprocess.run(
                f"./iozone -Rab {self.directory}/iozone.xls",
                cwd="/root/osmts_tmp/iozone/iozone3_508/src/current",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE
            )
        except subprocess.CalledProcessError as e:
            raise RunError(e.returncode,e.stderr.decode('utf-8'))


    def run(self):
        print("开始进行iozone测试")
        self.pre_test()
        self.run_test()
        print("iozone测试结束")