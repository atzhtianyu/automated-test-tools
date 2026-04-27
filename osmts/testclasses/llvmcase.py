from pathlib import Path
import subprocess,shutil

from .errors import GitCloneError,RunError,DefaultError


class Llvmcase():
    def __init__(self, **kwargs):
        self.rpms = {'gcc', 'gcc-g++', 'gcc-gfortran', 'cmake', 'ninja-build'}
        self.believe_tmp: bool = kwargs.get('believe_tmp')
        self.path = Path('/root/osmts_tmp/llvm-project')
        self.directory: Path = kwargs.get('saved_directory') / 'llvmcase'


    def pre_test(self):
        if not self.directory.exists():
            self.directory.mkdir(parents=True,exist_ok=True)
        if self.path.exists() and self.believe_tmp:
            pass
        else:
            shutil.rmtree(self.path, ignore_errors=True)
            # 拉取源码
            try:
                subprocess.run(
                    "git clone https://github.com/llvm/llvm-project.git",
                    cwd="/root/osmts_tmp",
                    shell=True,check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
            except subprocess.CalledProcessError as e:
                raise GitCloneError(e.returncode,'https://github.com/llvm/llvm-project.git',e.stderr.decode())

        # 编译llvm
        try:
            subprocess.run(
                args='cmake -S llvm -B build -DLLVM_PARALLEL_LINK_JOBS=3 -DLLVM_ENABLE_PROJECTS="clang" -DLLVM_TARGETS_TO_BUILD="RISCV" -DCMAKE_BUILD_TYPE="Release" -G Ninja && ninja -C build',
                cwd=self.path,
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"llvmcase测试出错.编译llvm失败,报错信息:{e.stderr.decode('utf-8')}")


    def run_test(self):
        try:
            run_clang = subprocess.run(
                "bin/clang -v",
                cwd=self.path / "build",
                shell=True,check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
        except subprocess.CalledProcessError as e:
            raise RunError(e.returncode,e.stderr.decode('utf-8'))
        else:
            with open(self.directory / 'llvmcase.log', 'w') as file:
                file.write(run_clang.stdout.decode('utf-8'))


    def run(self):
        print('开始进行llvmcase测试')
        self.pre_test()
        self.run_test()
        print('llvm测试结束')
