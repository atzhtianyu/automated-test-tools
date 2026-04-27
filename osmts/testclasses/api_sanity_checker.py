import shutil
import subprocess
import os
from pathlib import Path

from .errors import DefaultError, GitCloneError


class APISanityChecker:
    def __init__(self, **kwargs):
        self.rpms ={'ctags','gcc-c++'}
        self.believe_tmp: bool = kwargs.get('believe_tmp')
        self.abi_compliance_checker = Path('/root/osmts_tmp/abi-compliance-checker')
        self.api_sanity_checker = Path('/root/osmts_tmp/api-sanity-checker')
        self.directory: Path = kwargs.get('saved_directory') / 'api_sanity_checker'
        self.gcc_version = kwargs.get('gcc_version','auto')
        self.gcc_target = ''
        self.gcc_lib_dir = Path()
        self.version = ''


    # 生成版本字符串的排序键，数字段按数值大小排序，字母段按字典序
    def _version_sort_key(self, version: str):
        sort_key = []
        for part in version.replace('-', '.').split('.'):
            if part.isdigit():
                sort_key.append((0, int(part)))
            else:
                sort_key.append((1, part))
        return tuple(sort_key)


    # 通过 gcc -dumpmachine 获取目标架构并定位 gcc 库目录
    def _resolve_gcc_version(self):
        gcc_target = subprocess.run(
            "gcc -dumpmachine",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if gcc_target.returncode != 0:
            raise DefaultError(f"api-sanity-checker测试出错.获取gcc目标架构失败,报错信息:{gcc_target.stderr.decode('utf-8')}")

        self.gcc_target = gcc_target.stdout.decode('utf-8', errors='replace').strip()
        if not self.gcc_target:
            raise DefaultError("api-sanity-checker测试出错.gcc -dumpmachine未返回有效的目标架构")

        self.gcc_lib_dir = Path('/usr/lib/gcc') / self.gcc_target
        if not self.gcc_lib_dir.exists():
            raise DefaultError(f"api-sanity-checker测试出错.{self.gcc_lib_dir}目录不存在")

        available_versions = sorted(
            [path.name for path in self.gcc_lib_dir.iterdir() if path.is_dir()],
            key=self._version_sort_key,
            reverse=True,
        )
        if not available_versions:
            raise DefaultError(f"api-sanity-checker测试出错.{self.gcc_lib_dir}目录下未找到gcc版本")

        if self.gcc_version != 'auto':
            if (self.gcc_lib_dir / self.gcc_version).exists():
                self.version = self.gcc_version
                return
            print(f"用户输入的gcc_version={self.gcc_version}无效,试图自动查找")

        self.version = available_versions[0]


    def pre_test(self):
        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir(parents=True)
        if self.abi_compliance_checker.exists() and self.believe_tmp:
            pass
        else:
            shutil.rmtree(self.abi_compliance_checker,ignore_errors=True)
            try:
                subprocess.run(
                    "git clone https://gitcode.com/gh_mirrors/ab/abi-compliance-checker.git",
                    cwd="/root/osmts_tmp",
                    shell=True,check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
            except subprocess.CalledProcessError as e:
                raise GitCloneError(e.returncode,'https://gitcode.com/gh_mirrors/ab/abi-compliance-checker.git',e.stderr.decode())


        if self.api_sanity_checker.exists() and self.believe_tmp:
            pass
        else:
            shutil.rmtree(self.api_sanity_checker,ignore_errors=True)
            try:
                subprocess.run(
                    "cd /root/osmts_tmp && git clone https://github.com/lvc/api-sanity-checker.git",
                    cwd="/root/osmts_tmp",
                    shell=True,check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
            except subprocess.CalledProcessError as e:
                raise GitCloneError(e.returncode,'https://github.com/lvc/api-sanity-checker.git',e.stderr.decode())

        # 开始安装
        subprocess.run(
            f"cd /root/osmts_tmp/abi-compliance-checker && make install prefix=/usr",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            f"cd /root/osmts_tmp/api-sanity-checker && make install prefix=/usr",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        test = subprocess.run(
            "api-sanity-checker -test",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if test.returncode != 0:
            print(f"api-snity-checcker测试出错.api-sanity-checker -test验证命令失败,报错信息:{test.stderr.decode('utf-8')}")
            print('osmts继续运行')

        # 生成GCC_VERSION.xml
        self._resolve_gcc_version()

        with open(f"{self.directory}/GCC_VERSION.xml",'w') as file:
            file.writelines(['<version>\n',f'\t{self.version}\n','</version>\n'])
            file.writelines(['<headers>\n',f'\t{self.gcc_lib_dir}/{self.version}/include\n','</headers>\n'])
            file.writelines(['<libs>\n',f'\t{self.gcc_lib_dir}/{self.version}\n','</libs>'])


    def run_test(self):
        osmts_dir = os.getcwd()
        os.chdir(self.directory)
        checker = subprocess.run(
            f"api-sanity-checker -lib NAME -d GCC_VERSION.xml -gen -build -run",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if checker.returncode != 0:
            print(f"api-snity-checcker测试出错.测试命令运行失败,报错信息:{checker.stderr.decode('utf-8')}")
            return
        # test_results/NAME/12/test_results.html
        shutil.copy2(f"test_results/NAME/{self.version}/test_results.html",self.directory)
        os.chdir(osmts_dir)


    def run(self):
        print('开始进行API Sanity Checker测试')
        self.pre_test()
        self.run_test()
        print('API Sanity Checker测试结束')
