import shutil
import subprocess
import os
import asyncio
import numpy
from pathlib import Path
from openpyxl import Workbook
from tqdm import tqdm
from tqdm.asyncio import tqdm as tqdm_asyncio

from .errors import DefaultError


class GpgCheck:
    def __init__(self, **kwargs):
        self.rpms = set()
        self.path = Path('/root/osmts_tmp/gpgcheck')
        self.directory: Path = kwargs.get('saved_directory') / 'gpgcheck'
        self.packages = []
        self.results = []
        self.download_failures = []

        # 创建Excel表格
        self.wb = Workbook()
        self.ws = self.wb.active


    # 清空并重建 GPG 校验的下载目录
    def _reset_download_dir(self):
        shutil.rmtree(self.path, ignore_errors=True)
        self.path.mkdir(parents=True, exist_ok=True)


    async def rpm_check_each(self,package_name):
        rpm_check = await asyncio.create_subprocess_shell(
            f"rpm -K /root/osmts_tmp/gpgcheck/{package_name}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await rpm_check.communicate()
        stdout_text = stdout.decode('utf-8', errors='replace').strip()
        stderr_text = stderr.decode('utf-8', errors='replace').strip()

        if rpm_check.returncode == 0:
            result = 'PASS'
            details = stdout_text or '校验通过'
        else:
            result = 'FAIL'
            details = stderr_text or stdout_text or '校验失败'

        self.results.append((package_name, result, details))


    async def rpm_check_all(self):
        packages = list(os.walk(self.path))[0][2]
        # 对每个rpm包创建一个测试任务
        tasks = [asyncio.create_task(self.rpm_check_each(package)) for package in packages]
        await tqdm_asyncio.gather(*tasks,leave=False,desc='rpm check')


    def save_log(self, total_count, pass_count, fail_count):
        log_lines = [
            '统计信息',
            f'total: {total_count}',
            f'pass: {pass_count}',
            f'fail: {fail_count}',
            '',
        ]

        if self.download_failures:
            log_lines.append('下载失败批次')
            for packages, error_text in self.download_failures:
                log_lines.append('packages: ' + ' '.join(packages))
                if error_text:
                    log_lines.append(error_text)
                log_lines.append('')

        for package_name, result, details in self.results:
            status_text = 'success' if result == 'PASS' else 'failed'
            log_lines.append(f'rpm pkg: {package_name}')
            if details:
                log_lines.append(details)
            log_lines.append(f'{package_name} gpg check {status_text}')
            log_lines.append('')

        with open(self.directory / 'gpgcheck.log', 'w', encoding='utf-8') as log_file:
            log_file.write('\n'.join(log_lines).rstrip() + '\n')


    def pre_test(self):
        if not self.directory.exists():
            self.directory.mkdir()
        self._reset_download_dir()

        # 更新缓存以便后面下载
        try:
            subprocess.run(
                "dnf clean all && dnf makecache",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"gpgcheck测试出错.创建repo缓存失败/")

        # 引入openEuler的gpg验证密钥
        try:
            subprocess.run(
                "rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-openEuler",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"gpgcheck测试出错.import gpg文件失败,报错信息:{e.stderr.decode('utf-8')}")


        # 获取已安装的所有rpm包名
        try:
            dnf_list = subprocess.run(
                "dnf list available | awk '/Available Packages/{flag=1; next} flag' | awk '{print $1}'",
                shell=True,check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"gpgcheck测试出错.获取所有已安装的rpm包名失败,报错信息:{e.stderr.decode('utf-8')}")
        else:
            self.rpm_package_list = dnf_list.stdout.decode('utf-8').splitlines()

        self.ws.title = 'summary'


    def run_test(self):
        # 排除掉不符合测试要求的包名
        for package in self.rpm_package_list:
            if package.endswith('.src') or 'debug' in package:
                continue
            else:
                self.packages.append(package)

        print(f"  当前线程的event loop策略:{asyncio.get_event_loop_policy()}")
        # 根据包名批量下载并测试rpm包
        # 分批次原因: shell无法解析长度过大的文本
        piece = max(int(len(self.packages) / 100), 1)
        for package_list in tqdm(numpy.array_split(self.packages,indices_or_sections=piece),desc='处理包进度',unit='次'):
            package_names = [str(package) for package in package_list.tolist()]
            rpm_download = subprocess.run(
                f"dnf download {' '.join(package_names)} --destdir={self.path}",
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            if rpm_download.returncode != 0:
                error_text = rpm_download.stderr.decode('utf-8', errors='replace').strip()
                print(f"gpgcheck测试出错.下载待测的rpm包失败,报错信息:{error_text}")
                print(f"本批次下载出错的rpm包为:{package_names}")
                self.download_failures.append((package_names, error_text))

            try:
                asyncio.run(self.rpm_check_all())
            finally:
                self._reset_download_dir()

        self.results.sort(key=lambda item: item[0])
        total_count = len(self.results)
        pass_count = sum(1 for _, result, _ in self.results if result == 'PASS')
        fail_count = sum(1 for _, result, _ in self.results if result == 'FAIL')

        self.ws.append(['统计项', '数量'])
        self.ws.append(['total', total_count])
        self.ws.append(['pass', pass_count])
        self.ws.append(['fail', fail_count])

        self.ws.append([])
        self.ws.append(['rpm包', '测试结果', '结果详情'])
        for package_name, result, details in self.results:
            self.ws.append([package_name, result, details])

        self.save_log(total_count, pass_count, fail_count)
        self.wb.save(self.directory / 'gpgcheck.xlsx')


    def post_test(self):
        shutil.rmtree(self.path, ignore_errors=True)


    def run(self):
        print('开始进行gpgcheck测试')
        try:
            self.pre_test()
            self.run_test()
        finally:
            self.post_test()
        print('gpgcheck测试结束')
