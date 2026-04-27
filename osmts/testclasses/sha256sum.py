import hashlib
import requests,shutil
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, unquote

from .errors import DefaultError



class Sha256sum():
    def __init__(self, **kwargs):
        self.rpms = set()
        self.path = Path('/root/osmts_tmp/sha256sumISO')
        self.directory: Path = kwargs.get('saved_directory') / 'sha256sumISO'
        self.sha256sumISO:str = kwargs.get('sha256sumISO')
        self.iso_filename: str = ''
        self.sha256sum_filename: str = ''
        self.expected_hash: str = ''
        self.actual_hash: str = ''
        self.verification_result: str = ''
        self.iso_file_path: Optional[Path] = None


    # 从 URL 路径中解析出文件名（处理 URL 编码）
    def _filename_from_url(self, url: str) -> str:
        return Path(unquote(urlparse(url).path)).name


    # 分块读取文件并计算 SHA256 哈希值，避免大文件内存溢出
    def _calculate_sha256(self, file_path: Path) -> str:
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b''):
                sha256.update(chunk)
        return sha256.hexdigest()


    # 将校验结果写入 .result 摘要文件和 .log 哈希对照文件
    def _write_result_files(self):
        result_content = '\n'.join([
            f"ISO文件名: {self.iso_filename}",
            f"sha256sum文件名: {self.sha256sum_filename}",
            f"官方sha256sum值: {self.expected_hash}",
            f"实际sha256sum值: {self.actual_hash}",
            f"校验结果: {self.verification_result}",
            '',
        ])

        with open(self.directory / 'sha256sumISO.result', 'w', encoding='utf-8') as result:
            result.write(result_content)

        with open(self.directory / 'sha256sumISO.log', 'w', encoding='utf-8') as log:
            log.write(f"{self.expected_hash}  {self.iso_filename}\n")
            log.write(f"{self.actual_hash}  {self.iso_filename}\n")


    # 带重试机制地下载 ISO 文件到临时路径，完成后重命名为正式文件名
    def _download_iso(self) -> Path:
        headers = {
            'Accept': 'text / html, application / xhtml + xml, application / xml;q = 0.9, * / *;q = 0.8',
            'Connection': 'keep-alive',
            'User-Agent':'Mozilla/5.0 (X11; Linux x86_64; rv:138.0) Gecko/20100101 Firefox/138.0',
            'Referer': 'https://gitee.com/April_Zhao/osmts'
        }
        target_path = self.path / self.iso_filename
        temp_path = self.path / f"{self.iso_filename}.part"
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):
            if temp_path.exists():
                temp_path.unlink()

            try:
                with requests.get(
                    self.sha256sumISO,
                    headers=headers,
                    stream=True,
                    timeout=(30, None),
                ) as response:
                    response.raise_for_status()
                    with open(temp_path, 'wb') as file:
                        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                            if chunk:
                                file.write(chunk)
            except requests.exceptions.RequestException as e:
                if attempt == max_attempts:
                    raise DefaultError(f"sha256sumISO测试下载ISO失败，报错信息:{str(e)}")
                continue

            if target_path.exists():
                target_path.unlink()
            temp_path.rename(target_path)
            return target_path

        raise DefaultError("sha256sumISO测试下载ISO失败，已达到最大重试次数")


    def pre_test(self):
        if not self.sha256sumISO:
            raise DefaultError("sha256sumISO测试缺少ISO下载地址，请检查sha256sumISO配置项")

        self.iso_filename = self._filename_from_url(self.sha256sumISO)
        if not self.iso_filename:
            raise DefaultError("sha256sumISO测试无法从下载地址中解析ISO文件名")
        self.sha256sum_filename = f"{self.iso_filename}.sha256sum"

        if self.path.exists():
            shutil.rmtree(self.path)
        self.path.mkdir(parents=True)

        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir(parents=True)

        try:
            response = requests.get(
                self.sha256sumISO + '.sha256sum',
                headers={
                    'Accept':'text / html, application / xhtml + xml, application / xml;q = 0.9, * / *;q = 0.8',
                    'Connection':'keep-alive',
                    'Referer': 'https://gitee.com/April_Zhao/osmts',
                    'User-Agent':'Mozilla/5.0 (X11; Linux x86_64; rv:138.0) Gecko/20100101 Firefox/138.0'
                }
            )
        except requests.exceptions.Timeout:
            raise DefaultError('https请求超时，请检查网络')
        if not response.ok:
            raise DefaultError("sha256sumISO测试sha256sum文件下载失败")

        sha256sum_content = response.text.strip()
        if not sha256sum_content:
            raise DefaultError("sha256sumISO测试下载到的sha256sum文件为空")

        with open(self.directory / self.sha256sum_filename, 'w', encoding='utf-8') as file:
            file.write(sha256sum_content + '\n')

        first_line = next((line.strip() for line in sha256sum_content.splitlines() if line.strip()), '')
        parts = first_line.split()
        if not parts:
            raise DefaultError("sha256sumISO测试无法解析sha256sum文件内容")
        self.expected_hash = parts[0]


    def run_test(self):
        self.iso_file_path = self._download_iso()
        if not self.iso_file_path.exists():
            raise DefaultError(f"sha256sumISO测试未找到下载后的ISO文件:{self.iso_file_path}")

        self.actual_hash = self._calculate_sha256(self.iso_file_path)
        if self.actual_hash.lower() == self.expected_hash.lower():
            self.verification_result = "校验成功"
        else:
            self.verification_result = "校验失败"

        self._write_result_files()

    def post_test(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def run(self):
        self.pre_test()
        self.run_test()
        self.post_test()
