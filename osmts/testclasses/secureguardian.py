import re
import shutil
import subprocess
import json
from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import Alignment

from .errors import DefaultError,RunError,SummaryError


class SecureGuardian:
    def __init__(self, **kwargs):
        self.rpms = {'jq'}
        self.path = kwargs.get('/root/osmts_tmp/secureguardian')
        self.directory: Path = kwargs.get('saved_directory') / 'secureguardian'
        self.test_result = ''


    # 解析 secureguardian 输出的单行 JSON 检查项，解析失败时用正则兜底
    def _parse_check_line(self, line: str):
        line = line.strip().rstrip(',')
        if not line or line in ('[', ']'):
            return None

        try:
            return json.loads(line, strict=False)
        except json.JSONDecodeError:
            pass

        pattern = re.compile(
            r'^\{"id":"(?P<id>.*?)","description":"(?P<description>.*?)","level":\s*"(?P<level>.*?)","status":"(?P<status>.*?)","details":"(?P<details>.*)","link":"(?P<link>[^"]*)"\}$'
        )
        match = pattern.match(line)
        if not match:
            raise ValueError(f'无法解析secureguardian结果行: {line[:200]}')

        return {
            'id': match.group('id'),
            'description': match.group('description'),
            'level': match.group('level'),
            'status': match.group('status'),
            'details': match.group('details'),
            'link': match.group('link'),
        }


    # 当逐行解析失败时，用正则从整体输出中批量提取所有检查项
    def _extract_checks_fallback(self, content: str):
        pattern = re.compile(
            r'\{"id":"(?P<id>.*?)","description":"(?P<description>.*?)","level":\s*"(?P<level>.*?)","status":"(?P<status>.*?)","details":"(?P<details>.*?)","link":"(?P<link>[^"]*)"\}',
            re.DOTALL,
        )
        checks = []
        for match in pattern.finditer(content):
            checks.append({
                'id': match.group('id'),
                'description': match.group('description'),
                'level': match.group('level'),
                'status': match.group('status'),
                'details': match.group('details'),
                'link': match.group('link'),
            })
        return checks


    def pre_test(self):
        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir(parents=True)
        try:
            subprocess.run(
                "dnf install -y https://eulermaker.compass-ci.openeuler.openatom.cn/api/ems5/repositories/openEuler-24.09:epol/openEuler:24.09/x86_64/history/223fa6b8-65fc-11ef-9cf1-324c421ef8df/steps/upload/cbs.6161130/secureguardian-1.0.0-1.oe2409.noarch.rpm",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"secureguardian测试出错.安装rpm包失败,报错信息:{e.stderr.decode('utf-8')}")


    def run_test(self):
        try:
            run_checks = subprocess.run(
                "run_checks",
                shell=True,check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise RunError(e.returncode,f"secureguardian测试出错.run_checks命令运行失败,报错信息:{e.stderr.decode('utf-8')}")

        self.test_result = run_checks.stdout.decode('utf-8')
        shutil.copy2("/usr/local/secureguardian/reports/all_checks.results.html", self.directory)
        shutil.copy2("/usr/local/secureguardian/reports/all_checks.results.json", self.directory)


    def result2summary(self):
        json_path = self.directory / 'all_checks.results.json'
        with open(json_path, 'r', encoding='utf-8') as file:
            content = file.read()

        sanitized_content = content.replace('\x00', '')
        sanitized_content = ''.join(
            ch for ch in sanitized_content
            if ch in ('\n', '\r', '\t') or ord(ch) >= 32
        )

        if not sanitized_content.strip():
            raise ValueError('all_checks.results.json内容为空')

        checks = None
        parse_errors = []

        for candidate in (content, sanitized_content):
            try:
                checks = json.loads(candidate, strict=False)
                break
            except json.JSONDecodeError as e:
                parse_errors.append(str(e))

        if checks is None:
            checks = self._extract_checks_fallback(sanitized_content)

        if not isinstance(checks, list):
            raise ValueError('all_checks.results.json不是预期的列表结构')
        if not checks:
            error_text = '; '.join(parse_errors) if parse_errors else '未知解析错误'
            raise ValueError(f'all_checks.results.json解析失败: {error_text}')

        wb = Workbook()
        ws = wb.active
        ws.title = 'secureguardian'
        ws.append(['id', 'description', 'level', 'riscv status', 'riscv details', 'link'])

        for check in checks:
            details = check.get('details', '')
            details = re.sub(r'<br\s*/?>', '\n', details, flags=re.IGNORECASE)
            ws.append([
                check.get('id', ''),
                check.get('description', ''),
                check.get('level', ''),
                check.get('status', ''),
                details,
                check.get('link', ''),
            ])

        ws.freeze_panes = 'A2'
        for row in ws.iter_rows():
            for cell in row:
                cell.alignment = Alignment(vertical='top', wrap_text=True)

        widths = {
            'A': 12,
            'B': 36,
            'C': 10,
            'D': 14,
            'E': 80,
            'F': 48,
        }
        for column, width in widths.items():
            ws.column_dimensions[column].width = width

        wb.save(self.directory / 'secureguardian.xlsx')


    def run(self):
        print('开始进行secureguardian测试')
        self.pre_test()
        self.run_test()
        try:
            self.result2summary()
        except Exception as e:
            logFile = self.directory / 'secureguardian_summary_error.log'
            with open(logFile, 'w') as log:
                log.write(str(e))
            raise SummaryError(logFile)
        print('secureguardian测试结束')
