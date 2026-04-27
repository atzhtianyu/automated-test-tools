from pathlib import Path
import subprocess,re,shutil
from openpyxl.workbook import Workbook
from pySmartDL import SmartDL
from tqdm import tqdm
import re
import sys
from collections import OrderedDict

from .errors import DefaultError


class Fio:
    def __init__(self, **kwargs):
        self.rpms = {'fio'}
        self.path = Path('/root/osmts_tmp/fio')
        self.directory: Path = kwargs.get('saved_directory') / 'fio'
        self.test_result:str = ''

        self.download_iso_file:SmartDL = SmartDL(
            urls = [
                "https://fast-mirror.isrc.ac.cn/openeuler/openEuler-preview/openEuler-24.03-LLVM-Preview/ISO/riscv64/openEuler-24.03-LLVM-riscv64-dvd.iso", # 下载速度最快
                "https://repo.openeuler.openatom.cn/openEuler-preview/openEuler-24.03-LLVM-Preview/ISO/riscv64/openEuler-24.03-LLVM-riscv64-dvd.iso",       # 下载速度慢
                "https://repo.openeuler.org/openEuler-preview/openEuler-24.03-LLVM-Preview/ISO/riscv64/openEuler-24.03-LLVM-riscv64-dvd.iso",               # 有时候无法访问
                "https://mirrors.ustc.edu.cn/openeuler/openEuler-preview/openEuler-24.03-LLVM-Preview/ISO/riscv64/openEuler-24.03-LLVM-riscv64-dvd.iso",    # USTC repo
            ],
            dest = str(self.path / 'openEuler-24.03-LLVM-riscv64-dvd.iso'),
            threads = 16,
            progress_bar = False,
            timeout = 10,
            request_args = {
                "headers" : {
                'Connection': 'keep-alive',
                # User-Agent会自动生成
                'Referer': 'https://gitee.com/zhtianyu/osmts'
                }
            }
        )
        # 如果iso文件已经存在则不重复下载(用哈希值校验文件)
        self.download_iso_file.add_hash_verification(algorithm='sha256',hash='74e9ac072b6b72744f21fec030fbe67ea331047ae44b26277f9d5ef41ab6776d')
        self.download_iso_file.start(blocking=False)

    def parse_fio_log(self, log_path):
        """解析fio日志文件，返回格式化数据字典"""
        with open(log_path, "r", encoding="utf-8") as f:
            content = f.read()

        # 区分每个测试块，形如: read-4k:, randrw-256k:
        blocks = re.split(r'(?m)^\s*([^\s]+-[0-9]+k):', content)
        # split返回形如 ['', blockname1, block1_text, blockname2, block2_text, ...]
        entry_map = {}

        for i in range(1, len(blocks), 2):
            test_case = blocks[i]
            block_text = blocks[i + 1]
            if test_case not in entry_map:
                entry_map[test_case] = {}

            # 匹配 read 或 write 段
            rw_sections = re.findall(r"((read|write):.*?)(?=read:|write:|$)", block_text, flags=re.DOTALL)
            for section_text, rw_type in rw_sections:
                lat_match = re.search(r"^ *lat \((usec|msec)\):.*?avg=([\d\.]+)", section_text, flags=re.MULTILINE)
                bw_match = re.search(r"^ *bw\s*\(\s*(KiB|MiB)/s\s*\):.*?avg=([\d\.]+)", section_text, flags=re.MULTILINE)
                iops_match = re.search(r"^ *iops\s*:\s*.*?avg=([\d\.]+)", section_text, flags=re.MULTILINE)

                if lat_match and bw_match and iops_match:
                    bw_val = float(bw_match.group(2))
                    if bw_match.group(1) == "MiB":
                        bw_val *= 1024  # 转成 KiB/s

                    entry_map[test_case][rw_type] = {
                        "lat_unit": lat_match.group(1),
                        "lat_avg": float(lat_match.group(2)),
                        "bw_avg_KiBps": bw_val,
                        "iops_avg": float(iops_match.group(1))
                    }

        # 组织数据为 { rw_mode: { metric: { bs: value } } }
        data_structured = {}
        for test_case, ops in entry_map.items():
            parts = test_case.split('-')
            if len(parts) != 2:
                continue
            rw_mode = parts[0]
            bs = int(parts[1].replace('k', ''))

            for op_type, metrics in ops.items():
                key = f"{rw_mode}" if op_type == 'read' else f"{rw_mode}-write"
                if key not in data_structured:
                    data_structured[key] = {"IOPS": {}, "bw(KiB/s)": {}, "lat(usec)": {}, "lat(msec)": {}}

                data_structured[key]["IOPS"][bs] = metrics["iops_avg"]
                data_structured[key]["bw(KiB/s)"][bs] = metrics["bw_avg_KiBps"]
                lat_field = f"lat({metrics['lat_unit']})"
                data_structured[key].setdefault(lat_field, {})
                data_structured[key][lat_field][bs] = metrics["lat_avg"]

        return data_structured


    def save_to_excel(self, data_structured, output_dir):
        """将解析结果保存到Excel文件，格式化输出为你需要的样式"""
        output_path = Path(output_dir)
        wb = Workbook()
        ws = wb.active
        ws.title = "fio_summary"

        for rw_mode, metrics in data_structured.items():
            for metric, bs_values in metrics.items():
                # 写标题行，比如: read  IOPS
                ws.append([rw_mode, metric])
                for bs in sorted(bs_values.keys()):
                    ws.append(["", bs, bs_values[bs]])
                ws.append([])  # 空行分隔区块

        excel_file = output_path / "fio.xlsx"
        wb.save(excel_file)
        return excel_file

    def run_test(self):
        wb = Workbook()
        ws = wb.active
        ws.title = "fio"
        baseline = 1
        if self.download_iso_file is not None:
            self.download_iso_file.wait()
        filename = "/root/osmts_tmp/fio/openEuler-24.03-LLVM-riscv64-dvd.iso"
        numjobs = 10
        iodepth = 10
        pbar = tqdm(total=48,desc="fio运行进度")
        for rw in ("read","write","rw","randread","randwrite","randrw"):
            for bs in (4,16,32,64,128,256,512,1024):
                try:
                    if rw == "randrw" or rw == "rw":
                        fio = subprocess.run(
                            f"fio -filename={filename} -direct=1 -iodepth {iodepth} -thread -rw={rw} -rwmixread=70 -ioengine=libaio -bs={bs}k -size=1G -numjobs={numjobs} -runtime=30 -group_reporting -name={rw}-{bs}k",
                            shell=True,check=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE
                        )
                    else:
                        fio = subprocess.run(
                            f"fio -filename={filename} -direct=1 -iodepth {iodepth} -thread -rw={rw} -ioengine=libaio -bs={bs}k -size=1G -numjobs={numjobs} -runtime=30 -group_reporting -name={rw}-{bs}k",
                            shell=True,check=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE
                        )
                except subprocess.CalledProcessError as e:
                    raise DefaultError(f"fio测试出错:fio进程运行报错,此时rw为{rw}.报错信息:{e.stderr.decode('utf-8')}")

                # 保存fio命令的输出结果
                result = fio.stdout.decode('utf-8')
                self.test_result += result + '\n'
                pbar.update(1)
        pbar.close()
        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir(parents=True)
        with open(self.directory / 'fio.log', 'w') as file:
            file.write(self.test_result)
        parsed_data = self.parse_fio_log(self.directory / 'fio.log')
        self.save_to_excel(parsed_data, output_dir=self.directory)


    def run(self):
        print("开始进行fio测试")
        self.run_test()
        print("fio测试结束")