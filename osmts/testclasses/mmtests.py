import os
from pathlib import Path
import subprocess,shutil
import humanfriendly
import tarfile,requests,time
from openpyxl import Workbook
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from tqdm import tqdm

from .errors import DefaultError,GitCloneError


headers = {
    'User-Agent':'Mozilla/5.0 (X11; Linux x86_64; rv:137.0) Gecko/20100101 Firefox/137.0'
}

MMTESTS_CONFIGS = (
'config-buildtest-hpc-blas',
'config-buildtest-hpc-cmake',
'config-buildtest-hpc-fftw',
'config-buildtest-hpc-gmp',
'config-buildtest-hpc-metis',
'config-buildtest-hpc-mpfr',
'config-buildtest-hpc-revocap',
'config-db-sqlite-insert-small',
'config-example-tuning-sysctl',
'config-functional-ltp-containers',
'config-hpc-graph500-omp-infant',
'config-hpc-scimarkc-small',
'config-io-dbench4-async',
'config-io-fio-randread-sync-heavywrite',
'config-io-fio-randread-sync-randwrite',
'config-io-fio-scaling',
'config-io-fio-ssd',
'config-io-sparsetruncate-small',
'config-io-trunc',
'config-memdb-redis-benchmark',
'config-multi-tbench__netperf-tcp-rr',
'config-network-iperf-s14-r10000-tcp-unbound',
'config-network-netperf-cross-socket',
'config-network-netperf-rr-unbound',
'config-network-netperf-unix-unbound',
'config-network-netpipe',
'config-network-obsolete-netperf-rr-cstate',
'config-network-obsolete-netperf-unbound',
'config-pagereclaim-stutter',
'config-scheduler-adrestia-single-unbound',
'config-scheduler-lat_proc',
'config-scheduler-saladfork',
'config-scheduler-sysbench-cpu',
'config-scheduler-sysbench-mutex',
'config-scheduler-sysbench-thread',
'config-workload-aim9-pagealloc',
'config-workload-ebizzy',
'config-workload-freqmine',
'config-workload-poundsyscall',
'config-workload-spinplace-long',
'config-workload-spinplace-short',
'config-multi-tbench__netperf-tcp-rr',
'config-workload-stream-omp-llcs',
'config-workload-stream-omp-nodes',
'config-workload-stream-single',
'config-workload-stressng-class-io-parallel',
'config-workload-stressng-context',
'config-workload-stressng-get',
'config-workload-stressng-mmap',
'config-workload-thotdata',
'config-workload-thpscale',
'config-workload-thpscale-madvhugepage',
'config-workload-unixbench',
'config-workload-unixbenchd-syscall-context1',
'config-workload-unixbenchd-syscall-getpid',
'config-workload-unixbench-io-fsbuffer',
'config-workload-unixbench-io-fsdisk',
'config-workload-unixbench-io-fstime',
'config-workload-usemem',
'config-workload-will-it-scale-io-processes',
'config-workload-will-it-scale-io-threads',
'config-workload-will-it-scale-pf-processes',
'config-workload-will-it-scale-sys-processes',
# longtime
'config-db-pgbench-timed-ro-scale1',
'config-functional-ltp-cve',
'config-functional-ltp-lite',
'config-functional-ltp-mm',
'config-functional-ltp-netstress',
'config-functional-ltp-realtime',
'config-io-blogbench',
'config-io-fio-randread-direct-multi',
'config-io-fio-sync-maxrandwrite',
'config-io-paralleldd-read-small',
'config-io-pgioperf',
'config-io-xfsio',
'config-ipc-scale-short',
'config-network-netperf-cross-node',
'config-network-netperf-cstate',
'config-network-netperf-rr-cstate',
'config-network-netperf-stream-unbound',
'config-network-netperf-unbound',
'config-network-obsolete-netperf-cross-node',
'config-network-obsolete-netperf-cross-socket',
'config-network-obsolete-netperf-cstate',
'config-workload-coremark',
'config-workload-futexbench',
'config-workload-pft-process',
'config-workload-pft-threads',
'config-workload-sembench-futex',
'config-workload-shellscripts',
'config-workload-stockfish',
'config-workload-usemem-stress-numa-compact',
# long long time
'config-network-sockperf-unbound',
'config-scheduler-adrestia-periodic-unbound',
'config-workload-johnripper',
'config-workload-usemem-swap-ramdisk',
'config-workload-wp-tlbflush',
'config-workload-will-it-scale-pf-threads',
'config-workload-will-it-scale-sys-threads',
'config-monitor',
)



class MMTests:
    def __init__(self, **kwargs):
        self.rpms = {
            'expect','expect-devel','pcre-devel','bzip2-devel','xz-devel','libcurl-devel',
            'libcurl','texinfo','gcc-gfortran','java-1.8.0-openjdk-devel',
            'wget','libXt-devel','readline-devel','glibc-headers','gcc-c++',
            'zlib','zlib-devel','bc','httpd','net-tools','m4','flex','bison',
            'byacc','keyutils-libs-devel','lksctp-tools-devel','xfsprogs-devel',
            'libacl-devel','openssl','openssl-devel','numactl-devel','libaio-devel',
            'glibc-devel','libcap-devel','patch','findutils','libtirpc','libtirpc-devel',
            'kernel-headers','glibc-headers','hwloc-devel','numactl','automake','fio',
            'sysstat','time','psmisc','popt-devel','libstdc++','libstdc++-static',
            'elfutils-libelf-devel','slang-devel','libbabeltrace-devel','zstd-devel',
            'gtk2-devel','systemtap','libtool','rpcgen','vim','autoconf','automake',
            'python3-rpm-macros','binutils-devel','coreutils','kernel-tools','e2fsprogs',
            'gawk','hdparm','hostname','iproute','nmap','perl-File-Slurp','perl-Time-HiRes',
            'tcl','util-linux','xfsprogs','btrfs-progs','numad','tuned','perl-Try-Tiny',
            'perl-JSON','perl-GD','perl-List-BinarySearch','perl-Math-Gradient','R'
        }
        self.believe_tmp: bool = kwargs.get('believe_tmp')
        self.path = Path('/root/osmts_tmp/mmtests')
        self.directory: Path = kwargs.get('saved_directory') / 'mmtests'
        self.logs:Path = self.directory / 'logs'

        self.wb = Workbook()
        self.ws = self.wb.active
        self.ws.title = 'MMTests'
        self.ws.append(['config','测试结果','返回值','失败原因','运行时间','日志文件'])


    # 从命令输出中提取关键失败原因行，返回码为 0 时返回空字符串
    def _extract_failure_reason(self, output: str, returncode: int) -> str:
        if returncode == 0:
            return ''
        if not output or not output.strip():
            return f'返回码非0({returncode})，且日志为空'

        keywords = (
            'FAILED',
            'FAIL',
            'ERROR',
            'error:',
            'not found',
            'No such file or directory',
            'Command exited with non-zero status',
            'unsupported',
            'Permission denied',
            'Cannot',
            'timed out',
            'Traceback',
        )

        lines = [line.strip() for line in output.splitlines() if line.strip()]
        for line in reversed(lines):
            if any(keyword in line for keyword in keywords):
                return line[:300]
        return f'返回码非0({returncode})，请查看日志'



    # 下载/编译/安装R包
    def prepare_R(self):
        R_Dir = Path('/usr/local/R')
        R_Src_Path,R_Dst_Path = R_Dir / 'bin/R',Path('/usr/bin/R')
        Rscript_Src_Path,Rscript_Dst_Path = R_Dir / 'bin/Rscript',Path('/usr/bin/Rscript')

        if self.believe_tmp:
            if R_Dir.exists():
                return
        else:
            if R_Dir.exists():
                shutil.rmtree(R_Dir)
            if R_Dst_Path.exists():
                shutil.rmtree(R_Dst_Path)
            if Rscript_Dst_Path.exists():
                shutil.rmtree(Rscript_Dst_Path)
        R_Dir.mkdir(parents=True)
        response = requests.get(
            url="https://mirror.lzu.edu.cn/CRAN/src/base/R-4/R-4.4.0.tar.gz",
            headers=headers
        )
        response.raise_for_status()
        with tarfile.open(fileobj=BytesIO(response.content), mode="r:gz") as tar:
            tar.extractall('/opt/')
        try:
            subprocess.run(
                f"./configure --enable-R-shlib=yes --with-tcltk --prefix={R_Dir} && "
                f"make -j {os.cpu_count()} && make install",
                cwd="/opt/R-4.4.0",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"mmtests测试出错.R-4.4.0构建失败,报错信息:{e.stderr.decode('utf-8')}")

        # 创建软链接
        shutil.copyfile(R_Src_Path,R_Dst_Path,follow_symlinks=True)
        shutil.copyfile(Rscript_Src_Path,Rscript_Dst_Path)



    # 下载/编译/安装List-BinarySearch
    def prepare_L(self):
        response = requests.get(
            url="https://gitee.com/April_Zhao/osmts/releases/download/v1.0/List-BinarySearch.tar.xz",
            headers=headers
        )
        with tarfile.open(fileobj=BytesIO(response.content), mode="r:xz") as tar:
            tar.extractall('/opt/')
        try:
            subprocess.run(
                f"cd /opt/List-BinarySearch && "
                f"echo y|perl Makefile.PL && make && make test && make install",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"mmtests测试出错.List-BinarySearch构建失败,报错信息:{e.stderr.decode('utf-8')}")



    # 下载/编译/安装File-Slurp
    def prepare_F(self):
        response = requests.get(
            url="https://cpan.metacpan.org/authors/id/C/CA/CAPOEIRAB/File-Slurp-9999.32.tar.gz",
            headers=headers
        )
        response.raise_for_status()
        with tarfile.open(fileobj=BytesIO(response.content), mode="r:gz") as tar:
            tar.extractall('/opt/')
        try:
            build = subprocess.run(
                args="perl Makefile.PL -y && "
                f"make -j {os.cpu_count()} && make test && make install",
                cwd="/opt/File-Slurp-9999.32",
                shell=True,check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise DefaultError(f"mmtests测试出错.File-Slurp构建失败,报错信息:{e.stderr.decode('utf-8')}")


    # 准备mmtests
    def prepare_M(self):
        # 获取mmtests的源码
        if self.path.exists() and self.believe_tmp:
            pass
        else:
            shutil.rmtree(self.path, ignore_errors=True)
            try:
                subprocess.run(
                    "git clone https://github.com/gormanm/mmtests.git",
                    cwd="/root/osmts_tmp/",
                    shell=True,check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
            except subprocess.CalledProcessError as e:
                raise GitCloneError(e.returncode,'https://github.com/gormanm/mmtests.git'.stderr.decode('utf-8'))



    def pre_test(self):
        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.directory.mkdir(parents=True)
        self.logs.mkdir(parents=True)

        with ThreadPoolExecutor(max_workers=4) as pool:
            #pool.submit(self.prepare_R)
            pool.submit(self.prepare_L)
            pool.submit(self.prepare_F)
            pool.submit(self.prepare_M)


    def mmtests_each_test(self,config):
        start = time.time()
        log_path = self.logs / f"{config}.log"
        try:
            run_mmtests = subprocess.run(
                f"./run-mmtests.sh --no-monitor --config configs/{config} {config}",
                cwd="/root/osmts_tmp/mmtests",
                shell=True,timeout=6 * 60 * 60,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        except subprocess.TimeoutExpired:
            with open(log_path, "w", encoding="utf-8") as log:
                log.write('mmtests执行超时')
            return (
                config,
                '失败',
                '/',
                '超时',
                '超时',
                f'logs/{config}.log'
            )

        output = run_mmtests.stdout.decode('utf-8', errors='replace')
        with open(log_path, "w", encoding="utf-8") as log:
            log.write(output)

        result = '成功' if run_mmtests.returncode == 0 else '失败'
        reason = self._extract_failure_reason(output, run_mmtests.returncode)
        return (
            config,
            result,
            run_mmtests.returncode,
            reason,
            humanfriendly.format_timespan(time.time() - start),
            f'logs/{config}.log'
        )


    def run_test(self):
        # MMTests测试太消耗内存,不建议开太多线程同时运行
        with ThreadPoolExecutor(max_workers=max(int(os.cpu_count()/2),2)) as pool:
            results = list(tqdm(pool.map(self.mmtests_each_test,MMTESTS_CONFIGS),total=len(MMTESTS_CONFIGS)))
            for result in results:
                self.ws.append(result)
        self.wb.save(self.directory / 'mmtests.xlsx')


    def run(self):
        print("开始进行MMTests测试")
        self.pre_test()
        self.run_test()
        print("MMTests测试结束")
