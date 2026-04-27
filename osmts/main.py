#! /usr/bin/env python3
# encoding: utf-8


import sys,psutil,time
import tomllib,ipaddress
import subprocess,argparse,humanfriendly
from pathlib import Path
import shutil
import os

from rich.console import Console
from rich.traceback import install
from rich.table import Table
from setproctitle import setproctitle
from testclasses import osmts_tests
from testclasses.errors import *
from performance_compare import compare_perf


setproctitle('osmts')
console = Console(color_system='256',file=sys.stdout)
install(show_locals=True)
table = Table(show_header=True, header_style="bold magenta")
table.add_column('测试名',justify="left")
table.add_column('成功/失败',justify="left")
table.add_column('失败原因',justify="left")
table.add_column('注解',justify="left")


osmts_tmp_dir = Path('/root/osmts_tmp/')
fio_flag = False
netserver_created_by_osmts = False



def fio_judge():
    root_part_free_size = psutil.disk_usage('/').free
    # 避免下载大文件导致系统崩溃
    if root_part_free_size < 10 * 1024 * 1024 * 1024:
        print(f"当前机器的/分区剩余容量过低[{humanfriendly.format_size(root_part_free_size)}],无法进行fio测试.\n请参考 https://github.com/openeuler-riscv/oerv-team/blob/main/cases/2024.10.19-OERV-UEFI%E5%90%AF%E5%8A%A8%E7%A3%81%E7%9B%98%E5%88%B6%E4%BD%9C-%E8%B5%B5%E9%A3%9E%E6%89%AC.md#%E9%99%84%E5%BD%95%E4%BA%8C 扩展根分区容量之后重试.")
        sys.exit(1)
    global fio_flag
    fio_flag = True



def netperf_judge(netperf_server_ip:str):
    # netperf需要server支持
    if netperf_server_ip is None:
        print(f"用户选择测试netperf,但输入的服务端ip有误,请检查netperf_server_ip字段.")
        sys.exit(1)
    try:
        ipaddress.IPv4Address(netperf_server_ip)
    except ipaddress.AddressValueError:
        print(f"输入的netperf服务端ip不符合ipv4规范,请检查netperf_server_ip字段.")
        sys.exit(1)
    if netperf_server_ip in ('127.0.0.1', 'localhost'):
        if 'netserver' not in [process.name() for process in tuple(psutil.process_iter())]:
            choice = input("未检测到netserver,是否在本机启动netserver?(Y/n) [netperf测试结束后会自动关闭netserver] ")
            if choice == 'Y' or choice == 'y':
                install_netperf = subprocess.run(
                    "dnf install netperf -y",
                    shell=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE
                )
                if install_netperf.returncode != 0:
                    print(f"netperf测试出错:rpm包安装失败.报错信息:{install_netperf.stderr.decode('utf-8')}")
                    sys.exit(1)
                subprocess.run(
                    "netserver -p 10000",
                    shell=True,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                global netserver_created_by_osmts
                netserver_created_by_osmts = True
            else:
                print("请手动执行netserver -p 10000以进行netperf的测试")
                sys.exit(1)



def from_tests_to_tasks(run_tests:list,parameter_list:dict) -> dict:
    support_tests = list(osmts_tests.keys())
    support_tests_count = len(support_tests)
    tasks = set()
    if "performance-test" in run_tests:
        tasks |= {"fio", "stream", "iozone", "unixbench", "libmicro", "nmap", "lmbench", "netperf"}
        run_tests.remove("performance-test")
    if "ALL" in run_tests:
        tasks |= set(support_tests)
        run_tests.remove("ALL")
    for run_test in run_tests:
        if type(run_test) == int:
            if run_test > support_tests_count:
                print(f"输入的编号{run_test}超出了osmts的支持范围,编号必须小于{support_tests_count}")
                sys.exit(1)
            elif run_test < 1:
                print(f"输入的编号{run_test}不合法")
                sys.exit(1)
            tasks.add(support_tests[run_test - 1])
            continue
        if run_test not in support_tests:
            print(f"osmts当前支持的测试项目:{support_tests}")
            print(f"run_tests中出现了不在osmts支持列表里的测试项目:{run_test},请检查配置文件.")
            sys.exit(1)
        tasks.add(run_test)

    # 需要额外资源的测试进行检查
    if 'netperf' in tasks:
        netperf_judge(parameter_list.get('netperf_server_ip'))
    if 'fio' in tasks:
        fio_judge()

    # 调整测试脚本的执行顺序,确保ltp_stress在最后,fio在靠后位置,ltp_cve和ltp_posix在ltp后面
    tasks = list(tasks)
    if 'ltp' in tasks:
        if 'ltp_cve' in tasks:
            tasks.remove("ltp_cve")
            tasks.append("ltp_cve")
        if 'ltp_posix' in tasks:
            tasks.remove("ltp_posix")
            tasks.append("ltp_posix")
    if 'fio' in tasks:
        tasks.remove("fio")
        tasks.append("fio")
    if 'ltp_stress' in tasks:
        tasks.remove("ltp_stress")
        tasks.append("ltp_stress")


    testclasses = {}
    all_need_rpms = set()
    for task in tasks:
        testclass = osmts_tests[task](**parameter_list)
        all_need_rpms |= testclass.rpms
        testclasses[task] = testclass
    # 统一安装所有所需的rpm包
    dnf_command = f"dnf install -y --nobest --skip-broken gcc clang make git cmake htop iotop python3-ipython {' '.join(all_need_rpms)}"
    install_rpms = subprocess.run(
        args=dnf_command,
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE
    )
    if install_rpms.returncode != 0:
        error_log = install_rpms.stderr.decode('utf-8')
        print(f"安装所有所需的rpm包失败.报错信息:{error_log}")
        if 'Bad value of LRO_MAXPARALLELDOWNLOADS' in error_log:
            print("请检查/etc/dnf/dnf.conf配置文件里的max_parallel_doanloads参数,值最大为20!")
            sys.exit(1)
        print(f'用户可以手动执行以下命令安装所需的rpm包:  {dnf_command}  ')
        cont = input('是否继续往下运行osmts?(Y/n)')
        if cont in ('N','n'):
            sys.exit(1)

    return testclasses



def parse_config(config:dict) -> dict:
    saved_directory = config.get("saved_directory", None)
    compiler = config.get("compiler", None)
    netperf_server_ip = str(config.get("netperf_server_ip", None))
    netperf_server_password = config.get("netperf_server_password", None)
    believe_tmp: bool = bool(config.get("believe_tmp", None))
    gcc_version: str = str(config.get("gcc_version", "auto"))
    wrk_seconds: int = int(config.get("wrk_seconds", 60))
    wrk_url: str = str(config.get("wrk_url", "http://www.baidu.com"))
    sha256sumISO:str = str(config.get("sha256sumISO", ""))
    sysbench_table_size: int = int(config.get("sysbench_table_size", 100000))
    sysbench_tables: int = int(config.get("sysbench_tables", 16))
    sysbench_time: int = int(config.get("sysbench_time", 60))
    sysbench_threads: int = int(config.get("sysbench_threads", min(os.cpu_count() or 1, 4)))
    sysbench_prepare_threads: int = int(config.get("sysbench_prepare_threads", sysbench_threads))
    benchmarksql_java_heap_mb: int = int(config.get("benchmarksql_java_heap_mb", 512))
    benchmarksql_mysql_warehouses: int = int(config.get("benchmarksql_mysql_warehouses", 5))
    benchmarksql_mysql_load_workers: int = int(config.get("benchmarksql_mysql_load_workers", 4))
    benchmarksql_mysql_terminals: int = int(config.get("benchmarksql_mysql_terminals", 2))
    benchmarksql_mysql_run_txns_per_terminal: int = int(config.get("benchmarksql_mysql_run_txns_per_terminal", 0))
    benchmarksql_mysql_run_mins: int = int(config.get("benchmarksql_mysql_run_mins", 10))
    benchmarksql_mysql_limit_txns_per_min: int = int(config.get("benchmarksql_mysql_limit_txns_per_min", 1000000000))
    benchmarksql_postgres_warehouses: int = int(config.get("benchmarksql_postgres_warehouses", 1))
    benchmarksql_postgres_load_workers: int = int(config.get("benchmarksql_postgres_load_workers", 4))
    benchmarksql_postgres_terminals: int = int(config.get("benchmarksql_postgres_terminals", 1))
    benchmarksql_postgres_run_txns_per_terminal: int = int(config.get("benchmarksql_postgres_run_txns_per_terminal", 10))
    benchmarksql_postgres_run_mins: int = int(config.get("benchmarksql_postgres_run_mins", 0))
    benchmarksql_postgres_limit_txns_per_min: int = int(config.get("benchmarksql_postgres_limit_txns_per_min", 300))
    compare: bool = bool(config.get("compare", None))
    os_version: str = str(config.get("os_version", ""))
    compare_version: str = str(config.get("compare_version", ""))
    compare_path: str = str(config.get("compare_path", ""))
    compare_device = config.get("compare_device", None)


    # csmith测试输入参数
    csmith_count: int = config.get("csmith_count", 1000)
    if csmith_count < 100:
        console.print('csmith_count数量过小,不建议小于100,现在设置csmith_count=1000')
        csmith_count = 1000
    elif csmith_count > 5000:
        console.print('csmith_count数量过大,不建议大于5000,现在设置csmith_count=1000')
        csmith_count = 1000

    # yarpgen测试输入参数
    yarpgen_count: int = config.get("yarpgen_count", 100)
    if yarpgen_count < 10:
        console.print("yarpgen_count数量过小,不建议小于10,现在设置yarpgen_count=100")
        yarpgen_count = 100
    elif yarpgen_count > 1000:
        console.print("yarpgen_count数量过大,不建议大于1000,现在设置yarpgen_count=100")
        yarpgen_count = 100

    if saved_directory is None:
        saved_directory = '/root/osmts_result/'
    elif saved_directory in ('/', '/etc', '/dev', '/proc', '/boot'):
        console.print(f"{saved_directory}为系统关键目录,不建议把结果存放在该路径")
        choice = input("是否使用osmts推荐的路径?(Y/n)")
        if choice == 'N' or choice == 'n':
            print('本次测试退出.')
            sys.exit(1)
        console.print("已设定存放结果的目录为/root/osmts_result")
        saved_directory = '/root/osmts_result/'
    saved_directory = Path(saved_directory)
    saved_directory.mkdir(parents=True, exist_ok=True)

    if compiler not in ("gcc", "clang"):
        console.print("编译器必为gcc或者clang之一,否则默认为gcc,请检查compiler字段")
        continue_test = input("是否继续测试?(y/N)")
        if continue_test in ("y", "Y"):
            compiler = "gcc"
        else:
            sys.exit(1)
    return {
        'saved_directory':saved_directory,
        'compiler' : compiler,
        'netperf_server_ip' : netperf_server_ip,
        "netperf_server_password":netperf_server_password,
        'netserver_created_by_osmts' : netserver_created_by_osmts,
        'csmith_count' : csmith_count,
        'believe_tmp' : believe_tmp,
        'yarpgen_count' : yarpgen_count,
        'gcc_version' : gcc_version,
        'wrk_seconds' : wrk_seconds,
        'wrk_url' : wrk_url,
        'sha256sumISO':sha256sumISO,
        'sysbench_table_size': sysbench_table_size,
        'sysbench_tables': sysbench_tables,
        'sysbench_time': sysbench_time,
        'sysbench_threads': sysbench_threads,
        'sysbench_prepare_threads': sysbench_prepare_threads,
        'benchmarksql_java_heap_mb': benchmarksql_java_heap_mb,
        'benchmarksql_mysql_warehouses': benchmarksql_mysql_warehouses,
        'benchmarksql_mysql_load_workers': benchmarksql_mysql_load_workers,
        'benchmarksql_mysql_terminals': benchmarksql_mysql_terminals,
        'benchmarksql_mysql_run_txns_per_terminal': benchmarksql_mysql_run_txns_per_terminal,
        'benchmarksql_mysql_run_mins': benchmarksql_mysql_run_mins,
        'benchmarksql_mysql_limit_txns_per_min': benchmarksql_mysql_limit_txns_per_min,
        'benchmarksql_postgres_warehouses': benchmarksql_postgres_warehouses,
        'benchmarksql_postgres_load_workers': benchmarksql_postgres_load_workers,
        'benchmarksql_postgres_terminals': benchmarksql_postgres_terminals,
        'benchmarksql_postgres_run_txns_per_terminal': benchmarksql_postgres_run_txns_per_terminal,
        'benchmarksql_postgres_run_mins': benchmarksql_postgres_run_mins,
        'benchmarksql_postgres_limit_txns_per_min': benchmarksql_postgres_limit_txns_per_min,
        'compare':compare,
        'os_version':os_version,
        'compare_version':compare_version,
        'compare_path':compare_path,
        'compare_device':compare_device
    }



def run_all_tests():
    console.print(f"本次osmts脚本执行将进行的测试:{list(testClasses.keys())}(代表执行顺序)")
    console.print(f"运行时请勿删除{osmts_tmp_dir}和{parameter_list.get('saved_directory')}")

    for testName,testClass in testClasses.items():
        try:
            testClass.run()
        except KeyboardInterrupt:
            table.add_row(testName,'skipped','用户按下Ctrl+C退出测试','/')
            console.print("检测到了用户执行 Ctrl + C 键盘中断,正在退出测试...")
            console.print(table)
            console.print(f"osmts运行结束,本次运行总耗时{humanfriendly.format_timespan(time.time() - start_time)}")
            sys.exit(1)
        except GitCloneError as e:
            console.print(f"{testName}出现git clone错误,退出测试")
            table.add_row(testName,'failed',f'git clone运行失败,返回值:{e.error_code}',e.url)
        except CompileError as e:
            console.print(f"{testName}出现编译失败问题,退出测试")
            table.add_row(testName,'failed',f'编译失败,返回值:{e.error_code}',e.stderr)
        except SummaryError as e:
            console.print(f"{testName}在把运行结果总结为Excel时出错,退出测试.详细信息请查看:{e.fileName}")
            table.add_row(testName,'failed','运行成功但是总结为Excel时出错,有必要时请给osmts项目提交issue','详细报错信息请查看文件:' + str(e.fileName))
        except RunError as e:
            console.print(f"{testName}在运行测试命令时出错,退出测试")
            table.add_row(testName,'failed',f'测试运行时出错,返回值:{e.error_code}',e.stderr)
        except DefaultError as e:
            console.print(f"{testName}抛出一个默认异常,{e}")
            table.add_row(testName,'failed','默认异常',str(e))
        except DnfError as e:
            console.print(f"{testName}在使用yum/dnf包管理器时异常")
            table.add_row(testName,'failed','使用yum/dnf包管理器时发生错误,请检查网络或repo环境',e.stderr)
        except DBusNoSuchUnitError as e:
            console.print(f"{testName}测试中用到的service服务不存在.")
            table.add_row(testName, 'failed', 'systemd service missing.', str(e))
        except Exception as e:
            console.print(f"出现了未被预料的错误,{e}")
            console.print("如有需要可以向osmts项目提交issue")
            table.add_row(testName, 'failed', '也许是osmts的设计缺陷导致', str(e))
        else:
            table.add_row(testName,'success','/')



if __name__ == '__main__':
    # 记录osmts运行所需时间
    start_time = time.time()

    # 命令行提示
    parser = argparse.ArgumentParser(
        description="osmts为一键管理openEuler测试脚本的项目,大多数情况下直接运行main.py即可",
        allow_abbrev=True,
        add_help=True,
        epilog="更多帮助信息请参考https://gitee.com/April_Zhao/osmts项目的README"
    )
    parser.add_argument(
        "--config","-c",
        type = str,default = "osmts_config.toml",
        help = "指定osmts运行所需的Toml格式config文件"
    )
    osmts_config_file = parser.parse_args().config

    try:
        config:dict = tomllib.loads(open(osmts_config_file).read())
    except FileNotFoundError:
        console.print(f"您指定的文件{osmts_config_file}不存在,请检查文件或目录名是否正确")
        sys.exit(1)

    if config == {}:
        console.print("您指定的配置文件是空的,请检查输入的toml格式文件")
        sys.exit(1)
    try:
        run_tests: list = config['run_tests']
    except KeyError:
        console.print(f"您指定的配置文件{osmts_config_file}中的字段run_tests为空,请检查输入的toml文件")
        sys.exit(1)
    parameter_list = parse_config(config)

    if not osmts_tmp_dir.exists():
        osmts_tmp_dir.mkdir()

    # 保存所有测试类
    testClasses = from_tests_to_tasks(run_tests,parameter_list)

    # 正式开始测试
    run_all_tests()

    console.print(table)
    console.print(f"osmts运行结束,本次运行总耗时{humanfriendly.format_timespan(time.time() - start_time)}")

    if parameter_list.get("compare"):
        source_dir = parameter_list.get("saved_directory")
        target_dir = f"./{parameter_list.get('os_version')}/{parameter_list.get('compare_device')[0]}"
        os.makedirs(os.path.dirname(target_dir), exist_ok=True)
        shutil.copytree(source_dir, target_dir, dirs_exist_ok=True)
        source_dir = parameter_list.get("compare_path")
        target_dir = f"./{parameter_list.get('compare_version')}"
        os.makedirs(os.path.dirname(target_dir), exist_ok=True)
        shutil.copytree(source_dir, target_dir, dirs_exist_ok=True)
        versions = [parameter_list.get("os_version"),parameter_list.get("compare_version")]
        console.print(f"osmts开始对比当前系统版本（{parameter_list.get('os_version')}）与 {parameter_list.get('compare_version')} 的性能测试结果")
        compare_perf.compare_perf(versions,parameter_list.get("compare_device"))
        target_dir = f"./{parameter_list.get('saved_directory')}/performance"
        os.makedirs(os.path.dirname(target_dir), exist_ok=True)
        shutil.move("comparison_sg2042.xlsx", target_dir)
        console.print(f"osmts开始对比性能测试结果完成，结果保存在：/{parameter_list.get('saved_directory')}/performance")
