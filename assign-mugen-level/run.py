import os
import tomllib
import pandas as pd
from pathlib import Path
import shutil
import subprocess
import re
import shlex


class TestCaseGrader:

    def __init__(self, excel_path, testcase_path, mugen_dir, output_dir = None):
        
        self.package_level_file_dir = Path(excel_path)
        self.testcases_file_dir = Path(testcase_path)
        self.mugen_dir = Path(mugen_dir)
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            self.output_dir = self.package_level_file_dir.parent / 'mugen_testcase_grades.xlsx'
        # print ('self.output_dir>>>', self.output_dir)

    def get_package_level(self):
        """获取Excel中的软件包等级信息"""
        df = pd.read_excel(self.package_level_file_dir)
        # print ('package level df', df)
        # print ('df.values', df.values)
        packagelevel_dict_list = []
        for col in df.values:
            packagelevel_dict = {'name': col[0], 'level': col[2]}
            packagelevel_dict_list.append(packagelevel_dict)
        # print ('packagelevel_dict_list', packagelevel_dict_list)
        # print ('packagelevel_dict_list', len(self.packagelevel_dict_list))
        return packagelevel_dict_list

    
    def classify_test_cases(self, package_level_list):
        """为测试用例设置等级"""
        
        df = pd.read_excel(self.testcases_file_dir)
        # print ('df.values', df.values[0])
        # print ('df.columns', df.columns.tolist())
        # header = df.columns.tolist()
        ### 新增2列并添加预设值
        df['level'] = 'P'
        df['package'] = 'package'

        ### 将嵌入式测试用例设置为P3等级
        mask = df.iloc[:, 0].astype(str).str.startswith('embedded')
        df.loc[mask, 'level'] = 'P3'
        df.loc[mask, 'package'] = '嵌入式测试用例不支持'

        ### 根据测试套名称和与其对应的软件包名，将相关测试用例设置为相应等级
        for i, row in enumerate(df.values):
            # print(f"第 {i} 行（从0开始）: {row[0]}")
            # print (df.iloc[i, 0])
            if df.iloc[i, df.columns.get_loc('level')] == 'P':
               for pkg_level in package_level_list:
                    if df.iloc[i, 0] == pkg_level['name']:
                        df.iloc[i, df.columns.get_loc('level')] = pkg_level['level']
                        df.iloc[i, df.columns.get_loc('package')] = pkg_level['name']
        
        ### 搜索测试用例内容，将根据安装的软件包，将相关测试用例设置为相应等级
        testcases_dir = os.path.join(self.mugen_dir, 'testcases')
        level_dict = {item['name']: item['level'] for item in package_level_list}
        package_name_set = {item['name'] for item in package_level_list}
        for i, row in enumerate(df.values):
            if df.iloc[i, df.columns.get_loc('level')] == 'P':
               testcase_base = df.iloc[i, 1]
               testcase_name = testcase_base +'.sh'
               for root, dirs, files in os.walk(testcases_dir):
                   for file in files:
                        if file == testcase_name:
                            filepath = os.path.join(root, file)
                            try:
                                with open(filepath, 'r', encoding='utf-8') as f:
                                    content = f.read()
                                    pattern = r'DNF_INSTALL\s+((?:\\\n|[^#;&|\n])+)(?=\s*(?:#|;|\||&|$))'
                                    matches = re.findall(pattern, content, re.DOTALL | re.MULTILINE)
                                    if len(matches) > 0:
                                        pkgs_install = []
                                        for match in matches:
                                            parts = shlex.split(match)
                                            filtered_parts = [p for p in parts if not p.isdigit()]
                                            pkgs_install.extend(filtered_parts)
                                        
                                        filtered_pkgs = [pkg for pkg in pkgs_install if pkg in testcase_base]
                                        if filtered_pkgs:
                                            pkgs_install = filtered_pkgs
                                        
                                        if not any(pkg in package_name_set for pkg in pkgs_install):
                                            df.iloc[i, df.columns.get_loc('level')] = 'P2'
                                            df.iloc[i, df.columns.get_loc('package')] = ' '.join(pkgs_install)
                                        else:
                                            level_list = []
                                            pkg_list = []
                                            for pkg in pkgs_install:
                                                level = level_dict.get(pkg)
                                                if level:
                                                    level_list.append(level)
                                                    pkg_list.append(pkg)
                                                    level_str = ' '.join(level_list)
                                                    pkg_str = ' '.join(pkg_list)
                                                    df.iloc[i, df.columns.get_loc('level')] = level_str
                                                    df.iloc[i, df.columns.get_loc('package')] = pkg_str        
                                    else:
                                        df.iloc[i, df.columns.get_loc('level')] = 'P1'
                                        df.iloc[i, df.columns.get_loc('package')] = '系统功能测试'
                            except:
                                print ('Cannot read ' + filepath)
        
        print('new df>>>', df)
        print(self.output_dir)
        df.to_excel(self.output_dir, index=False)     
        

def get_arguments():
    config_file_path = Path(os.path.join(os.getcwd(), 'config.toml'))
    with config_file_path.open('rb') as f:
        config = tomllib.load(f)
    # print('config', config)
    return config


def main():
    args = get_arguments()
    # print ('args', args)
    grader = TestCaseGrader(args['package_level_file'], args['mugen_testcases_file'], args['mugen_dir'], args['output_file'])
    package_level_list = grader.get_package_level()
    grader.classify_test_cases(package_level_list)



if __name__=="__main__":
    main()