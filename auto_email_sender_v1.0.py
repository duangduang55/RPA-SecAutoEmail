import smtplib
import os
import time
import datetime
import schedule
import chinese_calendar
import re
import csv
import configparser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# ================= 1. 初始化与配置加载 =================
CONFIG = {}

def load_system_config():
    """从 config.ini 加载系统级配置 (支持完美相对路径)"""
    # 获取当前代码文件所在的绝对路径目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "config.ini")
    
    if not os.path.exists(config_path):
        print(f"严重错误: 在 {current_dir} 下找不到配置文件 config.ini，请检查！")
        exit(1) 
        
    parser = configparser.ConfigParser()
    # 读取配置文件
    parser.read(config_path, encoding='utf-8')
    
    try:
        CONFIG['smtp_server'] = parser.get('Email', 'smtp_server')
        CONFIG['smtp_port'] = parser.getint('Email', 'smtp_port') 
        CONFIG['sender_email'] = parser.get('Email', 'sender_email')
        CONFIG['sender_password'] = parser.get('Email', 'sender_password')
        
        # 处理相对路径
        raw_base_dir = parser.get('System', 'base_dir')
        CONFIG['base_dir'] = os.path.abspath(os.path.join(current_dir, raw_base_dir))
        raw_daily_statements_dir = parser.get('System', 'daily_statements_dir')
        CONFIG['daily_statements_dir'] = os.path.abspath(os.path.join(current_dir, raw_daily_statements_dir))
        CONFIG['log_dir'] = os.path.join(CONFIG['base_dir'], "log")
        CONFIG['mapping_csv'] = os.path.join(CONFIG['base_dir'], "email_mapping.csv")
        
        # 解析时间列表
        times_str = parser.get('Schedule', 'trigger_times')
        CONFIG['trigger_times'] = [t.strip() for t in times_str.split(',')]
        
        CONFIG['wait_seconds'] = parser.getint('Retry', 'wait_minutes') * 60 
        CONFIG['max_retries'] = parser.getint('Retry', 'max_retries')
        
        os.makedirs(CONFIG['log_dir'], exist_ok=True)
    except Exception as e:
        print(f"解析 config.ini 失败，请检查格式是否正确！错误信息: {e}")
        exit(1)

# 启动时立即加载一次系统配置
load_system_config()

# ================= 2. 核心功能函数 =================

def write_log(message, is_error=False):
    """自定义日志记录"""
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    log_file = f"{today_str}_error.log" if is_error else f"{today_str}.log"
    log_path = os.path.join(CONFIG['log_dir'], log_file)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_content = f"[{timestamp}] {message}\n"
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(log_content)
    print(log_content.strip()) 

def get_t_minus_1_trading_day(current_date):
    """计算 T-1 交易日"""
    t_minus_1 = current_date - datetime.timedelta(days=1)
    while not chinese_calendar.is_workday(t_minus_1):
        t_minus_1 -= datetime.timedelta(days=1)
    return t_minus_1

def load_mapping_from_csv(csv_path):
    """动态加载资金账号映射关系 (支持热更新)"""
    mapping = {}
    if not os.path.exists(csv_path):
        write_log(f"严重错误: 找不到名单配置文件 {csv_path}，请检查！", is_error=True)
        return mapping
    try:
        with open(csv_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                account_id = row.get('资金账号', '').strip()
                emails_str = row.get('收件人邮箱', '').strip()
                if account_id and emails_str:
                    emails_str = emails_str.replace('；', ';') 
                    email_list = [email.strip() for email in emails_str.split(';') if email.strip()]
                    mapping[account_id] = email_list
    except Exception as e:
        write_log(f"严重错误: 读取 CSV 配置文件失败: {str(e)}", is_error=True)
    return mapping

def send_email_once(to_emails, subject, body, file_paths):
    """发送单次邮件核心逻辑"""
    msg = MIMEMultipart()
    msg['From'] = CONFIG['sender_email']
    msg['To'] = ";".join(to_emails) 
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain', 'utf-8'))
    
    for file_path in file_paths:
        with open(file_path, 'rb') as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
            msg.attach(part)

    try:
        # 根据邮箱要求，如果不需要SSL，请改用 smtplib.SMTP(CONFIG['smtp_server'], CONFIG['smtp_port']),建立 SMTP 连接并开启 STARTTLS 安全加密.
        server = smtplib.SMTP(CONFIG['smtp_server'], CONFIG['smtp_port'])       #不启用SSL
        # server = smtplib.SMTP_SSL(CONFIG['smtp_server'], CONFIG['smtp_port'])   #启用SSL
        # server.starttls()   根据实际情况，选择发送加密方式
        server.login(CONFIG['sender_email'], CONFIG['sender_password'])
        server.sendmail(CONFIG['sender_email'], to_emails, msg.as_string())
        server.quit()
        return True, ""  
    except Exception as e:
        return False, str(e) 

# ================= 3. 主控工作流 =================

def job_controller():
    """主控工作流 (包含批处理与统一重试)"""
    today = datetime.datetime.now()
    if not chinese_calendar.is_workday(today):
        print(f"[{today.strftime('%H:%M:%S')}] {today.strftime('%Y-%m-%d')} 非交易日，跳过。")
        return
        
    t_minus_1_date = get_t_minus_1_trading_day(today)
    t_minus_1_str = t_minus_1_date.strftime("%Y%m%d")
    
    folder_path = os.path.join(CONFIG['daily_statements_dir'], t_minus_1_str)
    write_log(f"--- 开始执行 {today.strftime('%H:%M')} 批次，检查 {t_minus_1_str} 对账单 ---")
    
    account_email_mapping = load_mapping_from_csv(CONFIG['mapping_csv'])
    if not account_email_mapping:
        write_log("提示: 邮箱映射表为空或读取失败，终止本次任务。")
        return
    
    if not os.path.exists(folder_path):
        write_log(f"提示: 文件夹 {folder_path} 不存在，跳过。")
        return
        
    files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.xlsx', '.xls', '.rar', '.zip'))]
    if not files:
        write_log(f"提示: 文件夹 {folder_path} 中暂无附件，跳过。")
        return
        
    sent_record_path = os.path.join(folder_path, "sent_records.txt")
    sent_accounts = set()
    if os.path.exists(sent_record_path):
        with open(sent_record_path, 'r', encoding='utf-8') as f:
            sent_accounts = set(f.read().splitlines())
            
    account_files = {}
    for file_name in files:
        match = re.match(r"^(\d+)", file_name)
        if match:
            account_id = match.group(1)
            if account_id not in account_files:
                account_files[account_id] = []
            account_files[account_id].append(file_name)

    pending_tasks = []
    for account_id, file_names in account_files.items():
        if account_id in sent_accounts:
            continue 
            
        if account_id in account_email_mapping:
            to_emails = account_email_mapping[account_id]
            subject = os.path.splitext(file_names[0])[0]
            body = f"尊敬的客户，您好！\n\n附件是您 {t_minus_1_str} 的账户对账单相关文件，请查收。\n\n祝好！"
            file_paths = [os.path.join(folder_path, fn) for fn in file_names]
            
            pending_tasks.append({
                "account_id": account_id,
                "to_emails": to_emails,
                "subject": subject,
                "body": body,
                "file_paths": file_paths,
                "file_names_str": ", ".join(file_names)
            })
        else:
            write_log(f"警告: 找不到资金账号 {account_id} 的邮箱映射，跳过", is_error=True)

    if not pending_tasks:
        write_log("提示: 所有匹配文件均已发送，无新任务。")
        return

    for attempt in range(CONFIG['max_retries'] + 1):
        if not pending_tasks:
            break 
            
        if attempt > 0:
            wait_mins = CONFIG['wait_seconds'] // 60
            write_log(f"【重试等待】有 {len(pending_tasks)} 个账号失败，等待 {wait_mins} 分钟后进行第 {attempt} 次重试...")
            time.sleep(CONFIG['wait_seconds']) 

        failed_tasks_this_round = [] 
        for task in pending_tasks:
            is_success, error_msg = send_email_once(
                task["to_emails"], task["subject"], task["body"], task["file_paths"]
            )
            
            if is_success:
                write_log(f"成功: [{task['file_names_str']}] -> {task['to_emails']}")
                with open(sent_record_path, 'a', encoding='utf-8') as f:
                    f.write(task["account_id"] + "\n")
            else:
                write_log(f"失败 (第{attempt}次): [{task['file_names_str']}] -> {task['to_emails']} 报错: {error_msg}", is_error=True)
                failed_tasks_this_round.append(task)
        
        pending_tasks = failed_tasks_this_round

    if pending_tasks:
        for task in pending_tasks:
            write_log(f"【最终失败】: 账号 {task['account_id']} 发送失败已达上限。", is_error=True)

# ================= 4. 调度执行 =================

if __name__ == "__main__":
    print("自动化系统已启动！配置已成功加载。")
    print(f"当前设定的每日触发时刻: {', '.join(CONFIG['trigger_times'])}")

    # 动态注册调度时间
    for t in CONFIG['trigger_times']:
        schedule.every().day.at(t).do(job_controller)

    # 【测试专用】取消下面这行的注释，运行代码时会立刻强制执行一次，不用等时间！
    # job_controller() 

    while True:
        schedule.run_pending()
        time.sleep(1)