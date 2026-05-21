import re

def refactor_master():
    with open("master.py", "r", encoding="utf-8") as f:
        content = f.read()
    content = re.sub(r'\bprint\(', 'master_logger.info(', content)
    import_stmt = "from logger_config import master_logger\n"
    if "from logger_config import" not in content:
        content = content.replace("import sys\n", "import sys\n" + import_stmt)
    with open("master.py", "w", encoding="utf-8") as f:
        f.write(content)

def refactor_worker():
    with open("worker.py", "r", encoding="utf-8") as f:
        content = f.read()
    content = re.sub(r'\bprint\(', 'worker_logger.info(', content)
    import_stmt = "from logger_config import worker_logger\n"
    if "from logger_config import" not in content:
        content = content.replace("import sys\n", "import sys\n" + import_stmt)
    with open("worker.py", "w", encoding="utf-8") as f:
        f.write(content)

def refactor_config():
    with open("config.py", "r", encoding="utf-8") as f:
        content = f.read()
    content = re.sub(r'\bprint\(', 'config_logger.warning(', content)
    import_stmt = "from logger_config import setup_logger\nconfig_logger = setup_logger('CONFIG')\n"
    if "import socket\n" in content and "config_logger" not in content:
        content = content.replace("import socket\n", "import socket\n" + import_stmt)
    with open("config.py", "w", encoding="utf-8") as f:
        f.write(content)

refactor_master()
refactor_worker()
refactor_config()
print("Done refactoring.")
