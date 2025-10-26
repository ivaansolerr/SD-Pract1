from colorama import Fore, Style

def ok(msg: str): print(Fore.GREEN + msg + Style.RESET_ALL)
def warn(msg: str): print(Fore.YELLOW + msg + Style.RESET_ALL)
def err(msg: str): print(Fore.RED + msg + Style.RESET_ALL)
def info(msg: str): print(Fore.CYAN + msg + Style.RESET_ALL)