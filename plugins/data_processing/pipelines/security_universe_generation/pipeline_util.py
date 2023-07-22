import os
import stat
import json
import logging
import subprocess

from base import credentials_conf
from pipelines.prices_ingestion.config import get_config


ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ*@#"


# Logging settings
log_format = "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s"
log_formatter = logging.Formatter(log_format, "%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger("utils")
_logger.setLevel(logging.DEBUG)


# ====================================================================================
# Generic auxiliary functions
# ====================================================================================

def read_txt_file(path, filename):
    with open(os.path.join(path, filename), "r") as outfile:
        data = [line.rstrip("\n") for line in outfile]
    return data


def store_txt_file(data, path, filename):
    with open(os.path.join(path, filename), "w") as outfile:
        for item in data:
            outfile.write("%s\n" % item)
    _logger.info("File %s stored to %s" % (filename, path))


def read_dict_from_json(path, filename):
    with open(os.path.join(path, filename), 'r') as outfile:
        data = json.load(outfile)
    return data


def store_dict_to_json(data, path, filename):
    with open(os.path.join(path, filename), "w") as f:
        json.dump(data, f)
    _logger.info("File %s stored to %s" % (filename, path))


# ====================================================================================
# Functions to operate with Github
# ====================================================================================

def execute_shell_command(cmd, work_dir):
    """
    Executes a shell command in a subprocess, waiting until it has completed.
    :param cmd: Command to execute.
    :param work_dir: Working directory path.
    """
    _logger.info("Executing: %s" % cmd)

    pipe = subprocess.Popen(cmd, shell=True, cwd=work_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, error) = pipe.communicate()

    if len(out):
        _logger.info("Result: %s" % '. '.join(list(filter(lambda x: x, out.split('\n')))))

    if len(error):
        _logger.error('. '.join(list(filter(lambda x: x, error.split('\n')))))

    pipe.wait()


def git_clone(repo_url, repo_dir, git_branch):
    """
    Clones the remote Git repository at supplied URL into the local directory at supplied path.
    The local directory to which the repository is to be clone is assumed to be empty.
    :param repo_url: URL of remote git repository.
    :param repo_dir: Directory which to clone the remote repository into.
    """
    git_username = credentials_conf["git"]["username"]
    git_password = credentials_conf["git"]["password"]

    # Compile https link to remote git repo
    origin = "https://{username}:{password}@github.com/{repo_url}.git".format(username=git_username,
                                                                              password=git_password,
                                                                              repo_url=repo_url)
    cmd = 'git clone -b %s %s' % (git_branch, origin)
    execute_shell_command(cmd, repo_dir)


def git_pull(repo_dir):
    """
    This method downloads and integrates remote changes
    :param repo_dir: Directory which to clone the remote repository into.
    :return:
    """
    cmd = 'git pull'
    execute_shell_command(cmd, repo_dir)


def git_commit(commit_message, repo_dir):
    """
    Commits the Git repository located in supplied repository directory with the supplied commit message.
    :param commit_message: Commit message.
    :param repo_dir: Directory containing Git repository to commit.
    """
    cmd = 'git commit -am "%s"' % commit_message
    execute_shell_command(cmd, repo_dir)


def git_push(repo_dir):
    """
    Pushes any changes in the Git repository located in supplied repository directory to remote git repository.
    :param repo_dir: Directory containing git repository to push.
    """
    cmd = 'git push'
    execute_shell_command(cmd, repo_dir)


def rmtree(top):
    """
    Remove folder recursively. Set read rights so to be able to delete all files
    :param top: Path to the directory to be deleted.
    """
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            filename = os.path.join(root, name)
            os.chmod(filename, stat.S_IWUSR)
            os.remove(filename)
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(top)
    _logger.info("Folder %s deleted" % top)


# ====================================================================================
# Auxiliary functions to process CUSIP
# ====================================================================================

def get_cusip_check_digit(cusip):
    if len(cusip) != 8:
        _logger.info("get_cusip_check_digit(cusip) only accepts 8 digit cusip")
        raise BaseException

    sum_ = 0
    for i, c in enumerate(cusip):
        c_int = ALPHABET.index(c) + 10 if c in ALPHABET else int(c)
        if (i+1) % 2 == 0:
            c_int = c_int * 2
        sum_ = sum_ + int(c_int/10) + c_int % 10
    return (10 - (sum_ % 10)) % 10


def convert_cusip_to_nine_digits(cusip):
    if len(cusip) < 8:
        print("convert_cusip_check_digit(cusip) does not accept cusip length < 8")
        raise BaseException
    if len(cusip) == 8:
        return "{}{}".format(cusip, get_cusip_check_digit(cusip))
    else:
        return cusip[:9]
