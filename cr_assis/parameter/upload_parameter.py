import os, datetime
from github import Github

def upload_parameter(local_file: str, git_file: str, folder: str):
    with open(f"{os.environ['HOME']}/.git-credentials", "r") as f:
        data = f.read()
    access_token = data.split(":")[-1].split("@")[0]
    g = Github(login_or_token= access_token)
    repo = g.get_repo("Coinrisings/parameters")
    contents = repo.get_contents(f"excel/{folder}")
    for content_file in contents:
        if git_file in content_file.name:
            repo.delete_file(content_file.path, message = f"ssh removes {content_file.name} at {datetime.datetime.now()}", sha = content_file.sha)
            print(f"ssh removes {content_file.name} at {datetime.datetime.now()}")
    with open(local_file, "rb") as f:
        data = f.read()
        name = f"excel/{folder}/{git_file}"+".xlsx"
        repo.create_file(name, f"uploaded by ssh at {datetime.datetime.now()}", data)
        print(f"{name} uploaded")

local_file = "/Users/chelseyshao/Downloads/parameter_ssh.xlsx"
git_file = "parameter_ssh"
folder = "dt"
upload_parameter(local_file, git_file, folder)