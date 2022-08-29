import shutil
from pathlib import Path
def archiver(source,dest, dirname):
    Path(dirname).mkdir(parents=True,exist_ok=True)
    print(source, dest,dirname)
    shutil.move(source,dest)
