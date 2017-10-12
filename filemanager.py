from os import walk, makedirs, remove
from os.path import join, isfile, exists, basename,abspath
import glob, shutil, argparse
from multiprocessing import Process, Semaphore, cpu_count


def async_copy(sourcepath, distpath, semaphore):
    with semaphore:
        shutil.copy(sourcepath, distpath)


def copy_to_folder(sourcepath, distpath, semaphore):
    processes = []
    if not exists(distpath):
        makedirs(distpath)
    if isfile(sourcepath):
        process = Process(target=async_copy, args=(sourcepath, distpath, semaphore))
        processes.append(process)
        process.start()
    else:
        distpath = join(distpath, basename(sourcepath))
        for (dirpath, dirnames, filenames) in walk(sourcepath):
            newdir = distpath + dirpath.replace(sourcepath, "")
            if not exists(newdir):
                makedirs(newdir)
            for filename in filenames:
                process = Process(target=async_copy, args=(join(dirpath, filename), newdir, semaphore))
                processes.append(process)
                process.start()
    for process in processes:
        process.join()


def copy(sourcepaths, distpath, processes=1):
    for sourcepath in sourcepaths:
        sp = Semaphore(processes)
        if sourcepath.count("*") == 0:
            copy_to_folder(sourcepath, distpath, sp)
        else:
            paths = glob.glob(sourcepath)
            for path in paths:
                copy_to_folder(path, distpath, sp)
    print("DONE")


def move(sourcepaths, distpath, processes=1):
    for sourcepath in sourcepaths:
        sp = Semaphore(processes)
        if sourcepath.count("*") == 0:
            copy_to_folder(sourcepath, distpath, sp)
            if isfile(sourcepath):
                remove(sourcepath)
            else:
                shutil.rmtree(sourcepath)
        else:
            paths = glob.glob(sourcepath)
            for path in paths:
                copy_to_folder(path, distpath, sp)
                if isfile(path):
                    remove(path)
                else:
                    shutil.rmtree(path)
    print("DONE")


operation = {"copy": copy,
             "move": move}

def main():
    file_parser = argparse.ArgumentParser(description="File manager parser")
    file_parser.add_argument('--operation', type=str, choices=["move", "copy"], default="copy")
    file_parser.add_argument('--from', type=str, nargs="+", default=[abspath(".\copytest")])
    file_parser.add_argument('--to', type=str, default=abspath(".\distcopytest"))
    file_parser.add_argument('--threads', type=int, default=1)
    output = vars(file_parser.parse_args())
    operation[output["operation"]](output["from"], output["to"], output["threads"])



if __name__ == "__main__":
    main()


