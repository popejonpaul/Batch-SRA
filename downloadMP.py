import pandas
import subprocess
from subprocess import CalledProcessError, PIPE
import os, shutil
import string
import pathlib
import multiprocessing
from multiprocessing import Process

def prefetch(record):
    os.chdir('/scrfs/storage/jappleseed/')
    command = ["/scrfs/storage/jappleseed/home/sratoolkit.3.0.6-ubuntu64/bin/prefetch","-O",
    "/scrfs/storage/jappleseed/SRA/"] + [record]
    try:
        print('Executing command "' + ' '.join(command) + '"')
        subprocess.run(command, check=True, stderr=PIPE)
    except CalledProcessError as bad_proc:
        print('Failed to prefetch record ' + record + '.')
        print(bad_proc.stderr)
        
def dump(sra_id):            
    os.chdir('/scrfs/storage/jappleseed/SRA/')
    command = ["/scrfs/storage/jappleseed/home/sratoolkit.3.0.6-ubuntu64/bin/fasterq-dump",
    "--fasta","--outdir","/scrfs/storage/jappleseed/Fasta/","--split-spot"]+ [sra_id]
    try:
        print('Executing command "' + ' '.join(command) + '"')
        subprocess.run(command, check=True, stderr=PIPE)
    except CalledProcessError as bad_proc:
        print('Failed to fasterq-dump record ' + sra_id + '.')
        print(bad_proc.stderr)
        
def delete_sra_record(accession):
    print("Clearing SRA")
    folder = '/scrfs/storage/jappleseed/SRA/'+accession
    try:
        shutil.rmtree(folder)
    except Exception as e:
        print('Failed to clear %s. Reason: %s' % (folder, e))

def read_file(sra_numbers, filename):
    sra_file = pandas.read_csv(filename)
    for row in sra_file['Experiment Accession']:
        sra_numbers.append(row)
    return sra_numbers

if(__name__ == '__main__'):
    #os.chdir('/scrfs/storage/jappleseed/home/')
    sra_numbers1 = []
    filename = 'sra_result.csv'
    sra_numbers = read_file(sra_numbers1,filename)
    targets = []
    previous = []
    with open('downloaded.txt','r') as f:
        previous2=[line.strip() for line in f]
    os.chdir('/scrfs/storage/jappleseed/')
    previous.append(previous2)
    #22,341 downloaded of 573,499 in Pete
    #30,917 downloaded total after uark
    #8,000 in uark~
    #half=286749.5
    #divide to 10 jobs starting at halfway mark, each using 32 processers on their own node
    half=286749
    div=28675
    job=7 
    if job==1:
        sra_numbers=sra_numbers[half:half+div]
    elif job==2:
        sra_numbers=sra_numbers[half+div:half+div*2]
    elif job==3:
        sra_numbers=sra_numbers[half+div*2:half+div*3]
    elif job==4:
        sra_numbers=sra_numbers[half+div*3:half+div*4]
    elif job==5:
        sra_numbers=sra_numbers[half+div*4:half+div*5]
    elif job==6:
        sra_numbers=sra_numbers[half+div*5:half+div*6]
    elif job==7:
        sra_numbers=sra_numbers[half+div*6:half+div*7]
    elif job==8:
        sra_numbers=sra_numbers[half+div*7:half+div*8]
    elif job==9:
        sra_numbers=sra_numbers[half+div*8:half+div*9]
    elif job==10:
        sra_numbers=sra_numbers[half+div*9:half+div*10]
    
    for i in range(32):             #32 processors                   
        targets.append([sra_numbers[j] for j in range(i, len(sra_numbers), 32)])
    #8,061 fasta in uark  in 72 hours
    for i in range(len(targets[0])):
        threads=[]
        for j in range(32):
            threads.append(Process(target=prefetch, args=[targets[j][i]]))#last half
        for j in range(32):
            threads[j].start()
        for j in range(32):
            threads[j].join()
        sra_ids=[]
        # try:
        pre_path = pathlib.Path('/scrfs/storage/jappleseed/SRA/') 
        for item in pre_path.iterdir():
            sra_id = item.as_posix().split('/')[-1] 
            if sra_id not in previous:
                sra_ids.append(sra_id)
        # except:
            # pass
        threads2 = []
        for k in range(len(sra_ids)-1):
            threads2.append(Process(target=dump, args=[sra_ids[k]]))
        for k in range(len(sra_ids)-1):
            threads2[k].start()
        for k in range(len(sra_ids)-1):              
            threads2[k].join()                   
            previous.append(sra_ids[k])      
            delete_sra_record(sra_ids[k])
    with open('downloaded_post.txt', 'a') as f:
        for item in previous:
            f.write(sra_id+'\n')