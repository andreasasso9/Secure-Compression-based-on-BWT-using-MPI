import sbwt.sbwt as sbwt
import bmtf.bmtf as bmtf
import rle.rle as rle
import pc.pc as pc
import time
import multiprocessing
import random
import subprocess
import math
import os

"""""
def block_bwt(input, key, index, return_dict):
    outputBWT = sbwt.bwt_from_suffix(input, key)
    return_dict[index] = outputBWT
"""""



def block_bwt(args):
    file_path, offset, block_length, key, index = args

    with open(file_path, "rb") as f:
        f.seek(offset)
        data = f.read(block_length)

    stringInput = data.decode() + "\003"
    return index, sbwt.bwt_from_suffix(stringInput, key)


def compressione(file_name: str, secret_key: str, mode: int):
    filePath = "TestFiles/Input/" + file_name
       
    start_time = time.time()
    #BWT
    print("starting sBWT...")
    bwtStartTime = time.time()

    # Codice per eseguire la BWT a blocchi

    #block_lenght = 1024*30
    using_blocks = True
    outputBWT = ""

    #Salvo la chiave per la BWT
    r = str(random.randint(0, 9999999))
    rFile = open("TestFiles/Output/rfile.txt", "w")
    rFile.write(r)
    rFile.close()

    fileSize = os.path.getsize(filePath)
    #Ottego il numero di processori disponibili per dividere in blocchi la BWT e salvo
    nproc = multiprocessing.cpu_count()
    num_blocks = max(nproc, int(nproc * (math.log10(fileSize)))) #Euristica per il numero di blocchi
    print("Using ", nproc, " processors for BWT")
    block_length = math.ceil(fileSize/ num_blocks)# Divide in nproc blocchi
    
    print("Block length for BWT: ", block_length)
    bFile = open("TestFiles/Output/bfile.txt", "w")
    bFile.write(str(block_length))
    bFile.close()

    if using_blocks and fileSize > nproc * 10:
        print("block mode")
        time_start = time.time()
        num_tasks = fileSize // block_length
        chunksize = max(1, num_tasks // (nproc * 2)) #Definisco la dimensione dei chunk per ogni processo 
        # preparo i task
        tasks = []
        j = 0
        for offset in range(0, fileSize, block_length):
            tasks.append((filePath, offset, block_length, r + secret_key, j))
            j += 1
        
        with  multiprocessing.Pool(nproc) as pool:
            results = pool.imap_unordered(block_bwt, tasks, chunksize) # processa i task in parallelo, se finisce un task prende il successivo

            # ricostruzione ordinata
            output = [None] * j
            for index, res in results:
                output[index] = res

        outputBWT = "".join(output)

        eelapsed_time = time.time() - time_start    
        print(str(eelapsed_time) + "  -> elapsed time of sBWT blocks")
    else:
        print("full file mode")
        inputFile = open(filePath, "rb")
        listInput = inputFile.read()
        stringInput = listInput.decode()
        inputFile.close()

        stringInput += "\003" # Add EOF
        outputBWT = sbwt.bwt_from_suffix(stringInput, secret_key)

    bwtElapsedTime = time.time() - bwtStartTime
    print(str(bwtElapsedTime) + "  -> elapsed time of sBWT")
    fileOutputBWT = open("TestFiles/Output/outputBWT.txt", "w+")
    fileOutputBWT.write(outputBWT)
    fileOutputBWT.close()
    #salvo il dizionario della BWT
    fileOutputDictBWT = open("TestFiles/Output/outputDictBWT.txt", "wb")
    dictStr = ""
    for element in set(outputBWT):
        dictStr += element
    fileOutputDictBWT.write(dictStr.encode())
    fileOutputDictBWT.close()   
    #MTF
    print("starting bMTF...")
    dictionary = sorted(dictStr)

    block_size = 1024 # 1/2((math.log2(len(stringInput))/math.log2(len(dictionary)))) The real formula is this one
    
    mtf_start_time = time.time()
    #print(sorted(dictionary))
    #outputMTF = mtf.encode(plain_text=outputBWT, dictionary=sorted(dictionary)) 
    outputMTF = bmtf.secure_encode(outputBWT, dictionary, secret_key, block_size)
    mtf_elapsed_time = time.time() - mtf_start_time
    print(str(mtf_elapsed_time) + "  -> elapsed time of bMTF")
    fileOutputMTF = open("TestFiles/Output/outputMTF.txt", "w+")
    fileOutputMTF.write(str(outputMTF).replace(" ", ""))
    fileOutputMTF.close()   
    #RLE
    print("starting RLE")
    rleModule = rle.Rle()
    rle_start_time = time.time()
    outputRLE = rle.Rle.rle_encode(rleModule, data=list(map(str, outputMTF))) # trasformo la lista di interi in lista di stringhe
    rle_elapsed_time = time.time() - rle_start_time
    print(str(rle_elapsed_time) + "  -> elapsed time of RLE")
    fileOutputRLE = open("TestFiles/Output/outputRLE.txt", "w+")
    fileOutputRLE.write(str(outputRLE))
    fileOutputRLE.close()

    #PC
    print("starting PC")
    pc_start_time = time.time()
    pc.compress(outputRLE, mode)
    pc_elapsed_time = time.time() - pc_start_time
    print(str(pc_elapsed_time) + "  -> elapsed time of PC")
    total_elapsed_time = time.time() - start_time
    print(str(total_elapsed_time) + "  -> elapsed time of compression")

