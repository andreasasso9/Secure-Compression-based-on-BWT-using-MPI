import sbwt.sbwt as sbwt
import bmtf.bmtf as bmtf
import rle.rle as rle
import pc.pc as pc
import time
import multiprocessing
import random
import subprocess
import math

"""""
def block_bwt(input, key, index, return_dict):
    outputBWT = sbwt.bwt_from_suffix(input, key)
    return_dict[index] = outputBWT
"""""

def block_bwt(task):
    input_block, key, index = task
    return index, sbwt.bwt_from_suffix(input_block, key)

def compressione(file_name: str, secret_key: str, mode: int):
    filePath = "TestFiles/Input/" + file_name
    inputFile = open(filePath, "rb")
    
    listInput = inputFile.read()
    stringInput = listInput.decode()
    inputFile.close()

    dictionary = set(stringInput)
    dictionary.add("\003")
    dictionary = sorted(dictionary)
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

    #Ottego il numero di processori disponibili per dividere in blocchi la BWT e salvo
    nproc = multiprocessing.cpu_count()
    num_blocks = max(nproc, int(nproc * (math.log10(len(stringInput)) ** 2))) #Euristica per il numero di blocchi
    print("Using ", nproc, " processors for BWT")
    block_lenght = math.ceil(len(stringInput) / num_blocks)# Divide in nproc blocchi
    
    print("Block length for BWT: ", block_lenght)
    bFile = open("TestFiles/Output/bfile.txt", "w")
    bFile.write(str(block_lenght))
    bFile.close()

    if using_blocks and len(stringInput) > block_lenght:
        print("block mode")
        time_start = time.time()
        num_tasks = len(stringInput) // block_lenght
        chunksize = max(1, num_tasks // (nproc * 2)) #Definisco la dimensione dei chunk per ogni processo 
        # preparo i task
        tasks = []
        j = 0
        for i in range(0, len(stringInput), block_lenght):
            input_block = stringInput[i:i+block_lenght] + "\003"
            tasks.append((input_block, r + secret_key, j)) #ogni task è un blocco da processare e il suo indice j
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

