import sbwt.sbwt as sbwt
import bmtf.bmtf as bmtf
import rle.rle as rle
import pc.pc as pc
import time
import multiprocessing
import random
import subprocess
import math

def block_bwt(input, key, index, return_dict):
    outputBWT = sbwt.bwt_from_suffix(input, key)
    return_dict[index] = outputBWT

def compressione(file_name: str, secret_key: str, mode: int):
    filePath = "TestFiles/Input/" + file_name
    inputFile = open(filePath, "rb")
    
    listInput = inputFile.read()
    stringInput = listInput.decode()

    dictionary = set(stringInput)
    dictionary.add("\003")
    dictionary = sorted(dictionary)
    start_time = time.time()
    #BWT
    print("starting sBWT...")
    bwtStartTime = time.time()

    # Codice per eseguire la BWT a blocchi

    block_lenght = 1024*30
    using_blocks = True
    outputBWT = ""
    r = str(random.randint(0, 9999999))
    rFile = open("TestFiles/Output/rfile.txt", "w")
    rFile.write(r)
    if using_blocks and len(stringInput) > block_lenght:
        print("block mode")
        # Creo il dizionario condiviso
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        # Creo l'indice e i processi 
        j=0
        process_list = []
        for i in range(0, len(stringInput),block_lenght):
            input_block = stringInput[i:i+block_lenght] + "\003" # Add EOF
            p = multiprocessing.Process(target=block_bwt, args=(input_block, r + secret_key, j, return_dict))
            j+=1
            process_list.append(p)
            p.start()

        for p in process_list:
            p.join()

        for i in range(0, j):
            outputBWT += return_dict[i]

    else:
        print("full file mode")
        stringInput += "\003" # Add EOF
        outputBWT = sbwt.bwt_from_suffix(stringInput, secret_key)

    bwtElapsedTime = time.time() - bwtStartTime
    print(str(bwtElapsedTime) + "  -> elapsed time of sBWT")
    fileOutputBWT = open("TestFiles/Output/outputBWT.txt", "w+")
    fileOutputBWT.write(outputBWT)
    #salvo il dizionario della BWT
    fileOutputDictBWT = open("TestFiles/Output/outputDictBWT.txt", "wb")
    dictStr = ""
    for element in set(outputBWT):
        dictStr += element
    fileOutputDictBWT.write(dictStr.encode())

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

