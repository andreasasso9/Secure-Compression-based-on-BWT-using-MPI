import sbwt.customSort as customSort
import bmtf.mtf as mtf
import hashlib
from typing import List

initialization_vector = "Vettore di inizializzazione"

def secure_encode(input, alphabeth, key, block_size) -> List[int]:
    # Divido in blocchi
    block_list = []
    if len(input) > block_size:
        for i in range(0, len(input), block_size):
            block_list.append(input[i:i+block_size])
    else:
        block_list.append(input)

    hash = hashlib.md5(initialization_vector.encode()).hexdigest()
    # Computo la MTF su ogni blocco permutando l'alfabeto
    output_list = []
    for block in block_list:
        block_alphabeth = customSort.getListSecretSort(alphabeth, key + hash)
        block_output = mtf.encode(block, block_alphabeth)
        output_list.extend(block_output)
        hash = hashlib.md5(str(block_output).encode()).hexdigest()

    return output_list


def secure_decode(input: List[int], alphabeth, key, block_size) -> str:
    
    hash = hashlib.md5(initialization_vector.encode()).hexdigest()
    # Computo la MTF su ogni blocco permutando l'alfabeto
    output_string = []
    for i in range(0, len(input), block_size):
        block_alphabeth = customSort.getListSecretSort(alphabeth, key + hash)
        output_block = mtf.decode(input[i:i+block_size], block_alphabeth)
        output_string.append(output_block)
        hash = hashlib.md5(str(input[i:i+block_size]).encode()).hexdigest() # Bisogna calcolare l'hash sul blocco decodificato (dobbiamo invertire anche questo)

    return "".join(output_string)

if __name__ == "__main__":

    input = "banana" + "\003"
    block_size = 3
    alphabeth = sorted(set(input))
    key = "Chiave"
    enc = secure_encode(input, alphabeth, key, block_size)
    print("CODIFICA:", enc)
    output = secure_decode(enc, alphabeth, key, block_size)
    print("DECODIFICA:", output)