import sbwt.suffix as suffix
import sbwt.customSort as customSort
#from pydivsufsort import divsufsort

# versione non efficiente del calcolo dell'array dei suffissi (non viene più usata)
def suffix_array(string): 
    return(list(sorted(range(len(string)), key=lambda i:string[i:])))

# versione della BWT che utilizza gli array dei suffissi
def bwt_from_suffix(string, key, s_array=None):
    if s_array is None:
        #s_array = divsufsort(string)
        s_array = suffix.buildSuffixArrayDC3(string, key)
    
	# Costruisco la BWT usando l'array dei suffissi
    # string[idx-1] corrisponde a un carattere dell'ultima colonna della matrice delle rotazioni della BWT
    return("".join(string[idx - 1] for idx in s_array))

# inversa della BWT che utilizza gli array dei suffissi
def ibwt_from_suffix(string, key):
    """
    Inversa della BWT ottimizzata.
    """
    # 1. Recupero alfabeto e applico ordinamento segreto
    alphabeth = sorted(set(string))
    remap_dict = customSort.getSecretSort(alphabeth, key)
    
    # Ottengo l'alfabeto nell'ordine segreto determinato dalla chiave
    secret_alphabeth = sorted(remap_dict.keys(), key=lambda k: remap_dict[k])
    
    # 2. Calcolo P (Rank array)
    # P[i] indica quanti caratteri uguali a string[i] sono apparsi prima di i
    P = [0] * len(string)
    count_occ = {char: 0 for char in alphabeth} # Contatore temporaneo locale
    
    for i, char in enumerate(string):
        P[i] = count_occ[char]
        count_occ[char] += 1
        
    # 3. Calcolo C (Cumulative Count) basato sull'alfabeto segreto
    # C[ch] deve contenere la somma delle occorrenze di tutti i caratteri che precedono 'ch'
    # nell'ordine dell'alfabeto segreto.
    
    C = {}
    total = 0
    for char in secret_alphabeth:
        C[char] = total
        total += count_occ[char] # count_occ alla fine del ciclo precedente contiene i totali
        
    # 4. Ricostruzione (LF Mapping)
    n = len(string)
    out = [None] * n
    i = 0 # Si parte dall'indice che corrisponde al carattere '$' (o EOF), che nella BWT classica finisce in posizione 0 della F-column se è il minore.
    
    # Ottimizzazione: lookup veloce fuori dal ciclo
    # Creiamo una lista per C_mapped dove C_mapped[i] = C[string[i]]
    # Questo evita il lookup nel dizionario dentro il ciclo while
    C_mapped_vals = [C[char] for char in string]

    # Ricostruzione a ritroso
    for j in range(n - 1, -1, -1):
        char = string[i]
        out[j] = char
        # Passo LF: i = P[i] + C[char]
        i = P[i] + C_mapped_vals[i]
        
    # Ritorna la stringa escludendo il primo carattere (che è l'EOF/EOS aggiunto in compressione)
    return "".join(out[1:])

# inversa della BWT che non usa i vettori dei suffissi (non viene più usata)
def ibwt(r: str) -> str:
    """Apply inverse Burrows–Wheeler transform."""
    table = [""] * len(r)  # Make empty table
    for i in range(len(r)):
        table = sorted(r[i] + table[i] for i in range(len(r)))  # Add a column of r
    s = [row for row in table if row.endswith("\003")][0]  # Find the correct row (ending in ETX)
    return s.rstrip("\003")  # Get rid of start and end markers

# lista di metodi che servono per cercare in maniera efficiente in una stringa, potrebbero tornare utili ma per ora non li usiamo

def lf_mapping(bwt, letters=None):
    if letters is None:
        letters = set(bwt)
        
    result = {letter:[0] for letter in letters}
    result[bwt[0]] = [1]
    for letter in bwt[1:]:
        for i, j in result.items():
            j.append(j[-1] + (i == letter))
    return(result)


from collections import Counter

def count_occurences(string, letters=None):
    count = 0
    result = {}
    
    if letters is None:
        letters = set(string)
        
    c = Counter(string)
    
    for letter in sorted(letters):
        result[letter] = count
        count += c[letter]
    return result


def update(begin, end, letter, lf_map, counts, string_length):
    beginning = counts[letter] + lf_map[letter][begin - 1] + 1
    ending = counts[letter] + lf_map[letter][end]
    return(beginning,ending)



def generate_all(input_string, s_array=None, eos="$"):
    letters = set(input_string)
    try:
        assert eos not in letters
    
        counts = count_occurences(input_string, letters)

        input_string = "".join([input_string, eos])
        if s_array is None:
            s_array = suffix_array(input_string)
        bwt = bwt_from_suffix(input_string, s_array)
        lf_map = lf_mapping(bwt, letters | set([eos]))

        for i, j in lf_map.items():
            j.extend([j[-1], 0]) # for when pointers go off the edges

        return letters, bwt, lf_map, counts, s_array

    except:
        print("End of string character found in text, deleted EOS from input string")
        input_string = input_string.replace(eos, "")
        letters = set(input_string)
        counts = count_occurences(input_string, letters)

        input_string = "".join([input_string, eos])
        if s_array is None:
            s_array = suffix_array(input_string)
        bwt = bwt_from_suffix(input_string, s_array)
        lf_map = lf_mapping(bwt, letters | set([eos]))

        for i, j in lf_map.items():
            j.extend([j[-1], 0]) # for when pointers go off the edges

        return letters, bwt, lf_map, counts, s_array

    
def find(search_string, input_string, mismatches=0, bwt_data=None, s_array=None):
    
    results = []
     
    if len(search_string) == 0:
        return("Empty Query String")
    if bwt_data is None:
        bwt_data = generate_all(input_string, s_array=s_array)
    
    letters, bwt, lf_map, count, s_array = bwt_data
    
    if len(letters) == 0:
        return("Empty Search String")

    if not set(search_string) <= letters:
        return []

    length = len(bwt)
    

    class Fuzzy(object):
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    fuz = [Fuzzy(search_string=search_string, begin=0, end=len(bwt) - 1,
                        mismatches=mismatches)]

    while len(fuz) > 0:
        p = fuz.pop()
        search_string = p.search_string[:-1]
        last = p.search_string[-1]
        all_letters = [last] if p.mismatches == 0 else letters
        for letter in all_letters:
            begin, end = update(p.begin, p.end, letter, lf_map, count, length)
            if begin <= end:
                if len(search_string) == 0:
                    results.extend(s_array[begin : end + 1])
                else:
                    miss = p.mismatches
                    if letter != last:
                        miss = max(0, p.mismatches - 1)
                    fuz.append(Fuzzy(search_string=search_string, begin=begin,
                                            end=end, mismatches=miss))
    return sorted(set(results))

if __name__ == '__main__':
    bwt_from_suffix("asd")