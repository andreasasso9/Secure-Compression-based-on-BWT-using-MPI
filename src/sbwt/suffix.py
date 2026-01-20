import sbwt.customSort as customSort
# Python3 program for building suffix
# array of a given text

# Class to store information of a suffix
class suffix:
	
	def __init__(self):
		
		self.index = 0
		self.rank = [0, 0]

# This is the main function that takes a
# string 'txt' of size n as an argument,
# builds and return the suffix array for
# the given string
def buildSuffixArray(txt, n, key):

	# A structure to store suffixes
	# and their indexes
	suffixes = [suffix() for _ in range(n)]

	# Estraggo l'alfabeto e randomizzo il mapping dei caratteri
	alfabeto = sorted(set(txt))
	remap_dict = customSort.getSecretSort(alfabeto, key)
	#print(dict(sorted(remap_dict.items(), key=lambda item: item[1])))
	# Store suffixes and their indexes in
	# an array of structures. The structure
	# is needed to sort the suffixes alphabetically
	# and maintain their old indexes while sorting
	for i in range(n):
		#print("LUNGHEZZA:", n)
		#print("TXT[i]:", txt[i] ,remap_dict[txt[i]], "TXT[i+1]:", txt[i+1] ,remap_dict[txt[i+1]])
		suffixes[i].index = i 
		suffixes[i].rank[0] = remap_dict[txt[i]]
		suffixes[i].rank[1] = remap_dict[txt[i+1]] if ((i + 1) < n) else -1

	# Sort the suffixes according to the rank
	# and next rank
	suffixes = sorted(
		suffixes, key = lambda x: (
			x.rank[0], x.rank[1]))

	# At this point, all suffixes are sorted
	# according to first 2 characters. Let
	# us sort suffixes according to first 4
	# characters, then first 8 and so on
	ind = [0] * n # This array is needed to get the
				# index in suffixes[] from original
				# index.This mapping is needed to get
				# next suffix.
	k = 4
	while (k < 2 * n):
		
		# Assigning rank and index
		# values to first suffix
		rank = 0
		prev_rank = suffixes[0].rank[0]
		suffixes[0].rank[0] = rank
		ind[suffixes[0].index] = 0

		# Assigning rank to suffixes
		for i in range(1, n):
			
			# If first rank and next ranks are
			# same as that of previous suffix in
			# array, assign the same new rank to
			# this suffix
			if (suffixes[i].rank[0] == prev_rank and
				suffixes[i].rank[1] == suffixes[i - 1].rank[1]):
				prev_rank = suffixes[i].rank[0]
				suffixes[i].rank[0] = rank
				
			# Otherwise increment rank and assign
			else:
				prev_rank = suffixes[i].rank[0]
				rank += 1
				suffixes[i].rank[0] = rank
			ind[suffixes[i].index] = i

		# Assign next rank to every suffix
		for i in range(n):
			nextindex = suffixes[i].index + k // 2
			suffixes[i].rank[1] = suffixes[ind[nextindex]].rank[0] \
				if (nextindex < n) else -1

		# Sort the suffixes according to
		# first k characters
		suffixes = sorted(
			suffixes, key = lambda x: (
				x.rank[0], x.rank[1]))

		k *= 2

	# Store indexes of all sorted
	# suffixes in the suffix array
	suffixArr = [0] * n
	
	for i in range(n):
		suffixArr[i] = suffixes[i].index

	# Return the suffix array
	return suffixArr

# A utility function to print an array
# of given size
def printArr(arr, n):
	
	for i in range(n):
		print(arr[i], end = " ")
		
	print()


def buildSuffixArrayDC3(txt, key):
	# 1. Recupero l'alfabeto (caratteri unici)
	alfabeto = sorted(set(txt))
	
	# 2. Ottengo il mapping segreto (carattere -> float casuale)
	remap_dict_floats = customSort.getSecretSort(alfabeto, key)
	
	# 3. TRASFORMA I FLOAT IN RANGHI INTERI (Fondamentale per il Counting Sort)

	# Ordiniamo i caratteri dell'alfabeto in base al loro valore float segreto
	alfabeto_ordinato_segreto = sorted(alfabeto, key=lambda c: remap_dict_floats[c])
	
	# Creiamo un nuovo dizionario che mappa ogni carattere al suo rango intero
	rank_dict = {char: i + 1 for i, char in enumerate(alfabeto_ordinato_segreto)}
	
	# 4. Trasformo la stringa in interi positivi
	# Ora s conterrà solo interi: 1, 2, 3...
	# s è il txt trasformato in interi secondo il mapping segreto
	s = [rank_dict[c] for c in txt]
	
	# 5. Eseguo l'algoritmo DC3
	# Ora max sarà un intero (il numero di caratteri distinti)
	max = len(alfabeto)
	#print(rank_dict, '\n\n')
	return dc3(s, len(s), max)

def dc3(s, n, max):
	# Caso base
	if n == 0: return []
	if n == 1: return [0]

	# aggiungo 3 zeri afffinchè ci sia almeno una tripletta completa
	s_pad = s + [0, 0, 0]

	# 1. Definizione degli indici per i gruppi S12 (i % 3 != 0)
	# Se n % 3 == 1, aggiungiamo un indice fittizio 'n' per effettuare il confronto con s0 peché c'è bisogno di un valore in più
	# s12 = [i for i in range(n + (1 if n % 3 == 1 else 0)) if i % 3 != 0]

	s12 = []
	for i in range(n + (1 if n % 3 == 1 else 0)):
		if i % 3 != 0:
			s12.append(i)

	# 2. Ordinamento iniziale delle triplette

	#for i in s12:
	#	print(s_pad[i], s_pad[i+1], s_pad[i+2])

	#print('prima stampa\n', s12, '\n\n')
	s12 = customSort.counting_sort(s12, s_pad, 2, max) # Terzo elemento

	s12 = customSort.counting_sort(s12, s_pad, 1, max) # Secondo elemento

	s12 = customSort.counting_sort(s12, s_pad, 0, max) # Primo elemento

	# 3. Assegnazione dei nomi (naming)
	# Se due triplette sono uguali, ricevono lo stesso nome (rango)
	names = [0] * (n + 3)
	name = 0
	last_triplet = (-1, -1, -1)
	for i in s12:
		if (s_pad[i], s_pad[i+1], s_pad[i+2]) != last_triplet:
			name += 1
			last_triplet = (s_pad[i], s_pad[i+1], s_pad[i+2])
		names[i] = name

	# 4. Controllo ricorsione
	if name < len(s12):
		# Se ci sono nomi duplicati, dobbiamo risolvere l'ordine ricorsivamente
		s1_idx = [i for i in range(n + (1 if n % 3 == 1 else 0)) if i % 3 == 1]
		s2_idx = [i for i in range(n + (1 if n % 3 == 1 else 0)) if i % 3 == 2]
		
		# Stringa ridotta: concatenazione dei nomi dei suffissi mod 1 e mod 2
		s_next = [names[i] for i in s1_idx] + [names[i] for i in s2_idx]
		sa_next = dc3(s_next, len(s_next), name)
		
		# Ri-mappiamo i ranghi ottenuti dalla ricorsione nel vettore names
		s12_map = s1_idx + s2_idx
		for i, pos_in_s_next in enumerate(sa_next):
			names[s12_map[pos_in_s_next]] = i + 1
		
		# Costruiamo S12 ordinato (filtrando l'indice dummy n)
		s12_sorted = [s12_map[p] for p in sa_next if s12_map[p] < n]

		name = len(s12)
	else:
		# Se i nomi sono già tutti distinti, S12 è già ordinato
		s12_sorted = [i for i in s12 if i < n]

	# 5. Ordinamento del gruppo S0 (i % 3 == 0)
	# Un suffisso i mod 0 è determinato dalla coppia (carattere attuale, rango del suffisso i+1)
	# Nota: names[i+1] è sempre disponibile perché i+1 è mod 1 (parte di S12)
	s0 = [i for i in range(n) if i % 3 == 0]

	s0 = customSort.counting_sort(s0, names, 1, name) # Ordina in base al rango del successivo
	s0 = customSort.counting_sort(s0, s_pad, 0, max) # Ordina in base al carattere attuale

	# 6. Merge finale tra S12_sorted e S0
	res = []
	i12, i0 = 0, 0
	while i12 < len(s12_sorted) and i0 < len(s0):
		p12, p0 = s12_sorted[i12], s0[i0]
		
		# Confronto basato sulla tecnica Difference Cover modulo 3
		if p12 % 3 == 1:
			# Caso mod 1: basta un confronto (s[i], rank[i+1])
			is_less = (s_pad[p12], names[p12+1]) < (s_pad[p0], names[p0+1])
		else:
			# Caso mod 2: serve un confronto (s[i], s[i+1], rank[i+2])
			is_less = (s_pad[p12], s_pad[p12+1], names[p12+2]) < (s_pad[p0], s_pad[p0+1], names[p0+2])
		
		if is_less:
			res.append(p12)
			i12 += 1
		else:
			res.append(p0)
			i0 += 1
			
	res.extend(s12_sorted[i12:])
	res.extend(s0[i0:])
	return res

# Driver code
if __name__ == "__main__":
	
	txt = "banana"
	txt += "\003"
	n = len(txt)
	
	suffixArr = buildSuffixArray(txt, n, "Chiave Segreta")
	
	print("Following is suffix array for", txt)
	
	printArr(suffixArr, n)

# This code is contributed by debrc

